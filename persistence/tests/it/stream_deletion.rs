//! Deleting a stream takes what Replay keeps about it, in one transaction with the
//! caller's own read-model rows (funkode-io/replay#292, ADR-0033).
//!
//! The consumer's shape: a catalog entry is hard-deleted and later created again under
//! the same name, so the new aggregate's stream has the old one's id. Every test asserts
//! what an operator or a consumer can see — what a Policy delivers, what is parked, what a
//! read model holds — never the statements the deletion ran.

use std::time::Duration;

use futures::future::BoxFuture;
use sqlx::{postgres::PgPoolOptions, PgConnection, PgPool};
use urn::Urn;

use replay_macros::define_aggregate;
use replay_persistence::{
    DeletedStreams, Dispatch, EventStore, ObservedEvent, PersistedEvent, PolicySettings,
    PostgresEventStore, StartAt,
};

use crate::common;
use common::policy_harness::{PolicyDaemonHarness, Probe, ProbeCommand, ProbeEvent, ProbeUrn};
use common::postgres_image::{start_postgres_server, PostgresServer, POSTGRES_PORT};

/// The stream id a probe named `name` is written under.
fn probe(name: &str) -> Urn {
    ProbeUrn::new(name).expect("a valid NSS").into()
}

/// A deletion whose caller keeps no read model of its own.
fn no_read_models<'c>(
    _conn: &'c mut PgConnection,
    _deleted: &'c [Urn],
) -> BoxFuture<'c, Result<(), replay::Error>> {
    Box::pin(async { Ok(()) })
}

/// A Policy that echoes every ping to a stream named after the tag, so what it delivered
/// is readable as the commands it dispatched, and an echo never lands in a stream a test
/// deletes.
fn echo_back(event: &ObservedEvent<ProbeEvent>) -> Vec<Dispatch> {
    match &event.data {
        ProbeEvent::Pinged { tag } => vec![Dispatch::to::<Probe>(
            ProbeUrn::new(format!("{tag}-echo")).expect("a valid NSS"),
            ProbeCommand::Echo { tag: tag.clone() },
        )],
        ProbeEvent::Echoed { .. } => vec![],
    }
}

/// A Policy whose every reaction is refused, so each ping parks one command.
fn refuse_everything(event: &ObservedEvent<ProbeEvent>) -> Vec<Dispatch> {
    match &event.data {
        ProbeEvent::Pinged { tag } => vec![Dispatch::to::<Probe>(
            ProbeUrn::new(format!("{tag}-target")).expect("a valid NSS"),
            ProbeCommand::Refuse {
                reason: tag.clone(),
            },
        )],
        ProbeEvent::Echoed { .. } => vec![],
    }
}

/// The incident a hand delete leaves: the Policy's place in the old stream outlives it,
/// and the new stream's first events sit at or below that place, so they are never
/// delivered. Deleting the place with the stream is what lets the new one be read from
/// its first event.
#[tokio::test]
async fn a_stream_recreated_under_a_deleted_id_is_delivered_from_its_first_event_postgres_test()
{
    let harness = PolicyDaemonHarness::start("recreated", |builder, policy| {
        builder.register_policy_fn::<ProbeEvent, _>(
            policy,
            PolicySettings::new().starting_at(StartAt::Beginning),
            echo_back,
        )
    })
    .await;

    harness.ping("brand", "old-1").await;
    harness.ping("brand", "old-2").await;
    let last = harness.ping("brand", "old-3").await;
    harness.await_passed(last.global_position).await;
    assert_eq!(harness.place_in(&last.stream_id).await, Some(3));

    let deleted = harness
        .store()
        .delete_streams(&[probe("brand")], no_read_models)
        .await
        .expect("deleting the stream must succeed");

    assert_eq!(
        deleted,
        DeletedStreams {
            streams: 1,
            events: 3,
            places: 1,
            dead_letters: 0,
        }
    );
    assert_eq!(
        harness.place_in(&last.stream_id).await,
        None,
        "the Policy's place in the deleted stream goes with it"
    );

    let recreated = harness.ping("brand", "new-1").await;
    harness
        .await_dispatch_caused_by(recreated.global_position)
        .await;
    harness.await_passed(recreated.global_position).await;
    assert_eq!(harness.place_in(&recreated.stream_id).await, Some(1));

    harness.shutdown().await;
}

/// A dead letter whose triggering event is gone can never be retried, and used to abort
/// the bulk retry of everything its Policy had parked. The deletion archives it with its
/// stream; a dead letter of another stream is left as it was.
#[tokio::test]
async fn the_dead_letters_of_a_deleted_stream_leave_the_active_set_postgres_test() {
    let harness = PolicyDaemonHarness::start("parked", |builder, policy| {
        builder.register_policy_fn::<ProbeEvent, _>(
            policy,
            PolicySettings::new().starting_at(StartAt::Beginning),
            refuse_everything,
        )
    })
    .await;

    let doomed = harness.ping("doomed", "doomed").await;
    let kept = harness.ping("kept", "kept").await;
    harness.await_dead_letters(2).await;

    let deleted = harness
        .store()
        .delete_streams(&[probe("doomed")], no_read_models)
        .await
        .expect("deleting the stream must succeed");
    assert_eq!(deleted.dead_letters, 1);

    let parked = harness.dead_letters().await;
    assert_eq!(parked.len(), 1, "only the deleted stream's row leaves");
    assert_eq!(parked[0].event_id, kept.event_id);

    let archived = harness.archived_dead_letters().await;
    assert_eq!(archived.len(), 1);
    assert_eq!(
        archived[0].reason, "stream_deleted",
        "archived, not destroyed, and named for why it left"
    );
    assert_ne!(
        Some(archived[0].dead_letter_id),
        parked.first().map(|row| row.id)
    );

    // The bulk retry walks what is left without tripping over an event that is gone.
    let summary = harness.retry_parked().await;
    assert_eq!(summary.reactions_still_failing, 1);
    assert!(
        harness
            .dead_letters()
            .await
            .iter()
            .all(|row| row.event_id != doomed.event_id),
        "nothing parks the deleted event again"
    );

    harness.shutdown().await;
}

/// The caller's read model is deleted in the deletion's own transaction, so a failure of
/// the caller's half undoes Replay's half: the stream, its events, the Policy's place,
/// its dead letters and the read-model rows are all still there.
#[tokio::test]
async fn a_failing_read_model_delete_rolls_the_whole_deletion_back_postgres_test() {
    let harness = PolicyDaemonHarness::start("rolled_back", |builder, policy| {
        builder.register_policy_fn::<ProbeEvent, _>(
            policy,
            PolicySettings::new().starting_at(StartAt::Beginning),
            refuse_everything,
        )
    })
    .await;
    sqlx::query("CREATE TABLE brand_view (stream_id text PRIMARY KEY)")
        .execute(harness.pool())
        .await
        .expect("creating the read model must succeed");

    let event = harness.ping("brand", "brand").await;
    harness.await_dead_letters(1).await;
    harness.await_passed(event.global_position).await;
    sqlx::query("INSERT INTO brand_view (stream_id) VALUES ($1)")
        .bind(&event.stream_id)
        .execute(harness.pool())
        .await
        .expect("the read model row must be written");

    let refused = harness
        .store()
        .delete_streams(&[probe("brand")], |conn, deleted| {
            Box::pin(async move {
                let ids: Vec<String> = deleted.iter().map(Urn::to_string).collect();
                sqlx::query("DELETE FROM brand_view WHERE stream_id = ANY($1)")
                    .bind(&ids)
                    .execute(&mut *conn)
                    .await
                    .map_err(replay_persistence::db_error)?;
                Err(replay::Error::invalid_input("the read model refuses"))
            })
        })
        .await
        .expect_err("the caller's failure is the deletion's");
    assert_eq!(refused.kind(), replay::ErrorKind::InvalidInput);

    let view_rows: i64 = sqlx::query_scalar("SELECT count(*) FROM brand_view")
        .fetch_one(harness.pool())
        .await
        .expect("the read model must be readable");
    let events: i64 = sqlx::query_scalar("SELECT count(*) FROM events WHERE stream_id = $1")
        .bind(&event.stream_id)
        .fetch_one(harness.pool())
        .await
        .expect("the log must be readable");
    let streams: i64 = sqlx::query_scalar("SELECT count(*) FROM streams WHERE id = $1")
        .bind(&event.stream_id)
        .fetch_one(harness.pool())
        .await
        .expect("the streams must be readable");

    assert_eq!(view_rows, 1, "the caller's own delete is rolled back");
    assert_eq!(events, 1);
    assert_eq!(streams, 1);
    assert_eq!(harness.place_in(&event.stream_id).await, Some(1));
    assert_eq!(harness.dead_letters().await.len(), 1);
    assert!(harness.archived_dead_letters().await.is_empty());

    harness.shutdown().await;
}

/// Ids that name no stream are nothing to delete, and an empty set is not an error.
#[tokio::test]
async fn deleting_streams_that_do_not_exist_deletes_nothing_postgres_test() {
    let (pool, _container) = start_pool().await;
    let store = PostgresEventStore::new(pool.clone());

    assert_eq!(
        store
            .delete_streams(&[probe("never-written")], no_read_models)
            .await
            .expect("an unknown id is not an error"),
        DeletedStreams::default()
    );
    assert_eq!(
        store
            .delete_streams(&[], no_read_models)
            .await
            .expect("an empty set is not an error"),
        DeletedStreams::default()
    );
}

// ── Inline projections ─────────────────────────────────────────────────────────

define_aggregate! {
    Shelf {
        namespace: "shelf",
        state: {
            items: i64,
        },
        commands: {
            Stock { sku: String },
        },
        events: {
            Stocked { sku: String },
        }
    }
}

impl replay::EventStream for Shelf {
    type Event = ShelfEvent;

    fn stream_type() -> String {
        "Shelf".to_string()
    }

    fn apply(&mut self, _event: Self::Event) {
        self.items += 1;
    }
}

impl replay::Aggregate for Shelf {
    type Command = ShelfCommand;
    type Error = replay::Error;
    type Services = ();

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        let ShelfCommand::Stock { sku } = command;
        Ok(vec![ShelfEvent::Stocked { sku }])
    }
}

async fn start_pool() -> (PgPool, PostgresServer) {
    let container = start_postgres_server().await;
    let host = container.get_host().await.unwrap().to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("Error getting docker port");

    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&format!(
            "postgres://postgres:postgres@{host}:{port}/postgres"
        ))
        .await
        .expect("Failed to connect to Postgres");

    common::migrations::MIGRATOR
        .run(&pool)
        .await
        .expect("Failed to run migrations");

    (pool, container)
}

/// A store with an inline projection writing one view row per event, keyed by the event
/// so a row applied twice is a duplicate-key failure rather than absorbed. No background
/// catch-up: the test decides when one runs.
async fn store_with_view(pool: &PgPool) -> PostgresEventStore {
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS shelf_view (event_id uuid PRIMARY KEY, stream_id text NOT NULL)",
    )
    .execute(pool)
    .await
    .expect("creating the view must succeed");

    PostgresEventStore::builder(pool.clone())
        .projection_catch_up_interval(Duration::ZERO)
        .register_postgres_event_handler::<ShelfEvent, _>(
            "shelf_view",
            1,
            |conn, events: &[PersistedEvent<ShelfEvent>]| {
                Box::pin(async move {
                    for event in events {
                        sqlx::query("INSERT INTO shelf_view (event_id, stream_id) VALUES ($1, $2)")
                            .bind(event.id)
                            .bind(event.stream_id.to_string())
                            .execute(&mut *conn)
                            .await
                            .map_err(replay_persistence::db_error)?;
                    }
                    Ok(())
                })
            },
        )
        .build()
        .await
        .expect("building the store must succeed")
}

async fn stock(store: &PostgresEventStore, shelf: &str, sku: &str) {
    store
        .store_events::<Shelf>(
            &ShelfUrn::new(shelf).unwrap(),
            "Shelf".to_string(),
            replay::Metadata::default(),
            &[ShelfEvent::Stocked {
                sku: sku.to_string(),
            }],
            None,
        )
        .await
        .expect("append must succeed");
}

/// The view rows of `shelf`, as the event ids they were written for.
async fn view_of(pool: &PgPool, shelf: &str) -> Vec<uuid::Uuid> {
    sqlx::query_scalar("SELECT event_id FROM shelf_view WHERE stream_id = $1 ORDER BY event_id")
        .bind(ShelfUrn::new(shelf).unwrap().to_urn().to_string())
        .fetch_all(pool)
        .await
        .expect("reading the view must succeed")
}

async fn events_of(pool: &PgPool, shelf: &str) -> Vec<uuid::Uuid> {
    sqlx::query_scalar("SELECT id FROM events WHERE stream_id = $1 ORDER BY id")
        .bind(ShelfUrn::new(shelf).unwrap().to_urn().to_string())
        .fetch_all(pool)
        .await
        .expect("reading the log must succeed")
}

/// An inline projection cannot see a delete, so its rows are the caller's to delete, in
/// the deletion's transaction. A stream recreated under the deleted id is then projected
/// like any other: live by an append that applies the projection, and by a catch-up for
/// an append from a process that does not — the applied marks of the deleted stream's
/// transactions stand in nobody's way.
#[tokio::test]
async fn a_stream_recreated_under_a_deleted_id_is_projected_live_and_by_catch_up_postgres_test()
{
    let (pool, _container) = start_pool().await;
    let store = store_with_view(&pool).await;

    stock(&store, "front", "old-1").await;
    stock(&store, "front", "old-2").await;
    stock(&store, "back", "kept").await;
    assert_eq!(view_of(&pool, "front").await.len(), 2);

    let deleted = store
        .delete_streams(&[ShelfUrn::new("front").unwrap().into()], |conn, deleted| {
            Box::pin(async move {
                let ids: Vec<String> = deleted.iter().map(Urn::to_string).collect();
                sqlx::query("DELETE FROM shelf_view WHERE stream_id = ANY($1)")
                    .bind(&ids)
                    .execute(&mut *conn)
                    .await
                    .map_err(replay_persistence::db_error)?;
                Ok(())
            })
        })
        .await
        .expect("deleting the stream must succeed");
    assert_eq!(deleted.events, 2);
    assert!(view_of(&pool, "front").await.is_empty());
    assert_eq!(view_of(&pool, "back").await.len(), 1, "another stream is untouched");

    // Recreated live, through the store that applies the projection.
    stock(&store, "front", "new-1").await;
    // And by a process without it — a rolling deploy's old pod.
    let without_view = PostgresEventStore::new(pool.clone());
    stock(&without_view, "front", "new-2").await;

    assert_eq!(
        store
            .catch_up_projections()
            .await
            .expect("catching up must succeed"),
        1,
        "only the append that did not apply it is owed"
    );
    assert_eq!(
        view_of(&pool, "front").await,
        events_of(&pool, "front").await,
        "the view holds the recreated stream's events, each once"
    );
    assert_eq!(view_of(&pool, "front").await.len(), 2);
}
