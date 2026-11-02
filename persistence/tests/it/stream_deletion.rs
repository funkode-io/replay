//! Deleting a stream takes what Replay keeps about it, in one transaction with the
//! caller's own read-model rows (funkode-io/replay#292, ADR-0033).
//!
//! The consumer's shape: a catalog entry is hard-deleted and later created again under
//! the same name, so the new aggregate's stream has the old one's id. Every test asserts
//! what an operator or a consumer can see — what a Policy delivers, what is parked, what a
//! read model holds — never the statements the deletion ran.

use std::{sync::Arc, time::Duration};

use futures::future::BoxFuture;
use sqlx::{postgres::PgPoolOptions, PgConnection, PgPool};
use tokio::sync::Notify;
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
async fn a_stream_recreated_under_a_deleted_id_is_delivered_from_its_first_event_postgres_test() {
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
/// the bulk retry of everything its Policy had parked. A deletion is a cleanup, so it
/// deletes it with its stream, archiving nothing; a dead letter of another stream is left
/// as it was.
#[tokio::test]
async fn the_dead_letters_of_a_deleted_stream_are_deleted_with_it_postgres_test() {
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

    assert!(
        harness.archived_dead_letters().await.is_empty(),
        "deleted, not archived: nothing of it is left behind"
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
            .delete_streams(&[probe("never-written")], |conn, ids| {
                assert!(
                    ids.is_empty(),
                    "the closure only receives streams that exist"
                );
                no_read_models(conn, ids)
            })
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

fn delete_shelf_view<'c>(
    conn: &'c mut PgConnection,
    deleted: &'c [Urn],
) -> BoxFuture<'c, Result<(), replay::Error>> {
    Box::pin(async move {
        let ids: Vec<String> = deleted.iter().map(Urn::to_string).collect();
        sqlx::query("DELETE FROM shelf_view WHERE stream_id = ANY($1)")
            .bind(&ids)
            .execute(conn)
            .await
            .map_err(replay_persistence::db_error)?;
        Ok(())
    })
}

/// A stream created under an id the deletion was asked for, but which did not exist when
/// the deletion locked its streams, is left whole: the caller's read-model deletion never
/// saw it, so deleting its events would leave its view rows behind.
#[tokio::test]
async fn a_stream_created_during_deletion_keeps_its_events_and_view_postgres_test() {
    let (pool, _container) = start_pool().await;
    let store = store_with_view(&pool).await;
    stock(&store, "existing", "old").await;
    let ready = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    let deleting = tokio::spawn({
        let store = store.clone();
        let ready = Arc::clone(&ready);
        let resume = Arc::clone(&resume);
        async move {
            store
                .delete_streams(
                    &[
                        ShelfUrn::new("existing").unwrap().into(),
                        ShelfUrn::new("new").unwrap().into(),
                    ],
                    |conn, ids| {
                        Box::pin(async move {
                            assert_eq!(ids.len(), 1, "only the stream that exists is handed over");
                            delete_shelf_view(conn, ids).await?;
                            ready.notify_one();
                            resume.notified().await;
                            Ok(())
                        })
                    },
                )
                .await
        }
    });
    tokio::time::timeout(Duration::from_secs(10), ready.notified())
        .await
        .expect("the read-model deletion must start");
    stock(&store, "new", "concurrent").await;
    resume.notify_one();
    let deleted = deleting.await.unwrap().expect("deletion must succeed");
    assert_eq!(deleted.streams, 1);
    assert_eq!(deleted.events, 1);
    assert!(events_of(&pool, "existing").await.is_empty());
    assert!(view_of(&pool, "existing").await.is_empty());
    assert_eq!(events_of(&pool, "new").await.len(), 1);
    assert_eq!(view_of(&pool, "new").await, events_of(&pool, "new").await);
}

/// An inline projection whose view has a foreign key to `streams` takes the stream's row
/// when it writes; a build of it running while a deletion starts must finish first rather
/// than deadlock against the deletion's lock on that row.
#[tokio::test]
async fn a_deletion_waits_for_a_build_with_a_stream_foreign_key_postgres_test() {
    let (pool, _container) = start_pool().await;
    let store = PostgresEventStore::new(pool.clone());
    stock(&store, "building", "first").await;
    sqlx::query(
        "CREATE TABLE shelf_view (event_id uuid PRIMARY KEY, \
         stream_id text NOT NULL REFERENCES streams(id))",
    )
    .execute(&pool)
    .await
    .unwrap();
    let ready = Arc::new(Notify::new());
    let resume = Arc::new(Notify::new());
    let building = tokio::spawn({
        let pool = pool.clone();
        let ready = Arc::clone(&ready);
        let resume = Arc::clone(&resume);
        async move {
            PostgresEventStore::builder(pool)
                .projection_catch_up_interval(Duration::ZERO)
                .register_postgres_event_handler::<ShelfEvent, _>(
                    "shelf_view",
                    1,
                    move |conn, events: &[PersistedEvent<ShelfEvent>]| {
                        let ready = Arc::clone(&ready);
                        let resume = Arc::clone(&resume);
                        Box::pin(async move {
                            ready.notify_one();
                            resume.notified().await;
                            for event in events {
                                sqlx::query(
                                    "INSERT INTO shelf_view (event_id, stream_id) VALUES ($1, $2)",
                                )
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
        }
    });
    tokio::time::timeout(Duration::from_secs(10), ready.notified())
        .await
        .expect("the build must reach the projection");
    let deleting = tokio::spawn(async move {
        store
            .delete_streams(
                &[ShelfUrn::new("building").unwrap().into()],
                delete_shelf_view,
            )
            .await
    });
    await_a_lock_wait(&pool).await;
    resume.notify_one();
    let (built, deleted) = tokio::time::timeout(Duration::from_secs(10), async {
        tokio::join!(building, deleting)
    })
    .await
    .expect("the build and deletion must finish");
    built.unwrap().expect("the build must not deadlock");
    assert_eq!(deleted.unwrap().expect("deletion must succeed").events, 1);
    assert!(events_of(&pool, "building").await.is_empty());
    assert!(view_of(&pool, "building").await.is_empty());
}

/// An inline projection cannot see a delete, so its rows are the caller's to delete, in
/// the deletion's transaction. A stream recreated under the deleted id is then projected
/// like any other: live by an append that applies the projection, and by a catch-up for
/// an append from a process that does not — the applied marks of the deleted stream's
/// transactions stand in nobody's way.
#[tokio::test]
async fn a_stream_recreated_under_a_deleted_id_is_projected_live_and_by_catch_up_postgres_test() {
    let (pool, _container) = start_pool().await;
    let store = store_with_view(&pool).await;

    stock(&store, "front", "old-1").await;
    stock(&store, "front", "old-2").await;
    stock(&store, "back", "kept").await;
    assert_eq!(view_of(&pool, "front").await.len(), 2);

    let deleted = store
        .delete_streams(
            &[ShelfUrn::new("front").unwrap().into()],
            |conn, deleted| {
                Box::pin(async move {
                    let ids: Vec<String> = deleted.iter().map(Urn::to_string).collect();
                    sqlx::query("DELETE FROM shelf_view WHERE stream_id = ANY($1)")
                        .bind(&ids)
                        .execute(&mut *conn)
                        .await
                        .map_err(replay_persistence::db_error)?;
                    Ok(())
                })
            },
        )
        .await
        .expect("deleting the stream must succeed");
    assert_eq!(deleted.events, 2);
    assert!(view_of(&pool, "front").await.is_empty());
    assert_eq!(
        view_of(&pool, "back").await.len(),
        1,
        "another stream is untouched"
    );

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

// ── Waiting for writers in flight ──────────────────────────────────────────────

/// Start an append to `shelf` and hold it open: it has taken the stream's row and
/// written its event, and nobody else can see either until it ends.
async fn hold_an_append_open(
    pool: &PgPool,
    shelf: &str,
) -> sqlx::Transaction<'static, sqlx::Postgres> {
    let mut tx = pool.begin().await.expect("the append must begin");
    sqlx::query(
        "SELECT id FROM write_event(gen_random_uuid(), '{\"Stocked\":{\"sku\":\"held\"}}'::jsonb, \
             '{}'::jsonb, 'Stocked', $1, 'Shelf', NULL, FALSE)",
    )
    .bind(ShelfUrn::new(shelf).unwrap().to_urn().to_string())
    .execute(&mut *tx)
    .await
    .expect("the held event must be written");
    tx
}

/// Wait until some backend is queued on a lock, read off `pg_stat_activity` rather than
/// slept for, so a busy machine delays the test instead of failing it.
async fn await_a_lock_wait(pool: &PgPool) {
    common::wait::wait_for_count(
        pool,
        "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock'",
        1,
        Duration::from_secs(10),
        "the deletion to queue behind the writer holding what it needs",
    )
    .await;
}

/// An append in flight when the deletion starts is not left behind: the deletion waits
/// for it, then deletes its event with the rest.
#[tokio::test]
async fn a_deletion_waits_for_an_append_in_flight_and_takes_its_event_too_postgres_test() {
    let (pool, _container) = start_pool().await;
    let store = PostgresEventStore::new(pool.clone());
    stock(&store, "busy", "first").await;

    let held = hold_an_append_open(&pool, "busy").await;
    let deleting = tokio::spawn({
        let store = store.clone();
        async move {
            store
                .delete_streams(&[ShelfUrn::new("busy").unwrap().into()], no_read_models)
                .await
        }
    });
    await_a_lock_wait(&pool).await;
    held.commit().await.expect("the append must commit");

    let deleted = deleting
        .await
        .expect("the deletion must not panic")
        .expect("the deletion must succeed once the append is done");
    assert_eq!(
        deleted.events, 2,
        "the append it waited for is deleted with the rest"
    );
    assert!(events_of(&pool, "busy").await.is_empty());
}

/// A deletion stuck behind an append for longer than the stream lock wait gives up as
/// `Unavailable` — temporary, to be retried — and deletes nothing.
#[tokio::test]
async fn a_deletion_that_waits_past_the_stream_lock_wait_is_unavailable_postgres_test() {
    let (pool, _container) = start_pool().await;
    let store = PostgresEventStore::builder(pool.clone())
        .stream_lock_wait(Duration::from_millis(200))
        .build()
        .await
        .expect("building the store must succeed");
    stock(&store, "stuck", "first").await;

    let held = hold_an_append_open(&pool, "stuck").await;
    let refused = store
        .delete_streams(&[ShelfUrn::new("stuck").unwrap().into()], no_read_models)
        .await
        .expect_err("a deletion cannot take a stream an append holds");
    held.rollback().await.expect("the append must roll back");

    assert_eq!(refused.kind(), replay::ErrorKind::Unavailable, "{refused}");
    assert_eq!(
        events_of(&pool, "stuck").await.len(),
        1,
        "nothing was deleted"
    );
}

/// The advisory-lock class an inline projection's catch-up and rebuild lock in — `RPJC`,
/// as the store spells it.
const PROJECTION_LOCK_CLASS: i32 = 0x5250_4A43;

/// A catch-up of an inline projection running in another process — one the deleting
/// store never registered — finishes before the deletion reads on, so it cannot write
/// rows for the deleted events after the caller has deleted its own.
#[tokio::test]
async fn a_deletion_waits_for_a_catch_up_registered_in_another_process_postgres_test() {
    let (pool, _container) = start_pool().await;
    let with_view = store_with_view(&pool).await;
    stock(&with_view, "shared", "first").await;

    // The other process's catch-up, holding the projection's lock as one does.
    let mut catching_up = pool.begin().await.expect("the catch-up must begin");
    sqlx::query("SELECT pg_advisory_xact_lock($1, hashtext('shelf_view'))")
        .bind(PROJECTION_LOCK_CLASS)
        .execute(&mut *catching_up)
        .await
        .expect("the catch-up must take its lock");

    let deleting = tokio::spawn({
        let store = PostgresEventStore::new(pool.clone());
        async move {
            store
                .delete_streams(&[ShelfUrn::new("shared").unwrap().into()], no_read_models)
                .await
        }
    });
    await_a_lock_wait(&pool).await;
    catching_up
        .commit()
        .await
        .expect("the catch-up must commit");

    let deleted = deleting
        .await
        .expect("the deletion must not panic")
        .expect("the deletion must succeed once the catch-up is done");
    assert_eq!(deleted.events, 1);
}

/// The advisory-lock class of the inline projection registry as a whole — `RPJR`.
const REGISTRY_LOCK_CLASS: i32 = 0x5250_4A52;

/// An inline projection being registered for the first time has no registry row yet for
/// the deletion to find, but its build replays the log, deleted streams included: the
/// deletion waits for it as a whole, so the build's rows are there for the caller to
/// delete.
#[tokio::test]
async fn a_deletion_waits_for_a_first_registration_still_building_postgres_test() {
    let (pool, _container) = start_pool().await;
    let store = PostgresEventStore::new(pool.clone());
    stock(&store, "new-view", "first").await;

    // A build in another process, holding the registry as `build()` does.
    let mut building = pool.acquire().await.expect("the build must connect");
    sqlx::query("SELECT pg_advisory_lock_shared($1, 0)")
        .bind(REGISTRY_LOCK_CLASS)
        .execute(&mut *building)
        .await
        .expect("the build must take the registry");

    let deleting = tokio::spawn({
        let store = store.clone();
        async move {
            store
                .delete_streams(&[ShelfUrn::new("new-view").unwrap().into()], no_read_models)
                .await
        }
    });
    await_a_lock_wait(&pool).await;
    sqlx::query("SELECT pg_advisory_unlock_all()")
        .execute(&mut *building)
        .await
        .expect("the build must release the registry");

    let deleted = deleting
        .await
        .expect("the deletion must not panic")
        .expect("the deletion must succeed once the build is done");
    assert_eq!(deleted.events, 1);
}

/// A stream whose first append has not committed has no row the deletion can lock, so it
/// is not the deletion's: it is not handed to the closure, and its events survive the
/// deletion whether they commit during it or after it.
///
/// Forced rather than raced for: the first append is held open across the whole deletion.
#[tokio::test]
async fn a_first_append_in_flight_is_not_deleted_postgres_test() {
    let (pool, _container) = start_pool().await;
    let store = PostgresEventStore::new(pool.clone());
    stock(&store, "existing", "old").await;

    let first_append = hold_an_append_open(&pool, "fresh").await;
    let deleted = store
        .delete_streams(
            &[
                ShelfUrn::new("existing").unwrap().into(),
                ShelfUrn::new("fresh").unwrap().into(),
            ],
            |conn, ids| {
                assert_eq!(
                    ids,
                    &[Urn::from(ShelfUrn::new("existing").unwrap())][..],
                    "a stream with no committed row is not handed over"
                );
                no_read_models(conn, ids)
            },
        )
        .await
        .expect("the deletion must not wait for an uncommitted stream");
    first_append
        .commit()
        .await
        .expect("the first append must commit");

    assert_eq!(deleted.streams, 1);
    assert_eq!(deleted.events, 1);
    assert!(events_of(&pool, "existing").await.is_empty());
    assert_eq!(
        events_of(&pool, "fresh").await.len(),
        1,
        "the first append's event survives the deletion"
    );
}
