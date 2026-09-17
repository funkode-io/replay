//! Same-stream events must replay in append order, even when their `created`
//! timestamps disagree with their versions — why, and why `global_position` is the key
//! instead: `docs/adr/0018-every-event-read-is-ordered-by-global-position.md`.
//!
//! These tests pin that order where it is written as well as where it is read: a plain
//! append whose `created` stamps are inverted, the rows compaction writes, and a
//! `created` filter, which selects rows without deciding their order.

use futures::TryStreamExt;
use sqlx::PgPool;
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};
use urn::Urn;

use replay_macros::define_aggregate;
use replay_persistence::{AggregateVersion, CompactionOutcome, EventStore};

mod common;
use common::postgres_image::postgres_container;

const POSTGRES_PORT: u16 = 5432;

define_aggregate! {
    Acl {
        namespace: "acl",
        state: {
            members: Vec<String>,
        },
        commands: {
            Grant { member: String },
            Revoke { member: String },
        },
        events: {
            Granted { member: String },
            Revoked { member: String },
        }
    }
}

impl replay::EventStream for Acl {
    type Event = AclEvent;

    fn stream_type() -> String {
        "Acl".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            AclEvent::Granted { member } => self.members.push(member),
            AclEvent::Revoked { member } => self.members.retain(|m| m != &member),
        }
    }
}

impl replay::Aggregate for Acl {
    type Command = AclCommand;
    type Error = replay::Error;
    type Services = ();

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        Ok(match command {
            AclCommand::Grant { member } => vec![AclEvent::Granted { member }],
            AclCommand::Revoke { member } => vec![AclEvent::Revoked { member }],
        })
    }
}

async fn start_postgres() -> (
    PgPool,
    testcontainers_modules::testcontainers::ContainerAsync<postgres::Postgres>,
) {
    let container = postgres_container().start().await.unwrap();
    let host = container.get_host().await.unwrap().to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("Error getting docker port");

    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(5)
        .connect(&format!(
            "postgres://postgres:postgres@{host}:{port}/postgres"
        ))
        .await
        .expect("Failed to connect to Postgres");

    sqlx::migrate!("./tests/migrations")
        .run(&pool)
        .await
        .expect("Failed to run migrations");

    (pool, container)
}

/// Insert one event directly, letting `global_position` auto-assign in insert order.
async fn insert_event(
    pool: &PgPool,
    stream_id: &str,
    version: i64,
    event_type: &str,
    data: serde_json::Value,
    created: chrono::DateTime<chrono::Utc>,
) {
    sqlx::query(
        "INSERT INTO events (id, data, metadata, stream_id, type, version, created)
         VALUES ($1, $2, '{}', $3, $4, $5, $6)",
    )
    .bind(uuid::Uuid::new_v4())
    .bind(data)
    .bind(stream_id)
    .bind(event_type)
    .bind(version)
    .bind(created)
    .execute(pool)
    .await
    .expect("seeding event row must succeed");
}

/// A grant (v1) followed by a revoke (v2) whose `created` is *earlier* than the grant's
/// — the shape a lock-order/transaction-start inversion produces. Replayed in append
/// order the member is revoked; replayed in `created` order the revoke runs first as a
/// no-op and the member is wrongly re-granted.
#[tokio::test]
async fn same_stream_replays_in_append_order_not_created_order_postgres_test() {
    let (pool, _container) = start_postgres().await;

    let stream_id = AclUrn::new("acl-order-1").unwrap();
    let stream_key: String = Into::<Urn>::into(stream_id.clone()).to_string();

    sqlx::query("INSERT INTO streams (id, type, version) VALUES ($1, 'Acl', 2)")
        .bind(&stream_key)
        .execute(&pool)
        .await
        .expect("seeding stream row must succeed");

    let later = chrono::DateTime::parse_from_rfc3339("2025-01-01T00:00:01Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let earlier = chrono::DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);

    // v1 grants, stamped LATER; v2 revokes, stamped EARLIER. Inserted in version order,
    // so `global_position` is monotonic with `version` while `created` is inverted.
    insert_event(
        &pool,
        &stream_key,
        1,
        "Granted",
        serde_json::json!({ "Granted": { "member": "alice" } }),
        later,
    )
    .await;
    insert_event(
        &pool,
        &stream_key,
        2,
        "Revoked",
        serde_json::json!({ "Revoked": { "member": "alice" } }),
        earlier,
    )
    .await;

    let cqrs = replay_persistence::Cqrs::new(replay_persistence::PostgresEventStore::new(pool));
    let acl = cqrs
        .fetch_aggregate::<Acl>(&stream_id)
        .await
        .expect("fetch must succeed");

    assert_eq!(
        acl.members,
        Vec::<String>::new(),
        "the revoke is the higher-versioned (later-appended) event, so the member must \
         not remain — replay must follow append order, not `created` order"
    );
}

impl replay::Compactable for Acl {
    /// The minimal ACL: one `Granted` per surviving member, in the order they were
    /// granted. It is the shortest stream that replays to the same state and is a
    /// fixpoint, as `Compactable` requires. `members` is a `Vec`, so that order is part
    /// of the state — which is what makes the order the rows are *written* in
    /// observable.
    async fn compacted_events(
        &self,
        events: impl futures::TryStream<Ok = AclEvent, Error = replay::Error> + Send,
    ) -> replay::Result<replay::Compaction<AclEvent>> {
        let members = events
            .try_fold(Vec::<String>::new(), |mut members, event| async move {
                match event {
                    AclEvent::Granted { member } => members.push(member),
                    AclEvent::Revoked { member } => members.retain(|m| m != &member),
                }
                Ok(members)
            })
            .await?;

        Ok(replay::Compaction::Rewrite(
            members
                .into_iter()
                .map(|member| AclEvent::Granted { member })
                .collect(),
        ))
    }
}

/// The events of one stream generation in `global_position` order — the order a read
/// hands them over in.
async fn events_in_position_order(
    pool: &PgPool,
    stream_key: &str,
    aggregate_version: Option<i32>,
) -> Vec<AclEvent> {
    sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT data FROM events
          WHERE stream_id = $1
            AND aggregate_version IS NOT DISTINCT FROM $2
          ORDER BY global_position",
    )
    .bind(stream_key)
    .bind(aggregate_version)
    .fetch_all(pool)
    .await
    .expect("reading back the stream must succeed")
    .into_iter()
    .map(|data| serde_json::from_value(data).expect("a stored row must deserialize"))
    .collect()
}

/// Compaction writes its snapshot rows itself, one INSERT per compacted event, and a read
/// hands them back in `global_position` order — the order those INSERTs ran in. So the
/// sequence `compacted_events` returns only survives compaction while the write loop
/// emits it in order: any other write order hands a non-commutative stream back
/// scrambled, which is the #199 failure arriving from the write side.
#[tokio::test]
async fn compaction_writes_snapshot_rows_in_the_order_the_rewrite_returned_postgres_test() {
    let (pool, _container) = start_postgres().await;

    let cqrs =
        replay_persistence::Cqrs::new(replay_persistence::PostgresEventStore::new(pool.clone()));
    let stream_id = AclUrn::new("acl-compaction-1").unwrap();
    let stream_key: String = Into::<Urn>::into(stream_id.clone()).to_string();
    let meta = replay::Metadata::default();

    for command in [
        AclCommand::Grant {
            member: "alice".to_string(),
        },
        AclCommand::Grant {
            member: "bob".to_string(),
        },
        AclCommand::Revoke {
            member: "alice".to_string(),
        },
        AclCommand::Grant {
            member: "carol".to_string(),
        },
    ] {
        cqrs.execute::<Acl>(&stream_id, meta.clone(), command, &(), None)
            .await
            .expect("append must succeed");
    }

    let acl = cqrs.fetch_aggregate::<Acl>(&stream_id).await.unwrap();
    let before = vec!["bob".to_string(), "carol".to_string()];
    assert_eq!(acl.members, before);

    let outcome = cqrs.compact(&acl, meta).await.expect("compaction succeeds");
    assert_eq!(outcome, CompactionOutcome::Compacted { archive_version: 1 });

    assert_eq!(
        events_in_position_order(&pool, &stream_key, None).await,
        vec![
            AclEvent::Granted {
                member: "bob".to_string()
            },
            AclEvent::Granted {
                member: "carol".to_string()
            },
        ],
        "the snapshot rows must come back in the order `compacted_events` returned them"
    );
    assert_eq!(
        cqrs.fetch_aggregate::<Acl>(&stream_id)
            .await
            .unwrap()
            .members,
        before,
        "the compacted stream reproduces the pre-compaction state — including the order \
         its members are in, which is the order the rows were written in"
    );

    let archived: Vec<_> = replay_persistence::PostgresEventStore::new(pool.clone())
        .stream_events_by_stream_id::<Acl>(&stream_id, AggregateVersion::Version(1), None, None)
        .try_collect()
        .await
        .expect("reading the archived generation must succeed");
    assert_eq!(
        archived.len(),
        4,
        "archiving stamps the originals in place; they keep the positions they were \
         appended at"
    );
    let original = cqrs
        .fetch_aggregate_at::<Acl>(&stream_id, AggregateVersion::Version(1), None, None)
        .await
        .expect("replaying the archived generation must succeed");
    assert_eq!(
        original.members, before,
        "the archived generation still replays to the pre-compaction state"
    );

    // The fixture's half of the contract: compacting what compaction produced yields the
    // same stream, so the assertions above pin the write order rather than a drift the
    // rewrite introduced.
    let compacted = cqrs.fetch_aggregate::<Acl>(&stream_id).await.unwrap();
    cqrs.compact(&compacted, replay::Metadata::default())
        .await
        .expect("a second compaction succeeds");
    assert_eq!(
        events_in_position_order(&pool, &stream_key, None).await,
        vec![
            AclEvent::Granted {
                member: "bob".to_string()
            },
            AclEvent::Granted {
                member: "carol".to_string()
            },
        ],
        "`compacted_events` must be a fixpoint"
    );
}

/// `created` stays a legitimate *filter* for time-travel reads. It selects which rows a
/// read returns; `global_position` decides the order they arrive in. The two are
/// independent, which the inverted seed makes visible: filtering on `created` keeps the
/// second-appended event and drops the first.
#[tokio::test]
async fn created_filter_selects_rows_without_ordering_them_postgres_test() {
    let (pool, _container) = start_postgres().await;

    let stream_id = AclUrn::new("acl-time-travel-1").unwrap();
    let stream_key: String = Into::<Urn>::into(stream_id.clone()).to_string();

    sqlx::query("INSERT INTO streams (id, type, version) VALUES ($1, 'Acl', 2)")
        .bind(&stream_key)
        .execute(&pool)
        .await
        .expect("seeding stream row must succeed");

    let later = chrono::DateTime::parse_from_rfc3339("2025-01-01T00:00:01Z")
        .unwrap()
        .with_timezone(&chrono::Utc);
    let earlier = chrono::DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);

    insert_event(
        &pool,
        &stream_key,
        1,
        "Granted",
        serde_json::json!({ "Granted": { "member": "alice" } }),
        later,
    )
    .await;
    insert_event(
        &pool,
        &stream_key,
        2,
        "Revoked",
        serde_json::json!({ "Revoked": { "member": "alice" } }),
        earlier,
    )
    .await;

    let store = replay_persistence::PostgresEventStore::new(pool);

    let at_earlier: Vec<_> = store
        .stream_events_by_stream_id::<Acl>(
            &stream_id,
            AggregateVersion::Latest,
            None,
            Some(earlier),
        )
        .try_collect()
        .await
        .expect("time-travel read must succeed");

    assert_eq!(
        at_earlier.len(),
        1,
        "only the event stamped `earlier` is in range"
    );
    assert!(
        matches!(at_earlier[0].data, AclEvent::Revoked { .. }),
        "the row a `created` bound keeps is decided by `created`, not by position"
    );

    let at_later: Vec<_> = store
        .stream_events_by_stream_id::<Acl>(&stream_id, AggregateVersion::Latest, None, Some(later))
        .try_collect()
        .await
        .expect("time-travel read must succeed");

    assert!(
        matches!(at_later[0].data, AclEvent::Granted { .. })
            && matches!(at_later[1].data, AclEvent::Revoked { .. }),
        "with both rows in range they arrive in append order, not `created` order"
    );
}
