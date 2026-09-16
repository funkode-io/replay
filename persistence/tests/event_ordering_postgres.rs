//! Same-stream events must replay in append order, even when their `created`
//! timestamps disagree with their versions.
//!
//! `created` defaults to `now()` = `transaction_timestamp()` (stamped at BEGIN), while
//! `version` is assigned later, under the `streams … FOR UPDATE` lock inside
//! `append_event`. A transaction that begins earlier but wins the lock later lands a
//! higher `version` carrying an older `created`, so a read that sorts by
//! `(created, version)` places it before its own predecessor and rebuilds the wrong
//! aggregate state. These tests pin `global_position` as the one sequencing key every
//! read sorts on: for a plain append, for the rows compaction writes, and against a
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
    /// Rewrites the live stream to itself. The minimal form of an ACL is one `Granted`
    /// per surviving member, which replays the same in any order and so would say
    /// nothing about the order the snapshot rows were written in. Keeping the
    /// grant/revoke pair makes the rewritten stream order-dependent, which is what the
    /// test below reads back.
    async fn compacted_events(
        &self,
        events: impl futures::TryStream<Ok = AclEvent, Error = replay::Error> + Send,
    ) -> replay::Result<replay::Compaction<AclEvent>> {
        events
            .try_collect::<Vec<AclEvent>>()
            .await
            .map(replay::Compaction::Rewrite)
    }
}

/// `version` and `global_position` within a stream generation, in position order.
async fn versions_in_position_order(
    pool: &PgPool,
    stream_key: &str,
    aggregate_version: Option<i32>,
) -> Vec<i64> {
    sqlx::query_scalar::<_, i64>(
        "SELECT version FROM events
          WHERE stream_id = $1
            AND aggregate_version IS NOT DISTINCT FROM $2
          ORDER BY global_position",
    )
    .bind(stream_key)
    .bind(aggregate_version)
    .fetch_all(pool)
    .await
    .expect("reading back the stream must succeed")
}

/// Compaction writes its snapshot rows itself, one INSERT per compacted event, so their
/// `global_position` order is only the order they replay in for as long as that loop
/// keeps emitting them in `version` order. Reads sort on `global_position` alone, so a
/// rewrite that emitted them any other way would hand a non-commutative stream back
/// inverted — the #199 failure arriving from the write side.
#[tokio::test]
async fn compaction_writes_snapshot_rows_in_version_order_postgres_test() {
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
    ] {
        cqrs.execute::<Acl>(&stream_id, meta.clone(), command, &(), None)
            .await
            .expect("append must succeed");
    }

    let acl = cqrs.fetch_aggregate::<Acl>(&stream_id).await.unwrap();
    assert_eq!(acl.members, vec!["bob".to_string()]);

    let outcome = cqrs.compact(&acl, meta).await.expect("compaction succeeds");
    assert_eq!(outcome, CompactionOutcome::Compacted { archive_version: 1 });

    assert_eq!(
        versions_in_position_order(&pool, &stream_key, None).await,
        vec![1, 2, 3],
        "the snapshot rows compaction wrote must carry ascending versions when read in \
         `global_position` order"
    );
    assert_eq!(
        versions_in_position_order(&pool, &stream_key, Some(1)).await,
        vec![1, 2, 3],
        "archiving stamps the originals in place, so the archived generation keeps the \
         positions it was appended at"
    );

    let compacted = cqrs.fetch_aggregate::<Acl>(&stream_id).await.unwrap();
    assert_eq!(
        compacted.members,
        vec!["bob".to_string()],
        "replaying the compacted stream must reproduce the pre-compaction state — the \
         revoke still follows the grant it revokes"
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
