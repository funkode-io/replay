//! Same-stream events must replay in append order, even when their `created`
//! timestamps disagree with their versions.
//!
//! `created` defaults to `now()` = `transaction_timestamp()` (stamped at BEGIN), while
//! `version` is assigned later, under the `streams … FOR UPDATE` lock inside
//! `append_event`. A transaction that begins earlier but wins the lock later lands a
//! higher `version` carrying an older `created`, so a read that sorts by
//! `(created, version)` places it before its own predecessor and rebuilds the wrong
//! aggregate state. This test pins the append order by reconstructing an ACL whose
//! events would invert under `created` ordering.

use sqlx::PgPool;
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};
use urn::Urn;

use replay_macros::define_aggregate;

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
