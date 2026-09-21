//! `created` is a filter, not a sort key — and the index behind it says so.
//!
//! Why the wide `(created, version, id)` index gave way to one on `created` alone:
//! `docs/adr/0018-every-event-read-is-ordered-by-global-position.md`.

use sqlx::{PgPool, Row};
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};

mod common;
use common::postgres_image::postgres_container;

const POSTGRES_PORT: u16 = 5432;

/// A time-travel read narrow enough that the `created` bound is the selective one: the
/// seeded rows are one second apart, so this keeps the ~20 oldest of 2 000. A bound that
/// matched the whole table would be answered by walking `global_position` instead, and
/// would prove nothing about the index under test.
const EXPLAIN_TIME_TRAVEL_READ: &str = "EXPLAIN (COSTS OFF) \
     SELECT id FROM events WHERE created <= now() - interval '1980 seconds' \
      ORDER BY global_position ASC";

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

async fn index_names(pool: &PgPool) -> Vec<String> {
    sqlx::query_scalar::<_, String>(
        "SELECT indexname FROM pg_indexes WHERE tablename = 'events' ORDER BY indexname",
    )
    .fetch_all(pool)
    .await
    .expect("reading the index list must succeed")
}

#[tokio::test]
async fn created_is_indexed_on_its_own_after_migration_postgres_test() {
    let (pool, _container) = start_postgres().await;

    let indexes = index_names(&pool).await;

    assert!(
        indexes.iter().any(|name| name == "idx_events_created"),
        "a `created` range filter still needs an index; found {indexes:?}"
    );
    assert!(
        !indexes
            .iter()
            .any(|name| name == "idx_events_created_version_id"),
        "nothing sorts on `(created, version, id)` any more, so the wide index must be \
         gone; found {indexes:?}"
    );
    assert!(
        !indexes
            .iter()
            .any(|name| name == "idx_events_created_version"),
        "0013 dropped this one; it must not come back; found {indexes:?}"
    );
}

/// The plan, not the index's existence: an index nothing plans with is dead weight of a
/// different kind.
#[tokio::test]
async fn time_travel_read_scans_the_created_index_postgres_test() {
    let (pool, _container) = start_postgres().await;

    sqlx::query("INSERT INTO streams (id, type, version) VALUES ('urn:acl:seed', 'Acl', 2000)")
        .execute(&pool)
        .await
        .expect("seeding the stream row must succeed");
    sqlx::query(
        "INSERT INTO events (id, data, metadata, stream_id, type, version, stream_seq, created)
         SELECT gen_random_uuid(), '{}'::jsonb, '{}'::jsonb, 'urn:acl:seed', 'Granted', v, v,
                now() - (v * interval '1 second')
           FROM generate_series(1, 2000) v",
    )
    .execute(&pool)
    .await
    .expect("seeding events must succeed");
    common::places::settle(&pool).await;
    sqlx::query("ANALYZE events")
        .execute(&pool)
        .await
        .expect("ANALYZE must succeed");

    // One connection: `enable_seqscan` is a session setting, and it is what makes the
    // choice deterministic on a table small enough to scan either way.
    let mut conn = pool.acquire().await.expect("a connection must be free");
    sqlx::query("SET enable_seqscan = off")
        .execute(&mut *conn)
        .await
        .expect("disabling seq scans must succeed");

    let plan: String = sqlx::query(EXPLAIN_TIME_TRAVEL_READ)
        .fetch_all(&mut *conn)
        .await
        .expect("EXPLAIN must succeed")
        .into_iter()
        .map(|row| row.get::<String, _>(0))
        .collect::<Vec<_>>()
        .join("\n");

    assert!(
        plan.contains("idx_events_created"),
        "the `created` bound must be answered from the index, not a scan:\n{plan}"
    );
}
