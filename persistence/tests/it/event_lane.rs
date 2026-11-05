//! Every event carries a lane, online by default, and the online feed's indexes hold
//! online rows only (funkode-io/replay#301, ADR-0034).
//!
//! What the migration must leave an existing deployment: every stored event online, every
//! existing caller of the writer unchanged, and the two indexes built without blocking
//! appends.

use sqlx::{PgPool, Row};

use crate::common;
use common::migrations::{through, MIGRATOR};
use common::postgres_image::{start_postgres_server, PostgresServer, POSTGRES_PORT};

/// The last migration before the lane existed.
const BEFORE_LANES: i64 = 38;

/// The two partial indexes, and the migration that builds each.
const ONLINE_INDEXES: [(i64, &str); 2] = [
    (40, "idx_events_online_global_position"),
    (41, "idx_events_online_commit_txid_position"),
];

async fn start_pool() -> (PgPool, PostgresServer) {
    let container = start_postgres_server().await;
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
    (pool, container)
}

async fn lane_of(pool: &PgPool, id: uuid::Uuid) -> String {
    sqlx::query_scalar("SELECT lane FROM events WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("the event must be readable")
}

/// An event stored before the migration is online after it, and every way the writer
/// was called before — `append_event`, and `write_event` with eight arguments, which is
/// what compaction and a process not yet rolled both send — still writes an online event.
#[tokio::test]
async fn existing_events_and_existing_writers_stay_online_postgres_test() {
    let (pool, _container) = start_pool().await;
    through(BEFORE_LANES)
        .run(&pool)
        .await
        .expect("migrating to before the lane must succeed");
    let stored = uuid::Uuid::new_v4();
    sqlx::query(
        "SELECT append_event($1, '{}'::jsonb, '{}'::jsonb, 'Stored', 'urn:probe:old', \
         'Probe', NULL)",
    )
    .bind(stored)
    .execute(&pool)
    .await
    .expect("appending before the migration must succeed");

    MIGRATOR.run(&pool).await.expect("migrations must succeed");

    assert_eq!(lane_of(&pool, stored).await, "online");

    let appended = uuid::Uuid::new_v4();
    sqlx::query(
        "SELECT append_event($1, '{}'::jsonb, '{}'::jsonb, 'Appended', 'urn:probe:old', \
         'Probe', NULL)",
    )
    .bind(appended)
    .execute(&pool)
    .await
    .expect("append_event keeps its signature");
    assert_eq!(lane_of(&pool, appended).await, "online");

    let written = uuid::Uuid::new_v4();
    sqlx::query(
        "SELECT id FROM write_event($1, '{}'::jsonb, '{}'::jsonb, 'Written', 'urn:probe:old', \
         'Probe', NULL, FALSE)",
    )
    .bind(written)
    .fetch_one(&pool)
    .await
    .expect("an eight-argument write_event still resolves");
    assert_eq!(lane_of(&pool, written).await, "online");

    let bulk = uuid::Uuid::new_v4();
    sqlx::query(
        "SELECT id FROM write_event($1, '{}'::jsonb, '{}'::jsonb, 'Bulk', 'urn:probe:old', \
         'Probe', NULL, FALSE, 'bulk')",
    )
    .bind(bulk)
    .fetch_one(&pool)
    .await
    .expect("the internal append path stamps the bulk lane");
    assert_eq!(lane_of(&pool, bulk).await, "bulk");

    let unknown = sqlx::query(
        "SELECT id FROM write_event(gen_random_uuid(), '{}'::jsonb, '{}'::jsonb, 'Lost', \
         'urn:probe:old', 'Probe', NULL, FALSE, 'neither')",
    )
    .fetch_one(&pool)
    .await;
    assert!(
        unknown.is_err(),
        "a lane no reader knows would make an event invisible to every consumer"
    );
}

/// Each index is built `CONCURRENTLY`, by a migration that runs outside a transaction —
/// so appends keep running while it is built on a populated table — and holds online
/// rows only.
#[tokio::test]
async fn the_online_indexes_are_partial_and_built_concurrently_postgres_test() {
    for (version, index) in ONLINE_INDEXES {
        let migration = MIGRATOR
            .iter()
            .find(|migration| migration.version == version)
            .unwrap_or_else(|| panic!("migration {version} exists"));
        assert!(
            migration.no_tx,
            "{index} must be built outside a transaction"
        );
        assert!(
            migration
                .sql
                .as_str()
                .contains(&format!("CREATE INDEX CONCURRENTLY {index}")),
            "{index} must be built concurrently"
        );
    }

    let (pool, _container) = start_pool().await;
    MIGRATOR.run(&pool).await.expect("migrations must succeed");
    for (_, index) in ONLINE_INDEXES {
        let definition: String =
            sqlx::query_scalar("SELECT indexdef FROM pg_indexes WHERE indexname = $1")
                .bind(index)
                .fetch_one(&pool)
                .await
                .unwrap_or_else(|_| panic!("{index} exists"));
        assert!(
            definition.ends_with("WHERE (lane = 'online'::text)"),
            "{index} must hold online rows only: {definition}"
        );
        let valid: bool =
            sqlx::query_scalar("SELECT indisvalid FROM pg_index WHERE indexrelid = $1::regclass")
                .bind(index)
                .fetch_one(&pool)
                .await
                .expect("the index is in the catalogue");
        assert!(valid, "{index} must be usable");
    }
}

/// The plans, not the indexes' existence: the online sweep and the late-write lookup walk
/// the partial indexes, so a bulk's rows are never read by them.
#[tokio::test]
async fn the_online_feed_plans_with_the_partial_indexes_postgres_test() {
    let (pool, _container) = start_pool().await;
    MIGRATOR.run(&pool).await.expect("migrations must succeed");
    sqlx::query("INSERT INTO streams (id, type, version) VALUES ('urn:probe:seed', 'Probe', 2000)")
        .execute(&pool)
        .await
        .expect("seeding the stream row must succeed");
    // Nine bulk rows for every online one: the shape an import leaves.
    sqlx::query(
        "INSERT INTO events (id, data, metadata, stream_id, type, version, stream_seq, lane)
         SELECT gen_random_uuid(), '{}'::jsonb, '{}'::jsonb, 'urn:probe:seed', 'Seeded', v, v,
                CASE WHEN v % 10 = 0 THEN 'online' ELSE 'bulk' END
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

    let plans = [
        (
            "idx_events_online_global_position",
            "EXPLAIN (COSTS OFF) SELECT global_position, stream_id FROM events \
              WHERE global_position > 0 AND lane = 'online' \
              ORDER BY global_position ASC LIMIT 10",
        ),
        (
            "idx_events_online_commit_txid_position",
            "EXPLAIN (COSTS OFF) SELECT stream_id, global_position FROM events \
              WHERE commit_txid = pg_current_xact_id() AND lane = 'online' \
              ORDER BY global_position LIMIT 10",
        ),
    ];
    for (index, explain) in plans {
        let plan: String = sqlx::query(explain)
            .fetch_all(&mut *conn)
            .await
            .expect("EXPLAIN must succeed")
            .into_iter()
            .map(|row| row.get::<String, _>(0))
            .collect::<Vec<_>>()
            .join("\n");
        assert!(
            plan.contains(index),
            "expected {index} in the plan:\n{plan}"
        );
    }
}
