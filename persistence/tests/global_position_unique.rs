//! A `global_position` identifies exactly one event, and the database says so.
//!
//! The policy feed steps its cursor one position at a time, so two events sharing a
//! position means one of them is stepped over and never delivered — the silent loss of
//! funkode-io/replay#164 arriving from the schema instead of from the reader. A
//! `BIGSERIAL` does not rule that out: it is a column, a sequence and a default, and no
//! constraint at all.
//!
//! These tests assert the refusal, not the index: an insert that duplicates a position
//! fails, a database that already holds duplicates stops the migration and is told which
//! positions they are, and the feed's read still reaches its rows through an index.

use sqlx::{PgPool, Row};
use testcontainers_modules::{
    postgres,
    testcontainers::{runners::AsyncRunner, ContainerAsync},
};

mod common;
use common::migrations::{through as migrations_through, MIGRATOR};
use common::postgres_image::postgres_container;

const POSTGRES_PORT: u16 = 5432;

/// The migration that adds the unique index, and the one before it: the tests that stage
/// a broken database migrate up to `BEFORE_UNIQUE` and then run the rest.
const BEFORE_UNIQUE: i64 = 13;

/// An empty database — every test decides for itself how far to migrate it.
async fn start_postgres() -> (ContainerAsync<postgres::Postgres>, PgPool) {
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

    (container, pool)
}

async fn seed_stream(pool: &PgPool, stream_id: &str) {
    sqlx::query("INSERT INTO streams (id, type, version) VALUES ($1, 'Acl', 0)")
        .bind(stream_id)
        .execute(pool)
        .await
        .expect("seeding stream row must succeed");
}

/// Inserts one event, writing `global_position` explicitly — the only way to produce a
/// duplicate, and what any "claim the position" writer would do.
///
/// `stream_seq` (migration 0027) is named only on a database that has it: one of these
/// tests stages the schema as it stood before 0015, where the column does not exist yet.
async fn insert_event_at(
    pool: &PgPool,
    stream_id: &str,
    version: i64,
    global_position: i64,
    schema: Schema,
) -> Result<(), sqlx::Error> {
    let statement = match schema {
        Schema::Current => {
            "INSERT INTO events (id, data, metadata, stream_id, type, version, stream_seq, global_position) \
             VALUES ($1, '{}', '{}', $2, 'Granted', $3, $3, $4)"
        }
        Schema::BeforeTheUniqueIndex => {
            "INSERT INTO events (id, data, metadata, stream_id, type, version, global_position) \
             VALUES ($1, '{}', '{}', $2, 'Granted', $3, $4)"
        }
    };

    let written = sqlx::query(statement)
        .bind(uuid::Uuid::new_v4())
        .bind(stream_id)
        .bind(version)
        .bind(global_position)
        .execute(pool)
        .await
        .map(|_| ());

    if written.is_ok() && matches!(schema, Schema::Current) {
        common::places::settle(pool).await;
    }
    written
}

/// Which schema a seeding statement is written against.
#[derive(Clone, Copy)]
enum Schema {
    Current,
    BeforeTheUniqueIndex,
}

/// Indexes on `events` that Postgres will not use: the debris a failed concurrent build
/// leaves behind.
async fn invalid_indexes(pool: &PgPool) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT indexrelid::regclass::text FROM pg_index \
         WHERE indrelid = 'events'::regclass AND NOT indisvalid",
    )
    .fetch_all(pool)
    .await
    .expect("reading pg_index must succeed")
}

async fn index_names(pool: &PgPool) -> Vec<String> {
    sqlx::query_scalar("SELECT indexname::text FROM pg_indexes WHERE tablename = 'events'")
        .fetch_all(pool)
        .await
        .expect("reading pg_indexes must succeed")
}

/// A second event cannot take a position a first one already holds — whatever wrote it.
#[tokio::test]
async fn a_duplicate_global_position_is_refused_postgres_test() {
    let (_container, pool) = start_postgres().await;
    MIGRATOR.run(&pool).await.expect("migrations must succeed");

    let stream_id = "urn:acl:unique-1";
    seed_stream(&pool, stream_id).await;
    insert_event_at(&pool, stream_id, 1, 1, Schema::Current)
        .await
        .expect("the first event takes position 1");

    let refusal = insert_event_at(&pool, stream_id, 2, 1, Schema::Current)
        .await
        .expect_err("a second event must not take position 1");

    assert_eq!(
        refusal
            .as_database_error()
            .and_then(|e| e.code())
            .as_deref(),
        Some("23505"),
        "the database must refuse the duplicate as a unique violation, not accept it: {refusal}"
    );

    let holders: i64 = sqlx::query_scalar("SELECT count(*) FROM events WHERE global_position = 1")
        .fetch_one(&pool)
        .await
        .expect("counting events must succeed");
    assert_eq!(
        holders, 1,
        "position 1 must still identify exactly one event"
    );
}

/// A database that already holds duplicates is told which positions they are, and is left
/// with no half-built index to clean up.
#[tokio::test]
async fn duplicate_positions_stop_the_migration_and_name_themselves_postgres_test() {
    let (_container, pool) = start_postgres().await;
    migrations_through(BEFORE_UNIQUE)
        .run(&pool)
        .await
        .expect("migrations up to the unique index must succeed");

    let stream_id = "urn:acl:unique-2";
    seed_stream(&pool, stream_id).await;
    for (version, position) in [(1, 41), (2, 41), (3, 42), (4, 42)] {
        insert_event_at(
            &pool,
            stream_id,
            version,
            position,
            Schema::BeforeTheUniqueIndex,
        )
        .await
        .expect("the pre-migration schema allows the duplicate");
    }

    let failure = MIGRATOR
        .run(&pool)
        .await
        .expect_err("the migration must refuse to run against duplicate positions");
    let message = failure.to_string();

    assert!(
        message.contains("41") && message.contains("42"),
        "the failure must name the duplicated positions, not just report a conflict: {message}"
    );
    assert_eq!(
        invalid_indexes(&pool).await,
        Vec::<String>::new(),
        "a migration that stops before the concurrent build leaves no invalid index behind"
    );
}

/// The index the unique one supersedes is gone, as is the partial index no query could
/// ever use, and the feed's read still reaches its rows through an index rather than a
/// sequential scan and a sort.
#[tokio::test]
async fn the_feed_read_still_scans_an_index_postgres_test() {
    let (_container, pool) = start_postgres().await;
    MIGRATOR.run(&pool).await.expect("migrations must succeed");

    let indexes = index_names(&pool).await;
    assert!(
        !indexes.contains(&"idx_events_global_position".to_string()),
        "the non-unique index the unique one supersedes must be dropped: {indexes:?}"
    );
    assert!(
        !indexes.contains(&"idx_events_policy_feed".to_string()),
        "the partial index no feed query can match must be dropped: {indexes:?}"
    );

    let stream_id = "urn:acl:unique-3";
    seed_stream(&pool, stream_id).await;
    for version in 1..=50 {
        insert_event_at(&pool, stream_id, version, version, Schema::Current)
            .await
            .expect("seeding events must succeed");
    }
    sqlx::query("ANALYZE events")
        .execute(&pool)
        .await
        .expect("ANALYZE must succeed");

    // Fifty rows fit in one page, so the planner would read them sequentially whatever
    // indexes exist. Taking that choice away asks the question this test is about: can
    // the ordered range read the policy feed issues be served by an index at all?
    let mut conn = pool.acquire().await.expect("acquiring a connection");
    sqlx::query("SET enable_seqscan = off")
        .execute(&mut *conn)
        .await
        .expect("disabling sequential scans must succeed");

    let plan: Vec<String> = sqlx::query(
        "EXPLAIN SELECT id, data, metadata, stream_id, type, version, created, \
         aggregate_version, global_position, compacted_snapshot FROM events \
         WHERE global_position > $1 ORDER BY global_position ASC LIMIT 100",
    )
    .bind(0_i64)
    .fetch_all(&mut *conn)
    .await
    .expect("EXPLAIN must succeed")
    .iter()
    .map(|row| row.get::<String, _>(0))
    .collect();

    assert!(
        plan.iter()
            .any(|line| line.contains("idx_events_global_position_unique")),
        "the policy feed's read must scan the unique index: {plan:?}"
    );
}
