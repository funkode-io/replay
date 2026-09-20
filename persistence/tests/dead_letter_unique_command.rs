//! A parked command is one row, and the schema says so.
//!
//! Parking was an unconditional INSERT into a table with no key over it, so a
//! redelivery of the triggering event left a second generation of rows for one
//! reaction (funkode-io/replay#220). These tests assert the key itself and the
//! migration that makes room for it: an insert that duplicates a parked command
//! is refused, and a table that already holds duplicates is collapsed rather
//! than left for an operator to discard row by row.
//!
//! The library's behaviour under a real redelivery is in
//! `policy_redelivery_parks_once.rs`; what is verified here is the schema.

use chrono::{DateTime, Duration, Utc};
use sqlx::{PgPool, Row};
use testcontainers_modules::{
    postgres,
    testcontainers::{runners::AsyncRunner, ContainerAsync},
};

mod common;
use common::migrations::{through as migrations_through, MIGRATOR};
use common::postgres_image::postgres_container;

const POSTGRES_PORT: u16 = 5432;

/// The migration that adds the delivery columns: everything up to and including
/// it is the schema a duplicated table is staged in, and the dedupe and the
/// unique index are what the tests then run against it.
const BEFORE_DEDUPE: i64 = 27;

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

/// One parked command, as the park path writes it.
struct Parked {
    event_id: uuid::Uuid,
    command_name: Option<&'static str>,
    dispatch_ordinal: Option<i32>,
    error_message: &'static str,
    created_at: DateTime<Utc>,
    deliveries: i32,
    retry_count: i32,
    last_retried_at: Option<DateTime<Utc>>,
}

impl Parked {
    /// A row for `command_name` at `ordinal`, parked `created_at`, with nothing
    /// tried on it.
    fn of(
        event_id: uuid::Uuid,
        command_name: Option<&'static str>,
        dispatch_ordinal: Option<i32>,
        error_message: &'static str,
        created_at: DateTime<Utc>,
    ) -> Self {
        Self {
            event_id,
            command_name,
            dispatch_ordinal,
            error_message,
            created_at,
            deliveries: 1,
            retry_count: 0,
            last_retried_at: None,
        }
    }

    fn retried(mut self, retry_count: i32, last_retried_at: DateTime<Utc>) -> Self {
        self.retry_count = retry_count;
        self.last_retried_at = Some(last_retried_at);
        self
    }
}

/// Insert a parked command, returning its id — the raw write the runner makes,
/// so a test can stage a table the runner can no longer produce.
async fn park(pool: &PgPool, policy: &str, row: &Parked) -> Result<i64, sqlx::Error> {
    sqlx::query_scalar(
        "INSERT INTO policy_dead_letters \
             (policy_name, global_position, event_id, error_kind, error_message, \
              aggregate_name, target_stream_id, command_name, dispatch_ordinal, \
              created_at, last_parked_at, deliveries, retry_count, last_retried_at) \
         VALUES ($1, 1, $2, 'Invalid Input', $3, \
                 CASE WHEN $4::text IS NULL THEN NULL ELSE 'app::Account' END, \
                 CASE WHEN $4::text IS NULL THEN NULL ELSE 'urn:account:1' END, \
                 $4, $5, $6, $6, $7, $8, $9) \
         RETURNING id",
    )
    .bind(policy)
    .bind(row.event_id)
    .bind(row.error_message)
    .bind(row.command_name)
    .bind(row.dispatch_ordinal)
    .bind(row.created_at)
    .bind(row.deliveries)
    .bind(row.retry_count)
    .bind(row.last_retried_at)
    .fetch_one(pool)
    .await
}

/// Every active row of `policy`, oldest id first.
async fn active(pool: &PgPool, policy: &str) -> Vec<sqlx::postgres::PgRow> {
    sqlx::query(
        "SELECT id, event_id, command_name, dispatch_ordinal, error_message, created_at, \
                last_parked_at, deliveries, retry_count, last_retried_at \
         FROM policy_dead_letters WHERE policy_name = $1 ORDER BY id ASC",
    )
    .bind(policy)
    .fetch_all(pool)
    .await
    .expect("reading the active table must succeed")
}

/// Every archived row of `policy`, oldest id first.
async fn archived(pool: &PgPool, policy: &str) -> Vec<sqlx::postgres::PgRow> {
    sqlx::query(
        "SELECT dead_letter_id, reason, error_message, deliveries, retry_count \
         FROM discarded_dead_letters WHERE policy_name = $1 ORDER BY id ASC",
    )
    .bind(policy)
    .fetch_all(pool)
    .await
    .expect("reading the archive must succeed")
}

/// Indexes on the dead-letter table Postgres will not use: the debris a failed
/// concurrent build leaves behind.
async fn invalid_indexes(pool: &PgPool) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT indexrelid::regclass::text FROM pg_index \
         WHERE indrelid = 'policy_dead_letters'::regclass AND NOT indisvalid",
    )
    .fetch_all(pool)
    .await
    .expect("reading pg_index must succeed")
}

/// A second row for a command already parked is refused, whatever writes it.
///
/// The park path turns that refusal into a refresh with `ON CONFLICT`; the key
/// is what makes a park that does not go through it — a second process, an
/// operator's script — impossible rather than merely unlikely.
#[tokio::test]
async fn a_second_row_for_one_parked_command_is_refused_postgres_test() {
    let (_container, pool) = start_postgres().await;
    MIGRATOR.run(&pool).await.expect("migrations must succeed");

    let event = uuid::Uuid::new_v4();
    let now = Utc::now();
    park(
        &pool,
        "unique-1",
        &Parked::of(event, Some("app::Withdraw"), Some(0), "first", now),
    )
    .await
    .expect("the first parking of a command must succeed");

    let refusal = park(
        &pool,
        "unique-1",
        &Parked::of(event, Some("app::Withdraw"), Some(0), "again", now),
    )
    .await
    .expect_err("a second row for the same parked command must be refused");

    assert_eq!(
        refusal
            .as_database_error()
            .and_then(|e| e.code())
            .as_deref(),
        Some("23505"),
        "the database must refuse it as a unique violation, not accept it: {refusal}"
    );

    // The reaction's own repeat is a different parked command, told apart by
    // where it sits in the reaction, and keeps its own row.
    park(
        &pool,
        "unique-1",
        &Parked::of(event, Some("app::Withdraw"), Some(1), "the second one", now),
    )
    .await
    .expect("the same command at another ordinal is another parked command");

    assert_eq!(active(&pool, "unique-1").await.len(), 2);
}

/// Two rows with no identity to name are one parked reaction, although every
/// column that identifies them is null.
///
/// A panic in `react` parks exactly one row per delivery, so without
/// `NULLS NOT DISTINCT` the case with nothing to collapse on would be the one
/// case that kept duplicating.
#[tokio::test]
async fn two_rows_with_a_null_identity_collapse_onto_one_reaction_postgres_test() {
    let (_container, pool) = start_postgres().await;
    MIGRATOR.run(&pool).await.expect("migrations must succeed");

    let event = uuid::Uuid::new_v4();
    let now = Utc::now();
    park(
        &pool,
        "unique-2",
        &Parked::of(event, None, None, "panic", now),
    )
    .await
    .expect("the first parking of a panicking reaction must succeed");

    let refusal = park(
        &pool,
        "unique-2",
        &Parked::of(event, None, None, "panic again", now),
    )
    .await
    .expect_err("a second row for the same reaction must be refused");

    assert_eq!(
        refusal
            .as_database_error()
            .and_then(|e| e.code())
            .as_deref(),
        Some("23505"),
        "nulls must be treated as equal by this key, not as all different: {refusal}"
    );
}

/// A table that already holds duplicate generations is collapsed by the
/// migration, and every row it retires is archived rather than deleted.
///
/// These duplicates are the library's own — an unconditional INSERT with no key
/// over it — so cleaning them is the library's job, unlike the hand-written
/// `events` positions 0014 refuses to clean. What survives is the newest
/// generation, carrying what the group knew between them.
#[tokio::test]
async fn the_dedupe_migration_collapses_duplicate_generations_postgres_test() {
    let (_container, pool) = start_postgres().await;
    migrations_through(BEFORE_DEDUPE)
        .run(&pool)
        .await
        .expect("migrations up to the dedupe must succeed");

    let event = uuid::Uuid::new_v4();
    let first_failed = Utc::now() - Duration::hours(3);
    let retried_at = Utc::now() - Duration::hours(2);

    // Three deliveries of one command, the middle one settled by a retry that
    // left it still failing — the row an operator has already worked on is not
    // the newest generation.
    let oldest = park(
        &pool,
        "dedupe",
        &Parked::of(
            event,
            Some("app::Withdraw"),
            Some(0),
            "delivery 1",
            first_failed,
        ),
    )
    .await
    .expect("staging the first generation");
    let middle = park(
        &pool,
        "dedupe",
        &Parked::of(
            event,
            Some("app::Withdraw"),
            Some(0),
            "delivery 2",
            first_failed + Duration::minutes(10),
        )
        .retried(2, retried_at),
    )
    .await
    .expect("staging the second generation");
    let newest = park(
        &pool,
        "dedupe",
        &Parked::of(
            event,
            Some("app::Withdraw"),
            Some(0),
            "delivery 3",
            first_failed + Duration::minutes(20),
        ),
    )
    .await
    .expect("staging the third generation");

    // A command of the same reaction that was parked once: nothing to collapse,
    // and the dedupe must leave it exactly as it is.
    let untouched = park(
        &pool,
        "dedupe",
        &Parked::of(
            event,
            Some("app::Freeze"),
            Some(1),
            "only once",
            first_failed,
        ),
    )
    .await
    .expect("staging the command parked once");

    MIGRATOR
        .run(&pool)
        .await
        .expect("the dedupe and the unique index must run against a duplicated table");

    let rows = active(&pool, "dedupe").await;
    assert_eq!(
        rows.iter()
            .map(|r| r.get::<i64, _>("id"))
            .collect::<Vec<_>>(),
        vec![newest, untouched],
        "one row per parked command, the newest generation of each: {rows:#?}"
    );

    let survivor = rows
        .iter()
        .find(|r| r.get::<i64, _>("id") == newest)
        .expect("the newest generation survives");
    assert_eq!(
        survivor.get::<String, _>("error_message"),
        "delivery 3",
        "the survivor keeps its own error: the last thing the command did"
    );
    assert_eq!(
        survivor.get::<DateTime<Utc>, _>("created_at"),
        first_failed,
        "and the earliest created_at of the group: when the command first failed"
    );
    assert_eq!(
        survivor.get::<i32, _>("deliveries"),
        3,
        "the generations it replaces are deliveries it counts"
    );
    assert_eq!(
        (
            survivor.get::<i32, _>("retry_count"),
            survivor.get::<Option<DateTime<Utc>>, _>("last_retried_at")
        ),
        (2, Some(retried_at)),
        "what an operator already tried on the group survives the collapse"
    );

    let retired = archived(&pool, "dedupe").await;
    assert_eq!(
        retired
            .iter()
            .map(|r| (
                r.get::<i64, _>("dead_letter_id"),
                r.get::<String, _>("reason")
            ))
            .collect::<Vec<_>>(),
        vec![
            (oldest, "superseded".to_string()),
            (middle, "superseded".to_string())
        ],
        "the losers leave the active set with a reason of their own — neither \
         retried nor discarded, since nobody invoked either: {retired:#?}"
    );
    assert_eq!(
        retired[1].get::<String, _>("error_message"),
        "delivery 2",
        "each archived generation keeps what it recorded"
    );
    assert_eq!(
        invalid_indexes(&pool).await,
        Vec::<String>::new(),
        "the concurrent build leaves no invalid index behind"
    );

    // And the table now refuses what it was just cleaned of.
    let refusal = park(
        &pool,
        "dedupe",
        &Parked::of(
            event,
            Some("app::Withdraw"),
            Some(0),
            "delivery 4",
            Utc::now(),
        ),
    )
    .await
    .expect_err("a fourth generation must be refused once the key exists");
    assert_eq!(
        refusal
            .as_database_error()
            .and_then(|e| e.code())
            .as_deref(),
        Some("23505")
    );
}
