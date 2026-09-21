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

use chrono::{DateTime, Duration, SubsecRound, Utc};
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

/// The `error_kind` a refused command is parked under: what a row that names no
/// dispatch carried before 0024 gave it identity columns.
const REFUSED_KIND: &str = "Invalid Input";

/// The `error_kind` a panicking reaction is parked under — `PANIC_ERROR_KIND`.
/// The one shape that parks exactly one row per delivery, and therefore the one
/// the dedupe may collapse.
const PANIC_KIND: &str = "Panic";

/// One parked command, as the park path writes it.
struct Parked {
    event_id: uuid::Uuid,
    command_name: Option<&'static str>,
    dispatch_ordinal: Option<i32>,
    error_kind: &'static str,
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
            error_kind: REFUSED_KIND,
            error_message,
            // Truncated to what `timestamptz` stores: a staged time that cannot
            // survive the round trip makes an assertion about which timestamp
            // survived the collapse fail on the microseconds rather than on the
            // migration. `Utc::now()` is nanosecond-resolution on Linux.
            created_at: created_at.trunc_subsecs(6),
            deliveries: 1,
            retry_count: 0,
            last_retried_at: None,
        }
    }

    /// The same row, as a panicking reaction parks it: no dispatch to name, and
    /// the kind that says the unwind settled the delivery.
    fn panicked(mut self) -> Self {
        self.error_kind = PANIC_KIND;
        self
    }

    fn retried(mut self, retry_count: i32, last_retried_at: DateTime<Utc>) -> Self {
        self.retry_count = retry_count;
        self.last_retried_at = Some(last_retried_at.trunc_subsecs(6));
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
         VALUES ($1, 1, $2, $10, $3, \
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
    .bind(row.error_kind)
    .fetch_one(pool)
    .await
}

/// Every active row of `policy`, oldest id first.
async fn active(pool: &PgPool, policy: &str) -> Vec<sqlx::postgres::PgRow> {
    sqlx::query(
        "SELECT id, event_id, command_name, dispatch_ordinal, error_kind, error_message, \
                created_at, \
                last_parked_at, deliveries, retry_count, last_retried_at \
         FROM policy_dead_letters WHERE policy_name = $1 ORDER BY id ASC",
    )
    .bind(policy)
    .fetch_all(pool)
    .await
    .expect("reading the active table must succeed")
}

/// Every archived row of `policy`, by the id the row had while it was active.
///
/// Not by the archive's own id: the migration archives through
/// `DELETE ... RETURNING`, whose order is the planner's business, so the row
/// numbers it hands out say nothing about which generation was which.
async fn archived(pool: &PgPool, policy: &str) -> Vec<sqlx::postgres::PgRow> {
    sqlx::query(
        "SELECT dead_letter_id, reason, error_message, deliveries, retry_count \
         FROM discarded_dead_letters WHERE policy_name = $1 ORDER BY dead_letter_id ASC",
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

/// The generations a redelivery left of a **panicking** reaction are collapsed
/// by the migration, and every row it retires is archived.
///
/// A panic settles the delivery by unwinding, so that shape parks exactly one
/// row per delivery — the one thing in this table that makes siblings provably
/// duplicates. What survives is the newest *parking*, which is not always the
/// greatest id: a row takes its id when it is inserted and its timestamps when
/// its transaction began.
#[tokio::test]
async fn the_dedupe_migration_collapses_duplicate_generations_postgres_test() {
    let (_container, pool) = start_postgres().await;
    migrations_through(BEFORE_DEDUPE)
        .run(&pool)
        .await
        .expect("migrations up to the dedupe must succeed");

    let event = uuid::Uuid::new_v4();
    let other_event = uuid::Uuid::new_v4();
    let first_failed = (Utc::now() - Duration::hours(3)).trunc_subsecs(6);
    let last_parked = first_failed + Duration::minutes(20);
    let retried_at = (Utc::now() - Duration::hours(2)).trunc_subsecs(6);

    // Three deliveries of one panicking reaction. The second is the last one
    // parked and the one a retry has already settled; the third was inserted
    // afterwards by a transaction that began before it, so the greatest id and
    // the newest parking are different rows.
    let oldest = park(
        &pool,
        "dedupe",
        &Parked::of(event, None, None, "delivery 1", first_failed).panicked(),
    )
    .await
    .expect("staging the first generation");
    let newest_parking = park(
        &pool,
        "dedupe",
        &Parked::of(event, None, None, "delivery 2", last_parked)
            .panicked()
            .retried(2, retried_at),
    )
    .await
    .expect("staging the second generation");
    let greatest_id = park(
        &pool,
        "dedupe",
        &Parked::of(
            event,
            None,
            None,
            "delivery 3",
            first_failed + Duration::minutes(10),
        )
        .panicked(),
    )
    .await
    .expect("staging the third generation");

    // A reaction to another event, parked once: nothing to collapse, and the
    // dedupe must leave it exactly as it is.
    let untouched = park(
        &pool,
        "dedupe",
        &Parked::of(other_event, None, None, "only once", first_failed).panicked(),
    )
    .await
    .expect("staging the reaction parked once");

    MIGRATOR
        .run(&pool)
        .await
        .expect("the dedupe and the unique index must run against a duplicated table");

    let rows = active(&pool, "dedupe").await;
    assert_eq!(
        rows.iter()
            .map(|r| r.get::<i64, _>("id"))
            .collect::<Vec<_>>(),
        vec![newest_parking, untouched],
        "one row per parked reaction, the last one parked of each: {rows:#?}"
    );

    let survivor = rows
        .iter()
        .find(|r| r.get::<i64, _>("id") == newest_parking)
        .expect("the newest parking survives");
    assert_eq!(
        survivor.get::<String, _>("error_message"),
        "delivery 2",
        "the survivor keeps its own error: the last thing the reaction did"
    );
    assert_eq!(
        survivor.get::<DateTime<Utc>, _>("created_at"),
        first_failed,
        "and the earliest created_at of the group: when it first failed"
    );
    assert_eq!(
        survivor.get::<DateTime<Utc>, _>("last_parked_at"),
        last_parked,
        "and the latest parking, so the recency signal cannot move backwards \
         over the collapse"
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
            (greatest_id, "superseded".to_string())
        ],
        "the losers leave the active set with a reason of their own — neither \
         retried nor discarded, since nobody invoked either: {retired:#?}"
    );
    assert_eq!(
        retired
            .iter()
            .find(|r| r.get::<i64, _>("dead_letter_id") == greatest_id)
            .map(|r| r.get::<String, _>("error_message")),
        Some("delivery 3".to_string()),
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
        &Parked::of(event, None, None, "delivery 4", Utc::now()).panicked(),
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

/// Rows parked before the identity columns existed are kept apart, not
/// collapsed — however little they say about themselves.
///
/// Before 0024, n commands failing on one event parked n rows that differed only
/// in free text (0024's own header). Such a group is *either* one command parked
/// by n deliveries or n commands parked by one, and the table cannot tell; only
/// a panic's row, which settles the delivery by unwinding, is provably one per
/// delivery. So these are numbered apart like any other ambiguous sibling, and
/// the panic row parked for the same event keeps the null ordinal the running
/// code still writes.
#[tokio::test]
async fn pre_identity_siblings_are_kept_apart_not_collapsed_postgres_test() {
    let (_container, pool) = start_postgres().await;
    migrations_through(BEFORE_DEDUPE)
        .run(&pool)
        .await
        .expect("migrations up to the dedupe must succeed");

    let event = uuid::Uuid::new_v4();
    let parked_at = (Utc::now() - Duration::hours(1)).trunc_subsecs(6);
    let first_command = park(
        &pool,
        "pre-identity",
        &Parked::of(event, None, None, "the fee command failed", parked_at),
    )
    .await
    .expect("staging the first pre-identity row");
    let second_command = park(
        &pool,
        "pre-identity",
        &Parked::of(
            event,
            None,
            None,
            "the ledger command failed",
            parked_at + Duration::seconds(1),
        ),
    )
    .await
    .expect("staging the second pre-identity row");
    let panicked = park(
        &pool,
        "pre-identity",
        &Parked::of(event, None, None, "reaction exploded", parked_at).panicked(),
    )
    .await
    .expect("staging the panic row");

    MIGRATOR
        .run(&pool)
        .await
        .expect("the migration must run against a pre-identity backlog");

    let rows = active(&pool, "pre-identity").await;
    assert_eq!(
        rows.iter()
            .map(|r| (
                r.get::<i64, _>("id"),
                r.get::<Option<i32>, _>("dispatch_ordinal")
            ))
            .collect::<Vec<_>>(),
        vec![
            (first_command, Some(-1)),
            (second_command, Some(-2)),
            (panicked, None)
        ],
        "every ambiguous row survives, numbered apart; the panic keeps the null \
         ordinal the running code parks it with: {rows:#?}"
    );
    assert!(
        archived(&pool, "pre-identity").await.is_empty(),
        "nothing was retired: nothing in this backlog could be shown to be a \
         duplicate"
    );
}

/// Two legacy rows that name the same command are kept apart, not collapsed.
///
/// A row parked before the ordinal existed records which command failed but not
/// its place in the reaction, so two of them are *either* a redelivery's
/// duplicate or a reaction that legitimately emitted that command twice — and
/// nothing recorded says which. Collapsing them would take an active failure out
/// of the table on a guess. They are made unique instead, by a synthetic
/// negative ordinal no dispatch can have.
#[tokio::test]
async fn legacy_siblings_naming_one_command_are_kept_not_collapsed_postgres_test() {
    let (_container, pool) = start_postgres().await;
    migrations_through(BEFORE_DEDUPE)
        .run(&pool)
        .await
        .expect("migrations up to the dedupe must succeed");

    let event = uuid::Uuid::new_v4();
    let parked_at = (Utc::now() - Duration::hours(1)).trunc_subsecs(6);
    let first = park(
        &pool,
        "legacy",
        &Parked::of(
            event,
            Some("app::Withdraw"),
            None,
            "first sibling",
            parked_at,
        ),
    )
    .await
    .expect("staging the first legacy row");
    let second = park(
        &pool,
        "legacy",
        &Parked::of(
            event,
            Some("app::Withdraw"),
            None,
            "second sibling",
            parked_at + Duration::seconds(1),
        ),
    )
    .await
    .expect("staging the second legacy row");

    MIGRATOR
        .run(&pool)
        .await
        .expect("the migration must run against a table of legacy siblings");

    let rows = active(&pool, "legacy").await;
    assert_eq!(
        rows.iter()
            .map(|r| (
                r.get::<i64, _>("id"),
                r.get::<Option<i32>, _>("dispatch_ordinal")
            ))
            .collect::<Vec<_>>(),
        vec![(first, Some(-1)), (second, Some(-2))],
        "both rows survive, numbered in the order they were parked and outside \
         the range a dispatch's index can take: {rows:#?}"
    );
    assert!(
        archived(&pool, "legacy").await.is_empty(),
        "nothing was retired, because nothing could be shown to be a duplicate"
    );

    // And the key is live over them: the command this release parks for the same
    // reaction takes its own ordinal, and a second copy of a legacy row is
    // refused.
    park(
        &pool,
        "legacy",
        &Parked::of(
            event,
            Some("app::Withdraw"),
            Some(0),
            "parked by this release",
            Utc::now(),
        ),
    )
    .await
    .expect("a dispatch's own ordinal cannot collide with a synthetic one");
    let refusal = park(
        &pool,
        "legacy",
        &Parked::of(
            event,
            Some("app::Withdraw"),
            Some(-1),
            "a copy of the first sibling",
            Utc::now(),
        ),
    )
    .await
    .expect_err("a row duplicating a numbered legacy row must be refused");
    assert_eq!(
        refusal
            .as_database_error()
            .and_then(|e| e.code())
            .as_deref(),
        Some("23505")
    );
}

/// The status read model's aggregate over the parked rows stays off the heap.
///
/// `PolicyStatus` counts a policy's parked commands and reads when it last
/// parked one, on every poll of a consumer's health endpoint. Reading
/// `last_parked_at` instead of `created_at` would have made that a heap fetch
/// per parked row; the payload column on `idx_dead_letters_policy_created_parked`
/// is what keeps it index-only.
#[tokio::test]
async fn the_status_aggregate_reads_only_the_index_postgres_test() {
    let (_container, pool) = start_postgres().await;
    MIGRATOR.run(&pool).await.expect("migrations must succeed");

    let event = uuid::Uuid::new_v4();
    for ordinal in 0..50 {
        park(
            &pool,
            "covered",
            &Parked::of(
                event,
                Some("app::Withdraw"),
                Some(ordinal),
                "failed",
                Utc::now(),
            ),
        )
        .await
        .expect("seeding parked rows must succeed");
    }
    sqlx::query("ANALYZE policy_dead_letters")
        .execute(&pool)
        .await
        .expect("ANALYZE must succeed");

    // Fifty rows fit in a page or two, so the planner would read them
    // sequentially whatever indexes exist, and a bitmap scan is never index-only
    // however well the index covers the query. Taking both choices away asks the
    // question this test is about: can the aggregate be answered from an index
    // alone?
    let mut conn = pool.acquire().await.expect("acquiring a connection");
    for off in ["SET enable_seqscan = off", "SET enable_bitmapscan = off"] {
        sqlx::query(off)
            .execute(&mut *conn)
            .await
            .expect("narrowing the planner's choices must succeed");
    }

    let plan: Vec<String> = sqlx::query(
        "EXPLAIN SELECT COUNT(*), MAX(last_parked_at) FROM policy_dead_letters \
         WHERE policy_name = $1",
    )
    .bind("covered")
    .fetch_all(&mut *conn)
    .await
    .expect("EXPLAIN must succeed")
    .iter()
    .map(|row| row.get::<String, _>(0))
    .collect();

    assert!(
        plan.iter().any(|line| line.contains("Index Only Scan")
            && line.contains("idx_dead_letters_policy_created_parked")),
        "the status aggregate must be answered from the index alone: {plan:?}"
    );

    let indexes: Vec<String> =
        sqlx::query_scalar("SELECT indexname::text FROM pg_indexes WHERE tablename = $1")
            .bind("policy_dead_letters")
            .fetch_all(&pool)
            .await
            .expect("reading pg_indexes must succeed");
    assert!(
        !indexes.contains(&"idx_dead_letters_policy".to_string()),
        "and the index it supersedes must be dropped, not left to cost the park \
         path a second write: {indexes:?}"
    );
}
