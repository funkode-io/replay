//! Every event records the transaction that wrote it (funkode-io/replay#193).
//!
//! The stamp is data only: nothing reads `commit_txid` yet — funkode-io/replay#171
//! carries the read-path change — so these tests assert against the column itself
//! rather than through any behaviour.
//!
//! Two of them hold a database at the schema a live deployment is on when 0018 reaches
//! it, and one of those runs 0018 by hand so it can keep its lock while an append waits.

use std::time::{Duration, Instant};

use sqlx::{postgres::PgPoolOptions, AssertSqlSafe, Executor, PgPool, Row};
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};

use replay_macros::define_aggregate;
use replay_persistence::{Cqrs, PostgresEventStore};

mod common;
use common::migrations::{self, through as migrations_through, MIGRATOR};
use common::postgres_image::postgres_container;

const POSTGRES_PORT: u16 = 5432;

/// The migration that adds the stamp, and the one before it: the tests that stage a
/// pre-stamp database migrate up to `BEFORE_STAMP` and then run the rest.
const BEFORE_STAMP: i64 = 17;
const STAMP: i64 = 18;

/// The sentinel every event that predates the migration carries: InvalidTransactionId,
/// which Postgres never assigns and which orders before every real id.
const SENTINEL: u64 = 0;

/// How long a test waits for an append to become visibly blocked on the migration's lock.
const BLOCKED_TIMEOUT: Duration = Duration::from_secs(10);

/// Upper bound on the rows any observation reads. These tests write a handful of events;
/// a query that returned more is not observing what it thinks it is.
const OBSERVATION_LIMIT: i64 = 1_000;

define_aggregate! {
    Ledger {
        namespace: "ledger",
        state: {
            balance: f64,
        },
        commands: {
            Add { amount: f64 },
            AddTwice { amount: f64 },
        },
        events: {
            Added { amount: f64 },
        }
    }
}

impl replay::EventStream for Ledger {
    type Event = LedgerEvent;

    fn stream_type() -> String {
        "Ledger".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        let LedgerEvent::Added { amount } = event;
        self.balance += amount;
    }
}

impl replay::Aggregate for Ledger {
    type Command = LedgerCommand;
    type Error = replay::Error;
    type Services = ();

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        Ok(match command {
            LedgerCommand::Add { amount } => vec![LedgerEvent::Added { amount }],
            // Two events from one command, hence one transaction: the pair is what makes
            // "the transaction that wrote it" distinguishable from "one id per event".
            LedgerCommand::AddTwice { amount } => {
                vec![LedgerEvent::Added { amount }, LedgerEvent::Added { amount }]
            }
        })
    }
}

impl replay::Compactable for Ledger {
    async fn compacted_events(
        &self,
        events: impl futures::TryStream<Ok = LedgerEvent, Error = replay::Error> + Send,
    ) -> replay::Result<replay::Compaction<LedgerEvent>> {
        use futures::TryStreamExt;
        // The running total as a single event: a fixpoint, since folding one `Added`
        // yields that same `Added` back.
        let balance = events
            .try_fold(0.0, |total, LedgerEvent::Added { amount }| async move {
                Ok(total + amount)
            })
            .await?;

        Ok(vec![LedgerEvent::Added { amount: balance }].into())
    }
}

// ── Fixtures ─────────────────────────────────────────────────────────────────

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

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&format!(
            "postgres://postgres:postgres@{host}:{port}/postgres"
        ))
        .await
        .expect("Failed to connect to Postgres");

    (pool, container)
}

/// One event as this ticket sees it.
#[derive(Debug)]
struct Stamp {
    position: i64,
    /// `xid8` arrives as text: sqlx has no decoder for it, and the value is an unsigned
    /// 64-bit counter that `i64` could not hold in full.
    txid: u64,
    snapshot: bool,
    archived: bool,
}

/// Every event, in `global_position` order.
async fn stamps(pool: &PgPool) -> Vec<Stamp> {
    sqlx::query(
        "SELECT global_position, commit_txid::text AS txid, compacted_snapshot, \
                aggregate_version \
           FROM events ORDER BY global_position LIMIT $1",
    )
    .bind(OBSERVATION_LIMIT)
    .fetch_all(pool)
    .await
    .expect("reading the stamps must succeed")
    .into_iter()
    .map(|row| Stamp {
        position: row.get("global_position"),
        txid: row
            .get::<String, _>("txid")
            .parse()
            .expect("a transaction id is an unsigned 64-bit counter"),
        snapshot: row.get("compacted_snapshot"),
        archived: row.get::<Option<i32>, _>("aggregate_version").is_some(),
    })
    .collect()
}

/// Append `command` to `ledger`, through the store.
async fn append(cqrs: &Cqrs<PostgresEventStore>, ledger: &LedgerUrn, command: LedgerCommand) {
    cqrs.execute::<Ledger>(ledger, replay::Metadata::default(), command, &(), None)
        .await
        .expect("appending must succeed");
}

// ── Tests ────────────────────────────────────────────────────────────────────

/// One command, one transaction, one id — and a later command a strictly higher one.
#[tokio::test]
async fn an_appended_event_carries_the_transaction_that_wrote_it_postgres_test() {
    let (pool, _container) = start_postgres().await;
    MIGRATOR.run(&pool).await.expect("migrations must succeed");

    let cqrs = Cqrs::new(PostgresEventStore::new(pool.clone()));
    let ledger = LedgerUrn::new("stamped").unwrap();

    append(&cqrs, &ledger, LedgerCommand::AddTwice { amount: 10.0 }).await;
    append(&cqrs, &ledger, LedgerCommand::Add { amount: 5.0 }).await;

    let stamps = stamps(&pool).await;
    assert_eq!(stamps.len(), 3, "three events were appended: {stamps:?}");
    assert!(
        stamps.iter().all(|stamp| stamp.txid != SENTINEL),
        "an appended event carries a real transaction id: {stamps:?}"
    );
    assert_eq!(
        stamps[0].txid, stamps[1].txid,
        "two events from one command were written by one transaction: {stamps:?}"
    );
    assert!(
        stamps[2].txid > stamps[1].txid,
        "a later append is a later transaction: {stamps:?}"
    );
}

/// Compaction inserts its snapshot rows straight into `events`, bypassing `append_event`.
/// They are stamped all the same, by the transaction that compacted.
#[tokio::test]
async fn compaction_stamps_the_snapshot_rows_it_writes_postgres_test() {
    let (pool, _container) = start_postgres().await;
    MIGRATOR.run(&pool).await.expect("migrations must succeed");

    let cqrs = Cqrs::new(PostgresEventStore::new(pool.clone()));
    let ledger = LedgerUrn::new("compacted").unwrap();

    append(&cqrs, &ledger, LedgerCommand::AddTwice { amount: 10.0 }).await;
    append(&cqrs, &ledger, LedgerCommand::Add { amount: 5.0 }).await;

    let aggregate = cqrs.fetch_aggregate::<Ledger>(&ledger).await.unwrap();
    cqrs.compact(&aggregate, replay::Metadata::default())
        .await
        .expect("compaction must succeed");

    let stamps = stamps(&pool).await;
    let snapshots: Vec<&Stamp> = stamps.iter().filter(|stamp| stamp.snapshot).collect();
    assert!(
        !snapshots.is_empty(),
        "compaction wrote at least one snapshot row: {stamps:?}"
    );
    assert!(
        snapshots.iter().all(|stamp| stamp.txid != SENTINEL),
        "a compaction snapshot carries a real transaction id: {stamps:?}"
    );

    let archived_high = stamps
        .iter()
        .filter(|stamp| stamp.archived)
        .map(|stamp| stamp.txid)
        .max()
        .expect("compaction archived the originals");
    assert!(
        snapshots.iter().all(|stamp| stamp.txid > archived_high),
        "the compacting transaction is later than the ones it archived: {stamps:?}"
    );
}

/// Events already in the log when the migration arrives keep their order, ahead of
/// everything written afterwards.
#[tokio::test]
async fn events_written_before_the_migration_carry_the_sentinel_postgres_test() {
    let (pool, _container) = start_postgres().await;
    migrations_through(BEFORE_STAMP)
        .run(&pool)
        .await
        .expect("migrations up to the stamp must succeed");

    let cqrs = Cqrs::new(PostgresEventStore::new(pool.clone()));
    let ledger = LedgerUrn::new("pre-migration").unwrap();
    append(&cqrs, &ledger, LedgerCommand::AddTwice { amount: 10.0 }).await;

    MIGRATOR
        .run(&pool)
        .await
        .expect("the rest of the migrations must succeed");

    append(&cqrs, &ledger, LedgerCommand::Add { amount: 5.0 }).await;

    let stamps = stamps(&pool).await;
    assert_eq!(stamps.len(), 3, "three events exist: {stamps:?}");
    assert!(
        stamps[0].txid == SENTINEL && stamps[1].txid == SENTINEL,
        "an event that predates the migration carries the sentinel: {stamps:?}"
    );
    assert!(
        stamps[2].txid != SENTINEL,
        "an event appended after the migration carries a real id: {stamps:?}"
    );

    let commit_order: Vec<i64> = sqlx::query_scalar(
        "SELECT global_position FROM events ORDER BY commit_txid, global_position LIMIT $1",
    )
    .bind(OBSERVATION_LIMIT)
    .fetch_all(&pool)
    .await
    .expect("ordering by the stamp must succeed");
    assert_eq!(
        commit_order,
        stamps.iter().map(|s| s.position).collect::<Vec<_>>(),
        "the sentinel sorts the old events first, in the order they already had"
    );
}

/// The migration is safe to run under load: an append that starts while it holds the
/// table waits, and commits stamped for real rather than with the sentinel.
#[tokio::test]
async fn an_append_blocked_by_the_migration_is_stamped_for_real_postgres_test() {
    let (pool, _container) = start_postgres().await;
    migrations_through(BEFORE_STAMP)
        .run(&pool)
        .await
        .expect("migrations up to the stamp must succeed");

    let cqrs = Cqrs::new(PostgresEventStore::new(pool.clone()));
    let ledger = LedgerUrn::new("under-load").unwrap();
    append(&cqrs, &ledger, LedgerCommand::Add { amount: 10.0 }).await;

    // Hold the migration open: `ALTER TABLE` has taken ACCESS EXCLUSIVE on `events` and
    // keeps it until this transaction commits.
    let mut migration = pool.begin().await.expect("beginning the migration");
    migration
        .execute(sqlx::raw_sql(AssertSqlSafe(migrations::sql(STAMP))))
        .await
        .unwrap_or_else(|e| panic!("migration {STAMP} must apply: {e}"));

    // The append blocks on that lock rather than failing.
    let appending = tokio::spawn({
        let cqrs = cqrs.clone();
        let ledger = ledger.clone();
        async move { append(&cqrs, &ledger, LedgerCommand::Add { amount: 5.0 }).await }
    });
    await_blocked_on_events(&pool).await;

    migration.commit().await.expect("committing the migration");
    appending.await.expect("the blocked append must finish");

    let stamps = stamps(&pool).await;
    assert_eq!(stamps.len(), 2, "two events exist: {stamps:?}");
    assert!(
        stamps[1].txid != SENTINEL,
        "an append that ran during the migration is stamped for real: {stamps:?}"
    );
}

/// Ordering by the stamp then the position is served by the index, not by a sort.
#[tokio::test]
async fn ordering_by_transaction_then_position_is_an_index_scan_postgres_test() {
    let (pool, _container) = start_postgres().await;
    MIGRATOR.run(&pool).await.expect("migrations must succeed");

    let cqrs = Cqrs::new(PostgresEventStore::new(pool.clone()));
    let ledger = LedgerUrn::new("planned").unwrap();
    append(&cqrs, &ledger, LedgerCommand::AddTwice { amount: 10.0 }).await;

    let mut conn = pool.acquire().await.expect("a connection for the plan");
    // A handful of rows is cheaper to seq-scan whatever the indexes say, and this test is
    // about which access paths exist, not about what the planner costs them at.
    conn.execute("SET enable_seqscan = off")
        .await
        .expect("discouraging the sequential scan");

    let plan: Vec<String> =
        sqlx::query_scalar("EXPLAIN SELECT id FROM events ORDER BY commit_txid, global_position")
            .fetch_all(&mut *conn)
            .await
            .expect("explaining must succeed");
    let plan = plan.join("\n");

    assert!(
        plan.contains("idx_events_commit_txid_position"),
        "the plan reads the stamp index:\n{plan}"
    );
    assert!(
        !plan.contains("Sort"),
        "the index scan already returns commit order, so nothing is sorted:\n{plan}"
    );
}

/// Wait until something is queued behind a lock on `events`.
async fn await_blocked_on_events(pool: &PgPool) {
    let deadline = Instant::now() + BLOCKED_TIMEOUT;
    loop {
        let blocked: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_locks l \
               JOIN pg_class c ON c.oid = l.relation \
              WHERE c.relname = 'events' AND NOT l.granted",
        )
        .fetch_one(pool)
        .await
        .expect("reading pg_locks must succeed");

        if blocked > 0 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the append never blocked on the migration's lock"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
