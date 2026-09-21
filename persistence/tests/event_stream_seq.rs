//! Every event carries its place in its stream (funkode-io/replay#224).
//!
//! `version` cannot be that place: compaction renumbers a stream's live events from 1,
//! so `(stream_id, version)` names two different events over a stream's lifetime.
//! `stream_seq` never resets, so it names one event for good — which is what
//! funkode-io/replay#195 needs to ask "have I got everything for this stream?".
//!
//! The sequence is data only: nothing reads it yet, so these tests assert against the
//! column rather than through any behaviour.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use sqlx::{postgres::PgPoolOptions, AssertSqlSafe, Executor, PgPool, Row};
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};

use replay_macros::define_aggregate;
use replay_persistence::{Cqrs, PostgresEventStore};

mod common;
use common::migrations::{self, through as migrations_through, MIGRATOR};
use common::postgres_image::postgres_container;

const POSTGRES_PORT: u16 = 5432;

/// Upper bound on the rows any observation reads. These tests write a handful of events;
/// a query that returned more is not observing what it thinks it is.
const OBSERVATION_LIMIT: i64 = 1_000;

/// The migration that adds the sequence, and the one before it: the tests that stage a
/// pre-sequence database migrate up to `BEFORE_SEQUENCE` and then run the rest.
const BEFORE_SEQUENCE: i64 = 26;
const SEQUENCE: i64 = 27;

/// How many appends race for one stream. Enough that they overlap on a pool of ten
/// connections; the test is about contention, not about volume.
const RACERS: u16 = 8;

/// How long a test waits for an append to become visibly blocked on the migration's lock.
const BLOCKED_TIMEOUT: Duration = Duration::from_secs(10);

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
            // the sequence distinguishable from "one number per transaction".
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
        .max_connections(10)
        .connect(&format!(
            "postgres://postgres:postgres@{host}:{port}/postgres"
        ))
        .await
        .expect("Failed to connect to Postgres");

    (pool, container)
}

/// One event as this ticket sees it.
#[derive(Debug)]
struct Place {
    stream_id: String,
    position: i64,
    /// The replay axis: restarts at 1 when the stream is compacted.
    version: i64,
    /// The delivery axis: never restarts.
    stream_seq: i64,
    snapshot: bool,
    archived: bool,
}

/// Every event, in `global_position` order.
async fn places(pool: &PgPool) -> Vec<Place> {
    sqlx::query(
        "SELECT stream_id, global_position, version, stream_seq, compacted_snapshot, \
                aggregate_version \
           FROM events ORDER BY global_position LIMIT $1",
    )
    .bind(OBSERVATION_LIMIT)
    .fetch_all(pool)
    .await
    .expect("reading the sequence must succeed")
    .into_iter()
    .map(|row| Place {
        stream_id: row.get("stream_id"),
        position: row.get("global_position"),
        version: row.get("version"),
        stream_seq: row.get("stream_seq"),
        snapshot: row.get("compacted_snapshot"),
        archived: row.get::<Option<i32>, _>("aggregate_version").is_some(),
    })
    .collect()
}

/// The sequence numbers each stream's events carry, in `global_position` order.
async fn sequences(pool: &PgPool) -> BTreeMap<String, Vec<i64>> {
    places(pool)
        .await
        .into_iter()
        .fold(BTreeMap::new(), |mut by_stream, place| {
            by_stream
                .entry(place.stream_id)
                .or_insert_with(Vec::new)
                .push(place.stream_seq);
            by_stream
        })
}

/// Compact `ledger` the way `compact` did before the sequence existed: archive the live
/// events in place and write one snapshot row whose `version` restarts at 1. The store's
/// own `compact` cannot be used to stage this — it writes through `write_event`, which
/// this database is too old to have.
async fn compact_as_the_old_schema_did(pool: &PgPool, ledger: &LedgerUrn) {
    sqlx::query(
        "UPDATE events SET aggregate_version = 1 \
          WHERE stream_id = $1 AND aggregate_version IS NULL",
    )
    .bind(ledger.to_string())
    .execute(pool)
    .await
    .expect("archiving the originals must succeed");

    let snapshot = serde_json::to_value(LedgerEvent::Added { amount: 20.0 })
        .expect("the snapshot event serialises");
    sqlx::query(
        "INSERT INTO events \
           (id, data, metadata, stream_id, type, version, aggregate_version, compacted_snapshot) \
         VALUES (gen_random_uuid(), $2, '{}'::jsonb, $1, 'Added', 1, NULL, TRUE)",
    )
    .bind(ledger.to_string())
    .bind(&snapshot)
    .execute(pool)
    .await
    .expect("writing the snapshot row must succeed");

    sqlx::query("UPDATE streams SET version = 1, last_compacted_version = 1 WHERE id = $1")
        .bind(ledger.to_string())
        .execute(pool)
        .await
        .expect("settling the stream must succeed");
}

/// Append `command` to `ledger`, through the store.
async fn append(cqrs: &Cqrs<PostgresEventStore>, ledger: &LedgerUrn, command: LedgerCommand) {
    cqrs.execute::<Ledger>(ledger, replay::Metadata::default(), command, &(), None)
        .await
        .expect("appending must succeed");
}

// ── Tests ────────────────────────────────────────────────────────────────────

/// The sequence counts a stream's own events, from 1, and counts each stream separately.
#[tokio::test]
async fn an_appended_event_carries_its_place_in_its_own_stream_postgres_test() {
    let (pool, _container) = start_postgres().await;
    MIGRATOR.run(&pool).await.expect("migrations must succeed");

    let cqrs = Cqrs::new(PostgresEventStore::new(pool.clone()));
    let counted = LedgerUrn::new("counted").unwrap();
    let other = LedgerUrn::new("other").unwrap();

    append(&cqrs, &counted, LedgerCommand::AddTwice { amount: 10.0 }).await;
    append(&cqrs, &other, LedgerCommand::Add { amount: 1.0 }).await;
    append(&cqrs, &counted, LedgerCommand::Add { amount: 5.0 }).await;

    let sequences = sequences(&pool).await;
    assert_eq!(
        sequences.values().cloned().collect::<Vec<_>>(),
        vec![vec![1, 2, 3], vec![1]],
        "each stream counts its own events from 1, in the order they were written: \
         {sequences:?}"
    );
}

/// Compaction restarts `version` so hydration still reads `1..N`, and does not restart
/// the sequence: the snapshot rows it writes continue it, and the next append continues
/// from there.
#[tokio::test]
async fn compaction_restarts_the_version_and_continues_the_sequence_postgres_test() {
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

    append(&cqrs, &ledger, LedgerCommand::Add { amount: 1.0 }).await;

    let places = places(&pool).await;
    assert_eq!(
        places.iter().map(|p| p.stream_seq).collect::<Vec<_>>(),
        vec![1, 2, 3, 4, 5],
        "the sequence runs unbroken across the compaction: {places:?}"
    );
    assert_eq!(
        places.iter().map(|p| p.version).collect::<Vec<_>>(),
        vec![1, 2, 3, 1, 2],
        "the version restarts at the snapshot, so hydration still reads 1..N: {places:?}"
    );
    assert!(
        places[..3].iter().all(|p| p.archived) && !places[3].archived,
        "the originals were archived and the snapshot is the live stream: {places:?}"
    );
    assert!(
        places[3].snapshot && !places[4].snapshot,
        "row 4 is the snapshot and row 5 is a real append: {places:?}"
    );
}

/// A log that predates the migration is numbered by it, in the order it was written —
/// including a stream whose `version` already restarted at a compaction — and the
/// counter picks up from there.
#[tokio::test]
async fn events_written_before_the_migration_are_numbered_in_order_postgres_test() {
    let (pool, _container) = start_postgres().await;
    migrations_through(BEFORE_SEQUENCE)
        .run(&pool)
        .await
        .expect("migrations up to the sequence must succeed");

    let cqrs = Cqrs::new(PostgresEventStore::new(pool.clone()));
    let compacted = LedgerUrn::new("already-compacted").unwrap();
    let plain = LedgerUrn::new("never-compacted").unwrap();

    append(&cqrs, &compacted, LedgerCommand::AddTwice { amount: 10.0 }).await;
    append(&cqrs, &plain, LedgerCommand::Add { amount: 1.0 }).await;
    compact_as_the_old_schema_did(&pool, &compacted).await;

    MIGRATOR
        .run(&pool)
        .await
        .expect("the rest of the migrations must succeed");

    let backfilled = sequences(&pool).await;
    assert_eq!(
        backfilled.values().cloned().collect::<Vec<_>>(),
        vec![vec![1, 2, 3], vec![1]],
        "the backfill numbers each stream in the order it was written, counting the \
         snapshot row that follows the two it archived: {backfilled:?}"
    );

    append(&cqrs, &compacted, LedgerCommand::Add { amount: 5.0 }).await;
    append(&cqrs, &plain, LedgerCommand::Add { amount: 5.0 }).await;

    let after = sequences(&pool).await;
    assert_eq!(
        after.values().cloned().collect::<Vec<_>>(),
        vec![vec![1, 2, 3, 4], vec![1, 2]],
        "the counter continues from what the backfill left, rather than from zero: \
         {after:?}"
    );
}

/// A place holds one event. The unique index is what makes that true of the stored data
/// rather than only of the code that writes it — funkode-io/replay#195's completeness
/// check reads it as a fact, and a write path that ever got it wrong would fail loudly
/// here instead of silently renumbering a stream.
#[tokio::test]
async fn two_events_of_a_stream_cannot_share_a_place_postgres_test() {
    let (pool, _container) = start_postgres().await;
    MIGRATOR.run(&pool).await.expect("migrations must succeed");

    let cqrs = Cqrs::new(PostgresEventStore::new(pool.clone()));
    let ledger = LedgerUrn::new("crowded").unwrap();
    append(&cqrs, &ledger, LedgerCommand::AddTwice { amount: 10.0 }).await;

    let moved = sqlx::query("UPDATE events SET stream_seq = 1 WHERE stream_seq = 2")
        .execute(&pool)
        .await;

    assert!(
        moved.is_err(),
        "moving an event onto a taken place is rejected, not accepted: {moved:?}"
    );
    let sequences = sequences(&pool).await;
    assert_eq!(
        sequences.values().cloned().collect::<Vec<_>>(),
        vec![vec![1, 2]],
        "and the stream still counts its events once each: {sequences:?}"
    );
}

/// Appends race for one stream. They serialise on the streams-row lock `append_event`
/// takes, so the places they are given are contiguous and the order they commit in is
/// the order they were numbered — the property funkode-io/replay#195 reads as
/// "a stream has no holes".
#[tokio::test]
async fn concurrent_appends_to_one_stream_leave_no_hole_postgres_test() {
    let (pool, _container) = start_postgres().await;
    MIGRATOR.run(&pool).await.expect("migrations must succeed");

    let cqrs = Cqrs::new(PostgresEventStore::new(pool.clone()));
    let ledger = LedgerUrn::new("contended").unwrap();
    // Create the stream first: racing to create it is a different question (the primary
    // key decides it), and not this one.
    append(&cqrs, &ledger, LedgerCommand::Add { amount: 1.0 }).await;

    let racers: Vec<_> = (0..RACERS)
        .map(|_| {
            let cqrs = cqrs.clone();
            let ledger = ledger.clone();
            tokio::spawn(
                async move { append(&cqrs, &ledger, LedgerCommand::Add { amount: 1.0 }).await },
            )
        })
        .collect();
    for racer in racers {
        racer.await.expect("every append must finish");
    }

    let places = places(&pool).await;
    assert_eq!(
        places.iter().map(|p| p.stream_seq).collect::<Vec<_>>(),
        (1..=i64::from(RACERS) + 1).collect::<Vec<_>>(),
        "the places are contiguous: {places:?}"
    );
    assert!(
        places
            .windows(2)
            .all(|pair| pair[0].position < pair[1].position),
        "and in the order the events were written, so a stream's two axes cannot \
         disagree: {places:?}"
    );
}

/// There is one way into the log, and a write path that forgets it fails loudly. The
/// column has no default, so an insert that names no place is rejected outright rather
/// than taking one that is held or leaving a hole behind it — which is what lets
/// `append_event` be the only writer without a trigger standing over the table.
#[tokio::test]
async fn an_insert_that_names_no_place_is_rejected_postgres_test() {
    let (pool, _container) = start_postgres().await;
    MIGRATOR.run(&pool).await.expect("migrations must succeed");

    let cqrs = Cqrs::new(PostgresEventStore::new(pool.clone()));
    let ledger = LedgerUrn::new("hand-written").unwrap();
    append(&cqrs, &ledger, LedgerCommand::AddTwice { amount: 10.0 }).await;

    let placeless = sqlx::query(
        "INSERT INTO events (id, data, metadata, stream_id, type, version) \
         VALUES (gen_random_uuid(), '{}'::jsonb, '{}'::jsonb, $1, 'Added', 99)",
    )
    .bind(ledger.to_string())
    .execute(&pool)
    .await;

    assert!(
        placeless.is_err(),
        "an event written without a place is rejected: {placeless:?}"
    );
    let sequences = sequences(&pool).await;
    assert_eq!(
        sequences.values().cloned().collect::<Vec<_>>(),
        vec![vec![1, 2]],
        "and the stream is left as it was: {sequences:?}"
    );
}

/// The migration takes `events` for its whole duration, so an append that starts while it
/// runs waits rather than failing — and is numbered after the rows the backfill numbered,
/// not alongside them.
#[tokio::test]
async fn an_append_blocked_by_the_migration_is_given_the_next_place_postgres_test() {
    let (pool, _container) = start_postgres().await;
    migrations_through(BEFORE_SEQUENCE)
        .run(&pool)
        .await
        .expect("migrations up to the sequence must succeed");

    let cqrs = Cqrs::new(PostgresEventStore::new(pool.clone()));
    let ledger = LedgerUrn::new("under-load").unwrap();
    append(&cqrs, &ledger, LedgerCommand::AddTwice { amount: 10.0 }).await;

    // Hold the migration open: it has taken ACCESS EXCLUSIVE on `events` and keeps it
    // until this transaction commits.
    let mut migration = pool.begin().await.expect("beginning the migration");
    migration
        .execute(sqlx::raw_sql(AssertSqlSafe(migrations::sql(SEQUENCE))))
        .await
        .unwrap_or_else(|e| panic!("migration {SEQUENCE} must apply: {e}"));

    let appending = tokio::spawn({
        let cqrs = cqrs.clone();
        let ledger = ledger.clone();
        async move { append(&cqrs, &ledger, LedgerCommand::Add { amount: 5.0 }).await }
    });
    await_blocked_on_events(&pool).await;

    migration.commit().await.expect("committing the migration");
    appending.await.expect("the blocked append must finish");

    let sequences = sequences(&pool).await;
    assert_eq!(
        sequences.values().cloned().collect::<Vec<_>>(),
        vec![vec![1, 2, 3]],
        "the append that waited took the place after the two the backfill numbered: \
         {sequences:?}"
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
