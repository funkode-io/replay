//! Chunked rebuild replay: registering or bumping an inline projection must stay
//! bounded in memory, however long the matching history is.
//!
//! [`inline_projection_chunking`](./inline_projection_chunking.rs) pins the same bound on
//! the *append* path. The rebuild path is the other feeder of `handle`: it used to load
//! all matching history with one `fetch_all` and call `handle` once, so the memory it
//! needed grew with the log — inside `build()`, i.e. at service startup. It now pages
//! through the history with a keyset cursor, inside the same transaction, flushing every
//! `projection_flush_size` events. These tests pin the bound, the delivery contract, and
//! the atomicity that must survive both.

use std::sync::{Arc, Mutex};

use sqlx::{postgres::PgPoolOptions, PgPool};
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};
use urn::Urn;

use replay_macros::define_aggregate;
use replay_persistence::{EventStore, InlineProjection, PersistedEvent};

const POSTGRES_PORT: u16 = 5432;

define_aggregate! {
    Ledger {
        namespace: "ledger",
        state: {
            balance: f64,
        },
        commands: {
            Add { amount: f64 },
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
        let LedgerCommand::Add { amount } = command;
        Ok(vec![LedgerEvent::Added { amount }])
    }
}

async fn start_postgres() -> (
    PgPool,
    testcontainers_modules::testcontainers::ContainerAsync<postgres::Postgres>,
) {
    let container = postgres::Postgres::default().start().await.unwrap();
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

    sqlx::migrate!("./tests/migrations")
        .run(&pool)
        .await
        .expect("Failed to run migrations");

    (pool, container)
}

/// Seed `count` events on one stream through a store with **no** projections
/// registered, so the backlog exists before any projection ever sees it.
async fn seed_history(pool: &PgPool, stream: &str, count: usize) -> LedgerUrn {
    let store = replay_persistence::PostgresEventStore::new(pool.clone());
    let stream_id = LedgerUrn::new(stream).unwrap();
    let events =
        futures::stream::iter((0..count).map(|i| Ok(LedgerEvent::Added { amount: i as f64 })));

    store
        .store_events_stream::<Ledger, _, _>(
            &stream_id,
            "Ledger".to_string(),
            replay::Metadata::default(),
            events,
            None,
            |_: &PersistedEvent<LedgerEvent>| {},
        )
        .await
        .expect("seeding history must succeed");

    stream_id
}

/// An inline projection that records the length and contents of every `handle` call.
#[derive(Clone, Default)]
struct CallLog {
    batches: Arc<Mutex<Vec<Vec<f64>>>>,
}

impl CallLog {
    fn batch_sizes(&self) -> Vec<usize> {
        self.batches
            .lock()
            .unwrap()
            .iter()
            .map(|b| b.len())
            .collect()
    }

    fn all_amounts(&self) -> Vec<f64> {
        self.batches
            .lock()
            .unwrap()
            .iter()
            .flatten()
            .copied()
            .collect()
    }
}

struct RecordingProjection {
    log: CallLog,
    version: i32,
}

impl InlineProjection for RecordingProjection {
    type Exec = sqlx::PgConnection;
    type Event = LedgerEvent;

    fn name(&self) -> &str {
        "rebuild_recording_projection"
    }

    fn version(&self) -> i32 {
        self.version
    }

    async fn init(&mut self, _conn: &mut Self::Exec) -> Result<(), replay::Error> {
        Ok(())
    }

    async fn handle(
        &mut self,
        _conn: &mut Self::Exec,
        events: &[PersistedEvent<Self::Event>],
    ) -> Result<(), replay::Error> {
        let batch = events
            .iter()
            .map(|e| match &e.data {
                LedgerEvent::Added { amount } => *amount,
            })
            .collect();
        self.log.batches.lock().unwrap().push(batch);
        Ok(())
    }
}

/// A first-time registration replays the existing backlog — and must do it in chunks
/// no larger than the configured flush size, reassembling into the original history.
#[tokio::test]
async fn first_registration_replays_history_in_bounded_chunks_postgres_test() {
    const HISTORY: usize = 35;
    const FLUSH: usize = 10;

    let (pool, _container) = start_postgres().await;
    seed_history(&pool, "rebuild-chunked-1", HISTORY).await;

    let log = CallLog::default();
    let _store = replay_persistence::PostgresEventStore::builder(pool.clone())
        .projection_flush_size(FLUSH)
        .register(RecordingProjection {
            log: log.clone(),
            version: 1,
        })
        .build()
        .await
        .expect("build store");

    let sizes = log.batch_sizes();

    assert!(
        sizes.iter().all(|&n| n <= FLUSH),
        "no replay chunk may exceed the configured flush size: {sizes:?}"
    );
    assert_eq!(
        sizes,
        vec![10, 10, 10, 5],
        "a history of {HISTORY} at a flush size of {FLUSH} replays as four chunks"
    );
    assert_eq!(
        log.all_amounts(),
        (0..HISTORY).map(|i| i as f64).collect::<Vec<_>>(),
        "chunking must not change which events are replayed, or their order"
    );
}

/// A version-drift rebuild takes the same path, and must be chunked the same way.
#[tokio::test]
async fn version_drift_rebuild_replays_history_in_bounded_chunks_postgres_test() {
    const HISTORY: usize = 25;
    const FLUSH: usize = 7;

    let (pool, _container) = start_postgres().await;
    seed_history(&pool, "rebuild-chunked-drift-1", HISTORY).await;

    // Version 1 registers and replays the backlog; the log of that run is discarded.
    let _v1 = replay_persistence::PostgresEventStore::builder(pool.clone())
        .projection_flush_size(FLUSH)
        .register(RecordingProjection {
            log: CallLog::default(),
            version: 1,
        })
        .build()
        .await
        .expect("build store at version 1");

    // Version 2 drifts: reset, then replay the same history again.
    let log = CallLog::default();
    let _v2 = replay_persistence::PostgresEventStore::builder(pool.clone())
        .projection_flush_size(FLUSH)
        .register(RecordingProjection {
            log: log.clone(),
            version: 2,
        })
        .build()
        .await
        .expect("build store at version 2");

    let sizes = log.batch_sizes();

    assert!(
        sizes.iter().all(|&n| n <= FLUSH),
        "no rebuild chunk may exceed the configured flush size: {sizes:?}"
    );
    assert_eq!(
        sizes.len(),
        HISTORY.div_ceil(FLUSH),
        "a history of {HISTORY} at a flush size of {FLUSH} rebuilds as ceil(H/N) calls"
    );
    assert_eq!(
        log.all_amounts(),
        (0..HISTORY).map(|i| i as f64).collect::<Vec<_>>(),
        "every event must reach handle exactly once, in order"
    );
}

/// A history shorter than the chunk still arrives in a single call, as it always did.
#[tokio::test]
async fn history_smaller_than_the_chunk_replays_in_one_call_postgres_test() {
    let (pool, _container) = start_postgres().await;
    seed_history(&pool, "rebuild-chunked-small-1", 3).await;

    let log = CallLog::default();
    let _store = replay_persistence::PostgresEventStore::builder(pool.clone())
        .projection_flush_size(10)
        .register(RecordingProjection {
            log: log.clone(),
            version: 1,
        })
        .build()
        .await
        .expect("build store");

    assert_eq!(log.batch_sizes(), vec![3]);
}

/// Paging with a keyset cursor is only safe if the key is a **total** order. Events
/// appended by separate transactions can share a `created` timestamp, and versions
/// restart per stream, so `(created, version)` alone is not unique — a `>` cursor on it
/// would silently skip every row after the first of a tied group. The tie-break must
/// keep them all.
#[tokio::test]
async fn replay_keeps_events_that_share_created_and_version_postgres_test() {
    let (pool, _container) = start_postgres().await;

    // Ten streams, one event each, all with the same `created` and `version` — the
    // shape produced by concurrent single-event appends landing in the same instant.
    let created = chrono::DateTime::parse_from_rfc3339("2025-01-01T00:00:00Z")
        .unwrap()
        .with_timezone(&chrono::Utc);

    for i in 0..10 {
        let stream_id: Urn = Into::<Urn>::into(LedgerUrn::new(format!("tied-{i}")).unwrap());
        let stream_id = stream_id.to_string();

        sqlx::query("INSERT INTO streams (id, type, version) VALUES ($1, 'Ledger', 1)")
            .bind(&stream_id)
            .execute(&pool)
            .await
            .expect("seeding stream row must succeed");

        sqlx::query(
            "INSERT INTO events (id, data, metadata, stream_id, type, version, created)
             VALUES ($1, $2, '{}', $3, 'Added', 1, $4)",
        )
        .bind(uuid::Uuid::new_v4())
        .bind(serde_json::json!({ "Added": { "amount": i as f64 } }))
        .bind(&stream_id)
        .bind(created)
        .execute(&pool)
        .await
        .expect("seeding event row must succeed");
    }

    let log = CallLog::default();
    let _store = replay_persistence::PostgresEventStore::builder(pool.clone())
        .projection_flush_size(1)
        .register(RecordingProjection {
            log: log.clone(),
            version: 1,
        })
        .build()
        .await
        .expect("build store");

    let mut replayed = log.all_amounts();
    replayed.sort_by(f64::total_cmp);

    assert_eq!(
        replayed,
        (0..10).map(|i| i as f64).collect::<Vec<_>>(),
        "every event must be replayed exactly once even when the sort key ties"
    );
}

/// A projection that appends — and commits — a new event from a **separate** connection
/// the first time it is handed a chunk, i.e. between two pages of the replay.
struct AppendsBetweenPagesProjection {
    pool: PgPool,
    log: CallLog,
    appended: bool,
}

impl InlineProjection for AppendsBetweenPagesProjection {
    type Exec = sqlx::PgConnection;
    type Event = LedgerEvent;

    fn name(&self) -> &str {
        "rebuild_appends_between_pages_projection"
    }

    fn version(&self) -> i32 {
        1
    }

    async fn init(&mut self, _conn: &mut Self::Exec) -> Result<(), replay::Error> {
        Ok(())
    }

    async fn handle(
        &mut self,
        _conn: &mut Self::Exec,
        events: &[PersistedEvent<Self::Event>],
    ) -> Result<(), replay::Error> {
        let batch = events
            .iter()
            .map(|e| match &e.data {
                LedgerEvent::Added { amount } => *amount,
            })
            .collect();
        self.log.batches.lock().unwrap().push(batch);

        if !self.appended {
            self.appended = true;
            // Committed on its own connection, so it is visible to any later snapshot.
            seed_history(&self.pool, "interloper", 1).await;
        }

        Ok(())
    }
}

/// A replay must read one snapshot, not one per page. The rebuild transaction is the
/// unit of atomicity, but on its own that only bounds what it *writes* — under the
/// default READ COMMITTED isolation every page query takes a fresh snapshot, so an
/// append committed mid-rebuild would be picked up by a later page and folded into a
/// replay of history it was never part of.
#[tokio::test]
async fn replay_does_not_see_events_committed_between_pages_postgres_test() {
    const HISTORY: usize = 20;
    const FLUSH: usize = 5;

    let (pool, _container) = start_postgres().await;
    seed_history(&pool, "rebuild-snapshot-1", HISTORY).await;

    let log = CallLog::default();
    let _store = replay_persistence::PostgresEventStore::builder(pool.clone())
        .projection_flush_size(FLUSH)
        .register(AppendsBetweenPagesProjection {
            pool: pool.clone(),
            log: log.clone(),
            appended: false,
        })
        .build()
        .await
        .expect("build store");

    // The interloper is appended after the replay starts, so its `created` sorts past
    // every page's cursor: a per-statement snapshot would hand it to the last page.
    assert_eq!(
        log.all_amounts(),
        (0..HISTORY).map(|i| i as f64).collect::<Vec<_>>(),
        "the replay must see the history its transaction started with, and nothing else"
    );

    // It is still in the log, for whoever reads the store next.
    let stored: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events")
        .fetch_one(&pool)
        .await
        .expect("counting events must succeed");

    assert_eq!(
        stored,
        HISTORY as i64 + 1,
        "the concurrent append itself must have committed"
    );
}

/// A projection that writes each replayed event to its own table and then fails, once
/// it has seen more than `fail_after` events — i.e. part-way through a later chunk.
struct FailsPartWayProjection {
    seen: usize,
    fail_after: usize,
    calls: Arc<Mutex<usize>>,
}

impl InlineProjection for FailsPartWayProjection {
    type Exec = sqlx::PgConnection;
    type Event = LedgerEvent;

    fn name(&self) -> &str {
        "rebuild_fails_part_way_projection"
    }

    fn version(&self) -> i32 {
        1
    }

    async fn init(&mut self, conn: &mut Self::Exec) -> Result<(), replay::Error> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS rebuild_chunk_writes (
                stream_id text NOT NULL,
                amount    double precision NOT NULL
            )",
        )
        .execute(conn)
        .await
        .map_err(replay_persistence::db_error)?;
        Ok(())
    }

    async fn handle(
        &mut self,
        conn: &mut Self::Exec,
        events: &[PersistedEvent<Self::Event>],
    ) -> Result<(), replay::Error> {
        *self.calls.lock().unwrap() += 1;

        for event in events {
            let LedgerEvent::Added { amount } = &event.data;

            sqlx::query("INSERT INTO rebuild_chunk_writes (stream_id, amount) VALUES ($1, $2)")
                .bind(event.stream_id.to_string())
                .bind(*amount)
                .execute(&mut *conn)
                .await
                .map_err(replay_persistence::db_error)?;

            self.seen += 1;
            if self.seen > self.fail_after {
                return Err(replay::Error::internal(
                    "projection failure in a later replay chunk (rollback test)",
                ));
            }
        }

        Ok(())
    }
}

/// Chunked replay must not weaken the rebuild's atomicity: a failure part-way through
/// rolls back the writes of every earlier chunk and leaves no recorded version behind.
#[tokio::test]
async fn failure_in_a_later_replay_chunk_rolls_back_the_whole_rebuild_postgres_test() {
    let (pool, _container) = start_postgres().await;
    let stream_id = seed_history(&pool, "rebuild-chunked-rollback-1", 20).await;
    let stream_id_str = Into::<Urn>::into(stream_id).to_string();

    let calls = Arc::new(Mutex::new(0usize));
    let result = replay_persistence::PostgresEventStore::builder(pool.clone())
        .projection_flush_size(5)
        .register(FailsPartWayProjection {
            seen: 0,
            fail_after: 12,
            calls: calls.clone(),
        })
        .build()
        .await;

    assert!(
        result.is_err(),
        "a failing replay chunk must fail the whole build"
    );
    assert!(
        *calls.lock().unwrap() >= 2,
        "the failure must land after earlier chunks had already written, got {} call(s)",
        calls.lock().unwrap()
    );

    // `init` created the table inside the rolled-back transaction, so its very
    // existence is part of what must have been undone.
    let table_exists: bool =
        sqlx::query_scalar("SELECT to_regclass('rebuild_chunk_writes') IS NOT NULL")
            .fetch_one(&pool)
            .await
            .expect("checking for the view table must succeed");

    if table_exists {
        let rows: i64 =
            sqlx::query_scalar("SELECT COUNT(*) FROM rebuild_chunk_writes WHERE stream_id = $1")
                .bind(&stream_id_str)
                .fetch_one(&pool)
                .await
                .expect("counting projection rows must succeed");

        assert_eq!(
            rows, 0,
            "writes from chunks that succeeded before the failure must roll back too"
        );
    }

    let recorded: Option<i32> =
        sqlx::query_scalar("SELECT version FROM projections WHERE name = $1")
            .bind("rebuild_fails_part_way_projection")
            .fetch_optional(&pool)
            .await
            .expect("reading the projection registry must succeed");

    assert_eq!(
        recorded, None,
        "a half-built view must never be recorded as current"
    );
}
