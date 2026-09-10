//! Chunked inline-projection flush: a streamed append must stay bounded in memory
//! when inline projections are registered.
//!
//! `store_events_stream` consumes its producer one event at a time, but it used to
//! retain every appended event as a `serde_json::Value` until `commit()` so the
//! projections could be applied once at the end — the only term in the append path
//! that grew with batch size. It now flushes projections in bounded chunks inside
//! the same transaction. These tests pin the bound, the atomicity that survives it,
//! and the delivery contract that changes because of it.

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
}

impl InlineProjection for RecordingProjection {
    type Exec = sqlx::PgConnection;
    type Event = LedgerEvent;

    fn name(&self) -> &str {
        "recording_projection"
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
        Ok(())
    }
}

/// The store must never hand a projection more than the configured chunk, however
/// large the append — and the chunks must reassemble into the original sequence.
#[tokio::test]
async fn streamed_append_flushes_inline_projections_in_bounded_chunks_postgres_test() {
    let (pool, _container) = start_postgres().await;

    let log = CallLog::default();
    let store = replay_persistence::PostgresEventStore::builder(pool.clone())
        .projection_flush_size(10)
        .register(RecordingProjection { log: log.clone() })
        .build()
        .await
        .expect("build store");

    let stream_id = LedgerUrn::new("chunked-1").unwrap();
    let events =
        futures::stream::iter((0..35).map(|i| Ok(LedgerEvent::Added { amount: i as f64 })));

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
        .expect("streamed append must succeed");

    let sizes = log.batch_sizes();

    assert!(
        sizes.iter().all(|&n| n <= 10),
        "no chunk may exceed the configured flush size: {sizes:?}"
    );
    assert_eq!(
        sizes,
        vec![10, 10, 10, 5],
        "35 events at a flush size of 10 arrive as four chunks"
    );
    assert_eq!(
        log.all_amounts(),
        (0..35).map(|i| i as f64).collect::<Vec<_>>(),
        "chunking must not change which events arrive, or their order"
    );
}

/// Below the flush size, nothing changes: one append, one call.
#[tokio::test]
async fn append_smaller_than_the_chunk_arrives_in_one_call_postgres_test() {
    let (pool, _container) = start_postgres().await;

    let log = CallLog::default();
    let store = replay_persistence::PostgresEventStore::builder(pool.clone())
        .projection_flush_size(10)
        .register(RecordingProjection { log: log.clone() })
        .build()
        .await
        .expect("build store");

    let stream_id = LedgerUrn::new("chunked-small-1").unwrap();
    let events = futures::stream::iter((0..3).map(|i| Ok(LedgerEvent::Added { amount: i as f64 })));

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
        .expect("streamed append must succeed");

    assert_eq!(log.batch_sizes(), vec![3]);
}

/// An inline projection that writes every event to its own table, then fails on the
/// first event *after* the `fail_after`-th — i.e. part-way through a later chunk, after
/// earlier chunks have written.
struct FailsPartWayProjection {
    seen: usize,
    fail_after: usize,
    calls: Arc<Mutex<usize>>,
}

impl InlineProjection for FailsPartWayProjection {
    type Exec = sqlx::PgConnection;
    type Event = LedgerEvent;

    fn name(&self) -> &str {
        "fails_part_way_projection"
    }

    fn version(&self) -> i32 {
        1
    }

    async fn init(&mut self, conn: &mut Self::Exec) -> Result<(), replay::Error> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS chunked_projection_writes (
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

            sqlx::query(
                "INSERT INTO chunked_projection_writes (stream_id, amount) VALUES ($1, $2)",
            )
            .bind(event.stream_id.to_string())
            .bind(*amount)
            .execute(&mut *conn)
            .await
            .map_err(replay_persistence::db_error)?;

            self.seen += 1;
            if self.seen > self.fail_after {
                return Err(replay::Error::internal(
                    "projection failure in a later chunk (rollback test)",
                ));
            }
        }

        Ok(())
    }
}

/// Chunked flushing must not weaken atomicity: a failure in a later chunk rolls back
/// the writes of every earlier chunk, along with the events themselves.
#[tokio::test]
async fn failure_in_a_later_chunk_rolls_back_earlier_chunks_postgres_test() {
    let (pool, _container) = start_postgres().await;

    let calls = Arc::new(Mutex::new(0usize));
    let store = replay_persistence::PostgresEventStore::builder(pool.clone())
        .projection_flush_size(5)
        .register(FailsPartWayProjection {
            seen: 0,
            fail_after: 12,
            calls: calls.clone(),
        })
        .build()
        .await
        .expect("build store");

    let stream_id = LedgerUrn::new("chunked-rollback-1").unwrap();
    let stream_id_str = Into::<Urn>::into(stream_id.clone()).to_string();
    let events =
        futures::stream::iter((0..20).map(|i| Ok(LedgerEvent::Added { amount: i as f64 })));

    let result = store
        .store_events_stream::<Ledger, _, _>(
            &stream_id,
            "Ledger".to_string(),
            replay::Metadata::default(),
            events,
            None,
            |_: &PersistedEvent<LedgerEvent>| {},
        )
        .await;

    assert!(result.is_err(), "a failing chunk must fail the append");

    // The point of the test: chunks 1 and 2 committed writes to the transaction
    // before chunk 3 failed. Without chunking there is only ever one call and the
    // assertions below hold vacuously.
    let calls_made = *calls.lock().unwrap();
    assert!(
        calls_made >= 2,
        "the failure must land after earlier chunks had already written, got {calls_made} call(s)"
    );

    let projection_rows: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM chunked_projection_writes WHERE stream_id = $1")
            .bind(&stream_id_str)
            .fetch_one(&pool)
            .await
            .expect("counting projection rows must succeed");

    assert_eq!(
        projection_rows, 0,
        "writes from chunks that succeeded before the failure must roll back too"
    );

    let event_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE stream_id = $1")
        .bind(&stream_id_str)
        .fetch_one(&pool)
        .await
        .expect("counting event rows must succeed");

    assert_eq!(event_rows, 0, "no events may commit either");
}

/// The bound must hold for an append far larger than the chunk — the case the whole
/// ticket exists for. Peak retention is measured in events, which is exactly what the
/// store holds: the buffer is cleared after every flush, so the largest batch a
/// projection sees *is* the buffer's high-water mark.
#[tokio::test]
async fn peak_retention_stays_bounded_for_a_large_append_postgres_test() {
    let (pool, _container) = start_postgres().await;

    const APPEND: usize = 2_000;
    const FLUSH: usize = 64;

    let log = CallLog::default();
    let store = replay_persistence::PostgresEventStore::builder(pool.clone())
        .projection_flush_size(FLUSH)
        .register(RecordingProjection { log: log.clone() })
        .build()
        .await
        .expect("build store");

    let stream_id = LedgerUrn::new("chunked-large-1").unwrap();
    let events =
        futures::stream::iter((0..APPEND).map(|i| Ok(LedgerEvent::Added { amount: i as f64 })));

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
        .expect("large streamed append must succeed");

    let sizes = log.batch_sizes();
    let peak = sizes.iter().copied().max().unwrap_or(0);

    assert!(
        peak <= FLUSH,
        "peak retained events must not exceed the flush size: {peak} > {FLUSH}"
    );
    assert_eq!(
        sizes.iter().sum::<usize>(),
        APPEND,
        "every appended event must still reach the projection exactly once"
    );
    assert_eq!(
        sizes.len(),
        APPEND.div_ceil(FLUSH),
        "an append of {APPEND} at a flush size of {FLUSH} arrives as ceil(B/N) calls"
    );
}
