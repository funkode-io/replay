//! Allocation regression tests for the row → event read seam.
//!
//! `PersistedEvent::<D>::try_from(PgRow)` is the single seam every read goes
//! through (`read_feed`, `fetch_aggregate_at`, `load_event_by_id`, projection
//! replay). It used to clone the decoded `data` document purely to satisfy
//! `serde_json::from_value`, so every row cost twice its payload. These tests pin
//! that down: reading a batch must allocate ≈1× the payload, and the diagnostic
//! context that clone used to feed must still be there when a row fails to
//! deserialize.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use serde::Deserialize;
use serde_json::{json, Value};
use sqlx::{postgres::PgPoolOptions, postgres::PgRow, PgPool};
use testcontainers_modules::{
    postgres,
    testcontainers::{runners::AsyncRunner, ContainerAsync},
};
use uuid::Uuid;

use replay_persistence::PersistedEvent;

const POSTGRES_PORT: u16 = 5432;

/// Counts the bytes allocated *by the calling thread*, so a measurement taken in a
/// synchronous block is unaffected by anything the test harness runs in parallel.
struct CountingAllocator;

thread_local! {
    static ALLOCATED: Cell<usize> = const { Cell::new(0) };
}

fn record(bytes: usize) {
    let _ = ALLOCATED.try_with(|allocated| allocated.set(allocated.get() + bytes));
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        record(new_size.saturating_sub(layout.size()));
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
}

#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// Runs `f` and returns the bytes it allocated on this thread.
fn allocated_bytes<T>(f: impl FnOnce() -> T) -> (T, usize) {
    let before = ALLOCATED.with(Cell::get);
    let value = f();
    let after = ALLOCATED.with(Cell::get);
    (value, after - before)
}

/// The stored shape of the events these tests read back: a few fat text blocks,
/// like the localized Markdown payloads that surfaced the amplification.
#[derive(Debug, Deserialize, PartialEq)]
struct FatEvent {
    reference: String,
    blocks: Vec<String>,
}

const EVENTS: i64 = 100;
const BLOCKS_PER_EVENT: usize = 4;
const BLOCK_BYTES: usize = 8 * 1024;

fn fat_event(version: i64) -> Value {
    let blocks: Vec<String> = (0..BLOCKS_PER_EVENT)
        .map(|block| format!("{}", version).repeat(BLOCK_BYTES / 4 + block))
        .collect();

    json!({ "reference": format!("event-{version}"), "blocks": blocks })
}

async fn start_postgres() -> (ContainerAsync<postgres::Postgres>, PgPool) {
    let container = postgres::Postgres::default().start().await.unwrap();

    let host = container.get_host().await.unwrap().to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("Error getting docker port");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&format!(
            "postgres://postgres:postgres@{}:{}/postgres",
            host, port
        ))
        .await
        .expect("Failed to create postgres pool");

    sqlx::migrate!("./tests/migrations")
        .run(&pool)
        .await
        .expect("Failed to run migrations");

    (container, pool)
}

/// Seeds `events` rows directly so the tests exercise the read seam alone.
async fn seed_events(pool: &PgPool, stream_id: &str, events: impl Iterator<Item = Value>) {
    sqlx::query("INSERT INTO streams (id, type, version) VALUES ($1, $2, 0)")
        .bind(stream_id)
        .bind("urn:fat-event")
        .execute(pool)
        .await
        .expect("failed to insert stream");

    for (index, data) in events.enumerate() {
        sqlx::query(
            "INSERT INTO events (id, data, metadata, stream_id, type, version) \
             VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .bind(Uuid::new_v4())
        .bind(&data)
        .bind(json!({ "tenant": "acme" }))
        .bind(stream_id)
        .bind("FatEvent")
        .bind(index as i64 + 1)
        .execute(pool)
        .await
        .expect("failed to insert event");
    }
}

async fn fetch_event_rows(pool: &PgPool, stream_id: &str) -> Vec<PgRow> {
    sqlx::query(
        "SELECT id, data, metadata, stream_id, type, version, created, aggregate_version \
         FROM events WHERE stream_id = $1 ORDER BY version",
    )
    .bind(stream_id)
    .fetch_all(pool)
    .await
    .expect("failed to fetch events")
}

#[tokio::test]
async fn reading_a_batch_of_events_allocates_once_per_payload() {
    let (_container, pool) = start_postgres().await;

    let stream_id = "urn:fat-event:alloc";
    seed_events(&pool, stream_id, (1..=EVENTS).map(fat_event)).await;

    let rows = fetch_event_rows(&pool, stream_id).await;
    assert_eq!(rows.len() as i64, EVENTS);

    // Baseline: decoding the `data` column of every row into a JSON document once.
    // That single decode is the irreducible cost of reading the batch — the read
    // seam must not allocate a meaningful multiple of it.
    let (_, payload_bytes) = allocated_bytes(|| {
        for row in &rows {
            let data: Value = sqlx::Row::get(row, "data");
            std::hint::black_box(&data);
        }
    });

    let (events, read_bytes) = allocated_bytes(|| {
        rows.into_iter()
            .map(PersistedEvent::<FatEvent>::try_from)
            .collect::<Result<Vec<_>, _>>()
            .expect("failed to read events")
    });

    assert_eq!(events.len() as i64, EVENTS);
    assert_eq!(events[0].data.blocks.len(), BLOCKS_PER_EVENT);

    // 1× the payload, not 2×: a deep copy of the decoded document (the old
    // `data_raw.clone()`) would push this to ~200%.
    let percent_of_payload = read_bytes * 100 / payload_bytes;
    assert!(
        percent_of_payload < 150,
        "reading {EVENTS} events allocated {read_bytes} bytes, {percent_of_payload}% of the \
         {payload_bytes} byte payload; the read path must not copy the payload again"
    );
}

#[tokio::test]
async fn a_row_that_fails_to_deserialize_reports_the_stored_json() {
    let (_container, pool) = start_postgres().await;

    let stream_id = "urn:fat-event:broken";
    seed_events(
        &pool,
        stream_id,
        std::iter::once(json!({ "reference": "event-1", "blocks": "not-a-list" })),
    )
    .await;

    let row = fetch_event_rows(&pool, stream_id)
        .await
        .pop()
        .expect("expected the seeded row");

    let error = PersistedEvent::<FatEvent>::try_from(row).expect_err("expected a deserialize error");

    let stored_json = error
        .context()
        .iter()
        .find(|(key, _)| *key == "stored_json")
        .map(|(_, value)| value)
        .expect("expected the failing error to carry the stored JSON");

    assert!(
        stored_json.contains("not-a-list"),
        "expected the stored JSON context to show the offending document, got: {stored_json}"
    );
}
