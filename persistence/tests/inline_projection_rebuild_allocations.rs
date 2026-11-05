//! Allocation regression test for the inline-projection **rebuild** replay.
//!
//! [`inline_projection_rebuild_chunking`](./inline_projection_rebuild_chunking.rs) asserts
//! the store hands a rebuilding projection no more than the configured chunk. That is a
//! bound on what `handle` *sees*, and it cannot see the rows and JSON trees the loader
//! holds behind it. This test measures what actually matters — bytes held live at once
//! during `build()` — through the shared allocation harness.
//!
//! Each event carries a fat payload, so the retained JSON dominates every other
//! allocation on the thread; replaying a history of `EVENTS` events with a flush size of
//! `FLUSH` must peak near `FLUSH` payloads, not `EVENTS` of them.

use sqlx::{postgres::PgPoolOptions, PgPool};

use replay_persistence::{EventStore, InlineProjection, PersistedEvent};

mod common;
use common::alloc::{peak_live_bytes, reset_peak, CountingAllocator};
use common::postgres_image::postgres_database;

/// Every test binary registers its own global allocator; the counting itself is
/// shared (`tests/common/alloc.rs`).
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

// ── Fixture: an aggregate whose events carry a fat payload ───────────────────

/// Bytes of filler per event. Large enough that the retained JSON dwarfs the
/// per-row bookkeeping sqlx does on the same thread.
const PAYLOAD_BYTES: usize = 4_096;

#[derive(Clone, Debug, PartialEq, serde::Serialize, serde::Deserialize)]
enum FatEvent {
    Recorded { blob: String },
}

impl replay::Event for FatEvent {
    fn event_type(&self) -> String {
        "Recorded".to_string()
    }
}

#[derive(Clone, Debug, Default)]
struct FatStream {
    id: FatUrn,
}

#[derive(Clone, Debug, PartialEq)]
struct FatUrn(urn::Urn);

impl Default for FatUrn {
    fn default() -> Self {
        FatUrn(urn::UrnBuilder::new("fat", "default").build().unwrap())
    }
}

impl From<FatUrn> for urn::Urn {
    fn from(u: FatUrn) -> urn::Urn {
        u.0
    }
}

impl TryFrom<urn::Urn> for FatUrn {
    type Error = String;

    fn try_from(u: urn::Urn) -> Result<Self, String> {
        if u.nid() == "fat" {
            Ok(FatUrn(u))
        } else {
            Err(format!("expected nid 'fat', got '{}'", u.nid()))
        }
    }
}

impl serde::Serialize for FatUrn {
    fn serialize<S: serde::Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(self.0.as_ref())
    }
}

impl<'de> serde::Deserialize<'de> for FatUrn {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        use std::str::FromStr;
        let s = String::deserialize(d)?;
        urn::Urn::from_str(&s)
            .map(FatUrn)
            .map_err(serde::de::Error::custom)
    }
}

impl replay::WithId for FatStream {
    type StreamId = FatUrn;

    fn with_id(id: Self::StreamId) -> Self {
        FatStream { id }
    }

    fn get_id(&self) -> &Self::StreamId {
        &self.id
    }
}

impl replay::EventStream for FatStream {
    type Event = FatEvent;

    fn stream_type() -> String {
        "FatStream".to_string()
    }

    fn apply(&mut self, _event: Self::Event) {}
}

/// An inline projection that does nothing with the events; the point is only that
/// registering it makes the store replay the backlog through it.
struct NoOpProjection;

impl InlineProjection for NoOpProjection {
    type Exec = sqlx::PgConnection;
    type Event = FatEvent;

    fn name(&self) -> &str {
        "noop_rebuild_allocation_projection"
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
        _events: &[PersistedEvent<Self::Event>],
    ) -> Result<(), replay::Error> {
        Ok(())
    }
}

async fn start_pool() -> (PgPool, common::postgres_image::TestDatabase) {
    let container = postgres_database().await;

    let pool = PgPoolOptions::new()
        .max_connections(2)
        .connect(&container.url())
        .await
        .expect("Failed to connect to Postgres");

    sqlx::migrate!("./tests/migrations")
        .run(&pool)
        .await
        .expect("Failed to run migrations");

    (pool, container)
}

/// Peak live bytes during a first-registration rebuild must scale with the flush size,
/// not with the history being replayed.
#[tokio::test]
async fn peak_live_bytes_scale_with_the_flush_size_not_the_history_postgres_test() {
    const EVENTS: usize = 400;
    const FLUSH: usize = 20;

    let (pool, _container) = start_pool().await;

    // Seed the backlog through a store with no projections registered, so nothing is
    // retained on the way in and the whole cost measured below belongs to the replay.
    let store = replay_persistence::PostgresEventStore::new(pool.clone());
    let stream_id = FatUrn(
        urn::UrnBuilder::new("fat", "rebuild-alloc-1")
            .build()
            .unwrap(),
    );
    let events = futures::stream::iter((0..EVENTS).map(|i| {
        Ok(FatEvent::Recorded {
            blob: format!("{i:0width$}", width = PAYLOAD_BYTES),
        })
    }));

    store
        .store_events_stream::<FatStream, _, _, _>(
            &stream_id,
            "FatStream".to_string(),
            replay::Metadata::default(),
            events,
            None,
            |_: &PersistedEvent<FatEvent>| {},
            replay_persistence::OpenGate,
        )
        .await
        .expect("seeding history must succeed");

    drop(store);

    reset_peak();

    let _store = replay_persistence::PostgresEventStore::builder(pool.clone())
        .projection_flush_size(FLUSH)
        .register(NoOpProjection)
        .build()
        .await
        .expect("build store");

    let peak = peak_live_bytes();

    // The loaded JSON is the dominant term. Budget six payloads per event in flight —
    // the `PgRow` bytes, the `Value` tree, the typed copy the erased bridge makes, and
    // slack for sqlx's per-row work on the same thread. Measured on this fixture:
    // ~215 KB with chunked replay, ~3.9 MB with the old `fetch_all`, against a ~492 KB
    // budget.
    let bounded = (FLUSH * PAYLOAD_BYTES * 6) as isize;
    let whole_history = (EVENTS * PAYLOAD_BYTES) as isize;

    assert!(
        peak < bounded,
        "peak live bytes {peak} must stay near the flush size (budget {bounded}); \
         loading the whole history would cost at least {whole_history}"
    );
}

/// The same bound on the catch-up (ADR-0032): applying a gap of `EVENTS` events another
/// process appended must peak near the flush size, not the gap. The gap is read with the
/// rebuild's pager, so the budget is the rebuild's.
#[tokio::test]
async fn peak_live_bytes_of_a_catch_up_scale_with_the_flush_size_not_the_gap_postgres_test() {
    const EVENTS: usize = 400;
    const FLUSH: usize = 20;

    let (pool, _container) = start_pool().await;

    let new_pod = replay_persistence::PostgresEventStore::builder(pool.clone())
        .projection_flush_size(FLUSH)
        .projection_catch_up_interval(std::time::Duration::ZERO)
        .register(NoOpProjection)
        .build()
        .await
        .expect("build store");

    // The gap: appended by a process that has not registered the projection.
    let old_pod = replay_persistence::PostgresEventStore::new(pool.clone());
    let stream_id = FatUrn(
        urn::UrnBuilder::new("fat", "catch-up-alloc-1")
            .build()
            .unwrap(),
    );
    let events = futures::stream::iter((0..EVENTS).map(|i| {
        Ok(FatEvent::Recorded {
            blob: format!("{i:0width$}", width = PAYLOAD_BYTES),
        })
    }));
    old_pod
        .store_events_stream::<FatStream, _, _, _>(
            &stream_id,
            "FatStream".to_string(),
            replay::Metadata::default(),
            events,
            None,
            |_: &PersistedEvent<FatEvent>| {},
            replay_persistence::OpenGate,
        )
        .await
        .expect("appending the gap must succeed");
    drop(old_pod);

    reset_peak();

    let read = new_pod
        .catch_up_projections()
        .await
        .expect("catch-up must succeed");

    let peak = peak_live_bytes();
    assert_eq!(read, EVENTS as u64, "the whole gap is owed");

    let bounded = (FLUSH * PAYLOAD_BYTES * 6) as isize;
    let whole_gap = (EVENTS * PAYLOAD_BYTES) as isize;

    assert!(
        peak < bounded,
        "peak live bytes {peak} must stay near the flush size (budget {bounded}); \
         loading the whole gap would cost at least {whole_gap}"
    );
}
