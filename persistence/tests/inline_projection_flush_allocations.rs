//! Allocation regression test for the inline-projection flush.
//!
//! [`inline_projection_chunking`](./inline_projection_chunking.rs) asserts the store hands a
//! projection no more than the configured chunk. That is a bound on the *buffer*, and it
//! cannot see a second structure quietly growing with the append. This test measures what
//! actually matters — bytes held live at once — through the same `#[global_allocator]`
//! harness as `read_path_allocations.rs`.
//!
//! Each event carries a fat payload, so the retained JSON dominates every other
//! allocation on the thread; an append of `EVENTS` events with a flush size of `FLUSH`
//! must peak near `FLUSH` payloads, not `EVENTS` of them.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use sqlx::{postgres::PgPoolOptions, PgPool};
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};

use replay_persistence::{EventStore, InlineProjection, PersistedEvent};

const POSTGRES_PORT: u16 = 5432;

/// Tracks bytes live *on the calling thread* — allocated minus freed — and the high-water
/// mark of that figure. `#[tokio::test]` runs a current-thread runtime, so the whole
/// append happens on this thread and nothing the harness does elsewhere is counted.
struct PeakAllocator;

thread_local! {
    static LIVE: Cell<isize> = const { Cell::new(0) };
    static PEAK: Cell<isize> = const { Cell::new(0) };
}

fn grow(bytes: usize) {
    let _ = LIVE.try_with(|live| {
        let now = live.get() + bytes as isize;
        live.set(now);
        let _ = PEAK.try_with(|peak| {
            if now > peak.get() {
                peak.set(now);
            }
        });
    });
}

fn shrink(bytes: usize) {
    let _ = LIVE.try_with(|live| live.set(live.get() - bytes as isize));
}

unsafe impl GlobalAlloc for PeakAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        grow(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        shrink(layout.size());
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if new_size >= layout.size() {
            grow(new_size - layout.size());
        } else {
            shrink(layout.size() - new_size);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        grow(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
}

#[global_allocator]
static ALLOCATOR: PeakAllocator = PeakAllocator;

fn reset_peak() {
    LIVE.with(|live| live.set(0));
    PEAK.with(|peak| peak.set(0));
}

fn peak_bytes() -> isize {
    PEAK.with(|peak| peak.get())
}

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
/// registering it makes the store retain them.
struct NoOpProjection;

impl InlineProjection for NoOpProjection {
    type Exec = sqlx::PgConnection;
    type Event = FatEvent;

    fn name(&self) -> &str {
        "noop_allocation_projection"
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
        .max_connections(2)
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

/// Peak live bytes during a streamed append must scale with the flush size, not with
/// the number of events appended.
#[tokio::test]
async fn peak_live_bytes_scale_with_the_flush_size_not_the_append_postgres_test() {
    const EVENTS: usize = 400;
    const FLUSH: usize = 20;

    let (pool, _container) = start_postgres().await;

    let store = replay_persistence::PostgresEventStore::builder(pool.clone())
        .projection_flush_size(FLUSH)
        .register(NoOpProjection)
        .build()
        .await
        .expect("build store");

    let stream_id = FatUrn(urn::UrnBuilder::new("fat", "alloc-1").build().unwrap());
    let events = futures::stream::iter((0..EVENTS).map(|i| {
        Ok(FatEvent::Recorded {
            blob: format!("{i:0width$}", width = PAYLOAD_BYTES),
        })
    }));

    reset_peak();

    store
        .store_events_stream::<FatStream, _, _>(
            &stream_id,
            "FatStream".to_string(),
            replay::Metadata::default(),
            events,
            None,
            |_: &PersistedEvent<FatEvent>| {},
        )
        .await
        .expect("streamed append must succeed");

    let peak = peak_bytes();

    // The retained JSON is the dominant term. Budget six payloads per buffered event —
    // the `Value` tree, the serialized row parameter, and slack for sqlx's per-row work
    // on the same thread. Measured on this fixture: ~204 KB with chunking, ~3.96 MB
    // without, against a ~492 KB budget.
    let bounded = (FLUSH * PAYLOAD_BYTES * 6) as isize;
    let whole_append = (EVENTS * PAYLOAD_BYTES) as isize;

    assert!(
        peak < bounded,
        "peak live bytes {peak} must stay near the flush size (budget {bounded}); \
         retaining the whole append would cost at least {whole_append}"
    );
}
