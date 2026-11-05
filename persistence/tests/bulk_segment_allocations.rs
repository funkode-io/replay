//! Allocation regression test for a bulk execution's segments (funkode-io/replay#303).
//!
//! `Cqrs::execute_bulk` commits its events in segments of `bulk_segment_size`, each one
//! transaction, so what a bulk holds at once is the segment, whatever the size of the
//! bulk. This test pins that in bytes: an inline projection is registered with a flush
//! size larger than either bulk, so the only thing that bounds the events the store
//! retains for it is the segment's commit. A bulk of `BASE` events and one twenty times
//! larger must both peak near `SEGMENT` payloads, and the larger no higher than the
//! smaller plus slack.

use futures::stream::BoxStream;
use futures::StreamExt;
use sqlx::{postgres::PgPoolOptions, PgPool};

use replay_persistence::{Cqrs, InlineProjection, PersistedEvent, PostgresEventStore};

mod common;
use common::alloc::{peak_live_bytes, reset_peak, CountingAllocator};
use common::postgres_image::postgres_database;
use common::report::report;

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
/// registering it makes the store retain them.
struct NoOpProjection;

impl InlineProjection for NoOpProjection {
    type Exec = sqlx::PgConnection;
    type Event = FatEvent;

    fn name(&self) -> &str {
        "noop_bulk_segment_projection"
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

/// A bulk import of `count` fat records, produced one at a time as the store pulls
/// them: the handler never builds a `Vec`, so whatever the bulk retains is the store's.
struct ImportFat {
    count: usize,
}

impl replay::Aggregate for FatStream {
    type Command = ImportFat;
    type Error = replay::Error;
    type Services = ();

    async fn handle_stream(
        &self,
        command: ImportFat,
        _services: &(),
    ) -> Result<BoxStream<'static, Result<FatEvent, replay::Error>>, replay::Error> {
        Ok(futures::stream::iter((0..command.count).map(|i| {
            Ok(FatEvent::Recorded {
                blob: format!("{i:0width$}", width = PAYLOAD_BYTES),
            })
        }))
        .boxed())
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

/// Peak live bytes of one bulk execution of `events` records on a stream of its own.
async fn peak_of_bulk(cqrs: &Cqrs<PostgresEventStore>, stream: &str, events: usize) -> isize {
    let stream_id = FatUrn(urn::UrnBuilder::new("fat", stream).build().unwrap());

    reset_peak();
    cqrs.execute_bulk::<FatStream>(
        &stream_id,
        replay::Metadata::default(),
        ImportFat { count: events },
        &(),
        None,
    )
    .await
    .expect("the bulk execution must succeed");
    peak_live_bytes()
}

/// Peak live bytes of a bulk execution scale with the segment size, not with the bulk:
/// the same at 1x and 20x events.
#[tokio::test]
async fn peak_live_bytes_of_a_bulk_scale_with_the_segment_not_the_bulk_postgres_test() {
    const SEGMENT: usize = 20;
    const BASE: usize = 2 * SEGMENT;
    const LARGE: usize = 20 * BASE;
    // Larger than either bulk, so only the segment's commit flushes the projection.
    const FLUSH: usize = 10 * LARGE;

    let (pool, _container) = start_pool().await;
    let store = PostgresEventStore::builder(pool.clone())
        .projection_flush_size(FLUSH)
        .bulk_segment_size(SEGMENT)
        .register(NoOpProjection)
        .build()
        .await
        .expect("build store");
    let cqrs = Cqrs::new(store);

    // Warm the pool's connections and the statement caches outside the measurement.
    peak_of_bulk(&cqrs, "warm-up", BASE).await;

    let base = peak_of_bulk(&cqrs, "bulk-1x", BASE).await;
    let large = peak_of_bulk(&cqrs, "bulk-20x", LARGE).await;

    // The retained JSON is the dominant term: six payloads per retained event, as the
    // flush test budgets them — the `Value` tree, the serialized row parameter, and slack
    // for sqlx's per-row work on the same thread. A bulk that kept its whole append for
    // the projection would hold at least `LARGE` payloads, twenty times the budget.
    let bounded = (SEGMENT * PAYLOAD_BYTES * 6) as isize;
    let whole_bulk = (LARGE * PAYLOAD_BYTES) as isize;

    report(
        "bulk_segment_1x",
        "peak_live_bytes",
        base as i128,
        bounded as i128,
        "bytes",
    );
    report(
        "bulk_segment_20x",
        "peak_live_bytes",
        large as i128,
        bounded as i128,
        "bytes",
    );

    assert!(
        base < bounded,
        "a bulk of {BASE} events peaked at {base} bytes, over the segment's budget {bounded}"
    );
    assert!(
        large < bounded,
        "a bulk of {LARGE} events peaked at {large} bytes, over the segment's budget \
         {bounded}; retaining the whole bulk would cost at least {whole_bulk}"
    );
    // Twenty times the events, the same segment: the peak must not grow with the bulk.
    let slack = (2 * PAYLOAD_BYTES) as isize;
    assert!(
        large <= base + slack,
        "a bulk twenty times larger peaked at {large} bytes against {base} at 1x: \
         something grows with the bulk"
    );
}
