//! Allocation guard for the erased inline-projection bridge (issue #148).
//!
//! The bridge that adapts an [`InlineProjection`] to the store's registry used to
//! deep-copy the whole append batch once per registered inline projection
//! (`event.data.clone()` plus a whole-`PersistedEvent` clone). With `P` inline
//! projections registered that made an append of size `B` cost roughly `(1 + P) × B`
//! bytes.
//!
//! This test pins the fixed behaviour: routing an append batch through `P` inline
//! projections must cost about the same as routing it through none. It uses a counting
//! global allocator, so it fails loudly if a per-inline-projection clone is ever
//! reintroduced.
//!
//! The measurement isolates the *payload* copy: the appended event carries a large
//! `blob` field that the inline projections' event type does not declare, so a
//! borrow-based deserialization allocates nothing for it while a `Value` clone must copy
//! all of it. The append also carries a bulky metadata document, so the same budget
//! covers the envelope: `Metadata` is an `Arc<Value>`, and cloning it per inline
//! projection must stay a handle copy rather than a deep copy of the document.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use replay::{Metadata, WithId};
use replay_macros::Event;
use replay_persistence::{EventStore, InMemoryEventStore, InlineProjection, PersistedEvent};
use serde::{Deserialize, Serialize};
use serde_json::json;
use urn::{Urn, UrnBuilder};

mod common;
use common::alloc::{allocated_bytes, CountingAllocator};
use common::report::report;

/// Every test binary registers its own global allocator; the counting itself is
/// shared (`tests/common/alloc.rs`).
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// The appended (write-side) event. `blob` is the bulky payload whose copies we count.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Event)]
enum LedgerEvent {
    Recorded { seq: u64, blob: String },
}

/// The inline projections' event type: the same variant *without* the bulky field.
///
/// Serde ignores the unknown `blob` field, so deserializing this from the persisted
/// JSON allocates only the small `seq`. Any copy of the payload therefore shows up in
/// the allocation count as bridge overhead rather than as the inline projection's own
/// cost.
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Event)]
enum SlimLedgerEvent {
    Recorded { seq: u64 },
}

#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
struct LedgerUrn(Urn);

impl From<LedgerUrn> for Urn {
    fn from(urn: LedgerUrn) -> Self {
        urn.0
    }
}

impl TryFrom<Urn> for LedgerUrn {
    type Error = String;

    fn try_from(urn: Urn) -> Result<Self, Self::Error> {
        Ok(LedgerUrn(urn))
    }
}

struct LedgerStream {
    id: LedgerUrn,
}

impl WithId for LedgerStream {
    type StreamId = LedgerUrn;

    fn with_id(id: Self::StreamId) -> Self {
        LedgerStream { id }
    }

    fn get_id(&self) -> &Self::StreamId {
        &self.id
    }
}

impl replay::EventStream for LedgerStream {
    type Event = LedgerEvent;

    fn stream_type() -> String {
        "Ledger".to_string()
    }

    fn apply(&mut self, _event: Self::Event) {}
}

/// Counts the events it is handed, so the test proves the batch really was routed.
struct SeqCounter {
    handled: Arc<AtomicUsize>,
}

impl InlineProjection for SeqCounter {
    type Exec = ();
    type Event = SlimLedgerEvent;

    fn name(&self) -> &str {
        "seq_counter"
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
        self.handled.fetch_add(events.len(), Ordering::SeqCst);
        Ok(())
    }
}

const BLOB_LEN: usize = 64 * 1024;
const BATCH: usize = 4;
const INLINE_PROJECTIONS: usize = 8;

fn batch() -> Vec<LedgerEvent> {
    (0..BATCH)
        .map(|seq| LedgerEvent::Recorded {
            seq: seq as u64,
            blob: "x".repeat(BLOB_LEN),
        })
        .collect()
}

/// A bulky metadata document, shared by every event of the append. `Metadata` is an
/// `Arc<Value>`, so re-enveloping per inline projection must copy the handle, not the
/// document.
fn fat_metadata() -> Metadata {
    Metadata::from_json(json!({ "correlation": "y".repeat(BLOB_LEN) }))
}

/// Append a batch to a store carrying `inline_projections` registered inline projections,
/// returning the bytes allocated during the append and the number of events handled.
fn append_measuring_allocations(inline_projections: usize) -> (usize, usize) {
    let handled = Arc::new(AtomicUsize::new(0));

    let mut store = InMemoryEventStore::new();
    for _ in 0..inline_projections {
        store = store.register_projection(SeqCounter {
            handled: handled.clone(),
        });
    }

    let stream_id = LedgerUrn(
        UrnBuilder::new("ledger", &format!("alloc-{inline_projections}"))
            .build()
            .unwrap(),
    );
    let events = batch();
    let metadata = fat_metadata();

    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();

    let (result, allocated) = allocated_bytes(|| {
        runtime.block_on(store.store_events::<LedgerStream>(
            &stream_id,
            "Ledger".to_string(),
            metadata,
            &events,
            None,
        ))
    });
    result.expect("append must succeed");

    (allocated, handled.load(Ordering::SeqCst))
}

/// Routing an append batch through `P` inline projections must not copy the payload
/// `P` times.
///
/// Before the borrow-deserialize fix each inline projection cloned the event's JSON
/// `data`, so the cost grew by ~one payload per inline projection: `(1 + P) × B`. Now
/// the per-inline-projection cost is independent of the payload size, so the measured
/// overhead over an append with no inline projections stays a small fraction of a single
/// payload.
#[test]
fn routing_through_many_inline_projections_allocates_about_one_payload() {
    let payload = BLOB_LEN * BATCH;

    let (baseline, none_handled) = append_measuring_allocations(0);
    let (with_inline_projections, handled) = append_measuring_allocations(INLINE_PROJECTIONS);

    assert_eq!(
        none_handled, 0,
        "no inline projections registered, nothing handled"
    );
    assert_eq!(
        handled,
        BATCH * INLINE_PROJECTIONS,
        "every inline projection must receive the whole batch"
    );

    let overhead = with_inline_projections.saturating_sub(baseline);
    let budget = payload / 10;

    report(
        "inline_projection_routing",
        "overhead_over_baseline",
        overhead as i128,
        budget as i128,
        "bytes",
    );
    assert!(
        overhead < budget,
        "routing {BATCH} events ({payload} B of payload) through {INLINE_PROJECTIONS} inline \
         projections allocated {overhead} B over the {baseline} B baseline, which exceeds the \
         {budget} B budget — the bridge is copying the payload per inline projection again"
    );
}
