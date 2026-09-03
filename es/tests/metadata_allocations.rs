//! Allocation regression tests for [`replay::Metadata`].
//!
//! `Metadata::new` round-trips its input through `serde_json::to_value`, which is
//! the right thing for a genuine `Serialize` input but a full deep copy when the
//! caller already holds a `Value` (every row read from the store). The owned
//! constructor must move instead, so building a `Metadata` from a `Value` costs a
//! constant, payload-independent number of bytes.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use replay::Metadata;
use serde_json::{json, Value};

/// Counts the bytes allocated *by the calling thread*, so measurements taken in a
/// synchronous block are unaffected by anything the test harness runs in parallel.
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

/// A metadata document big enough that a deep copy is unmistakable in the counts.
fn fat_metadata() -> Value {
    let blob = "m".repeat(64 * 1024);
    json!({
        "tenant": blob.clone(),
        "correlation": blob,
    })
}

#[test]
fn building_metadata_from_an_owned_value_does_not_re_serialize_it() {
    let payload_bytes = fat_metadata().to_string().len();

    // Warm up: first-touch lazy initialisation must not land inside a measurement.
    let _ = allocated_bytes(|| Metadata::from_json(fat_metadata()));

    // The documents are built *outside* the measured blocks: what is measured is
    // what each constructor allocates on top of a `Value` the caller already holds.
    let owned = fat_metadata();
    let to_serialize = fat_metadata();

    let (from_owned, owned_bytes) = allocated_bytes(|| Metadata::from_json(owned));
    let (from_serialize, serialize_bytes) = allocated_bytes(|| Metadata::new(to_serialize));

    // Same metadata either way: this is an allocation-only difference.
    assert_eq!(from_owned, from_serialize);

    // Moving an owned `Value` costs a fixed handful of bytes, whatever the payload.
    assert!(
        owned_bytes < 1024,
        "Metadata::from_json allocated {owned_bytes} bytes for a {payload_bytes} byte document; \
         it must move the Value, not re-serialize it"
    );

    // Calibration: the `Serialize` path really does deep-copy, so the assertion
    // above would fail if `from_json` regressed to a round-trip.
    assert!(
        serialize_bytes > payload_bytes,
        "expected Metadata::new to deep-copy the document (allocated {serialize_bytes} bytes \
         for {payload_bytes} bytes of JSON)"
    );
}
