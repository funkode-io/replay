//! Shared harness for the crate's allocation-regression tests.
//!
//! The crate's allocation tests measure in one of two ways: the bytes a block of code
//! requests (how much it copied), or the most it holds at once (how much it retained).
//! Both come from one counting allocator; only the `#[global_allocator]` registration
//! has to stay in each test binary, since a binary can register exactly one and it must
//! be its own.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    /// Bytes requested by the current thread since it started.
    static ALLOCATED: Cell<usize> = const { Cell::new(0) };
    /// Bytes requested but not yet freed by the current thread.
    static LIVE: Cell<isize> = const { Cell::new(0) };
    /// High-water mark of `LIVE` since the last [`reset_peak`].
    static PEAK: Cell<isize> = const { Cell::new(0) };
}

/// Counts the bytes allocated *by the calling thread*, so a measurement taken in a
/// synchronous block is unaffected by anything the test harness runs in parallel.
///
/// Answers two questions, and they are not interchangeable: [`allocated_bytes`] totals
/// what a block *requested* (how much copying it did), while [`peak_live_bytes`] is the
/// most it held *at once* (how much it retained).
///
/// Register it in the test binary that uses this module:
///
/// ```ignore
/// #[global_allocator]
/// static ALLOCATOR: common::alloc::CountingAllocator = common::alloc::CountingAllocator;
/// ```
pub struct CountingAllocator;

/// Const-initialized `Cell` with no destructor, so recording from inside `alloc`
/// neither allocates nor recurses.
fn record(bytes: usize) {
    let _ = ALLOCATED.try_with(|allocated| allocated.set(allocated.get() + bytes));
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

/// Records memory going back to the allocator. Only the live figure falls; the
/// cumulative `ALLOCATED` total never does.
fn release(bytes: usize) {
    let _ = LIVE.try_with(|live| live.set(live.get() - bytes as isize));
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        release(layout.size());
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if new_size >= layout.size() {
            record(new_size - layout.size());
        } else {
            release(layout.size() - new_size);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }

    unsafe fn alloc_zeroed(&self, layout: Layout) -> *mut u8 {
        record(layout.size());
        unsafe { System.alloc_zeroed(layout) }
    }
}

/// Runs `f` and returns the bytes it allocated on this thread.
///
/// `allow(dead_code)`: each test binary includes the whole module but uses only the
/// measurement it needs.
#[allow(dead_code)]
pub fn allocated_bytes<T>(f: impl FnOnce() -> T) -> (T, usize) {
    let before = ALLOCATED.with(Cell::get);
    let value = f();
    let after = ALLOCATED.with(Cell::get);
    (value, after - before)
}

/// Resets the high-water mark so the next [`peak_live_bytes`] measures from here.
///
/// Separate from the measurement because the code under test may be an `async` block,
/// which cannot be wrapped in a closure the way [`allocated_bytes`] wraps one.
#[allow(dead_code)]
pub fn reset_peak() {
    LIVE.with(|live| live.set(0));
    PEAK.with(|peak| peak.set(0));
}

/// The most bytes held live on this thread at any one moment since [`reset_peak`].
#[allow(dead_code)]
pub fn peak_live_bytes() -> isize {
    PEAK.with(Cell::get)
}
