//! Shared harness for the crate's allocation-regression tests.
//!
//! Both allocation tests in this crate measure the same way: count the bytes a
//! block of code requests, on the thread that runs it, and compare against a
//! budget. Only the `#[global_allocator]` registration itself has to stay in each
//! test binary — a binary can register exactly one, and it must be its own.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

thread_local! {
    /// Bytes requested by the current thread since it started.
    static ALLOCATED: Cell<usize> = const { Cell::new(0) };
}

/// Counts the bytes allocated *by the calling thread*, so a measurement taken in a
/// synchronous block is unaffected by anything the test harness runs in parallel.
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

/// Runs `f` and returns the bytes it allocated on this thread.
pub fn allocated_bytes<T>(f: impl FnOnce() -> T) -> (T, usize) {
    let before = ALLOCATED.with(Cell::get);
    let value = f();
    let after = ALLOCATED.with(Cell::get);
    (value, after - before)
}
