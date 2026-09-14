//! Allocation regression test for [`replay::ScopedUrn::to_slug`].
//!
//! `to_slug` exists only because it borrows. `unscoped` already answers the same
//! question authoritatively, and callers were splitting the NSS on `@` by hand
//! rather than pay for it — a slug accessor that allocates is one they would go
//! back to hand-rolling, so the budget here is zero rather than "small".

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::str::FromStr;

use replay::prelude::*;
use serde::{Deserialize, Serialize};
use urn::Urn;

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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
struct ProductUrn(Urn);

impl From<ProductUrn> for Urn {
    fn from(urn: ProductUrn) -> Self {
        urn.0
    }
}

impl TryFrom<Urn> for ProductUrn {
    type Error = String;

    fn try_from(urn: Urn) -> Result<Self, Self::Error> {
        if urn.nid() == "product" {
            Ok(ProductUrn(urn))
        } else {
            Err(format!("expected 'product', got '{}'", urn.nid()))
        }
    }
}

impl AsRef<Urn> for ProductUrn {
    fn as_ref(&self) -> &Urn {
        &self.0
    }
}

/// A scope long enough that rebuilding the URN is unmistakable in the counts.
fn scoped_product() -> ProductUrn {
    let catalog = "c".repeat(4 * 1024);
    ProductUrn(Urn::from_str(&format!("urn:product:sku123@catalog:{catalog}")).unwrap())
}

#[test]
fn reading_a_slug_does_not_allocate() {
    let product = scoped_product();

    // Warm up: first-touch lazy initialisation must not land inside a measurement.
    let _ = allocated_bytes(|| product.to_slug().len());

    let (slug_len, slug_bytes) = allocated_bytes(|| product.to_slug().len());

    assert_eq!(slug_len, "sku123".len());

    // Not "small": none. `to_slug` returns a slice of an NSS that already exists, and
    // the moment it stops doing so its callers are better off splitting on '@' again.
    assert_eq!(
        slug_bytes, 0,
        "to_slug allocated {slug_bytes} bytes; it must borrow the NSS, not rebuild the URN"
    );
}

#[test]
fn the_unscoped_path_it_replaces_really_does_allocate() {
    // Calibration for the assertion above: without this, `to_slug` could be quietly
    // reimplemented on top of `unscoped` and the zero-byte budget would look like it
    // still held on some future allocator.
    let product = scoped_product();

    let _ = allocated_bytes(|| product.unscoped().unwrap());

    let (_, unscoped_bytes) = allocated_bytes(|| {
        let base: Urn = product.unscoped().unwrap().into();
        base.nss().to_string()
    });

    assert!(
        unscoped_bytes > 0,
        "expected unscoped() to rebuild and therefore allocate; it allocated {unscoped_bytes} bytes"
    );
}
