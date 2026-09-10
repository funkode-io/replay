//! Support code shared by this crate's integration-test binaries.
//!
//! Lives under `tests/common/` so cargo treats it as a module to include with
//! `mod common;` rather than as a test binary of its own.

pub mod alloc;
pub mod report;
