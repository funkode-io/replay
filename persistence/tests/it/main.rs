//! The crate's integration suite, as one test binary.
//!
//! 28 files meant 28 compilations of `common/` and 28 links of the whole dependency
//! graph: 201 of the 210 CPU-seconds a one-line change to `src/lib.rs` cost (#249).
//! Each former file is a module here; test names are unchanged.
//!
//! The tracing subscriber is now one process-global for the whole suite and belongs to
//! the `#[traced_test]` tests that assert on log lines: no other test may install one.
//!
//! The `*_allocations.rs` budgets stay out of this binary. Their counters are per-thread
//! and tokio steals work across threads, so a co-resident test's allocations land in the
//! measurement: `policy_retry_allocations` read 2.1 MB against a 614 KB budget when it
//! ran here.

#[path = "../common/mod.rs"]
mod common;

mod bounded_queries;
mod created_filter_index;
mod dead_letter_unique_command;
mod event_commit_txid;
mod event_ordering_postgres;
mod event_stream_seq;
mod global_position_unique;
mod inline_projection_chunking;
mod inline_projection_rebuild_chunking;
mod integration_tests;
mod policy_cursor_backfill;
mod policy_daemon;
mod policy_dead_letter_identity;
mod policy_liveness;
mod policy_panic;
mod policy_park_once;
mod policy_per_stream_feed;
mod policy_redelivery_parks_once;
mod policy_retry_unit;
mod policy_supervision;
mod policy_timeout;
mod stream_creation_race;
mod stream_lock_wait;
