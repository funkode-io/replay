//! Machine-readable reporting for the crate's allocation budgets.
//!
//! Each allocation test asserts a measurement against a budget. The assertion alone
//! answers "did it regress?"; it does not answer "by how much, and which way is it
//! drifting?" — which is the question you want on a pull request, before a budget is
//! breached rather than after.
//!
//! So every budgeted measurement also emits one line on stdout:
//!
//! ```text
//! ALLOC name=read_path_batch metric=allocated_over_payload measured=106 budget=150 unit=percent
//! ```
//!
//! CI runs the allocation tests with `--nocapture`, collects these lines, and renders
//! them into the run summary and a pull-request comment. The format is deliberately
//! flat `key=value` so it survives being grepped out of interleaved test output.

/// Emits one budget line, then returns `measured` so a call reads as a passthrough:
///
/// ```ignore
/// assert!(report("read_path_batch", "allocated_over_payload", pct, 150, "percent") < 150);
/// ```
///
/// Called *before* the assertion, so a failing run still reports the number that
/// failed — the most interesting measurement is the one that broke the budget.
///
/// `allow(dead_code)`: each test binary includes the whole `common` module but uses
/// only the parts it needs.
#[allow(dead_code)]
pub fn report(name: &str, metric: &str, measured: i128, budget: i128, unit: &str) -> i128 {
    println!("ALLOC name={name} metric={metric} measured={measured} budget={budget} unit={unit}");
    measured
}
