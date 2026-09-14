//! Waiting for a condition the database will reach, instead of sleeping and hoping.
//!
//! Every daemon test in this suite has the same hazard: the thing under test happens
//! on a background task, so the test has to know when it has happened. A fixed sleep
//! encodes a guess about how fast the host and the server are, and that guess is what
//! breaks when either changes — a lost race then shows up as an assertion about
//! behaviour, pointing at the wrong culprit.
//!
//! So the tests poll a count the runner itself maintains and fail on a deadline,
//! which is a statement about liveness rather than about speed.
//!
//! `allow(dead_code)` module-wide: every test binary that says `mod common;` compiles
//! this module, including the ones that never wait for anything.
#![allow(dead_code)]

use std::time::{Duration, Instant};

use sqlx::{Pool, Postgres};

/// Polls `count_query` until it returns at least `at_least`, and returns the count.
///
/// Panics when the deadline passes first, reporting how far the count actually got —
/// `what` names the thing being waited for, so the failure says which liveness
/// property broke rather than which number was wrong.
pub async fn wait_for_count(
    pool: &Pool<Postgres>,
    count_query: &'static str,
    at_least: i64,
    within: Duration,
    what: &str,
) -> i64 {
    let deadline = Instant::now() + within;
    loop {
        let count = sqlx::query_scalar::<_, i64>(count_query)
            .fetch_one(pool)
            .await
            .expect("count query must succeed");
        if count >= at_least {
            return count;
        }
        assert!(
            Instant::now() < deadline,
            "timed out after {within:?} waiting for {what}: reached {count}/{at_least}"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
