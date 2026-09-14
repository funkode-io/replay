//! When a Policy that is blocked gets to say so, and how often.
//!
//! A Policy parked in front of a hole in `global_position` and a healthy idle one
//! both read an empty feed, so both used to emit exactly nothing — the silence that
//! turned a one-row anomaly into a three-day outage (funkode-io/replay#164). The
//! difference between them is a gap, and a gap that has outlived every append that
//! could have filled it is an incident rather than a millisecond of waiting.
//!
//! This module holds the two things that decision needs and the drain does not: the
//! **rate gate** that keeps a permanently blocked Policy from flooding the log, and
//! the **probe** that reads how far ahead the log is and how long the cursor has
//! been parked.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use sqlx::{Pool, Postgres, Row};

/// Built-in default for [`resolve_blocked_warn_after`].
///
/// A stop shorter than this is indistinguishable from an append still in flight,
/// which the feed is *designed* to wait for; a stop longer than it has outlived any
/// plausible commit.
const DEFAULT_BLOCKED_WARN_AFTER: Duration = Duration::from_secs(30);

/// Environment variable overriding the built-in blocked-warning interval, in seconds.
const BLOCKED_WARN_AFTER_ENV_VAR: &str = "REPLAY_BLOCKED_WARN_AFTER_SECS";

/// Resolve how long a Policy must be parked before it is reported at `warn`, which
/// is also the minimum spacing between repeats.
///
/// Precedence: `REPLAY_BLOCKED_WARN_AFTER_SECS` → built-in default (30s). A value of
/// `0` or an unparseable one falls back to the default rather than turning the
/// report into a flood.
pub(crate) fn resolve_blocked_warn_after() -> Duration {
    std::env::var(BLOCKED_WARN_AFTER_ENV_VAR)
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map_or(DEFAULT_BLOCKED_WARN_AFTER, Duration::from_secs)
}

/// Per-policy rate gate for the blocked-policy warning.
///
/// A blocked Policy polls forever, so the warning has to be spaced by something. The
/// spacing is deliberately the same duration as the escalation threshold: one knob,
/// and "it has been blocked for at least this long" and "you hear about it at most
/// this often" stay consistent.
///
/// Memory is one `Instant` per Policy that has been reported, so it is bounded by
/// the code that registers policies, never by the log.
pub(crate) struct BlockedWatch {
    /// Minimum time blocked before reporting, and minimum spacing between reports.
    interval: Duration,
    /// When each Policy was last reported.
    reported: Mutex<HashMap<String, Instant>>,
}

impl BlockedWatch {
    /// A gate that reports a blocked Policy no more often than once per `interval`.
    pub(crate) fn new(interval: Duration) -> Self {
        Self {
            interval,
            reported: Mutex::new(HashMap::new()),
        }
    }

    /// How long a Policy must be parked before it is worth a `warn`.
    pub(crate) fn interval(&self) -> Duration {
        self.interval
    }

    /// Whether `policy` may be reported at `now`.
    ///
    /// Peeks only: the caller records the report with [`reported`](Self::reported)
    /// once it has actually written one, so a stop too young to report does not
    /// spend the interval it has not used. The two calls straddle a database probe,
    /// so two drains of the same Policy racing each other could both pass the gate
    /// — worth one duplicate line, never a duplicate reaction, and only one leader
    /// drains a Policy anyway.
    pub(crate) fn due(&self, policy: &str, now: Instant) -> bool {
        match self.lock().get(policy) {
            Some(last) => now.duration_since(*last) >= self.interval,
            None => true,
        }
    }

    /// Record that `policy` was reported at `now`, starting the interval that
    /// silences the polls that follow.
    pub(crate) fn reported(&self, policy: &str, now: Instant) {
        self.lock().insert(policy.to_owned(), now);
    }

    /// Forget `policy`'s last report: a Policy that has moved is not blocked, and
    /// the next time it stops it should say so straight away rather than inherit the
    /// spacing of a blockage it has already recovered from.
    pub(crate) fn cleared(&self, policy: &str) {
        self.lock().remove(policy);
    }

    /// A poisoned lock carries no state worth aborting a drain over: the map is a
    /// rate gate, and the worst a recovered one does is allow an extra record.
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Instant>> {
        self.reported.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// What the database says about a Policy that read nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BlockedFor {
    /// Current global head (`MAX(global_position)`), so the report can say how much
    /// of the log is stranded behind the hole.
    pub(crate) head: i64,
    /// How long the cursor has been parked, measured from the last time it advanced
    /// (`policy_cursors.updated_at`).
    ///
    /// Persisted rather than in-memory on purpose: a restart or a leadership change
    /// must not reset the clock on an outage that is measured in days.
    pub(crate) elapsed: Duration,
}

/// Read the head of the log and how long `policy`'s cursor has been parked.
///
/// One row, two index probes (`MAX(global_position)` and the cursor's primary key);
/// the event log is never scanned. Runs only for a Policy that read an empty feed
/// with a hole in front of it, which is either a commit landing in the next
/// millisecond or an incident. Returns `None` when the Policy has no cursor row,
/// which means it has never run and so cannot be blocked.
pub(crate) async fn probe_blocked(
    pool: &Pool<Postgres>,
    policy: &str,
) -> Result<Option<BlockedFor>, replay::Error> {
    let row = sqlx::query(
        "SELECT COALESCE((SELECT MAX(global_position) FROM events), 0) AS head, \
         GREATEST(EXTRACT(EPOCH FROM (now() - pc.updated_at)) * 1000, 0)::bigint AS parked_ms \
         FROM policy_cursors pc WHERE pc.name = $1",
    )
    .bind(policy)
    .fetch_optional(pool)
    .await
    .map_err(crate::db_error)?;

    Ok(row.map(|row| BlockedFor {
        head: row.get("head"),
        elapsed: Duration::from_millis(row.get::<i64, _>("parked_ms") as u64),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A Policy that has just become blocked is reported the moment it is seen.
    #[test]
    fn a_policy_that_has_never_been_reported_is_due() {
        let watch = BlockedWatch::new(Duration::from_secs(30));

        assert!(watch.due("import_started", Instant::now()));
    }

    /// The bound the ticket asks for: a blocked Policy polls forever, and must not
    /// put a record in the log for every poll.
    #[test]
    fn a_report_silences_the_rest_of_the_interval() {
        let watch = BlockedWatch::new(Duration::from_secs(30));
        let start = Instant::now();

        watch.reported("import_started", start);

        assert!(!watch.due("import_started", start + Duration::from_secs(1)));
        assert!(!watch.due("import_started", start + Duration::from_secs(29)));
    }

    #[test]
    fn a_policy_still_blocked_after_the_interval_is_reported_again() {
        let watch = BlockedWatch::new(Duration::from_secs(30));
        let start = Instant::now();

        watch.reported("import_started", start);

        assert!(watch.due("import_started", start + Duration::from_secs(30)));
        assert!(
            watch.due("import_started", start + Duration::from_secs(90)),
            "the spacing runs from the last report, and this one was never recorded"
        );
    }

    /// A stop too young to report must not spend the interval: asking whether it is
    /// due cannot be what silences the poll that finally crosses the threshold.
    #[test]
    fn asking_without_reporting_silences_nothing() {
        let watch = BlockedWatch::new(Duration::from_secs(30));
        let start = Instant::now();

        assert!(watch.due("import_started", start));
        assert!(watch.due("import_started", start + Duration::from_millis(1)));
    }

    /// One Policy's report must not silence another's: nineteen policies stopped at
    /// the same hole in the incident, and each of them is a separate signal.
    #[test]
    fn policies_are_gated_independently() {
        let watch = BlockedWatch::new(Duration::from_secs(30));
        let start = Instant::now();

        watch.reported("import_started", start);

        assert!(!watch.due("import_started", start));
        assert!(watch.due("price_fanout", start));
    }

    #[test]
    fn a_policy_that_moved_reports_its_next_blockage_immediately() {
        let watch = BlockedWatch::new(Duration::from_secs(30));
        let start = Instant::now();

        watch.reported("import_started", start);
        watch.cleared("import_started");

        assert!(watch.due("import_started", start + Duration::from_millis(1)));
    }

    #[test]
    fn clearing_a_policy_that_was_never_blocked_is_a_no_op() {
        let watch = BlockedWatch::new(Duration::from_secs(30));

        watch.cleared("never_ran");

        assert!(watch.due("never_ran", Instant::now()));
    }

    #[test]
    fn the_warn_interval_falls_back_to_the_default() {
        // The env var is process-global and tests share the process, so the
        // resolved value is only asserted to be the default when it is unset.
        if std::env::var(BLOCKED_WARN_AFTER_ENV_VAR).is_err() {
            assert_eq!(resolve_blocked_warn_after(), DEFAULT_BLOCKED_WARN_AFTER);
        }
    }
}
