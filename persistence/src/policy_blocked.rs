//! When a blocked Policy gets to say so, and how often.
//!
//! A Policy parked in front of a hole in `global_position` and a healthy idle one
//! both read an empty feed, so both used to emit nothing (funkode-io/replay#164).
//! What separates them is a hole that outlives the appends that could have filled
//! it. This module holds the gate that decides when that has happened, and the
//! probe that describes it.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use sqlx::{Pool, Postgres, Row};

/// Built-in default for [`resolve_blocked_warn_after`]. Below it, a missing
/// position is an append still committing, which the feed is designed to wait for.
const DEFAULT_BLOCKED_WARN_AFTER: Duration = Duration::from_secs(30);

/// Environment variable overriding the built-in interval, in seconds.
const BLOCKED_WARN_AFTER_ENV_VAR: &str = "REPLAY_BLOCKED_WARN_AFTER_SECS";

/// How long a hole must persist before it is reported, and the minimum spacing
/// between repeats.
///
/// Precedence: `REPLAY_BLOCKED_WARN_AFTER_SECS` → 30s. `0` and unparseable values
/// fall back to the default rather than turning the report into a flood.
pub(crate) fn resolve_blocked_warn_after() -> Duration {
    std::env::var(BLOCKED_WARN_AFTER_ENV_VAR)
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map_or(DEFAULT_BLOCKED_WARN_AFTER, Duration::from_secs)
}

/// One Policy's history in front of one hole.
struct Sighting {
    /// The hole. A different one restarts the clock: it is a different wait.
    missing_position: i64,
    /// First poll that saw it.
    first_seen: Instant,
    /// Last poll that reported it.
    reported: Option<Instant>,
}

/// Decides which polls of a blocked Policy get to write a record.
///
/// Two conditions, both per Policy: the hole must have survived `interval` of
/// polling, and the last record must be at least `interval` old. The first is why
/// an append landing in the next millisecond stays silent; the second is why a
/// Policy blocked for three days does not write three days of log.
///
/// One entry per Policy, so memory is bounded by the code that registers them.
pub(crate) struct BlockedWatch {
    interval: Duration,
    seen: Mutex<HashMap<String, Sighting>>,
}

impl BlockedWatch {
    pub(crate) fn new(interval: Duration) -> Self {
        Self {
            interval,
            seen: Mutex::new(HashMap::new()),
        }
    }

    /// Record a poll that found `policy` parked in front of `missing_position`, and
    /// return whether this poll reports it.
    ///
    /// Decides and claims under one lock, so concurrent drains of the same Policy
    /// cannot both report.
    pub(crate) fn poll(&self, policy: &str, missing_position: i64, now: Instant) -> bool {
        let mut seen = self.lock();
        let is_new_hole = seen
            .get(policy)
            .is_none_or(|sighting| sighting.missing_position != missing_position);
        if is_new_hole {
            seen.insert(
                policy.to_owned(),
                Sighting {
                    missing_position,
                    first_seen: now,
                    reported: None,
                },
            );
        }
        let sighting = seen.get_mut(policy).expect("present or just inserted");

        if now.duration_since(sighting.first_seen) < self.interval {
            return false;
        }
        if sighting
            .reported
            .is_some_and(|last| now.duration_since(last) < self.interval)
        {
            return false;
        }

        sighting.reported = Some(now);
        true
    }

    /// Forget `policy`: it has advanced, so its next hole is a new wait.
    pub(crate) fn cleared(&self, policy: &str) {
        self.lock().remove(policy);
    }

    /// A poisoned lock is not worth failing a drain over: this is a rate gate, and
    /// the worst a recovered one does is allow an extra record.
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Sighting>> {
        self.seen.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// What the database says about a Policy that read nothing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct BlockedFor {
    /// `MAX(global_position)`: how much of the log is stranded behind the hole.
    pub(crate) head: i64,
    /// Time since the cursor last advanced (`policy_cursors.updated_at`).
    ///
    /// Persisted rather than in-memory so a restart or a leadership change does not
    /// reset the clock on an outage measured in days.
    pub(crate) elapsed: Duration,
}

/// Read the head and how long `policy`'s cursor has been parked.
///
/// One row, two index probes; the event log is never scanned. `None` when the
/// Policy has no cursor row, so it has never run and cannot be blocked.
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

    const INTERVAL: Duration = Duration::from_secs(30);

    /// A hole that clears within the interval is an append landing, not an
    /// incident: the case the whole gate exists for.
    #[test]
    fn a_hole_younger_than_the_interval_is_not_reported() {
        let watch = BlockedWatch::new(INTERVAL);
        let start = Instant::now();

        assert!(!watch.poll("import_started", 42, start));
        assert!(!watch.poll("import_started", 42, start + Duration::from_secs(29)));
    }

    #[test]
    fn a_hole_that_outlives_the_interval_is_reported() {
        let watch = BlockedWatch::new(INTERVAL);
        let start = Instant::now();

        assert!(!watch.poll("import_started", 42, start));
        assert!(watch.poll("import_started", 42, start + INTERVAL));
    }

    /// A blocked Policy polls forever and must not write a record per poll.
    #[test]
    fn a_report_silences_the_rest_of_the_interval() {
        let watch = BlockedWatch::new(INTERVAL);
        let start = Instant::now();

        assert!(!watch.poll("import_started", 42, start));
        assert!(watch.poll("import_started", 42, start + INTERVAL));

        assert!(!watch.poll(
            "import_started",
            42,
            start + INTERVAL + Duration::from_secs(1)
        ));
        assert!(!watch.poll(
            "import_started",
            42,
            start + INTERVAL + Duration::from_secs(29)
        ));
        assert!(watch.poll("import_started", 42, start + INTERVAL + INTERVAL));
    }

    /// The clock runs from the hole, not from the Policy: a cursor idle for an hour
    /// that then waits on an in-flight append must stay silent.
    #[test]
    fn a_different_hole_restarts_the_clock() {
        let watch = BlockedWatch::new(INTERVAL);
        let start = Instant::now();

        assert!(!watch.poll("import_started", 42, start));
        assert!(!watch.poll("import_started", 99, start + INTERVAL));
        assert!(watch.poll("import_started", 99, start + INTERVAL + INTERVAL));
    }

    /// Nineteen policies stopped at the same hole in the incident; each is its own
    /// signal.
    #[test]
    fn policies_are_gated_independently() {
        let watch = BlockedWatch::new(INTERVAL);
        let start = Instant::now();

        assert!(!watch.poll("import_started", 42, start));
        assert!(watch.poll("import_started", 42, start + INTERVAL));

        assert!(!watch.poll("price_fanout", 42, start + INTERVAL));
    }

    #[test]
    fn a_policy_that_advanced_starts_its_next_wait_from_scratch() {
        let watch = BlockedWatch::new(INTERVAL);
        let start = Instant::now();

        assert!(!watch.poll("import_started", 42, start));
        watch.cleared("import_started");

        assert!(!watch.poll("import_started", 42, start + Duration::from_secs(29)));
        assert!(watch.poll(
            "import_started",
            42,
            start + Duration::from_secs(29) + INTERVAL
        ));
    }

    #[test]
    fn clearing_a_policy_that_was_never_blocked_is_a_no_op() {
        let watch = BlockedWatch::new(INTERVAL);

        watch.cleared("never_ran");

        assert!(!watch.poll("never_ran", 42, Instant::now()));
    }

    #[test]
    fn the_interval_falls_back_to_the_default() {
        // Process-global, and tests share the process: only assert the default when
        // nothing has set it.
        if std::env::var(BLOCKED_WARN_AFTER_ENV_VAR).is_err() {
            assert_eq!(resolve_blocked_warn_after(), DEFAULT_BLOCKED_WARN_AFTER);
        }
    }
}
