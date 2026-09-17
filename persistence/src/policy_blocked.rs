//! When a Policy that reads nothing gets to say why, and how often.
//!
//! A Policy that reads nothing is either caught up or waiting: the feed only delivers
//! events whose writing transaction has ended, so a write that stays open holds back
//! everything committed after it ([`crate::policy_feed`]). The two look identical from
//! outside — an empty feed — and in funkode-io/replay#164 a healthy idle Policy and a
//! stopped one produced byte-identical output: nothing.
//!
//! This module holds the probe that tells them apart and the gate that decides when the
//! difference is worth reporting. The wait itself is not a fault: it ends when the write
//! ends. What an operator needs is to know it is happening, and for how long.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

use sqlx::{Pool, Postgres, Row};

use crate::commit_stamp::CommitStamp;
use crate::policy_feed::FeedPoint;

/// Built-in default for [`resolve_blocked_warn_after`]. Below it, a wait is an ordinary
/// append taking its time, which the feed is designed to wait for.
const DEFAULT_BLOCKED_WARN_AFTER: Duration = Duration::from_secs(30);

/// Environment variable overriding the built-in interval, in seconds.
const BLOCKED_WARN_AFTER_ENV_VAR: &str = "REPLAY_BLOCKED_WARN_AFTER_SECS";

/// How long a wait must persist before it is reported, and the minimum spacing
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

/// One Policy's history behind one withheld event.
struct Sighting {
    /// The oldest event being held back. A different one is a different wait.
    withheld_position: i64,
    /// First poll that saw it.
    first_seen: Instant,
    /// Last poll that reported it.
    reported: Option<Instant>,
}

impl Sighting {
    fn new(withheld_position: i64, first_seen: Instant) -> Self {
        Self {
            withheld_position,
            first_seen,
            reported: None,
        }
    }
}

/// Decides which polls of a waiting Policy get to write a record.
///
/// Two conditions, both per Policy: the wait must have survived `interval` of polling,
/// and the last record must be at least `interval` old. The first is why an append
/// committing in the next millisecond stays silent; the second is why a Policy behind a
/// write held open for three days does not write three days of log.
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

    /// Record a poll that found `policy` waiting behind `withheld_position`, and
    /// return whether this poll reports it.
    ///
    /// Decides and claims under one lock, so concurrent drains of the same Policy
    /// cannot both report. `now` is read before the lock, so two of them can arrive
    /// out of order; ages saturate, and the older one then skips a poll.
    pub(crate) fn poll(&self, policy: &str, withheld_position: i64, now: Instant) -> bool {
        let mut seen = self.lock();
        let is_new_wait = seen
            .get(policy)
            .is_none_or(|sighting| sighting.withheld_position != withheld_position);
        if is_new_wait {
            seen.insert(policy.to_owned(), Sighting::new(withheld_position, now));
        }
        let sighting = seen.get_mut(policy).expect("present or just inserted");

        if now.saturating_duration_since(sighting.first_seen) < self.interval {
            return false;
        }
        if sighting
            .reported
            .is_some_and(|last| now.saturating_duration_since(last) < self.interval)
        {
            return false;
        }

        sighting.reported = Some(now);
        true
    }

    /// Forget `policy`: it has advanced or caught up, so its next wait starts from
    /// scratch.
    pub(crate) fn cleared(&self, policy: &str) {
        self.lock().remove(policy);
    }

    /// A poisoned lock is not worth failing a drain over: this is a rate gate, and
    /// the worst a recovered one does is allow an extra record.
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Sighting>> {
        self.seen.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// What the database says about a Policy that read nothing while the log has moved on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct Waiting {
    /// `MAX(global_position)`: how much of the log is stranded behind the open write.
    pub(crate) head: i64,
    /// The oldest event the watermark holds back, and the transaction that wrote it.
    pub(crate) withheld: FeedPoint,
    /// `pg_snapshot_xmin`: the oldest transaction still running. Everything below it
    /// has ended, and everything the Policy is waiting for was written at or above it.
    pub(crate) watermark: CommitStamp,
    /// Time since the cursor last advanced (`policy_cursors.updated_at`).
    ///
    /// Persisted rather than in-memory so a restart or a leadership change does not
    /// reset the clock on an outage measured in days.
    pub(crate) elapsed: Duration,
}

/// The oldest event `policy` cannot be given yet, if there is one.
///
/// `None` is the ordinary answer: the Policy has everything that has finished being
/// written, which is a healthy idle Policy and stays silent. A row comes back only when
/// the cursor is still where the feed left it *and* an event past it sits at or above
/// the watermark — written by, or after, a transaction that has not ended.
///
/// Read on an empty poll only, where there is nothing else to pay for: one row, index
/// probes on `policy_cursors` and `idx_events_commit_txid_position`, no scan of the log.
pub(crate) async fn probe_waiting(
    pool: &Pool<Postgres>,
    policy: &str,
    cursor: FeedPoint,
) -> Result<Option<Waiting>, replay::Error> {
    let row = sqlx::query(
        "SELECT COALESCE((SELECT MAX(global_position) FROM events), 0) AS head, \
         pg_snapshot_xmin(pg_current_snapshot())::text AS watermark, \
         w.global_position AS withheld_position, \
         w.commit_txid::text AS withheld_commit_txid, \
         GREATEST(EXTRACT(EPOCH FROM (now() - pc.updated_at)) * 1000, 0)::bigint AS parked_ms \
         FROM policy_cursors pc \
         JOIN LATERAL ( \
             SELECT global_position, commit_txid FROM events \
             WHERE (commit_txid, global_position) > ($2::xid8, $3) \
               AND commit_txid >= pg_snapshot_xmin(pg_current_snapshot()) \
             ORDER BY commit_txid, global_position LIMIT 1 \
         ) w ON TRUE \
         WHERE pc.name = $1 AND pc.position = $3 AND pc.commit_txid = $2::xid8",
    )
    .bind(policy)
    .bind(cursor.commit_txid.to_string())
    .bind(cursor.position)
    .fetch_optional(pool)
    .await
    .map_err(crate::db_error)?;

    row.map(|row| {
        Ok(Waiting {
            head: row.get("head"),
            withheld: FeedPoint {
                commit_txid: CommitStamp::from_row(&row, "withheld_commit_txid")?,
                position: row.get("withheld_position"),
            },
            watermark: CommitStamp::from_row(&row, "watermark")?,
            elapsed: Duration::from_millis(row.get::<i64, _>("parked_ms") as u64),
        })
    })
    .transpose()
}

#[cfg(test)]
mod tests {
    use super::*;

    const INTERVAL: Duration = Duration::from_secs(30);

    /// A wait that clears within the interval is an append landing, not an incident:
    /// the case the whole gate exists for.
    #[test]
    fn a_wait_younger_than_the_interval_is_not_reported() {
        let watch = BlockedWatch::new(INTERVAL);
        let start = Instant::now();

        assert!(!watch.poll("import_started", 42, start));
        assert!(!watch.poll("import_started", 42, start + Duration::from_secs(29)));
    }

    #[test]
    fn a_wait_that_outlives_the_interval_is_reported() {
        let watch = BlockedWatch::new(INTERVAL);
        let start = Instant::now();

        assert!(!watch.poll("import_started", 42, start));
        assert!(watch.poll("import_started", 42, start + INTERVAL));
    }

    /// A waiting Policy polls forever and must not write a record per poll.
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

    /// The clock runs from the wait, not from the Policy: a cursor idle for an hour
    /// that then waits on an open write must stay silent.
    #[test]
    fn a_different_wait_restarts_the_clock() {
        let watch = BlockedWatch::new(INTERVAL);
        let start = Instant::now();

        assert!(!watch.poll("import_started", 42, start));
        assert!(!watch.poll("import_started", 99, start + INTERVAL));
        assert!(watch.poll("import_started", 99, start + INTERVAL + INTERVAL));
    }

    /// Nineteen policies stopped behind the same write in the incident; each is its
    /// own signal.
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
    fn clearing_a_policy_that_was_never_waiting_is_a_no_op() {
        let watch = BlockedWatch::new(INTERVAL);

        watch.cleared("never_ran");

        assert!(!watch.poll("never_ran", 42, Instant::now()));
    }

    /// Two drains can read the clock in one order and reach the lock in the other.
    /// The older timestamp must cost a poll, not the drain.
    #[test]
    fn a_timestamp_that_arrives_out_of_order_is_harmless() {
        let watch = BlockedWatch::new(INTERVAL);
        let start = Instant::now();

        assert!(!watch.poll("import_started", 42, start + INTERVAL));
        assert!(!watch.poll("import_started", 42, start));
        assert!(watch.poll("import_started", 42, start + INTERVAL + INTERVAL));
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
