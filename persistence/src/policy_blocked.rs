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

/// One Policy's current wait.
struct Sighting {
    /// First poll of this wait. It survives a change of withheld row: while the feed
    /// is waiting the Policy has not moved, and the row reported as earliest can
    /// change under it — a transaction that committed above the watermark sorts ahead
    /// of the one reported before it — so restarting the clock there would let
    /// staggered commits push the warning past its threshold indefinitely.
    first_seen: Instant,
    /// Last poll that reported it.
    reported: Option<Instant>,
}

impl Sighting {
    fn new(first_seen: Instant) -> Self {
        Self {
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
/// The clock belongs to the wait, not to the row being waited on: it starts when the
/// Policy first reads nothing and runs until [`cleared`](Self::cleared) — which the drain
/// calls the moment the feed moves or the Policy is caught up.
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

    /// Record a poll that found `policy` waiting, and return whether this poll
    /// reports it.
    ///
    /// Decides and claims under one lock, so concurrent drains of the same Policy
    /// cannot both report. `now` is read before the lock, so two of them can arrive
    /// out of order; ages saturate, and the older one then skips a poll.
    pub(crate) fn poll(&self, policy: &str, now: Instant) -> bool {
        let mut seen = self.lock();
        let sighting = seen
            .entry(policy.to_owned())
            .or_insert_with(|| Sighting::new(now));

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
    /// `MAX(global_position)`: the highest number the log has issued.
    ///
    /// Reported as evidence that the log is moving while this Policy is not, and for
    /// nothing else. It is not the feed's head and not a backlog: the feed advances in
    /// `(commit_txid, global_position)` order, where a later point can hold a lower
    /// position, so the arithmetic distance from a cursor to this number means nothing
    /// (ADR-0022). Logged as `log_max_position` so an incident cannot read it as one.
    pub(crate) log_max_position: i64,
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
        "SELECT COALESCE((SELECT MAX(global_position) FROM events), 0) AS log_max_position, \
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
            log_max_position: row.get("log_max_position"),
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

        assert!(!watch.poll("import_started", start));
        assert!(!watch.poll("import_started", start + Duration::from_secs(29)));
    }

    #[test]
    fn a_wait_that_outlives_the_interval_is_reported() {
        let watch = BlockedWatch::new(INTERVAL);
        let start = Instant::now();

        assert!(!watch.poll("import_started", start));
        assert!(watch.poll("import_started", start + INTERVAL));
    }

    /// A waiting Policy polls forever and must not write a record per poll.
    #[test]
    fn a_report_silences_the_rest_of_the_interval() {
        let watch = BlockedWatch::new(INTERVAL);
        let start = Instant::now();

        assert!(!watch.poll("import_started", start));
        assert!(watch.poll("import_started", start + INTERVAL));

        assert!(!watch.poll("import_started", start + INTERVAL + Duration::from_secs(1)));
        assert!(!watch.poll("import_started", start + INTERVAL + Duration::from_secs(29)));
        assert!(watch.poll("import_started", start + INTERVAL + INTERVAL));
    }

    /// The row reported as earliest changes while the wait goes on — a transaction
    /// that committed above the watermark sorts ahead of the one reported before it —
    /// and the clock must not restart with it. Staggered commits behind one long
    /// transaction would otherwise postpone the warning for as long as they kept
    /// arriving, which is exactly the outage it exists to name.
    #[test]
    fn a_new_earliest_withheld_row_does_not_restart_the_clock() {
        let watch = BlockedWatch::new(INTERVAL);
        let start = Instant::now();

        // Poll 1 is behind position 42; poll 2, still waiting, is behind 99.
        assert!(!watch.poll("import_started", start));
        assert!(watch.poll("import_started", start + INTERVAL));
    }

    /// Nineteen policies stopped behind the same write in the incident; each is its
    /// own signal.
    #[test]
    fn policies_are_gated_independently() {
        let watch = BlockedWatch::new(INTERVAL);
        let start = Instant::now();

        assert!(!watch.poll("import_started", start));
        assert!(watch.poll("import_started", start + INTERVAL));

        assert!(!watch.poll("price_fanout", start + INTERVAL));
    }

    /// The clock runs from the wait, not from the Policy: a cursor idle for an hour
    /// that then waits on an open write must stay silent until the wait itself is old.
    #[test]
    fn a_policy_that_advanced_starts_its_next_wait_from_scratch() {
        let watch = BlockedWatch::new(INTERVAL);
        let start = Instant::now();

        assert!(!watch.poll("import_started", start));
        watch.cleared("import_started");

        assert!(!watch.poll("import_started", start + Duration::from_secs(29)));
        assert!(watch.poll("import_started", start + Duration::from_secs(29) + INTERVAL));
    }

    #[test]
    fn clearing_a_policy_that_was_never_waiting_is_a_no_op() {
        let watch = BlockedWatch::new(INTERVAL);

        watch.cleared("never_ran");

        assert!(!watch.poll("never_ran", Instant::now()));
    }

    /// Two drains can read the clock in one order and reach the lock in the other.
    /// The older timestamp must cost a poll, not the drain.
    #[test]
    fn a_timestamp_that_arrives_out_of_order_is_harmless() {
        let watch = BlockedWatch::new(INTERVAL);
        let start = Instant::now();

        assert!(!watch.poll("import_started", start + INTERVAL));
        assert!(!watch.poll("import_started", start));
        assert!(watch.poll("import_started", start + INTERVAL + INTERVAL));
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
