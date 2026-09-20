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

use std::collections::hash_map::Entry;
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
    /// Where the Policy was waiting. The subject of the wait, and the only thing that
    /// ends one: a cursor that has moved is a Policy that advanced or was repositioned,
    /// so the next wait is a new outage and starts a new clock — including across two
    /// [`crate::PolicyRunner::drain`] calls, which load a fresh cursor each time and so
    /// never observe the transition between them.
    ///
    /// Not the withheld row, which changes *during* one wait: a transaction that
    /// committed above the watermark sorts ahead of the one reported before it, and
    /// restarting the clock there would let staggered commits push the warning past its
    /// threshold indefinitely.
    cursor: FeedPoint,
    /// First poll of this wait.
    first_seen: Instant,
    /// Last poll that reported it.
    reported: Option<Instant>,
}

impl Sighting {
    fn new(cursor: FeedPoint, first_seen: Instant) -> Self {
        Self {
            cursor,
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
/// Policy first reads nothing at a cursor and runs until that cursor moves — either
/// because the drain called [`cleared`](Self::cleared) when the feed moved, or because a
/// later poll arrives at a different one.
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

    /// Record a poll that found `policy` waiting at `cursor`, and return whether this
    /// poll reports it.
    ///
    /// Decides and claims under one lock, so concurrent drains of the same Policy
    /// cannot both report. `now` is read before the lock, so two of them can arrive
    /// out of order; ages saturate, and the older one then skips a poll.
    pub(crate) fn poll(&self, policy: &str, cursor: FeedPoint, now: Instant) -> bool {
        let mut seen = self.lock();
        let sighting = match seen.entry(policy.to_owned()) {
            // The wait this process is already timing.
            Entry::Occupied(waiting) if waiting.get().cursor == cursor => waiting.into_mut(),
            // A wait somewhere else: the Policy advanced or was repositioned in between,
            // whether or not this process saw it happen.
            Entry::Occupied(mut elsewhere) => {
                elsewhere.insert(Sighting::new(cursor, now));
                elsewhere.into_mut()
            }
            Entry::Vacant(first) => first.insert(Sighting::new(cursor, now)),
        };

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
    use crate::commit_stamp::CommitStamp;

    const INTERVAL: Duration = Duration::from_secs(30);

    /// A cursor a Policy is waiting at. Where it is does not matter; that it is the
    /// same one across polls of one wait does.
    fn parked_at(position: i64) -> FeedPoint {
        FeedPoint {
            commit_txid: CommitStamp::parse("91827").expect("a plain counter parses"),
            position,
        }
    }

    /// A wait that clears within the interval is an append landing, not an incident:
    /// the case the whole gate exists for.
    #[test]
    fn a_wait_younger_than_the_interval_is_not_reported() {
        let watch = BlockedWatch::new(INTERVAL);
        let start = Instant::now();

        assert!(!watch.poll("import_started", parked_at(7), start));
        assert!(!watch.poll(
            "import_started",
            parked_at(7),
            start + Duration::from_secs(29)
        ));
    }

    #[test]
    fn a_wait_that_outlives_the_interval_is_reported() {
        let watch = BlockedWatch::new(INTERVAL);
        let start = Instant::now();

        assert!(!watch.poll("import_started", parked_at(7), start));
        assert!(watch.poll("import_started", parked_at(7), start + INTERVAL));
    }

    /// A waiting Policy polls forever and must not write a record per poll.
    #[test]
    fn a_report_silences_the_rest_of_the_interval() {
        let watch = BlockedWatch::new(INTERVAL);
        let start = Instant::now();

        assert!(!watch.poll("import_started", parked_at(7), start));
        assert!(watch.poll("import_started", parked_at(7), start + INTERVAL));

        assert!(!watch.poll(
            "import_started",
            parked_at(7),
            start + INTERVAL + Duration::from_secs(1)
        ));
        assert!(!watch.poll(
            "import_started",
            parked_at(7),
            start + INTERVAL + Duration::from_secs(29)
        ));
        assert!(watch.poll("import_started", parked_at(7), start + INTERVAL + INTERVAL));
    }

    /// The row reported as earliest changes while the wait goes on — a transaction
    /// that committed above the watermark sorts ahead of the one reported before it —
    /// and the clock must not restart with it. Staggered commits behind one long
    /// transaction would otherwise postpone the warning for as long as they kept
    /// arriving, which is exactly the outage it exists to name.
    ///
    /// Pinned by the signature as much as by the polls: the withheld row is not an
    /// input here, so no change to it can reach the clock.
    #[test]
    fn a_new_earliest_withheld_row_does_not_restart_the_clock() {
        let watch = BlockedWatch::new(INTERVAL);
        let start = Instant::now();

        assert!(!watch.poll("import_started", parked_at(7), start));
        assert!(watch.poll("import_started", parked_at(7), start + INTERVAL));
    }

    /// A wait the process never saw end: two `drain()` calls either side of a cursor
    /// an external writer moved. Nothing calls `cleared`, so the cursor is the only
    /// evidence that the second wait is not the first one carrying on — and a fresh
    /// wait must serve its full interval rather than inherit a warning that is due.
    #[test]
    fn a_wait_at_a_different_cursor_starts_a_new_clock() {
        let watch = BlockedWatch::new(INTERVAL);
        let start = Instant::now();

        assert!(!watch.poll("import_started", parked_at(7), start));
        assert!(!watch.poll("import_started", parked_at(9), start + INTERVAL));
        assert!(watch.poll("import_started", parked_at(9), start + INTERVAL + INTERVAL));
    }

    /// And the suppression does not travel either: a reported wait must not silence the
    /// first report of the next one.
    #[test]
    fn a_wait_at_a_different_cursor_is_not_suppressed_by_the_last_report() {
        let watch = BlockedWatch::new(INTERVAL);
        let start = Instant::now();

        assert!(!watch.poll("import_started", parked_at(7), start));
        assert!(watch.poll("import_started", parked_at(7), start + INTERVAL));

        let moved_on = start + INTERVAL + Duration::from_secs(1);
        assert!(!watch.poll("import_started", parked_at(9), moved_on));
        assert!(watch.poll("import_started", parked_at(9), moved_on + INTERVAL));
    }

    /// Nineteen policies stopped behind the same write in the incident; each is its
    /// own signal.
    #[test]
    fn policies_are_gated_independently() {
        let watch = BlockedWatch::new(INTERVAL);
        let start = Instant::now();

        assert!(!watch.poll("import_started", parked_at(7), start));
        assert!(watch.poll("import_started", parked_at(7), start + INTERVAL));

        assert!(!watch.poll("price_fanout", parked_at(7), start + INTERVAL));
    }

    /// The clock runs from the wait, not from the Policy: a cursor idle for an hour
    /// that then waits on an open write must stay silent until the wait itself is old.
    #[test]
    fn a_policy_that_advanced_starts_its_next_wait_from_scratch() {
        let watch = BlockedWatch::new(INTERVAL);
        let start = Instant::now();

        assert!(!watch.poll("import_started", parked_at(7), start));
        watch.cleared("import_started");

        assert!(!watch.poll(
            "import_started",
            parked_at(7),
            start + Duration::from_secs(29)
        ));
        assert!(watch.poll(
            "import_started",
            parked_at(7),
            start + Duration::from_secs(29) + INTERVAL
        ));
    }

    #[test]
    fn clearing_a_policy_that_was_never_waiting_is_a_no_op() {
        let watch = BlockedWatch::new(INTERVAL);

        watch.cleared("never_ran");

        assert!(!watch.poll("never_ran", parked_at(7), Instant::now()));
    }

    /// Two drains can read the clock in one order and reach the lock in the other.
    /// The older timestamp must cost a poll, not the drain.
    #[test]
    fn a_timestamp_that_arrives_out_of_order_is_harmless() {
        let watch = BlockedWatch::new(INTERVAL);
        let start = Instant::now();

        assert!(!watch.poll("import_started", parked_at(7), start + INTERVAL));
        assert!(!watch.poll("import_started", parked_at(7), start));
        assert!(watch.poll("import_started", parked_at(7), start + INTERVAL + INTERVAL));
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
