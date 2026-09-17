//! The liveness axis: whether a Policy's worker is running.
//!
//! Only the process that runs the worker knows this, so it is published from
//! memory and never derived from the operational tables. It answers "is this
//! worker running"; [`crate::PolicyStatus`] answers "is this Policy moving".
//! Neither implies the other: a [Standby] runs and advances nothing, and the
//! [Leader] of a [Blocked policy] runs and advances nothing either.
//!
//! The durable half is one column, `policy_cursors.last_polled_at`, written
//! opportunistically by the worker that polls: it is what lets a consumer read a
//! Leader's last poll from a replica that is not the Leader. The column belongs
//! to the consumer's schema, so a database without it is a no-op rather than an
//! error ([`Heartbeat`]).
//!
//! [Standby]: https://github.com/funkode-io/replay/blob/main/CONTEXT.md#standby
//! [Leader]: https://github.com/funkode-io/replay/blob/main/CONTEXT.md#leader
//! [Blocked policy]: https://github.com/funkode-io/replay/blob/main/CONTEXT.md#blocked-policy

use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use sqlx::{Pool, Postgres};

/// Postgres `undefined_column`: the consumer's schema has no `last_polled_at`.
const UNDEFINED_COLUMN: &str = "42703";

// ── Liveness ─────────────────────────────────────────────────────────────────

/// What one Policy's worker is doing in this process.
///
/// Published by the worker and its supervisor, never read back out of the
/// database. A replica reports on the workers it runs, which is why a Policy
/// another replica leads is [`StandingBy`](Self::StandingBy) here and
/// [`Leading`](Self::Leading) there.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub enum Liveness {
    /// Elected for this Policy and draining its feed.
    Leading,
    /// Running, holding no advisory lock for this Policy, waiting to be elected.
    /// Healthy and deliberately idle — another replica leads, or none does yet.
    StandingBy,
    /// Dead and inside the backoff before its next restart
    /// ([`crate::WorkerSupervision`]). Nothing is reacting for the Policy right
    /// now, and something will be shortly.
    Restarting,
    /// Down for good: it spent its restart budget, or the lock manager that
    /// elects it stopped. Nothing in this process will start it again, and it
    /// has been escalated ([`crate::Escalation`]).
    Stopped,
    /// The worker has not said anything yet — it was spawned and has not reached
    /// its first election. Reported rather than guessed: "stopped" is a claim
    /// this axis makes only when a worker is genuinely down for good.
    Unknown,
}

impl Liveness {
    /// Stable string form, suitable for JSON/UI consumers.
    pub fn as_str(&self) -> &'static str {
        match self {
            Liveness::Leading => "Leading",
            Liveness::StandingBy => "StandingBy",
            Liveness::Restarting => "Restarting",
            Liveness::Stopped => "Stopped",
            Liveness::Unknown => "Unknown",
        }
    }
}

impl fmt::Display for Liveness {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// One worker's liveness, as the process running it knows it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerLiveness {
    /// The Policy this worker drives.
    pub policy: String,
    /// What the worker is doing.
    pub liveness: Liveness,
    /// When the worker last finished a poll of its feed, or `None` if it never
    /// has — a [`Liveness::StandingBy`] worker normally never has.
    ///
    /// Monotonic, so it survives a clock change, and readable as an age with
    /// [`Instant::elapsed`]. It ages while a poll is in progress: a worker held
    /// inside one long reaction is [`Liveness::Leading`] with a stamp that is
    /// getting old, which is the pair that tells it from an idle one.
    pub last_polled_at: Option<Instant>,
}

// ── The registry ─────────────────────────────────────────────────────────────

/// Every worker's liveness in one process, written by the workers and their
/// supervisors and read by [`crate::PolicyRunnerDaemon::liveness`].
///
/// Bounded by the number of registered policies: entries are created once, at
/// [`crate::PolicyRunner::start_polling`], and only overwritten afterwards.
#[derive(Clone, Default)]
pub(crate) struct LivenessRegistry(Arc<Mutex<BTreeMap<String, WorkerLiveness>>>);

impl LivenessRegistry {
    /// Announce a worker before it runs, so a Policy is never missing from the
    /// report — a worker that has not reached its first election is
    /// [`Liveness::Unknown`], not absent.
    pub(crate) fn register(&self, policy: &str) -> LivenessHandle {
        self.lock().insert(
            policy.to_string(),
            WorkerLiveness {
                policy: policy.to_string(),
                liveness: Liveness::Unknown,
                last_polled_at: None,
            },
        );
        LivenessHandle {
            registry: self.clone(),
            policy: policy.to_string(),
        }
    }

    /// Every registered worker, by Policy name.
    pub(crate) fn snapshot(&self) -> Vec<WorkerLiveness> {
        self.lock().values().cloned().collect()
    }

    fn publish(&self, policy: &str, liveness: Liveness) {
        if let Some(worker) = self.lock().get_mut(policy) {
            worker.liveness = liveness;
        }
    }

    fn polled(&self, policy: &str, at: Instant) {
        if let Some(worker) = self.lock().get_mut(policy) {
            worker.last_polled_at = Some(at);
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, WorkerLiveness>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// One worker's write end of the [`LivenessRegistry`], held by the worker task
/// and by the supervisor that owns it.
#[derive(Clone)]
pub(crate) struct LivenessHandle {
    registry: LivenessRegistry,
    policy: String,
}

impl LivenessHandle {
    /// Elected, and about to drain.
    pub(crate) fn leading(&self) {
        self.registry.publish(&self.policy, Liveness::Leading);
    }

    /// Running and unelected: another replica leads this Policy, or none does yet.
    pub(crate) fn standing_by(&self) {
        self.registry.publish(&self.policy, Liveness::StandingBy);
    }

    /// Dead, inside the backoff before the next attempt.
    pub(crate) fn restarting(&self) {
        self.registry.publish(&self.policy, Liveness::Restarting);
    }

    /// Down for good.
    pub(crate) fn stopped(&self) {
        self.registry.publish(&self.policy, Liveness::Stopped);
    }

    /// A poll of the feed finished at `at`.
    pub(crate) fn polled(&self, at: Instant) {
        self.registry.polled(&self.policy, at);
    }
}

// ── The durable heartbeat ────────────────────────────────────────────────────

/// Whether `policy_cursors.last_polled_at` exists, remembered for the life of
/// the process and shared by every worker: the answer is a property of the
/// schema, not of one Policy, and a restarted worker must not re-ask it.
#[derive(Clone, Default)]
pub(crate) struct HeartbeatColumn {
    /// Set the first time Postgres says the column is not there. One-way: a
    /// column added while the process runs is picked up on the next start,
    /// which is when the consumer's migration ran.
    absent: Arc<AtomicBool>,
}

impl HeartbeatColumn {
    /// A writer that stamps at most once per `min_gap`.
    pub(crate) fn writer(&self, min_gap: Duration) -> Heartbeat {
        Heartbeat {
            column: self.clone(),
            min_gap,
            next_write: None,
        }
    }
}

/// Writes one worker's last poll to its cursor row, when the consumer's schema
/// carries the column.
///
/// The write is rate-limited to one per `min_gap` because a poll is not a fixed
/// cost: a `NOTIFY` wakes a worker per append, so an unthrottled stamp would add
/// a write per event to a row the checkpoint is already updating.
pub(crate) struct Heartbeat {
    column: HeartbeatColumn,
    min_gap: Duration,
    /// Earliest instant the next write may happen; `None` before the first.
    next_write: Option<Instant>,
}

impl Heartbeat {
    /// Stamp `policy`'s cursor row with the database's clock — a wall-clock
    /// reading every replica compares the same way, unlike a local one.
    ///
    /// Never fails the poll it is called from: the heartbeat is a report about
    /// the worker, and a report that could stop the work would be worse than no
    /// report.
    pub(crate) async fn beat(&mut self, pool: &Pool<Postgres>, policy: &str, now: Instant) {
        if !self.due(now) {
            return;
        }

        let written =
            sqlx::query("UPDATE policy_cursors SET last_polled_at = now() WHERE name = $1")
                .bind(policy)
                .execute(pool)
                .await;

        if let Err(error) = written {
            if is_undefined_column(&error) {
                // Said once, then never attempted again: a consumer whose schema
                // predates the heartbeat reads liveness from the daemon, and a
                // line per poll about a column they have not added is noise.
                self.column.absent.store(true, Ordering::Relaxed);
                tracing::debug!(
                    policy = %policy,
                    "policy_cursors has no last_polled_at column; the durable heartbeat is off \
                     for this process. Liveness is readable from the daemon either way"
                );
            } else {
                tracing::debug!(policy = %policy, error = %error, "heartbeat write failed");
            }
        }
    }

    /// Whether a write is owed at `now`, charging it to the rate limit if so.
    fn due(&mut self, now: Instant) -> bool {
        if self.column.absent.load(Ordering::Relaxed) {
            return false;
        }
        if self.next_write.is_some_and(|next| now < next) {
            return false;
        }
        self.next_write = Some(now + self.min_gap);
        true
    }
}

/// Whether Postgres refused the write because the column is not there, as
/// opposed to any other reason a write can fail.
fn is_undefined_column(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::Database(db) => db.code().as_deref() == Some(UNDEFINED_COLUMN),
        _ => false,
    }
}

// ── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// A registered worker is reported before it has run, and every state it
    /// publishes replaces the previous one.
    #[test]
    fn a_worker_is_reported_from_registration_and_through_every_state() {
        let registry = LivenessRegistry::default();
        let worker = registry.register("policy_a");

        assert_eq!(
            registry.snapshot(),
            vec![WorkerLiveness {
                policy: "policy_a".to_string(),
                liveness: Liveness::Unknown,
                last_polled_at: None,
            }],
            "a worker that has not reached its first election is unknown, never stopped"
        );

        for (published, expected) in [
            (
                LivenessHandle::standing_by as fn(&LivenessHandle),
                Liveness::StandingBy,
            ),
            (LivenessHandle::leading, Liveness::Leading),
            (LivenessHandle::restarting, Liveness::Restarting),
            (LivenessHandle::stopped, Liveness::Stopped),
        ] {
            published(&worker);
            assert_eq!(registry.snapshot()[0].liveness, expected);
        }
    }

    /// Two workers are reported independently, by name.
    #[test]
    fn each_worker_reports_only_itself() {
        let registry = LivenessRegistry::default();
        let leader = registry.register("policy_a");
        let standby = registry.register("policy_b");

        leader.leading();
        standby.standing_by();

        let reported = registry.snapshot();
        assert_eq!(reported.len(), 2);
        assert_eq!(reported[0].policy, "policy_a");
        assert_eq!(reported[0].liveness, Liveness::Leading);
        assert_eq!(reported[1].policy, "policy_b");
        assert_eq!(reported[1].liveness, Liveness::StandingBy);
    }

    /// The last poll is carried alongside the state, and a restart does not
    /// erase it: when the worker last polled is still true while it is away.
    #[test]
    fn the_last_poll_survives_a_state_change() {
        let registry = LivenessRegistry::default();
        let worker = registry.register("policy_a");
        let polled_at = Instant::now();

        worker.leading();
        worker.polled(polled_at);
        worker.restarting();

        assert_eq!(registry.snapshot()[0].last_polled_at, Some(polled_at));
    }

    /// The durable stamp is written at most once per gap, however often the
    /// worker polls — a `NOTIFY` per append must not become a write per append.
    #[test]
    fn the_durable_stamp_is_rate_limited() {
        let gap = Duration::from_secs(1);
        let mut heartbeat = HeartbeatColumn::default().writer(gap);
        let start = Instant::now();

        assert!(heartbeat.due(start), "the first poll owes a stamp");
        assert!(
            !heartbeat.due(start + gap / 2),
            "a poll inside the gap owes nothing"
        );
        assert!(
            heartbeat.due(start + gap),
            "a poll past the gap owes one again"
        );
    }

    /// A schema without the column costs one failed write per process, not one
    /// per poll.
    #[test]
    fn an_absent_column_stops_being_attempted() {
        let column = HeartbeatColumn::default();
        let mut heartbeat = column.writer(Duration::ZERO);
        let now = Instant::now();

        assert!(heartbeat.due(now));
        column.absent.store(true, Ordering::Relaxed);
        assert!(!heartbeat.due(now));

        // Including for a worker that started afterwards: the column is a
        // property of the schema, and a restart re-asking would spam the log.
        assert!(!column.writer(Duration::ZERO).due(now));
    }
}
