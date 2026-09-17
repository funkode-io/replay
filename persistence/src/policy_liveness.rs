//! The liveness axis: whether a Policy's worker is running.
//!
//! Only the process that runs the worker knows this, so it is published from
//! memory and never derived from the operational tables. It answers "is this
//! worker running"; [`crate::PolicyStatus`] answers "is this Policy moving".
//! Neither implies the other: a [Standby] runs and advances nothing, and the
//! [Leader] of a [Blocked policy] runs and advances nothing either.
//!
//! The durable half is a [`Beat`]: the replica that holds a Policy's advisory
//! lock writes that worker's state, its last completed poll and its own name to
//! the cursor row, on a cadence fixed independently of the work
//! ([`crate::PolicyRunnerDaemon`] spawns the task that does it). A worker cannot
//! write its own — it is blocked precisely while a reaction hangs, which is the
//! case the beat is for. The columns belong to the consumer's schema, so a
//! database without them is a no-op rather than an error
//! ([`HeartbeatColumns`]).
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

/// Postgres `lock_not_available`: the beat's `lock_timeout` expired on a row
/// somebody else holds.
const LOCK_NOT_AVAILABLE: &str = "55P03";

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

/// One Policy's line in a beat.
///
/// Built from the [`LivenessRegistry`] at beat time, for the Policies this
/// process leads and no others: a row a replica does not lead is another
/// replica's to write.
pub(crate) struct Beat {
    pub(crate) policy: String,
    pub(crate) liveness: Liveness,
    /// How long ago the worker last finished a poll, or `None` if it never has.
    /// Sent as an age rather than an instant so the database's clock remains the
    /// only clock in the reading.
    pub(crate) polled_ago: Option<Duration>,
}

/// Whether the consumer's schema carries the heartbeat columns, remembered for
/// the life of the daemon that asked: the answer is a property of the schema,
/// not of one beat, so a restarted beat task must not re-ask it.
///
/// Scoped to the daemon rather than the process, so a consumer that stops a
/// runner, migrates and starts another one picks the columns up. The cost of
/// that scope is one refused statement per daemon built after a migration that
/// never came.
#[derive(Clone, Default)]
pub(crate) struct HeartbeatColumns {
    /// Set the first time Postgres says a column is not there. One-way within a
    /// daemon: nothing re-checks, because a migration cannot land without
    /// somebody deploying.
    absent: Arc<AtomicBool>,
}

impl HeartbeatColumns {
    /// A writer that gives up a tick rather than wait longer than `lock_wait`
    /// for the cursor row.
    pub(crate) fn writer(
        &self,
        replica_id: Option<String>,
        lock_wait: Duration,
    ) -> HeartbeatWriter {
        HeartbeatWriter {
            columns: self.clone(),
            replica_id,
            lock_wait,
            failure_reported: false,
        }
    }
}

/// Writes the beat: one statement per beat, covering every Policy this process
/// leads.
///
/// `SET LOCAL lock_timeout` is the reason for the transaction. The beat updates
/// the same row a checkpoint writes, and a beat that queued behind one would
/// arrive late for the same reason the worker is busy — the coupling the fixed
/// cadence exists to remove. A tick that cannot take the row is skipped instead,
/// which costs nothing against a threshold of several beats.
pub(crate) struct HeartbeatWriter {
    columns: HeartbeatColumns,
    /// What to write in `led_by`. `None` when the consumer named no replica and
    /// the process has no `HOSTNAME`: the column is then left null rather than
    /// carrying an invented identity.
    replica_id: Option<String>,
    /// How long the beat may wait for the cursor row before giving up the tick.
    lock_wait: Duration,
    /// Whether a failed beat has already been reported. A second line says
    /// nothing new: the database a beat goes to is the one the workers use, and
    /// a worker that is also failing says so at `error`.
    failure_reported: bool,
}

/// `now() - make_interval(...)` rather than a timestamp computed here: the
/// database's clock is the one every replica and every reader compares against,
/// and a replica whose clock has drifted must not be able to report a poll in
/// the future. `last_beat_at - last_polled_at` is therefore the worker's exact
/// poll age, taken from one clock.
///
/// The consequence is that `last_polled_at` is recomputed from an age on every
/// beat, so a worker that has not polled since sees it wobble by the round trip
/// rather than stand perfectly still. It carries beat-level precision, which is
/// all a staleness threshold of several beats can use.
///
/// A worker with no poll to report writes `null` rather than keeping what was
/// there: the row describes *this* replica's worker, and a fresh Leader that
/// inherited a row must not inherit a poll it never made. `null` says "leading,
/// nothing completed yet", which is the truth for the first poll interval after a
/// failover.
///
/// `FOR UPDATE SKIP LOCKED` is what keeps one Policy's contention from silencing
/// the rest: every led Policy is written in one statement, so a single row held
/// by somebody else — an operator part-way through a [Cursor move] in an open
/// transaction, say — would otherwise abort the whole beat and make every Policy
/// on this replica look leaderless. The contended row is skipped; its Policy
/// misses a beat, and nobody else does.
///
/// [Cursor move]: https://github.com/funkode-io/replay/blob/main/CONTEXT.md#cursor-move
const BEAT_SQL: &str = "\
    WITH beatable AS ( \
        SELECT name FROM policy_cursors \
         WHERE name = ANY($1::text[]) \
           FOR UPDATE SKIP LOCKED \
    ) \
    UPDATE policy_cursors AS pc \
       SET last_beat_at   = now(), \
           liveness       = v.liveness, \
           led_by         = $4, \
           last_polled_at = now() - make_interval(secs => v.polled_ago) \
      FROM unnest($1::text[], $2::text[], $3::float8[]) AS v(name, liveness, polled_ago) \
     WHERE pc.name = v.name \
       AND pc.name IN (SELECT name FROM beatable)";

impl HeartbeatWriter {
    /// Write one beat. Never returns an error: a report that could stop the work
    /// it reports on would be worse than no report.
    pub(crate) async fn beat(&mut self, pool: &Pool<Postgres>, beats: &[Beat]) {
        if beats.is_empty() || self.columns.absent.load(Ordering::Relaxed) {
            return;
        }

        let mut policies = Vec::with_capacity(beats.len());
        let mut states = Vec::with_capacity(beats.len());
        let mut polled_ago = Vec::with_capacity(beats.len());
        for beat in beats {
            policies.push(beat.policy.clone());
            states.push(beat.liveness.as_str().to_string());
            polled_ago.push(beat.polled_ago.map(|ago| ago.as_secs_f64()));
        }

        if let Err(error) = self.write(pool, &policies, &states, &polled_ago).await {
            self.report(&error);
        }
    }

    async fn write(
        &self,
        pool: &Pool<Postgres>,
        policies: &[String],
        states: &[String],
        polled_ago: &[Option<f64>],
    ) -> Result<(), sqlx::Error> {
        let mut tx = pool.begin().await?;
        // Derived from the cadence, not a constant: a wait longer than the gap
        // between beats would delay the next one, which is the coupling to
        // workload the fixed cadence exists to remove. `set_config(..., true)`
        // is `SET LOCAL` with a bind parameter, which `SET` itself does not take.
        sqlx::query("SELECT set_config('lock_timeout', $1, true)")
            .bind(format!("{}ms", self.lock_wait.as_millis().max(1)))
            .execute(&mut *tx)
            .await?;
        sqlx::query(BEAT_SQL)
            .bind(policies)
            .bind(states)
            .bind(polled_ago)
            .bind(self.replica_id.as_deref())
            .execute(&mut *tx)
            .await?;
        tx.commit().await
    }

    /// Report a failed beat once, at the level its cause deserves.
    ///
    /// A schema without the columns is a choice the consumer made, and a beat that
    /// gave up a contended row is this design working — both are `debug`. Anything
    /// else means the durable half is silently off while the README tells whoever
    /// reads it to page on staleness, so it is a `warn` naming the reason.
    fn report(&mut self, error: &sqlx::Error) {
        let expected = if is_undefined_column(error) {
            // Attempted once per daemon, then left alone: a consumer whose schema
            // has no heartbeat columns reads liveness from the daemon, and a line
            // per beat about columns they have not added is noise.
            self.columns.absent.store(true, Ordering::Relaxed);
            true
        } else {
            is_lock_not_available(error)
        };

        if self.failure_reported {
            return;
        }
        self.failure_reported = true;

        if expected {
            tracing::debug!(
                error = %error,
                durable_heartbeat = if self.columns.absent.load(Ordering::Relaxed) {
                    "off for this daemon"
                } else {
                    "retried on the next beat"
                },
                "the durable heartbeat was not written"
            );
        } else {
            tracing::warn!(
                error = %error,
                "the durable heartbeat write failed and is not being retried per beat; \
                 anything reading policy_cursors for liveness will see it go stale while \
                 this process is healthy. Liveness stays readable from the daemon"
            );
        }
    }
}

/// Whether Postgres refused the write because a column is not there, as opposed
/// to any other reason a write can fail.
fn is_undefined_column(error: &sqlx::Error) -> bool {
    has_code(error, UNDEFINED_COLUMN)
}

/// Whether the beat gave up waiting for a row somebody else holds — the tick it
/// is designed to skip, not a fault.
fn is_lock_not_available(error: &sqlx::Error) -> bool {
    has_code(error, LOCK_NOT_AVAILABLE)
}

fn has_code(error: &sqlx::Error, code: &str) -> bool {
    match error {
        sqlx::Error::Database(db) => db.code().as_deref() == Some(code),
        _ => false,
    }
}

// ── Unit tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use tracing_test::traced_test;

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

    /// A schema without the heartbeat columns costs one refused statement per
    /// process, not one per beat — and the answer outlives the task that found
    /// it, so a restarted beat task does not re-ask.
    #[test]
    fn absent_columns_stop_being_attempted() {
        let columns = HeartbeatColumns::default();
        let mut writer = columns.writer(None, Duration::from_millis(1));

        writer.report(&undefined_column());

        assert!(
            columns.absent.load(Ordering::Relaxed),
            "a refused column turns the durable heartbeat off for the process"
        );
        assert!(
            columns
                .writer(None, Duration::from_millis(1))
                .failure_reported
                || columns.absent.load(Ordering::Relaxed),
            "a beat task started afterwards inherits the answer rather than re-asking"
        );
    }

    /// Any other failure is reported once and then left alone — at `warn`, because
    /// the durable half being off while a consumer is told to page on staleness is
    /// not something to find out by reading `debug`.
    #[traced_test]
    #[test]
    fn an_unexpected_failure_is_warned_about_once() {
        let mut writer = HeartbeatColumns::default().writer(None, Duration::from_millis(1));

        writer.report(&sqlx::Error::PoolTimedOut);
        assert!(writer.failure_reported);
        assert!(
            !writer.columns.absent.load(Ordering::Relaxed),
            "a failure that is not a missing column leaves the heartbeat on"
        );
        assert!(logs_contain("the durable heartbeat write failed"));
        logs_assert(|lines| {
            match lines
                .iter()
                .find(|line| line.contains("the durable heartbeat write failed"))
            {
                Some(line) if line.contains("WARN") => Ok(()),
                Some(line) => Err(format!(
                    "an unexpected failure must not hide at debug: {line}"
                )),
                None => Err("nothing was reported at all".to_string()),
            }
        });
    }

    /// The two causes a beat expects — a schema without the columns, and a row
    /// somebody else holds — are this design working, and stay at `debug`.
    #[traced_test]
    #[test]
    fn an_expected_failure_stays_quiet() {
        let mut writer = HeartbeatColumns::default().writer(None, Duration::from_millis(1));
        writer.report(&undefined_column());

        assert!(!logs_contain("WARN"));
    }

    /// The `42703` a missing column raises, as sqlx surfaces it.
    fn undefined_column() -> sqlx::Error {
        // sqlx has no public constructor for a DatabaseError, so the check is
        // exercised through the one shape that matters to it: the SQLSTATE.
        struct Refused;
        impl std::fmt::Debug for Refused {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("column \"last_beat_at\" does not exist")
            }
        }
        impl std::fmt::Display for Refused {
            fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                f.write_str("column \"last_beat_at\" does not exist")
            }
        }
        impl std::error::Error for Refused {}
        impl sqlx::error::DatabaseError for Refused {
            fn message(&self) -> &str {
                "column \"last_beat_at\" does not exist"
            }
            fn code(&self) -> Option<std::borrow::Cow<'_, str>> {
                Some(std::borrow::Cow::Borrowed(UNDEFINED_COLUMN))
            }
            fn as_error(&self) -> &(dyn std::error::Error + Send + Sync + 'static) {
                self
            }
            fn as_error_mut(&mut self) -> &mut (dyn std::error::Error + Send + Sync + 'static) {
                self
            }
            fn into_error(self: Box<Self>) -> Box<dyn std::error::Error + Send + Sync + 'static> {
                self
            }
            fn kind(&self) -> sqlx::error::ErrorKind {
                sqlx::error::ErrorKind::Other
            }
        }
        sqlx::Error::Database(Box::new(Refused))
    }
}
