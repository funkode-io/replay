//! The native Policy runner.
//!
//! Where [`crate::policy`] is the portable *contract* (no Postgres/tokio types),
//! this module is the server-side *runtime*: it reads the global event feed,
//! routes events through registered [`Policy`]s, executes the [`Dispatch`]es they
//! return through [`Cqrs`], stamps causation metadata, and advances each policy's
//! persisted cursor.
//!
//! This is the #77 walking skeleton: a single, manual [`PolicyRunner::drain`].
//! The background daemon, leadership election, batching, and NOTIFY wake-ups are
//! later slices that build on this substrate.

use std::any::{Any, TypeId};
use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use chrono::{DateTime, Utc};
use futures::future::BoxFuture;
use futures::FutureExt;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sqlx::{Pool, Postgres, QueryBuilder, Row};
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;

use replay::{Aggregate, Dispatch, Metadata, ObservedEvent, Policy};

use crate::policy::{ClosurePolicy, PolicySettings, RegisteredPolicy, StartAt};
use crate::policy_frontier::{discovered_from_sweep, Discovered, Nominations, PollPlan};
use crate::policy_liveness::{
    Beat, HeartbeatColumns, HeartbeatWriter, LivenessHandle, LivenessRegistry, WorkerLiveness,
};
use crate::policy_narration::{Narration, Poll, Record, PROGRESS_EVERY};
use crate::{Cqrs, PersistedEvent, PostgresEventStore, StreamFilter};

/// Erased, services-bound execution path for one aggregate type.
///
/// Registered via [`PolicyRunnerBuilder::register_services`], which captures the
/// concrete aggregate `A` *and* its `Services`. At drain time the runner looks up
/// the executor by the [`Dispatch`]'s [`TypeId`] and hands the dispatch over; the
/// executor takes the `(A::StreamId, A::Command)` pair back out of it and runs it
/// through [`Cqrs::execute`]. The pair is moved rather than borrowed, which is why the
/// whole dispatch crosses this seam.
trait AggregateExecutor: Send + Sync {
    fn execute<'a>(
        &'a self,
        cqrs: &'a Cqrs<PostgresEventStore>,
        dispatch: Dispatch,
        metadata: Metadata,
    ) -> BoxFuture<'a, Result<(), replay::Error>>;
}

struct TypedExecutor<A: Aggregate> {
    services: A::Services,
}

impl<A> AggregateExecutor for TypedExecutor<A>
where
    A: Aggregate + 'static,
    A::Error: Into<replay::Error>,
    A::StreamId: 'static,
    A::Command: 'static,
    A::Services: Send + Sync + 'static,
{
    fn execute<'a>(
        &'a self,
        cqrs: &'a Cqrs<PostgresEventStore>,
        dispatch: Dispatch,
        metadata: Metadata,
    ) -> BoxFuture<'a, Result<(), replay::Error>> {
        Box::pin(async move {
            let expected_version = dispatch.expected_version();
            let (id, command) = dispatch.into_parts::<A>().map_err(|_| {
                replay::Error::internal("policy dispatch payload type mismatch")
                    .with_operation("policy_execute")
            })?;

            cqrs.execute::<A>(&id, metadata, command, &self.services, expected_version)
                .await
                .map(|_| ())
                .map_err(Into::into)
        })
    }
}

/// Postgres NOTIFY channel used to wake waiting policy tasks after an append.
/// Policy tasks LISTEN on this channel; `PostgresEventStore::store_events`
/// fires a NOTIFY on it after every successful commit.
pub const REPLAY_NOTIFY_CHANNEL: &str = "replay_events";

/// How often a replica writes the durable heartbeat for the Policies it leads,
/// unless the consumer sets another cadence
/// ([`PolicyRunnerBuilder::with_heartbeat`]).
///
/// Five seconds costs one statement per beat per replica — 0.2 a second — and
/// lets a consumer call a Leader gone after three missed beats, well inside the
/// time anybody notices. It is deliberately unrelated to the poll interval: a
/// deployment polling every 30s still wants its liveness answered in seconds.
pub const HEARTBEAT_CADENCE: Duration = Duration::from_secs(5);

/// The longest a beat waits for the cursor row before giving up the tick, when
/// half the cadence would be longer still.
const HEARTBEAT_LOCK_WAIT: Duration = Duration::from_secs(1);

/// The shortest cadence a consumer may ask for
/// ([`PolicyRunnerBuilder::with_heartbeat`]).
///
/// Ten beats a second is already far past what any staleness threshold can use,
/// and below it the beat stops being a report and becomes a write loop against
/// the row the checkpoint uses.
pub const HEARTBEAT_MIN_CADENCE: Duration = Duration::from_millis(100);

/// The `error_kind` written to `policy_dead_letters` when a reaction **panicked**
/// rather than returning an error.
///
/// The two demand different responses, so they are not merged into one kind: a
/// returned error is usually data (the reaction worked, the command was refused
/// or the dependency was down), a panic is usually a defect in the reaction
/// itself. Distinct from every [`replay::ErrorKind`] rendering, so
/// `WHERE error_kind = 'Panic'` finds exactly the reactions that blew up.
pub const PANIC_ERROR_KIND: &str = "Panic";

/// The `error_kind` written to `policy_dead_letters` when a dispatch was
/// **abandoned on its timeout** rather than returning an error.
///
/// Distinct from every [`replay::ErrorKind`] rendering and from
/// [`PANIC_ERROR_KIND`]: a hang is diagnosed by looking at what the command was
/// waiting for, and nothing else could carry that — a command that never
/// returned produced no error to take a kind from.
pub const TIMEOUT_ERROR_KIND: &str = "Timeout";

// ─────────────────────────────────────────────────────────────────────────────

/// Builds a [`PolicyRunner`] by registering aggregate services and policies.
pub struct PolicyRunnerBuilder {
    cqrs: Cqrs<PostgresEventStore>,
    pool: Pool<Postgres>,
    policies: Vec<Arc<RegisteredPolicy>>,
    executors: HashMap<TypeId, Arc<dyn AggregateExecutor>>,
    notifications: bool,
    heartbeat: Option<Duration>,
    replica_id: Option<String>,
    supervision: WorkerSupervision,
    on_escalation: EscalationHook,
}

impl PolicyRunnerBuilder {
    /// Register the `Services` for aggregate `A`, enabling policies to dispatch
    /// commands to it. The runner owns the services and injects them when it
    /// executes a [`Dispatch::to::<A>`].
    pub fn register_services<A>(mut self, services: A::Services) -> Self
    where
        A: Aggregate + 'static,
        A::Error: Into<replay::Error>,
        A::StreamId: 'static,
        A::Command: 'static,
        A::Services: Send + Sync + 'static,
    {
        self.executors
            .insert(TypeId::of::<A>(), Arc::new(TypedExecutor::<A> { services }));
        self
    }

    /// Register a policy, and the [`PolicySettings`] the runner drives it with. Its
    /// `name` becomes the stable cursor key.
    ///
    /// ```rust,ignore
    /// builder.register_policy(
    ///     DepositFee,
    ///     PolicySettings::new().starting_at(StartAt::Beginning),
    /// )
    /// ```
    pub fn register_policy<P>(mut self, policy: P, settings: PolicySettings) -> Self
    where
        P: Policy + 'static,
    {
        self.policies
            .push(Arc::new(RegisteredPolicy::new(policy, settings)));
        self
    }

    /// Disable the `LISTEN/NOTIFY` latency optimisation; the daemon will use
    /// the fixed poll interval only, with no `PgListener` connection.
    ///
    /// Useful in environments where `pg_notify` is unavailable or for
    /// deterministic testing without a NOTIFY wakeup.
    pub fn without_notifications(mut self) -> Self {
        self.notifications = false;
        self
    }

    /// Replace the cadence of the durable heartbeat ([`HEARTBEAT_CADENCE`]).
    ///
    /// The cadence is deliberately independent of the poll interval and of what
    /// any worker is doing: a beat that slowed down when a worker got busy could
    /// not be used to tell busy from dead, which is the only thing it is for. A
    /// consumer reads a Leader as gone after some multiple of this — three beats
    /// is the usual choice — so shortening it shortens detection, at one
    /// statement per beat per replica.
    ///
    /// Floored at [`HEARTBEAT_MIN_CADENCE`]: a cadence of zero is a write loop
    /// rather than a fast heartbeat, and turning the durable half off is
    /// [`without_heartbeat`](Self::without_heartbeat) rather than a cadence
    /// nobody could serve.
    pub fn with_heartbeat(mut self, cadence: Duration) -> Self {
        if cadence < HEARTBEAT_MIN_CADENCE {
            tracing::warn!(
                asked_ms = cadence.as_millis() as u64,
                using_ms = HEARTBEAT_MIN_CADENCE.as_millis() as u64,
                "heartbeat cadence below the floor; using the floor. To stop writing \
                 the heartbeat entirely, call without_heartbeat()"
            );
        }
        self.heartbeat = Some(cadence.max(HEARTBEAT_MIN_CADENCE));
        self
    }

    /// Stop writing the durable heartbeat altogether. Liveness stays readable
    /// from [`PolicyRunnerDaemon::liveness`]; nothing about this process becomes
    /// readable from the database.
    pub fn without_heartbeat(mut self) -> Self {
        self.heartbeat = None;
        self
    }

    /// Name this replica in the heartbeat's `led_by`, so an operator reading a
    /// beat knows whose logs to open.
    ///
    /// Defaults to `HOSTNAME`, which is the pod name under Kubernetes. When
    /// neither is set the column is left null rather than carrying an invented
    /// identity.
    pub fn replica_id(mut self, replica_id: impl Into<String>) -> Self {
        self.replica_id = Some(replica_id.into());
        self
    }

    /// Replace the default [`WorkerSupervision`]: how long the runner waits
    /// before restarting a worker that died outside its reaction, and how many
    /// such restarts it allows within a window before giving up on it.
    pub fn with_worker_supervision(mut self, supervision: WorkerSupervision) -> Self {
        self.supervision = supervision;
        self
    }

    /// Replace what happens when a worker is down for good: it died more often
    /// than its [`WorkerSupervision`] budget allows, or the lock manager that
    /// elects it has stopped ([`EscalationReason`]).
    ///
    /// The hook is called once per Policy, on the supervisor's task, after the
    /// stop has been recorded in [`PolicyRunnerDaemon::stopped_workers`]. It runs
    /// in place of the default, which **exits the process** with
    /// [`ESCALATION_EXIT_CODE`] — the only outcome that releases the Policy's
    /// advisory lock, so that a standby replica can take it over ([ADR-0019]).
    ///
    /// **A hook that returns leaves the Policy stopped in every replica**, so it
    /// must arrange the ending itself: fail a liveness probe, drain and exit,
    /// page someone.
    ///
    /// A test that starts a daemon should install a hook here for the same
    /// reason: a test binary is a process, and the default ends it.
    ///
    /// [ADR-0019]: https://github.com/funkode-io/replay/blob/main/docs/adr/0019-escalation-is-a-consumer-hook-that-exits-by-default.md
    ///
    /// ```rust,ignore
    /// runner_builder.on_escalation(|escalation| {
    ///     tracing::error!(policy = %escalation.policy, "policy is down: {}", escalation.reason);
    ///     liveness_probe.fail();          // Kubernetes restarts the pod, which exits it
    /// })
    /// ```
    pub fn on_escalation<F>(mut self, hook: F) -> Self
    where
        F: Fn(&Escalation) + Send + Sync + 'static,
    {
        self.on_escalation = Arc::new(hook);
        self
    }

    /// Register a policy using a **closure** instead of a full [`Policy`] impl.
    ///
    /// This is the low-ceremony path for simple, single-aggregate reactions where
    /// writing a dedicated struct + `impl Policy` would be boilerplate:
    ///
    /// ```rust,ignore
    /// runner_builder
    ///     .register_policy_fn::<BankAccountEvent, _>(
    ///         "deposit_fee",
    ///         PolicySettings::new().starting_at(StartAt::Beginning),
    ///         |event| match &event.data {
    ///             BankAccountEvent::Deposited { amount } => vec![
    ///                 Dispatch::to::<FeeLedger>(ledger_id.clone(), ChargeFee { amount: amount * 0.01 })
    ///             ],
    ///             _ => vec![],
    ///         },
    ///     )
    /// ```
    ///
    /// The closure runs through the exact same runner machinery as a `Policy` impl:
    /// causation stamping, failure handling, batching, advisory lock, etc.
    pub fn register_policy_fn<E, F>(
        self,
        name: impl Into<String>,
        settings: PolicySettings,
        react: F,
    ) -> Self
    where
        E: replay::Event + 'static,
        F: Fn(&ObservedEvent<E>) -> Vec<Dispatch> + Send + Sync + 'static,
    {
        self.register_policy(
            ClosurePolicy {
                name: name.into(),
                react,
                _phantom: std::marker::PhantomData,
            },
            settings,
        )
    }

    pub fn build(self) -> PolicyRunner {
        PolicyRunner {
            cqrs: self.cqrs,
            pool: self.pool,
            policies: self.policies,
            executors: self.executors,
            notifications: self.notifications,
            heartbeat: self.heartbeat,
            replica_id: self.replica_id,
            supervision: self.supervision,
            on_escalation: self.on_escalation,
            drained: tokio::sync::Mutex::new(HashMap::new()),
        }
    }
}

/// Outcome of [`PolicyRunner::retry_dead_letter`] for the row it was called
/// for. The replay it reports on settled every row of that row's reaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeadLetterRetry {
    /// The row's command succeeded, or the aggregate now declines it with a
    /// `BusinessRuleViolation`, or the reaction no longer emits it: the row was
    /// archived into `discarded_dead_letters` (reason `retried`).
    Resolved,
    /// The row's command failed permanently again: an existing row was updated
    /// in place with **its own** fresh error and its retry count incremented; a
    /// command the reaction had not parked before got a row of its own, untried.
    /// Either way no row was duplicated, and the row stays retryable.
    StillFailing,
    /// Another writer parked this row's command while the replay ran — a
    /// delivery of the event, or another retry of the same reaction — so what is
    /// parked is not what this retry concluded, and is no staler than it. The
    /// row is left as that writer wrote it: settling it would archive a failure
    /// nobody retried, or overwrite it with an error from a replay that started
    /// earlier (funkode-io/replay#227). Retry again to act on what is parked now.
    Superseded,
    /// No dead-letter row matched the supplied id: nothing to do.
    NotFound,
}

/// Outcome of [`PolicyRunner::discard_dead_letter`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeadLetterDiscard {
    /// The dead-letter row was archived into `discarded_dead_letters` (reason
    /// `discarded`) — a soft delete, not a hard delete.
    Discarded,
    /// No dead-letter row matched the supplied id: nothing to do.
    NotFound,
}

/// Summary of a bulk [`PolicyRunner::retry_policy_dead_letters`] run, counted in
/// **reactions** — a reaction being one replay of one Policy's reaction to one
/// event, however many of its commands were parked.
///
/// The unit is stated in the field names because it differs from
/// [`PolicyStatus::dead_letter_count`](crate::PolicyStatus::dead_letter_count),
/// which counts parked commands (rows).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct DeadLetterRetrySummary {
    /// Reactions whose every parked row resolved and was archived (reason
    /// `retried`).
    pub reactions_resolved: usize,
    /// Reactions with at least one row still parked after the replay.
    pub reactions_still_failing: usize,
}

/// Native runner that drives registered policies against the event feed.
pub struct PolicyRunner {
    cqrs: Cqrs<PostgresEventStore>,
    pool: Pool<Postgres>,
    policies: Vec<Arc<RegisteredPolicy>>,
    executors: HashMap<TypeId, Arc<dyn AggregateExecutor>>,
    notifications: bool,
    /// How often the durable heartbeat is written, or `None` when the consumer
    /// turned it off.
    heartbeat: Option<Duration>,
    /// What the heartbeat writes in `led_by`.
    replica_id: Option<String>,
    supervision: WorkerSupervision,
    /// What the consumer does about a worker this runner has given up on.
    on_escalation: EscalationHook,
    /// Where each policy's search had got to at the end of the last [`Self::drain`].
    ///
    /// A daemon worker keeps this for its leadership term; a manual drain has no term, so
    /// the runner keeps it instead. Dropping it between calls would reset the rotation
    /// that shares a poll between its discovery sources, and a batch too small to divide
    /// between them would then hand every slot to the same source for ever
    /// (funkode-io/replay#231 review).
    drained: tokio::sync::Mutex<HashMap<String, PolicyProgress>>,
}

/// Handle for background policy tasks spawned by [`PolicyRunner::start_polling`].
pub struct PolicyRunnerDaemon {
    shutdown_tx: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
    stopped: StoppedWorkers,
    liveness: LivenessRegistry,
}

impl PolicyRunnerDaemon {
    /// Signal all policy tasks to stop and await their completion.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        for task in self.tasks {
            let _ = task.await;
        }
    }

    /// The workers the runner has given up on, in the order they stopped.
    ///
    /// A worker that dies outside its reaction is restarted (see
    /// [`WorkerSupervision`]); one that dies more often than its budget allows is
    /// named here instead. Nothing is reacting for those policies, and nothing in
    /// this process will start them again.
    ///
    /// Every entry here was also escalated through the hook
    /// ([`PolicyRunnerBuilder::on_escalation`]), which by default ends the
    /// process — so a consumer reads a non-empty list only after replacing that
    /// default with one that returns.
    pub fn stopped_workers(&self) -> Vec<StoppedWorker> {
        self.stopped.snapshot()
    }

    /// What each of this process's workers is doing: leading, standing by,
    /// restarting, stopped or unknown, with the instant each last finished a
    /// poll ([`WorkerLiveness`]).
    ///
    /// This is the [Liveness] axis — "is this worker running" — and it is known
    /// only here: it is published from memory by the workers themselves and is
    /// never derived from the operational tables. [`crate::PolicyStatusStore`]
    /// answers the other one, "is this Policy moving", from the tables alone.
    /// Neither implies the other: a [Standby] runs and advances nothing, and a
    /// [Leader] parked in front of a hole runs and advances nothing either.
    ///
    /// Every registered Policy appears, including one whose worker has not
    /// reached its first election ([`Liveness::Unknown`]). A Policy another
    /// replica leads is [`Liveness::StandingBy`] here — a standby is healthy and
    /// deliberately idle, and reading it as down is the false alarm this
    /// accessor exists to prevent.
    ///
    /// ```rust,ignore
    /// for worker in daemon.liveness() {
    ///     tracing::info!(
    ///         policy = %worker.policy,
    ///         liveness = %worker.liveness,
    ///         last_poll_secs = worker.last_polled_at.map(|at| at.elapsed().as_secs()),
    ///         "policy worker"
    ///     );
    /// }
    /// ```
    ///
    /// [Liveness]: https://github.com/funkode-io/replay/blob/main/CONTEXT.md#liveness
    /// [Standby]: https://github.com/funkode-io/replay/blob/main/CONTEXT.md#standby
    /// [Leader]: https://github.com/funkode-io/replay/blob/main/CONTEXT.md#leader
    pub fn liveness(&self) -> Vec<WorkerLiveness> {
        self.liveness.snapshot()
    }
}

// ── Worker supervision ───────────────────────────────────────────────────────

/// How the runner supervises a worker that dies for a reason the per-event path
/// cannot contain — a panic in the drain loop, in cursor I/O, in the feed read.
///
/// Such a death kills the worker task. The runner restarts it, which costs at
/// most a checkpoint's worth of re-delivery because the restarted worker resumes
/// from the last durable checkpoint. Restarting is bounded twice over:
///
/// - each attempt waits [`initial_backoff`](Self::initial_backoff), doubled per
///   restart already spent in the window and capped at
///   [`max_backoff`](Self::max_backoff);
/// - at most [`max_restarts`](Self::max_restarts) restarts are allowed within
///   [`restart_window`](Self::restart_window); past that the worker stays down,
///   is named by [`PolicyRunnerDaemon::stopped_workers`] and is escalated
///   ([`PolicyRunnerBuilder::on_escalation`]).
///
/// The budget is per worker, and so is everything a restart touches: it re-reads
/// one policy's cursor and no other's, and leadership is unaffected because the
/// advisory locks live on the process's shared lock-manager session rather than
/// on the worker task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WorkerSupervision {
    max_restarts: u32,
    window: Duration,
    initial_backoff: Duration,
    max_backoff: Duration,
}

impl Default for WorkerSupervision {
    /// Five restarts a minute, backing off from 100 ms to at most 30 s: a
    /// transient fault is ridden out without a human, while a worker dying from a
    /// defect stops within about a minute instead of hiding in a restart loop.
    fn default() -> Self {
        Self {
            max_restarts: 5,
            window: Duration::from_secs(60),
            initial_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(30),
        }
    }
}

impl WorkerSupervision {
    /// How many restarts are allowed within [`restart_window`](Self::restart_window).
    ///
    /// `0` disables restarting: the first death stops the worker for good.
    pub fn max_restarts(mut self, max_restarts: u32) -> Self {
        self.max_restarts = max_restarts;
        self
    }

    /// The sliding window the restart budget is counted over. A restart older
    /// than this stops counting against it, so a worker that dies once a day is
    /// restarted every day.
    ///
    /// `Duration::ZERO` means no window: restarts never age out, so
    /// [`max_restarts`](Self::max_restarts) bounds the worker's whole life. A
    /// zero window must not read as "every restart has already expired", which
    /// would make the budget unbounded.
    pub fn restart_window(mut self, window: Duration) -> Self {
        self.window = window;
        self
    }

    /// How long the runner waits before the first restart in a window; each
    /// further restart in the same window doubles it.
    pub fn initial_backoff(mut self, initial_backoff: Duration) -> Self {
        self.initial_backoff = initial_backoff;
        self
    }

    /// The ceiling the doubling backoff stops at.
    pub fn max_backoff(mut self, max_backoff: Duration) -> Self {
        self.max_backoff = max_backoff;
        self
    }

    /// Backoff before the `restarts`-th restart of the current window (1-based).
    fn backoff_for(&self, restarts: u32) -> Duration {
        let doublings = restarts.saturating_sub(1).min(31);
        self.initial_backoff
            .saturating_mul(1u32 << doublings)
            .min(self.max_backoff)
    }
}

/// A worker the runner has given up on: it died more often than its
/// [`WorkerSupervision`] budget allows, so it was not restarted again.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoppedWorker {
    /// The policy whose worker stopped. Nothing is reacting to its feed.
    pub policy: String,
    /// How many times it was restarted before the budget ran out.
    pub restarts: u32,
}

/// The stopped workers, shared by the supervisors that record one and the
/// [`PolicyRunnerDaemon`] a consumer reads them from.
///
/// Bounded by the number of registered policies: a supervisor records exactly
/// one entry, and then it is done.
#[derive(Clone, Default)]
struct StoppedWorkers(Arc<Mutex<Vec<StoppedWorker>>>);

impl StoppedWorkers {
    fn record(&self, policy: &str, restarts: u32) {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(StoppedWorker {
                policy: policy.to_string(),
                restarts,
            });
    }

    fn snapshot(&self) -> Vec<StoppedWorker> {
        self.0
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

// ── Escalation ───────────────────────────────────────────────────────────────

/// Exit code the default escalation hook ends the process with.
///
/// `EX_SOFTWARE` from `sysexits.h`: a defect in the service, told apart from the
/// `1` an ordinary error exit uses, so a crash-looping pod's exit code says which
/// of the two it is.
pub const ESCALATION_EXIT_CODE: i32 = 70;

/// A Policy whose worker is down for good, handed to the consumer's escalation
/// hook.
///
/// One per worker: a supervisor escalates once and then stops supervising, so a
/// hook that counts its invocations counts Policies.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Escalation {
    /// The Policy nothing is reacting for.
    pub policy: String,
    /// What the runner gave up on.
    pub reason: EscalationReason,
}

/// Why a worker is down for good.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum EscalationReason {
    /// The worker died with its [`WorkerSupervision`] budget already spent:
    /// `restarts` restarts had been made within the window, and this death is the
    /// one none was left for. `cause` is its panic message.
    ///
    /// The worker therefore died `restarts + 1` times, and `restarts` is `0` when
    /// the budget was `max_restarts(0)` and the first death was terminal.
    BudgetExhausted { restarts: u32, cause: String },
    /// The lock manager that elects this worker has itself stopped for good, so
    /// the worker can never be elected again. No restart was attempted: there is
    /// nothing in this process left to restart it into.
    Abandoned,
}

impl EscalationReason {
    /// Restarts spent before giving up — zero when none was ever attempted.
    fn restarts(&self) -> u32 {
        match self {
            Self::BudgetExhausted { restarts, .. } => *restarts,
            Self::Abandoned => 0,
        }
    }
}

impl fmt::Display for EscalationReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BudgetExhausted { restarts, cause } => write!(
                f,
                "the worker died with its restart budget spent \
                 ({restarts} used in the window); cause: {cause}"
            ),
            Self::Abandoned => {
                f.write_str("the lock manager that elects the worker has stopped for good")
            }
        }
    }
}

/// What a consumer does about a Policy the runner has given up on.
///
/// Runs on the supervisor's task, after the stop has been recorded, so it may
/// end the process without losing the report.
type EscalationHook = Arc<dyn Fn(&Escalation) + Send + Sync>;

/// End the process, which is the only outcome that frees the Policy.
///
/// Leadership is held by this process's lock-manager session, not by the worker
/// task ([ADR-0008]), so a stopped worker's replica keeps the advisory lock and
/// no [Standby] takes over. Exiting drops the session, which releases the lock.
///
/// [ADR-0008]: https://github.com/funkode-io/replay/blob/main/docs/adr/0008-policy-runner-shared-connection-leadership.md
/// [Standby]: https://github.com/funkode-io/replay/blob/main/CONTEXT.md#standby
fn exit_the_process() -> EscalationHook {
    Arc::new(|escalation: &Escalation| {
        tracing::error!(
            policy = %escalation.policy,
            reason = %escalation.reason,
            exit_code = ESCALATION_EXIT_CODE,
            "escalating: exiting so this process releases the policy's advisory lock \
             and a standby replica can take it over"
        );
        std::process::exit(ESCALATION_EXIT_CODE);
    })
}

/// Why a supervised task came back of its own accord.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Stop {
    /// It was asked to stop, or the daemon that owns it is gone. Nothing to
    /// supervise: this is the end of the line for that task.
    Shutdown,
    /// Something it depends on has stopped for good — for a worker, the lock
    /// manager that elects it. Restarting cannot help, so the stop is published
    /// rather than retried.
    Abandoned,
}

/// What the supervisor does about a worker that just died.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum RestartDecision {
    /// Restart after `backoff`; this is restart number `restarts` in the window.
    Restart { backoff: Duration, restarts: u32 },
    /// The budget is spent: `restarts` restarts already happened in this window
    /// and the worker stays down.
    Exhausted { restarts: u32 },
}

/// One worker's restart budget: the restarts it has spent inside the current
/// window.
///
/// Bounded by `max_restarts` entries — the budget is declared exhausted rather
/// than recording one past it — and entries are dropped as they age out of the
/// window.
struct RestartBudget {
    supervision: WorkerSupervision,
    spent: VecDeque<Instant>,
}

impl RestartBudget {
    fn new(supervision: WorkerSupervision) -> Self {
        Self {
            supervision,
            spent: VecDeque::new(),
        }
    }

    /// Charge a death that happened at `now` to the budget.
    fn record_death(&mut self, now: Instant) -> RestartDecision {
        while !self.supervision.window.is_zero()
            && self
                .spent
                .front()
                .is_some_and(|spent| now.duration_since(*spent) >= self.supervision.window)
        {
            self.spent.pop_front();
        }

        let already_spent = self.spent.len() as u32;
        if already_spent >= self.supervision.max_restarts {
            return RestartDecision::Exhausted {
                restarts: already_spent,
            };
        }

        self.spent.push_back(now);
        let restarts = already_spent + 1;
        RestartDecision::Restart {
            backoff: self.supervision.backoff_for(restarts),
            restarts,
        }
    }
}

impl PolicyRunner {
    /// Begin configuring a runner bound to `cqrs` (and its connection pool).
    pub fn builder(cqrs: Cqrs<PostgresEventStore>) -> PolicyRunnerBuilder {
        let pool = cqrs.store().pool().clone();
        PolicyRunnerBuilder {
            cqrs,
            pool,
            policies: Vec::new(),
            executors: HashMap::new(),
            notifications: true,
            heartbeat: Some(HEARTBEAT_CADENCE),
            replica_id: std::env::var("HOSTNAME").ok().filter(|id| !id.is_empty()),
            supervision: WorkerSupervision::default(),
            on_escalation: exit_the_process(),
        }
    }

    /// Manually drain every registered policy once.
    ///
    /// For each policy: find the streams it is behind on, read each one's events past the
    /// place it has reached, `react`, execute the returned dispatches through [`Cqrs`],
    /// and advance that stream's place — one event at a time, advancing only after that
    /// event's commands have committed (at-least-once delivery; reactions must be
    /// idempotent).
    ///
    /// **The runner remembers where each policy's search had got to**, so repeated calls
    /// behave like a daemon's successive polls: the streams one call could not finish are
    /// looked at by the next, and the turn that shares a poll between the sweep and the
    /// reconciliation carries on rather than restarting. A fresh runner starts the search
    /// afresh — from what is stored, never from the beginning.
    ///
    /// Unlike a daemon's poll, this one always compares every stream's head with the
    /// policy's places rather than doing it on a cadence, so a place moved by hand is
    /// found by the very next call.
    ///
    /// Returns how many dispatches **committed** across all policies — a progress
    /// signal, not an audit. A delivery that fails does not contribute its
    /// partial work: a permanent failure counts only the dispatches that
    /// committed before it, an exhausted retry budget counts nothing for that
    /// event, and a contained panic likewise counts nothing. The cursor, not this
    /// number, is what records what was processed.
    pub async fn drain(&self) -> Result<usize, replay::Error> {
        let mut total = 0;
        for policy in &self.policies {
            total += self.drain_policy(policy.as_ref()).await?;
        }
        Ok(total)
    }

    /// Re-run a parked dead letter's reaction against current state, settling
    /// every row that reaction parked.
    ///
    /// Loads the dead-letter row `id`, looks up its owning policy by name, and
    /// replays the reaction of the event it pinned **once** through the same
    /// [`Cqrs`] path the live drain uses — taking no advisory lock and never
    /// touching `policy_cursors`. A reaction is `(policy_name, event_id)`, so
    /// the rows settled are every row that pair parked, not only `id`: the by-id
    /// and the bulk path must not conclude different things from one replay
    /// (ADR-0021).
    ///
    /// The replay **carries on past a failure**, as the drain does, and each row
    /// is then settled by its own command's outcome:
    ///
    /// - the command succeeds, is declined with a `BusinessRuleViolation`, or is
    ///   no longer emitted by the reaction at all → the row is **archived** into
    ///   `discarded_dead_letters` with reason `retried`.
    /// - it fails permanently again → the row is **updated in place** with its
    ///   own fresh error; no second row is ever inserted, and the row stays
    ///   retryable.
    /// - it **hangs** → it is abandoned on the same dispatch timeout the drain
    ///   applies, so a bulk retry of parked hangs returns instead of wedging the
    ///   operator's call. The row is updated to [`TIMEOUT_ERROR_KIND`], without
    ///   the drain's re-attempts: an operator retries.
    /// - the reaction **panics** → the dispatches that concluded before it still
    ///   settle their own rows; every other row of the group is updated to
    ///   [`PANIC_ERROR_KIND`] and the panic's message. The panic never reaches
    ///   the caller, so a bulk retry continues with the next reaction.
    ///
    /// A row that names no dispatch — parked before the identity migration, or
    /// parked for a panic in `react` itself — is settled by the replay as a
    /// whole: archived when nothing failed, re-parked with the replay's first
    /// failure otherwise.
    ///
    /// A dispatch that **fails and has no row** gets one. The replay runs the
    /// policy as its code defines it now, so a deploy between the park and the
    /// retry can make the reaction fail on a command it never parked; the retry
    /// parks it exactly as the drain would, and reports the reaction still
    /// failing. This is the only insert a retry makes: a row for a command
    /// already parked is never duplicated — and two concurrent retries of the
    /// same reaction settle it the same way, since the table now keys a parked
    /// command and the second insert leaves the first writer's row as it stands,
    /// reported [`DeadLetterRetry::Superseded`] (ADR-0024).
    ///
    /// Settling a row that already existed stamps its `retry_count` and
    /// `last_retried_at`, the archived copy included, so what has already been
    /// tried survives the error message being overwritten. A row this retry
    /// *parks* carries neither: it is a first parking, born untried exactly as
    /// the drain's rows are, and the column counts retries made on a row, not
    /// executions of a command.
    ///
    /// Re-execution safety comes from the target's idempotent command shape (the
    /// command carries a key the rule reads off the triggering event, ADR-0027) plus
    /// the optimistic-concurrency check in [`Cqrs::execute`], so retrying an
    /// already-applied reaction is a no-op.
    ///
    /// Returns the outcome for the row `id` names, and
    /// [`DeadLetterRetry::NotFound`] when no row matches it. Returns a clear
    /// error (never panics) when the dead letter's policy is not registered on
    /// this runner, when a reproduced dispatch targets an aggregate whose
    /// services are not registered, or when the pinned triggering event can no
    /// longer be found.
    pub async fn retry_dead_letter(&self, id: i64) -> Result<DeadLetterRetry, replay::Error> {
        const OPERATION: &str = "retry_dead_letter";

        let Some(row) = sqlx::query(
            "SELECT policy_name, global_position, event_id \
             FROM policy_dead_letters WHERE id = $1",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(crate::db_error)?
        else {
            return Ok(DeadLetterRetry::NotFound);
        };

        let reaction = ParkedReaction {
            policy_name: row.get("policy_name"),
            global_position: row.get("global_position"),
            event_id: row.get("event_id"),
        };
        let settled = self.retry_reaction(reaction, OPERATION, Some(id)).await?;

        // The row `id` names is normally in the group this replay settled; it is
        // absent when a concurrent discard took it out between the SELECT above
        // and the group read, which is the same nothing-to-do as an absent id.
        Ok(settled.asked_about.unwrap_or(DeadLetterRetry::NotFound))
    }

    /// Replay one reaction and settle every row it parked.
    ///
    /// The group is read here rather than by the caller, so the by-id and the
    /// bulk path settle exactly the same set. `asked_about` names the row a
    /// by-id retry wants the outcome of: the group is **folded** rather than
    /// returned, since it is not a set worth holding (funkode-io/replay#228).
    async fn retry_reaction(
        &self,
        reaction: ParkedReaction,
        operation: &'static str,
        asked_about: Option<i64>,
    ) -> Result<ReactionSettlement, replay::Error> {
        // The group as it stands before anything is re-executed, in a fixed
        // number of bytes ([`GroupDigest`]). A reaction whose every row was
        // discarded between the caller's read and this one has nothing left to
        // settle, and replaying it would dispatch commands on behalf of rows an
        // operator has just retired.
        let before = group_digest(&self.pool, &reaction).await?;
        if before.rows == 0 {
            return Ok(ReactionSettlement::default());
        }

        let ParkedReaction {
            policy_name,
            global_position,
            event_id,
        } = &reaction;
        let (policy_name, global_position, event_id) =
            (policy_name.as_str(), *global_position, *event_id);

        let policy = self
            .policies
            .iter()
            .find(|p| p.name() == policy_name)
            .ok_or_else(|| {
                replay::Error::invalid_input(
                    "no registered policy matches the dead letter's policy_name",
                )
                .with_operation(operation)
                .with_context("policy", policy_name)
            })?;

        let raw = load_event_by_id(&self.pool, event_id)
            .await?
            .ok_or_else(|| {
                replay::Error::not_found("triggering event for dead letter no longer exists")
                    .with_operation(operation)
                    .with_context("policy", policy_name)
                    .with_context("event_id", event_id)
            })?;

        // Reproduce and execute every dispatch the reaction now yields, carrying
        // on past a failure exactly as the forward drain does: stopping at the
        // first one is what used to lose the later commands' errors. A missing
        // executor is an operator misconfiguration (a clear error to the caller),
        // distinct from a dispatch that executes but fails permanently (which
        // re-parks its row in place).
        //
        // The concluded dispatches live outside the `catch_unwind` for the reason
        // the drain's do (ADR-0016): a panicking command handler settles the
        // replay by unwinding, and what its siblings concluded first must settle
        // their rows rather than vanish with the stack.
        let concluded = ReplayedDispatches::default();
        let attempt = AssertUnwindSafe(async {
            let delivery = Delivery {
                cqrs: &self.cqrs,
                pool: &self.pool,
                executors: &self.executors,
                policy_name,
                dispatch_timeout: resolve_dispatch_timeout(policy.settings()),
            };
            for (ordinal, dispatch) in policy.react_erased(&raw).into_iter().enumerate() {
                if !self.executors.contains_key(&dispatch.target()) {
                    return Err(replay::Error::invalid_input(
                        "no services registered for the aggregate targeted by a policy dispatch",
                    )
                    .with_operation(operation)
                    .with_context("policy", policy_name)
                    .with_context("aggregate", dispatch.aggregate_name()));
                }

                let identity = DispatchIdentity::of(ordinal, &dispatch);
                concluded.dispatching(identity.clone());
                let outcome = match delivery
                    .execute_dispatch_within(global_position, &raw, dispatch)
                    .await
                {
                    Ok(()) => None,
                    // Stale reaction: the aggregate now declines it. A clean
                    // resolution, so the row it belongs to is archived.
                    Err(declined) if declined.declined() => None,
                    Err(failure) => Some(Settlement::of(&failure)),
                };
                concluded.push(identity, outcome);
            }
            Ok(())
        })
        .catch_unwind()
        .await;

        let replay = match attempt {
            Ok(result) => {
                result?;
                Replay::Ran(concluded.take(None))
            }
            Err(payload) => {
                let message = panic_message(&*payload);
                tracing::error!(
                    policy   = %policy_name,
                    event_id = %event_id,
                    global_position,
                    panic    = %message,
                    "retried policy reaction panicked; every dead letter it did not \
                     resolve first stays parked"
                );
                Replay::Panicked {
                    concluded: concluded.take(Some(&message)),
                    message,
                }
            }
        };

        self.settle(&reaction, before, replay, asked_about).await
    }

    /// Settle each row of a reaction's group with what the replay concluded for
    /// **its** command, and park what failed with no row to settle.
    ///
    /// One transaction, so a database error part-way through leaves the group as
    /// the replay found it. Without that, a row archived early and an insert that
    /// then failed would take a still-failing command out of the table with
    /// nothing put back: the drain is long past the event and would never park it
    /// again.
    ///
    /// The group is settled a page at a time and never held whole
    /// (funkode-io/replay#228), so what guards it against a writer that moved it
    /// is not a per-row version but the pair a snapshot and a digest make: the
    /// [digest](GroupDigest) `before` catches what moved while the replay ran,
    /// and the transaction's snapshot catches what moves while it settles. A
    /// group that moved settles **nothing** — every row of it is reported
    /// [`DeadLetterRetry::Superseded`], which is one reaction still failing
    /// (ADR-0025).
    async fn settle(
        &self,
        reaction: &ParkedReaction,
        before: GroupDigest,
        replay: Replay,
        asked_about: Option<i64>,
    ) -> Result<ReactionSettlement, replay::Error> {
        let mut replay = replay;
        let moved = match self
            .settle_unmoved(reaction, before, &mut replay, asked_about)
            .await
        {
            Ok(Some(settled)) => return Ok(settled),
            // The group was already something else when the settlement read it.
            Ok(None) => "before the settlement began",
            // Postgres saw a writer move a row out from under the snapshot
            // before this transaction could, and refused it the inconsistent
            // read: the same conclusion, reached by the server — except when
            // what moved the group took the last row with it. Then there is
            // nothing left to settle and nowhere else for this replay's
            // failures to be recorded, so it is attempted once more against the
            // group as it now stands. A rolled-back settlement claimed nothing.
            Err(error) if error.kind() == replay::ErrorKind::Conflict => {
                let emptied = group_digest(&self.pool, reaction).await?;
                if emptied.rows == 0 {
                    replay.forget_claims();
                    match self
                        .settle_unmoved(reaction, emptied, &mut replay, asked_about)
                        .await
                    {
                        Ok(Some(settled)) => return Ok(settled),
                        // Rows again: it is a moved group after all.
                        Ok(None) => {}
                        Err(again) if again.kind() == replay::ErrorKind::Conflict => {}
                        Err(again) => return Err(again),
                    }
                }
                "while it was settling"
            }
            Err(error) => return Err(error),
        };

        tracing::info!(
            policy   = %reaction.policy_name,
            event_id = %reaction.event_id,
            moved,
            "a reaction moved while its retry replayed it; settling none of its \
             rows, so the writer that moved it keeps what it wrote"
        );
        Ok(ReactionSettlement {
            any_still_failing: true,
            asked_about: match asked_about {
                Some(id) => Some(unsettled(&self.pool, id).await?),
                None => None,
            },
            ..Default::default()
        })
    }

    /// The settlement itself, on one snapshot of a group nobody else is writing:
    /// `Some` when it ran, `None` when the group had already moved.
    async fn settle_unmoved(
        &self,
        reaction: &ParkedReaction,
        before: GroupDigest,
        replay: &mut Replay,
        asked_about: Option<i64>,
    ) -> Result<Option<ReactionSettlement>, replay::Error> {
        let mut settled = ReactionSettlement::default();
        let (mut tx, locked) = begin_settlement(&self.pool, reaction).await?;
        // A group emptied under the replay has no row to settle and no row to
        // mismatch about: every failure the replay found is one nothing speaks
        // for, and parking those is what the loop below is. Leaving it to "the
        // next retry" would leave nothing to retry — a discarded reaction is not
        // enumerable, and the drain is long past the event.
        if locked != before && locked.rows > 0 {
            return Ok(None);
        }

        // Which rows of the group name each dispatch the replay ran, read once
        // under the lock: a count over a page would answer for the page
        // ([`Replay::settlement_for`]).
        let naming = count_rows_naming(&mut tx, reaction, replay.concluded()).await?;

        for phase in [GroupPhase::Naming, GroupPhase::Nameless] {
            let mut after = 0;
            loop {
                let rows = load_parked_page(&mut tx, reaction, phase, after).await?;
                let Some(last) = rows.last().map(|row| row.id) else {
                    break;
                };
                let exhausted = (rows.len() as i64) < RETRY_ROW_PAGE_SIZE;
                after = last;

                for row in rows {
                    let outcome = match row.identity.as_ref() {
                        // The command this row was parked for ran again: its own
                        // outcome settles it.
                        Some(identity) => {
                            replay.settlement_for(identity, rows_naming(&naming, identity))
                        }
                        // The row names no command, so only the replay as a
                        // whole can settle it.
                        None => replay.verdict(),
                    };

                    // The group is locked and was verified unchanged, so every
                    // one of these statements matches its row; `unsettled` is
                    // the defence in depth for the day that stops being true.
                    let settlement = match outcome {
                        None => {
                            if move_dead_letter_to_archive(&mut *tx, row.id, "retried").await? {
                                DeadLetterRetry::Resolved
                            } else {
                                unsettled(&mut *tx, row.id).await?
                            }
                        }
                        Some(Settlement {
                            error_kind,
                            error_message,
                        }) => {
                            if re_park_dead_letter(&mut *tx, row.id, &error_kind, &error_message)
                                .await?
                            {
                                DeadLetterRetry::StillFailing
                            } else {
                                unsettled(&mut *tx, row.id).await?
                            }
                        }
                    };
                    settled.record(row.id, settlement, asked_about);
                }

                if exhausted {
                    break;
                }
            }
        }

        // A replay is the drain's equal, so it parks what failed: a dispatch
        // that failed and no row spoke for is a command this reaction did not
        // park before — the policy's code changed between the park and the
        // retry, which is the whole point of replaying the reaction as defined
        // now (ADR-0007). Without this it would be executed, fail, and leave
        // nothing behind while the retry reported the reaction resolved.
        //
        // The locks cover the rows that existed; this one is a row that does
        // not, so what guards it is the key itself: a command that acquired a
        // row while the replay ran has one from a writer whose error is no
        // staler than this one's, and the insert leaves it as it stands.
        for unclaimed in replay.unclaimed_failures() {
            let parked = write_dead_letter(
                &mut *tx,
                DeadLetterWrite {
                    policy_name: &reaction.policy_name,
                    global_position: reaction.global_position,
                    event_id: reaction.event_id,
                    identity: Some(&unclaimed.identity),
                    error_kind: &unclaimed.settlement.error_kind,
                    error_message: &unclaimed.settlement.error_message,
                    parking: Parking::Retry,
                },
            )
            .await?;
            tracing::warn!(
                policy    = %reaction.policy_name,
                event_id  = %reaction.event_id,
                aggregate = unclaimed.identity.aggregate_name,
                target    = %unclaimed.identity.target_stream_id,
                command   = unclaimed.identity.command_name,
                error     = %unclaimed.settlement.error_message,
                dead_letter_id = parked.id,
                superseded = parked.conflicted,
                "a retried reaction failed on a command it had not parked; parking it, \
                 or leaving the row another writer parked for it meanwhile"
            );
            settled.record(
                parked.id,
                if parked.conflicted {
                    DeadLetterRetry::Superseded
                } else {
                    DeadLetterRetry::StillFailing
                },
                asked_about,
            );
        }

        tx.commit().await.map_err(crate::db_error)?;

        Ok(Some(settled))
    }

    /// Discard a parked dead letter without re-running its reaction.
    ///
    /// Pure bookkeeping: **archives** the dead-letter row `id` into
    /// `discarded_dead_letters` (reason `discarded`) in a single statement —
    /// a soft delete that removes it from the active set the status read model
    /// scans while keeping the audit trail. Unlike
    /// [`retry_dead_letter`](Self::retry_dead_letter) it performs **no**
    /// `react`, executes **no** command, and therefore produces **no** new
    /// aggregate event. It takes no advisory lock and never touches
    /// `policy_cursors`. Once the row leaves the active table,
    /// [`PolicyStatusStore::list`] reports the policy leaving `Degraded`
    /// exactly as it does after a successful retry.
    ///
    /// Returns [`DeadLetterDiscard::NotFound`] when no active row matches `id`
    /// — a defined no-op, never a panic.
    pub async fn discard_dead_letter(&self, id: i64) -> Result<DeadLetterDiscard, replay::Error> {
        if move_dead_letter_to_archive(&self.pool, id, "discarded").await? {
            Ok(DeadLetterDiscard::Discarded)
        } else {
            Ok(DeadLetterDiscard::NotFound)
        }
    }

    /// Bulk-retry every reaction `policy_name` has parked, oldest-first.
    ///
    /// The unit is the **reaction**, not the row: the policy's parked rows are
    /// grouped by the reaction they came from — `(policy_name, event_id)`, since
    /// the event a reaction is a pure function of already identifies it
    /// (ADR-0003) — and each reaction is replayed **once** however many of its
    /// commands are parked, in ascending `global_position` order. Every row of a
    /// group is then settled from that one replay exactly as
    /// [`retry_dead_letter`](Self::retry_dead_letter) defines; this method adds
    /// only enumeration and ordering. It takes no advisory lock and never
    /// touches `policy_cursors`.
    ///
    /// Returns a [`DeadLetterRetrySummary`] counting **reactions**, as this run
    /// settled them: one whose every row was archived is resolved, one with any
    /// row still parked is still failing. `reactions_still_failing == 0` says
    /// the run settled everything it read, not that the table is empty now — a
    /// delivery parking a command while the run walks, or one statement after it
    /// returns, is a row the count cannot have seen. `PolicyStatus` is what
    /// answers "is anything parked", and the next bulk retry picks it up. A
    /// policy with no parked dead letters is a clean no-op (a zero summary), and
    /// so is a reaction whose rows were all discarded concurrently: it is
    /// skipped without being replayed.
    ///
    /// The backlog this drains is the one an outage leaves — one parked reaction
    /// per event the downstream refused — so the enumeration is **paged**:
    /// [`RETRY_PAGE_SIZE`] reactions are read at a time, settled, and the next
    /// page read from the last `(global_position, event_id)` of the previous
    /// one. Nothing here grows with the size of the backlog. A page settles
    /// before the next is read, so a reaction re-parked by this run is behind
    /// the keyset and is not replayed twice.
    /// The walk is bounded at both ends: a page at a time, and never past the
    /// last reaction parked when the call began. Retries take no advisory lock,
    /// so the policy's worker keeps parking reactions *above* the keyset while
    /// this runs — without a high-water mark an operator's call would chase a
    /// failing policy indefinitely instead of draining what they asked it to.
    pub async fn retry_policy_dead_letters(
        &self,
        policy_name: &str,
    ) -> Result<DeadLetterRetrySummary, replay::Error> {
        const OPERATION: &str = "retry_policy_dead_letters";

        let mut summary = DeadLetterRetrySummary::default();
        let Some(backlog) = last_parked_position(&self.pool, policy_name).await? else {
            return Ok(summary);
        };
        let mut after = ReactionKeyset::start();

        loop {
            let page = load_parked_reactions(&self.pool, policy_name, after, backlog).await?;
            let Some(last) = page.last().map(ReactionKeyset::after) else {
                return Ok(summary);
            };
            after = last;

            for reaction in page {
                let settled = self.retry_reaction(reaction, OPERATION, None).await?;

                if settled.any_still_failing {
                    summary.reactions_still_failing += 1;
                } else if settled.any_resolved {
                    summary.reactions_resolved += 1;
                }
                // A group whose every row was taken concurrently settled
                // nothing: there is no reaction left to count either way.
            }
        }
    }

    /// Start one long-lived worker task per registered policy, backed by two
    /// shared per-process connections rather than two connections per policy.
    ///
    /// The steady-state connection footprint is **constant** in the number of
    /// policies `P`, not `O(2P)`, which is what previously exhausted a shared
    /// SQLx pool when many policies were registered:
    ///
    /// - **One shared lock-manager connection** holds every policy's
    ///   `pg_advisory_lock`. Postgres session advisory locks are per-session and a
    ///   single session may hold many distinct lock keys, so one pinned connection
    ///   elects leadership for all `P` policies. Each key is tried independently,
    ///   so **different policies may still lead on different instances**, and
    ///   **exactly one** instance leads each policy (single-consumer correctness
    ///   for the ordered global feed). The manager publishes per-policy leadership
    ///   on a [`watch`] channel that each worker observes.
    /// - **One shared `PgListener` connection** receives `NOTIFY` and fans each
    ///   wakeup out to every worker in-process via a [`broadcast`] channel, instead
    ///   of one listener connection per policy. A missed or lagged broadcast is
    ///   harmless: the worker still wakes on the poll `interval` (polling is the
    ///   correctness baseline; `NOTIFY` is only a latency hint).
    ///
    /// **Leader failover is automatic**: the advisory locks are session-scoped, so
    /// when a leader process dies its lock-manager connection drops and releases
    /// all of its locks at once; a standby's lock manager acquires them on its next
    /// tick and each worker resumes from its stored cursor. The trade-off relative
    /// to the previous per-policy design is coarser failover granularity — a single
    /// instance's policies fail over together — which is acceptable because each
    /// policy is still recovered independently and exactly once.
    ///
    /// Use [`PolicyRunnerDaemon::shutdown`] to stop all tasks cleanly. Shutdown
    /// releases the held advisory locks so a standby can take over without waiting
    /// for a TCP-level session timeout.
    pub fn start_polling(&self, interval: Duration) -> PolicyRunnerDaemon {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut tasks = Vec::with_capacity(self.policies.len() + 2);
        let stopped_workers = StoppedWorkers::default();
        let liveness = LivenessRegistry::default();
        // One answer per process about the consumer's schema, shared across
        // restarts of the beat task.
        let heartbeat_columns = HeartbeatColumns::default();

        // ── Shared NOTIFY listener: one connection, broadcast fan-out ─────────
        // A single PgListener receives every append NOTIFY and rebroadcasts it to
        // all worker tasks. Workers that lag simply fall back to interval polling.
        let wake_tx = if self.notifications {
            let (wake_tx, _) = broadcast::channel::<()>(16);
            let pool = self.pool.clone();
            let listener_shutdown_rx = shutdown_rx.clone();
            let tx = wake_tx.clone();
            tasks.push(tokio::spawn(supervise(
                SupervisedTask {
                    kind: "notify listener",
                    policy: None,
                    stopped: stopped_workers.clone(),
                    liveness: None,
                    on_escalation: Arc::clone(&self.on_escalation),
                },
                self.supervision,
                shutdown_rx.clone(),
                move || {
                    tokio::spawn(run_notify_listener(
                        pool.clone(),
                        tx.clone(),
                        listener_shutdown_rx.clone(),
                        interval,
                    ))
                },
            )));
            Some(wake_tx)
        } else {
            None
        };

        // ── Shared lock manager: one connection holds every policy's lock ─────
        // Per-policy leadership is published on a `watch<bool>` each worker
        // observes. The manager keeps retrying keys it does not yet hold so a
        // standby takes over when a current leader releases on shutdown.
        let mut leadership_rx: HashMap<String, watch::Receiver<bool>> = HashMap::new();
        let mut leadership: Vec<(String, watch::Sender<bool>)> =
            Vec::with_capacity(self.policies.len());
        for policy in &self.policies {
            let name = policy.name().to_string();
            let (l_tx, l_rx) = watch::channel(false);
            leadership_rx.insert(name.clone(), l_rx);
            leadership.push((name, l_tx));
        }
        // The heartbeat beats for the Policies whose advisory lock this process
        // holds, which is what these channels say. Read from the same source the
        // workers are elected by, so a beat cannot claim a leadership a worker
        // does not have — including for a worker that has stopped while its
        // replica still holds the lock, the half-dead case worth reporting.
        let leadership_for_heartbeat: Arc<Vec<(String, watch::Receiver<bool>)>> = Arc::new(
            leadership_rx
                .iter()
                .map(|(name, rx)| (name.clone(), rx.clone()))
                .collect(),
        );

        {
            // The senders live here rather than inside the task, so a manager
            // that dies does not take every worker's leadership channel with it:
            // a restarted manager publishes on the same channels, and the
            // workers simply see leadership go false and come back.
            let leadership = Arc::new(leadership);
            let pool = self.pool.clone();
            let lock_shutdown_rx = shutdown_rx.clone();
            tasks.push(tokio::spawn(supervise(
                SupervisedTask {
                    kind: "lock manager",
                    policy: None,
                    stopped: stopped_workers.clone(),
                    liveness: None,
                    on_escalation: Arc::clone(&self.on_escalation),
                },
                self.supervision,
                shutdown_rx.clone(),
                move || {
                    tokio::spawn(run_lock_manager(
                        pool.clone(),
                        Arc::clone(&leadership),
                        lock_shutdown_rx.clone(),
                        interval,
                    ))
                },
            )));
        }

        // ── Per-policy supervised workers ─────────────────────────────────────
        // Every policy gets a supervisor that owns its worker task and restarts
        // it when it dies for a reason the per-event path cannot contain. The
        // supervisors are independent of one another, and none of them holds a
        // lock: a restart re-reads one policy's cursor and leaves every other
        // policy — and this process's leadership — untouched.
        for policy in &self.policies {
            let name = policy.name().to_string();
            let leader_rx = leadership_rx
                .remove(&name)
                .expect("every policy has a leadership channel");

            let worker = PolicyWorker {
                policy: Arc::clone(policy),
                cqrs: self.cqrs.clone(),
                pool: self.pool.clone(),
                executors: self.executors.clone(),
                shutdown_rx: shutdown_rx.clone(),
                leader_rx,
                wake_tx: wake_tx.clone(),
                interval,
                liveness: liveness.register(&name),
                name,
            };

            tasks.push(tokio::spawn(supervise(
                SupervisedTask {
                    kind: "policy worker",
                    policy: Some(worker.name.clone()),
                    stopped: stopped_workers.clone(),
                    liveness: Some(worker.liveness.clone()),
                    on_escalation: Arc::clone(&self.on_escalation),
                },
                self.supervision,
                shutdown_rx.clone(),
                move || tokio::spawn(worker.clone().run()),
            )));
        }

        // ── The durable heartbeat ─────────────────────────────────────────────
        // A sibling of the workers, not a layer above them: it reads the same
        // registry a consumer reads in-process and writes it out on a fixed
        // cadence. It has to be a task of its own because a worker awaiting a
        // hung dispatch cannot write anything — which is the case the beat is
        // there for.
        if let Some(cadence) = self.heartbeat {
            // Half a beat: long enough to outlast a checkpoint's single UPDATE,
            // short enough that a skipped tick is never a late one.
            let beat_lock_wait = (cadence / 2).min(HEARTBEAT_LOCK_WAIT);
            let pool = self.pool.clone();
            let liveness = liveness.clone();
            let leadership = leadership_for_heartbeat;
            let columns = heartbeat_columns.clone();
            let replica_id = self.replica_id.clone();
            let beat_shutdown_rx = shutdown_rx.clone();
            tasks.push(tokio::spawn(supervise(
                SupervisedTask {
                    kind: "heartbeat",
                    policy: None,
                    stopped: stopped_workers.clone(),
                    liveness: None,
                    on_escalation: Arc::clone(&self.on_escalation),
                },
                self.supervision,
                shutdown_rx.clone(),
                move || {
                    tokio::spawn(run_heartbeat(
                        pool.clone(),
                        liveness.clone(),
                        Arc::clone(&leadership),
                        columns.writer(replica_id.clone(), beat_lock_wait),
                        beat_shutdown_rx.clone(),
                        cadence,
                    ))
                },
            )));
        }

        PolicyRunnerDaemon {
            shutdown_tx,
            tasks,
            stopped: stopped_workers,
            liveness,
        }
    }

    async fn drain_policy(&self, policy: &RegisteredPolicy) -> Result<usize, replay::Error> {
        let name = policy.name().to_string();
        // Held across the drain: two manual drains of one policy at once would each work
        // from the other's stale search state, and the second would undo the first's turn.
        let mut drained = self.drained.lock().await;
        let progress = match drained.entry(name.clone()) {
            Entry::Occupied(kept) => kept.into_mut(),
            Entry::Vacant(first) => {
                first.insert(PolicyProgress::load(&self.pool, &name, policy.start_at()).await?)
            }
        };

        // A manual drain always pays for the frontier scan, whatever the cadence says. The
        // cadence bounds what a daemon's tight polling loop spends on a scan it mostly
        // does not need; a call made by hand is one poll and has to be a whole one — an
        // operator who moves a place and drains expects that drain to find it.
        progress.reconcile_now();

        let max_depth = resolve_max_depth(policy.settings());
        drain_policy_once(
            &self.cqrs,
            &self.pool,
            &self.executors,
            policy,
            progress,
            max_depth,
            &mut Reporting { narration: None },
        )
        .await
    }
}

/// Receive every append `NOTIFY` on one connection and fan it out to the workers.
///
/// A wakeup is a latency hint, never a correctness requirement: a worker that
/// misses one still drains on its poll interval, which is why every failure here
/// is a reconnect rather than an error anybody has to see.
async fn run_notify_listener(
    pool: Pool<Postgres>,
    wake_tx: broadcast::Sender<()>,
    mut shutdown_rx: watch::Receiver<bool>,
    interval: Duration,
) -> Stop {
    'reconnect: loop {
        if *shutdown_rx.borrow() {
            return Stop::Shutdown;
        }

        let mut listener = match sqlx::postgres::PgListener::connect_with(&pool).await {
            Ok(mut l) => match l.listen(REPLAY_NOTIFY_CHANNEL).await {
                Ok(()) => {
                    tracing::debug!(
                        channel = REPLAY_NOTIFY_CHANNEL,
                        "shared LISTEN active; fanning NOTIFY to workers"
                    );
                    l
                }
                Err(error) => {
                    tracing::warn!(
                        error = %error,
                        "shared LISTEN setup failed; workers fall back to polling"
                    );
                    tokio::select! {
                        _ = shutdown_rx.changed() => return Stop::Shutdown,
                        _ = tokio::time::sleep(interval) => {}
                    }
                    continue 'reconnect;
                }
            },
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    "shared PgListener connect failed; workers fall back to polling"
                );
                tokio::select! {
                    _ = shutdown_rx.changed() => return Stop::Shutdown,
                    _ = tokio::time::sleep(interval) => {}
                }
                continue 'reconnect;
            }
        };

        loop {
            tokio::select! {
                changed = shutdown_rx.changed() => {
                    if changed.is_err() || *shutdown_rx.borrow() {
                        return Stop::Shutdown;
                    }
                }
                res = listener.recv() => match res {
                    // Best-effort fan-out; a send error just means no
                    // live receivers right now, which is fine.
                    Ok(_) => {
                        let _ = wake_tx.send(());
                    }
                    Err(error) => {
                        tracing::warn!(
                            error = %error,
                            "shared PgListener recv error; reconnecting"
                        );
                        continue 'reconnect;
                    }
                }
            }
        }
    }
}

/// The lock manager's pinned connection, ended rather than returned when the
/// manager lets go of it.
///
/// Postgres releases a session advisory lock only when the *session* ends, and a
/// pooled connection outlives the task that borrowed it: sqlx pings a dropped
/// connection and puts it back in the idle queue with its session state intact,
/// locks and all. A lock manager that panicked would therefore leave every
/// Policy it led locked by an idle connection — unleadable here and in every
/// standby replica, and invisible, because a worker waiting to be elected looks
/// exactly like a healthy standby. A `Drop` rather than a line at the end of the
/// task, so the unwind path releases the locks too.
struct PinnedSession(sqlx::pool::PoolConnection<Postgres>);

impl PinnedSession {
    fn pin(connection: sqlx::pool::PoolConnection<Postgres>) -> Self {
        Self(connection)
    }
}

impl Drop for PinnedSession {
    fn drop(&mut self) {
        // Takes effect when the connection itself drops, immediately after this.
        self.0.close_on_drop();
    }
}

impl std::ops::Deref for PinnedSession {
    type Target = sqlx::PgConnection;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl std::ops::DerefMut for PinnedSession {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

/// Revoke every policy's leadership when the lock manager lets go of it.
struct RevokeLeadership(Arc<Vec<(String, watch::Sender<bool>)>>);

impl Drop for RevokeLeadership {
    fn drop(&mut self) {
        for (_name, tx) in self.0.iter() {
            let _ = tx.send(false);
        }
    }
}

/// Hold every policy's advisory lock on one pinned connection and publish who
/// leads what.
///
/// `leadership` is owned by the caller rather than by this task, so a manager
/// that dies and is restarted resumes publishing on the same channels instead of
/// closing them under the workers.
async fn run_lock_manager(
    pool: Pool<Postgres>,
    leadership: Arc<Vec<(String, watch::Sender<bool>)>>,
    mut shutdown_rx: watch::Receiver<bool>,
    interval: Duration,
) -> Stop {
    // Covers every way this task can end, the unwind included: leaving a `true`
    // behind would let this process's workers drain a policy whose advisory lock
    // the dead session has already released, and which a standby may already
    // hold. It also revokes on the way in, where a fresh session holds no locks
    // whatever its predecessor believed.
    let _revoke_on_exit = RevokeLeadership(Arc::clone(&leadership));
    for (_name, tx) in leadership.iter() {
        let _ = tx.send(false);
    }

    // Which keys this session currently leads. Preserved across
    // reconnects so a dropped session revokes exactly what it held.
    let mut held = vec![false; leadership.len()];

    // Revoke all locally-believed leadership. Used when the pinned
    // session is lost: Postgres has already released the locks
    // server-side, so we must stop the workers (set leadership false)
    // before any standby can also acquire and double-process.
    let revoke_all = |held: &mut [bool]| {
        for (idx, (_name, tx)) in leadership.iter().enumerate() {
            if held[idx] {
                held[idx] = false;
                let _ = tx.send(false);
            }
        }
    };

    'session: loop {
        if *shutdown_rx.borrow() {
            break 'session;
        }

        // (Re)acquire the single pinned connection. All of this
        // instance's advisory locks live on this one session; when it
        // ends they are all released together, so on reconnect we
        // recompete for every key from scratch.
        let mut lock_conn = loop {
            if *shutdown_rx.borrow() {
                return Stop::Shutdown;
            }
            match pool.acquire().await {
                Ok(conn) => break PinnedSession::pin(conn),
                Err(error) => {
                    tracing::error!(
                        error = %error,
                        "lock manager could not acquire its connection; retrying"
                    );
                    tokio::select! {
                        _ = shutdown_rx.changed() => return Stop::Shutdown,
                        _ = tokio::time::sleep(interval) => {}
                    }
                }
            }
        };

        loop {
            if *shutdown_rx.borrow() {
                // Clean shutdown: explicitly release every held lock on
                // the live connection so a standby can take over
                // immediately (without waiting for a TCP session timeout).
                for (idx, (name, tx)) in leadership.iter().enumerate() {
                    if held[idx] {
                        let _ = sqlx::query("SELECT pg_advisory_unlock(hashtext($1)::bigint)")
                            .bind(name)
                            .execute(&mut *lock_conn)
                            .await;
                        held[idx] = false;
                        let _ = tx.send(false);
                    }
                }
                break 'session;
            }

            // Liveness probe: if we already lead at least one policy,
            // verify the pinned session is still alive. A dropped
            // session releases ALL our advisory locks server-side, so we
            // must revoke leadership locally (stopping the workers before
            // a standby can also acquire) and reconnect to recompete.
            // This bounds any split-brain window to one poll interval.
            if held.iter().any(|h| *h) {
                if let Err(error) = sqlx::query("SELECT 1").execute(&mut *lock_conn).await {
                    tracing::warn!(
                        error = %error,
                        "lock manager connection lost; revoking leadership and reconnecting"
                    );
                    revoke_all(&mut held);
                    continue 'session;
                }
            }

            // Try to acquire any keys we do not yet hold.
            let mut connection_lost = false;
            for (idx, (name, tx)) in leadership.iter().enumerate() {
                if held[idx] {
                    continue;
                }
                // pg_try_advisory_lock is non-blocking: returns true only
                // when this session exclusively holds the lock for `name`.
                match sqlx::query_scalar::<_, bool>(
                    "SELECT pg_try_advisory_lock(hashtext($1)::bigint)",
                )
                .bind(name)
                .fetch_one(&mut *lock_conn)
                .await
                {
                    Ok(true) => {
                        tracing::info!(policy = %name, "acquired advisory lock; leading");
                        held[idx] = true;
                        let _ = tx.send(true);
                    }
                    Ok(false) => {
                        tracing::debug!(
                            policy = %name,
                            "advisory lock held by another instance; standing by"
                        );
                    }
                    Err(error) => {
                        // A query error may mean the session has dropped:
                        // revoke leadership and reconnect rather than
                        // continuing to believe we lead the held keys.
                        tracing::warn!(
                            policy = %name,
                            error = %error,
                            "advisory lock query failed; reconnecting"
                        );
                        connection_lost = true;
                        break;
                    }
                }
            }
            if connection_lost {
                revoke_all(&mut held);
                continue 'session;
            }

            tokio::select! {
                _ = shutdown_rx.changed() => {}
                _ = tokio::time::sleep(interval) => {}
            }
        }
    }

    Stop::Shutdown
}

/// Write the durable heartbeat on a fixed cadence, for the Policies this process
/// leads.
///
/// It is a sibling of the workers and reads the same in-memory registry a
/// consumer reads through [`PolicyRunnerDaemon::liveness`]. The separation is
/// the whole design: a worker awaiting a reaction that never returns cannot
/// write anything, so a beat it emitted would go silent exactly when a consumer
/// needs to tell "wedged" from "gone". Beating from here keeps the two questions
/// apart — a fresh beat says the process is alive and says what its supervisor
/// knows about each worker; an ageing `last_polled_at` in the same row says the
/// worker is not finishing polls.
///
/// Only Policies this replica holds the advisory lock for are written: one row
/// per Policy is shared by every replica, so a Standby writing it would overwrite
/// the Leader's beat with its own idleness. A Leader whose worker has *stopped*
/// still holds the lock, and still beats — `liveness = 'Stopped'` against a fresh
/// beat is the half-dead state nothing could see in funkode-io/replay#164.
///
/// Leadership is read from the same channels the workers are elected by, so a
/// beat is exactly as current as the election that drives the work. It is not a
/// fence: a replica whose pinned lock session has just dropped can write one more
/// beat before its lock manager notices and revokes, so a `led_by` naming the
/// previous Leader can survive a failover by up to a beat. That is the split-brain
/// window [ADR-0008] already bounds for the workers themselves, and here it costs
/// a stale line in a report rather than a double-processed event: the new Leader
/// overwrites the row on its next beat. Reading this row as authority over who
/// may act would be the mistake; it reports, and the advisory lock decides.
///
/// [ADR-0008]: https://github.com/funkode-io/replay/blob/main/docs/adr/0008-policy-runner-shared-connection-leadership.md
async fn run_heartbeat(
    pool: Pool<Postgres>,
    liveness: LivenessRegistry,
    leadership: Arc<Vec<(String, watch::Receiver<bool>)>>,
    mut writer: HeartbeatWriter,
    mut shutdown_rx: watch::Receiver<bool>,
    cadence: Duration,
) -> Stop {
    // A schedule rather than a sleep between beats: sleeping `cadence` *after*
    // each write would make every period `cadence + however long the write took`,
    // so a slow database would stretch the one interval a consumer's staleness
    // threshold is derived from. `Skip` drops a tick the previous beat ran into
    // instead of firing twice to catch up — a beat that missed its slot is of no
    // use, and the next one is due immediately anyway.
    let mut schedule = tokio::time::interval(cadence);
    schedule.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = shutdown_rx.changed() => return Stop::Shutdown,
            _ = schedule.tick() => {}
        }
        if *shutdown_rx.borrow() {
            return Stop::Shutdown;
        }

        // Bounded by the registered policies: one line each, at most.
        let led: std::collections::HashSet<&str> = leadership
            .iter()
            .filter(|(_, leader_rx)| *leader_rx.borrow())
            .map(|(name, _)| name.as_str())
            .collect();
        let now = Instant::now();
        let beats: Vec<Beat> = liveness
            .snapshot()
            .into_iter()
            .filter(|worker| led.contains(worker.policy.as_str()))
            .map(|worker| Beat {
                policy: worker.policy,
                liveness: worker.liveness,
                polled_ago: worker
                    .last_polled_at
                    .map(|at| now.saturating_duration_since(at)),
            })
            .collect();

        writer.beat(&pool, &beats).await;
    }
}
/// Everything one policy's worker needs to run, cloned afresh on each restart.
///
/// The supervisor keeps this and spawns each attempt from a clone, which is what
/// makes a restart possible at all.
#[derive(Clone)]
struct PolicyWorker {
    policy: Arc<RegisteredPolicy>,
    cqrs: Cqrs<PostgresEventStore>,
    pool: Pool<Postgres>,
    executors: HashMap<TypeId, Arc<dyn AggregateExecutor>>,
    shutdown_rx: watch::Receiver<bool>,
    leader_rx: watch::Receiver<bool>,
    /// Kept as the sender so each attempt subscribes its own receiver; a
    /// restarted worker misses the wakeups it was dead for, which costs latency
    /// and nothing else (polling is the correctness baseline).
    wake_tx: Option<broadcast::Sender<()>>,
    interval: Duration,
    /// Where this worker publishes what it is doing, shared with its supervisor.
    liveness: LivenessHandle,
    name: String,
}

impl PolicyWorker {
    /// Drive one policy until shutdown: wait to be elected, load the cursor,
    /// drain until leadership or the process ends.
    ///
    /// Returning is the *clean* exit, and says which one it was. Anything else —
    /// a panic in the drain loop, in cursor I/O, in the feed read — unwinds this
    /// task and is the supervisor's business.
    async fn run(self) -> Stop {
        let PolicyWorker {
            policy,
            cqrs,
            pool,
            executors,
            mut shutdown_rx,
            mut leader_rx,
            wake_tx,
            interval,
            liveness,
            name,
        } = self;
        let mut wake_rx = wake_tx.as_ref().map(broadcast::Sender::subscribe);

        let max_depth = resolve_max_depth(policy.settings());

        'lifetime: loop {
            if *shutdown_rx.borrow() {
                return Stop::Shutdown;
            }

            // Wait until the shared lock manager elects this worker leader.
            if !*leader_rx.borrow() {
                // Unelected and running: the state a replica that leads nothing
                // spends its whole life in, and the one an operator must never
                // read as down.
                liveness.standing_by();
            }
            while !*leader_rx.borrow() {
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            return Stop::Shutdown;
                        }
                    }
                    changed = leader_rx.changed() => {
                        // The channel is closed: the lock manager this worker is
                        // elected by is gone for good, so nothing will ever make
                        // it a leader again.
                        if changed.is_err() {
                            return Stop::Abandoned;
                        }
                    }
                }
            }

            liveness.leading();

            // Initialize progress from the stored places (or bootstrap).
            let mut progress = match PolicyProgress::load(&pool, &name, policy.start_at()).await {
                Ok(progress) => progress,
                Err(error) => {
                    tracing::error!(
                        policy = %name,
                        error = %error,
                        "leader failed to initialize its progress; retrying"
                    );
                    tokio::select! {
                        _ = shutdown_rx.changed() => return Stop::Shutdown,
                        _ = tokio::time::sleep(interval) => {}
                    }
                    continue 'lifetime;
                }
            };

            // Where this worker picks up, once per election. A Policy resumes per
            // stream, so what it announces is where its search resumes; what it has
            // processed is one row per stream in `policy_stream_cursors`, which is
            // where an operator looks for a Policy that is not moving.
            tracing::info!(
                policy = %name,
                swept_through = progress.swept_through,
                "policy worker is leading; resuming its search after its last sweep"
            );

            // One election, one bracket: a burst this worker does not finish is
            // abandoned rather than closed by whoever leads next.
            let mut narration = Narration::new(PROGRESS_EVERY);

            // Leadership polling loop.
            loop {
                if *shutdown_rx.borrow() || !*leader_rx.borrow() {
                    break;
                }

                // Narrated from inside the drain, as the Policy moves: a batch
                // whose dispatches take minutes is working throughout, and a
                // record earned only when the poll returns would be paced by the
                // work rather than by the clock.
                if let Err(error) = drain_policy_once(
                    &cqrs,
                    &pool,
                    &executors,
                    policy.as_ref(),
                    &mut progress,
                    max_depth,
                    &mut Reporting {
                        narration: Some(&mut narration),
                    },
                )
                .await
                {
                    // The failure earns no record of its own: it did not catch
                    // up, and the error below is what says so. Whatever the poll
                    // observed before failing — the window it opened on, the
                    // positions it advanced over — happened, and its records
                    // stand.
                    tracing::error!(
                        policy = %name,
                        error = %error,
                        "policy polling iteration failed"
                    );
                }

                // Published after the drain rather than before it, so the stamp
                // reads "last poll that came back": a worker held inside one long
                // reaction is leading with an ageing stamp, which is what tells it
                // from an idle one. The heartbeat task carries it to the database;
                // this worker never writes it, because it cannot write anything
                // while a reaction holds it.
                liveness.polled(Instant::now());

                // Wait for the next wakeup: NOTIFY broadcast (if enabled),
                // poll timeout, leadership change, or shutdown — whichever
                // fires first.
                if let Some(ref mut wake) = wake_rx {
                    tokio::select! {
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                break;
                            }
                        }
                        changed = leader_rx.changed() => {
                            if changed.is_err() {
                                return Stop::Abandoned;
                            }
                        }
                        _ = tokio::time::sleep(interval) => {}
                        res = wake.recv() => {
                            // Ok or Lagged both mean "drain now"; Closed
                            // means the listener stopped, fall back to polling.
                            if let Err(broadcast::error::RecvError::Closed) = res {
                                wake_rx = None;
                            }
                        }
                    }
                } else {
                    tokio::select! {
                        changed = shutdown_rx.changed() => {
                            if changed.is_err() || *shutdown_rx.borrow() {
                                break;
                            }
                        }
                        changed = leader_rx.changed() => {
                            if changed.is_err() {
                                return Stop::Abandoned;
                            }
                        }
                        _ = tokio::time::sleep(interval) => {}
                    }
                }
            }

            narration.stood_down();

            if *shutdown_rx.borrow() {
                return Stop::Shutdown;
            }
            // Lost leadership without shutdown: loop back and wait to be
            // re-elected before draining again.
        }
    }
}

/// What is under supervision, for the log and for whoever hears it stop.
struct SupervisedTask {
    /// What kind of task it is: `policy worker`, `lock manager`, `notify listener`.
    kind: &'static str,
    /// The policy a worker drives. `None` for the process-wide shared tasks.
    policy: Option<String>,
    /// Where a permanent stop is published.
    stopped: StoppedWorkers,
    /// Where a worker's own state is published. `None` for the process-wide
    /// shared tasks, which drive no Policy and so have no liveness of their own:
    /// what a consumer reads is the workers they abandon.
    liveness: Option<LivenessHandle>,
    /// What the consumer does about a permanent stop, once it is published.
    on_escalation: EscalationHook,
}

impl SupervisedTask {
    /// Publish a worker's permanent stop and hand it to the consumer.
    ///
    /// Recorded before the hook runs, in that order deliberately: the default
    /// hook never returns, and a supplied one may be defective, so the library's
    /// own report cannot depend on either. The hook is called only for a task
    /// that owns a Policy — a shared task that stops is escalated by each worker
    /// it abandons, naming the Policy an operator acts on rather than the
    /// plumbing.
    fn escalate(&self, reason: EscalationReason) {
        let Some(policy) = self.policy.clone() else {
            return;
        };
        self.stopped.record(&policy, reason.restarts());
        if let Some(liveness) = &self.liveness {
            liveness.stopped();
        }

        let escalation = Escalation { policy, reason };
        let hook = &self.on_escalation;
        if std::panic::catch_unwind(AssertUnwindSafe(|| hook(&escalation))).is_err() {
            tracing::error!(
                policy = %escalation.policy,
                "the escalation hook panicked; the worker stays stopped and this process \
                 keeps the policy's advisory lock"
            );
        }
    }
}

/// Own a task and restart it when it dies.
///
/// `spawn` is called for each attempt, so the caller decides what a restarted
/// task is built from. Three outcomes, and only one of them is quiet:
///
/// - the task returns [`Stop::Shutdown`] — it was asked to stop, and supervision
///   ends with it;
/// - the task returns [`Stop::Abandoned`] — something it depends on is gone for
///   good, so restarting it cannot help. It is recorded as stopped and escalated;
/// - the task panicked — charged to `supervision`'s [`RestartBudget`], which
///   either grants a restart after a backoff or declares the task stopped and
///   escalates it.
///
/// A stop is recorded against the policy a worker drives. The shared tasks name
/// no policy: a lock manager that stops takes every policy's leadership with it,
/// and each of those workers reports its own abandonment.
///
/// Nothing is recorded or escalated once the daemon is shutting down. A worker
/// racing the lock manager's dropped leadership channel reads that as
/// abandonment, and escalating it would exit the process on every clean shutdown.
async fn supervise<F>(
    task: SupervisedTask,
    supervision: WorkerSupervision,
    mut shutdown_rx: watch::Receiver<bool>,
    spawn: F,
) where
    F: Fn() -> JoinHandle<Stop>,
{
    let kind = task.kind;
    let policy_name = task.policy.as_deref().unwrap_or("-");
    let mut budget = RestartBudget::new(supervision);

    loop {
        if *shutdown_rx.borrow() {
            return;
        }

        let cause = match spawn().await {
            Ok(Stop::Shutdown) => return,
            Ok(Stop::Abandoned) => {
                if *shutdown_rx.borrow() {
                    return;
                }
                tracing::error!(
                    task = kind,
                    policy = policy_name,
                    "a task this one depends on has stopped; it cannot be restarted \
                     into a process that no longer runs it"
                );
                task.escalate(EscalationReason::Abandoned);
                return;
            }
            // Aborted from outside (a runtime shutting down): not a fault, and
            // respawning into a dying runtime helps nobody.
            Err(error) if error.is_cancelled() => return,
            Err(error) => panic_cause(error),
        };

        if *shutdown_rx.borrow() {
            return;
        }

        match budget.record_death(Instant::now()) {
            RestartDecision::Restart { backoff, restarts } => {
                tracing::warn!(
                    task = kind,
                    policy = policy_name,
                    restarts,
                    window_secs = supervision.window.as_secs(),
                    backoff_ms = backoff.as_millis() as u64,
                    cause = %cause,
                    "task died outside its reaction; restarting after backoff"
                );
                if let Some(liveness) = &task.liveness {
                    liveness.restarting();
                }
                tokio::select! {
                    _ = shutdown_rx.changed() => return,
                    _ = tokio::time::sleep(backoff) => {}
                }
            }
            RestartDecision::Exhausted { restarts } => {
                tracing::error!(
                    task = kind,
                    policy = policy_name,
                    restarts,
                    window_secs = supervision.window.as_secs(),
                    cause = %cause,
                    "task exhausted its restart budget and is stopped"
                );
                task.escalate(EscalationReason::BudgetExhausted { restarts, cause });
                return;
            }
        }
    }
}

/// Read a dead task's panic message, so a restart says what killed the worker.
fn panic_cause(error: tokio::task::JoinError) -> String {
    let payload = error.into_panic();
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        return (*message).to_string();
    }
    if let Some(message) = payload.downcast_ref::<String>() {
        return message.clone();
    }
    "panic carrying a payload of an unrecognised type".to_string()
}

/// Maximum number of times a retryable dispatch error is retried before the
/// event is dead-lettered.  Each retry is preceded by an exponential back-off
/// starting at 100 ms.
const MAX_DISPATCH_RETRIES: u32 = 3;

/// Returns `true` for errors whose cause may be transient and worth retrying.
fn is_retryable(kind: replay::ErrorKind) -> bool {
    use replay::ErrorKind::{Conflict, RateLimited, Unavailable};
    matches!(kind, Unavailable | RateLimited | Conflict)
}

/// A dispatch that did not complete, and why.
///
/// The arms stay apart all the way to the parked row: a returned error carries a
/// kind the aggregate chose, a timeout carries none because nothing came back to
/// produce one.
enum DispatchFailure {
    /// The command ran to completion and returned an error.
    Returned(replay::Error),
    /// The command was still running when `limit` expired, and was abandoned.
    TimedOut { limit: Duration },
}

impl DispatchFailure {
    /// The aggregate refused the command on a business rule: not a failure of
    /// the runner's, and the cursor advances past it.
    fn declined(&self) -> bool {
        matches!(self, Self::Returned(e) if e.kind() == replay::ErrorKind::BusinessRuleViolation)
    }

    /// A timeout is retryable by construction: the runner has no evidence the
    /// command cannot succeed, only that it did not succeed in time.
    fn retryable(&self) -> bool {
        match self {
            Self::Returned(e) => is_retryable(e.kind()),
            Self::TimedOut { .. } => true,
        }
    }

    /// The `error_kind` this failure is parked under.
    fn error_kind(&self) -> String {
        match self {
            Self::Returned(e) => e.kind().to_string(),
            Self::TimedOut { .. } => TIMEOUT_ERROR_KIND.to_string(),
        }
    }
}

impl std::fmt::Display for DispatchFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Returned(e) => write!(f, "{e}"),
            Self::TimedOut { limit } => write!(
                f,
                "policy dispatch abandoned after exceeding its {} ms timeout",
                limit.as_millis()
            ),
        }
    }
}

/// What one attempt at a delivery failed on, and which attempt it was.
#[derive(Default)]
struct Attempt {
    number: u32,
    failures: Vec<FailedDispatch>,
    /// The dispatch being awaited right now, if any. What names the failure when
    /// the delivery ends by unwinding out of a command handler instead of
    /// returning an error.
    in_flight: Option<DispatchIdentity>,
}

/// A dispatch that failed, and which dispatch it was.
struct FailedDispatch {
    identity: DispatchIdentity,
    failure: DispatchFailure,
}

/// What a parked row names, read off a [`Dispatch`] before it is executed.
#[derive(Clone)]
struct DispatchIdentity {
    aggregate_name: &'static str,
    target_stream_id: String,
    command_name: &'static str,
    /// The dispatch's index in the vector the reaction returned.
    ///
    /// What keeps a reaction's own repeats apart in the table: two commands of
    /// one type to one instance are two parked commands, and the key that makes
    /// a redelivery refresh a row rather than insert one must not merge them
    /// (funkode-io/replay#220). It is not what matches a row to a replayed
    /// dispatch — [`ParkedIdentity::names`] is, and it stays blind to the
    /// ordinal so a row parked before this column existed is matched the same
    /// way as one parked after.
    ordinal: i32,
}

impl DispatchIdentity {
    fn of(ordinal: usize, dispatch: &Dispatch) -> Self {
        Self {
            aggregate_name: dispatch.aggregate_name(),
            target_stream_id: dispatch.target_stream_id().to_string(),
            command_name: dispatch.command_name(),
            // A reaction returning more than 2^31 dispatches has exhausted
            // memory long before it reaches the ordinal's range.
            ordinal: i32::try_from(ordinal).unwrap_or(i32::MAX),
        }
    }
}

// ── Retry: the reaction is the unit ───────────────────────────────────

/// A reaction with rows parked against it: what one replay settles.
///
/// Identified by `(policy_name, event_id)` — no synthetic id, because the event
/// a reaction is a pure function of already identifies it (ADR-0003).
struct ParkedReaction {
    policy_name: String,
    global_position: i64,
    event_id: uuid::Uuid,
}

/// Where a page of parked reactions resumes: the last one the previous page
/// settled, exclusive.
///
/// `(global_position, event_id)` rather than an offset, so rows leaving the
/// table as they settle cannot make a page skip what it has not seen.
#[derive(Clone, Copy)]
struct ReactionKeyset {
    global_position: i64,
    event_id: uuid::Uuid,
}

impl ReactionKeyset {
    /// Before every reaction: `global_position` is a sequence value, so the
    /// lowest one a row can carry is 1.
    fn start() -> Self {
        Self {
            global_position: 0,
            event_id: uuid::Uuid::nil(),
        }
    }

    fn after(reaction: &ParkedReaction) -> Self {
        Self {
            global_position: reaction.global_position,
            event_id: reaction.event_id,
        }
    }
}

/// One parked row, as the retry path needs it: which row, and which dispatch it
/// was parked for.
struct ParkedRow {
    id: i64,
    /// The dispatch the row names. `None` where there is none to name: a row
    /// parked before the identity migration, or parked for a panic in `react`
    /// itself.
    identity: Option<ParkedIdentity>,
}

/// What a reaction's group looked like at a moment, in a fixed number of bytes.
///
/// A retry reads a reaction's rows, replays it, and settles them after — seconds
/// during which a delivery of the same event (a crash inside the checkpoint
/// window, or a [Cursor move]) can re-park one of its commands, and another
/// operator's retry can settle one. Settling by `id` alone would then archive a
/// failure nobody retried, or overwrite it with the staler error this replay
/// produced (funkode-io/replay#227).
///
/// The group is too large to carry row by row (funkode-io/replay#228), so what a
/// retry carries across the replay is this: aggregates over the whole group,
/// read before it and re-read under the rows' locks before the settlement. Every
/// write a concurrent writer can make moves one of them — a park bumps
/// `deliveries` and `last_parked_at`, a settlement bumps `retries`, an insert or
/// an archive moves `rows` — so a group that reads the same has not moved, and
/// one that has not is the group the replay ran against.
///
/// [Cursor move]: ../../CONTEXT.md#cursor-move
#[derive(PartialEq, Eq, Clone, Copy)]
struct GroupDigest {
    rows: i64,
    deliveries: i64,
    retries: i64,
    last_parked_at: Option<DateTime<Utc>>,
}

impl GroupDigest {
    /// Read a digest off a row of the four aggregates, so the locked and
    /// unlocked readings cannot drift into two different answers.
    fn of(row: &sqlx::postgres::PgRow) -> Self {
        Self {
            rows: row.get("rows"),
            deliveries: row.get("deliveries"),
            retries: row.get("retries"),
            last_parked_at: row.get("last_parked_at"),
        }
    }
}

/// How many rows of the group name `identity`, from the counts read before the
/// walk. Absent means no row of the group names a dispatch the replay ran, which
/// [`Replay::settlement_for`] resolves without the number.
fn rows_naming(counts: &[(ParkedIdentity, usize)], identity: &ParkedIdentity) -> usize {
    counts
        .iter()
        .find_map(|(named, count)| (named == identity).then_some(*count))
        .unwrap_or(0)
}

/// Which half of a group a page comes from.
///
/// Rows that name a command are settled first, whatever their ids: a row that
/// names none is judged by the replay *as a whole* and speaks for every dispatch
/// of it, so letting one go first — an upgrade's row is older than the rows a
/// later delivery parked, hence lower-numbered — would leave its neighbours
/// nothing of their own to take (ADR-0021).
///
/// Two walks rather than one ordered by "names a command": an expression in the
/// `ORDER BY` is a sort of the group on every page, where `id` alone is the
/// index's own order.
#[derive(Clone, Copy)]
enum GroupPhase {
    Naming,
    Nameless,
}

impl GroupPhase {
    /// The page query this phase reads with.
    ///
    /// Written out twice rather than composed: sqlx takes only a `'static` query
    /// string, which is the rule that keeps a query from being assembled out of
    /// anything a caller supplies.
    fn page(self) -> &'static str {
        match self {
            Self::Naming => {
                "SELECT id, aggregate_name, target_stream_id, command_name \
                 FROM policy_dead_letters \
                 WHERE policy_name = $1 AND global_position = $2 AND event_id = $3 \
                   AND id > $4 \
                   AND aggregate_name IS NOT NULL AND target_stream_id IS NOT NULL \
                   AND command_name IS NOT NULL \
                 ORDER BY id ASC \
                 LIMIT $5"
            }
            Self::Nameless => {
                "SELECT id, aggregate_name, target_stream_id, command_name \
                 FROM policy_dead_letters \
                 WHERE policy_name = $1 AND global_position = $2 AND event_id = $3 \
                   AND id > $4 \
                   AND (aggregate_name IS NULL OR target_stream_id IS NULL \
                        OR command_name IS NULL) \
                 ORDER BY id ASC \
                 LIMIT $5"
            }
        }
    }
}

/// What settling one reaction concluded, counted rather than listed.
///
/// The group can be larger than anything worth holding
/// (funkode-io/replay#228), and neither caller needs it row by row: a bulk retry
/// counts reactions, and a by-id retry wants the outcome of the one row it was
/// asked about.
#[derive(Default)]
struct ReactionSettlement {
    /// Whether any row of the group was archived `retried`.
    any_resolved: bool,
    /// Whether any row is still parked after the settlement — re-parked, parked
    /// anew, or left to the writer that superseded this replay.
    any_still_failing: bool,
    /// What the row a by-id retry asked about concluded.
    asked_about: Option<DeadLetterRetry>,
}

impl ReactionSettlement {
    /// Fold one row's outcome in, keeping the one the caller asked about.
    fn record(&mut self, id: i64, outcome: DeadLetterRetry, asked_about: Option<i64>) {
        match outcome {
            DeadLetterRetry::Resolved => self.any_resolved = true,
            DeadLetterRetry::StillFailing | DeadLetterRetry::Superseded => {
                self.any_still_failing = true
            }
            DeadLetterRetry::NotFound => {}
        }
        if asked_about == Some(id) {
            self.asked_about = Some(outcome);
        }
    }
}

/// The dispatch a parked row names, read back off the row.
///
/// The stored counterpart of [`DispatchIdentity`], which is what a live
/// `Dispatch` carries; the two are compared to match a row to the command a
/// replay just ran.
#[derive(PartialEq, Eq)]
struct ParkedIdentity {
    aggregate_name: String,
    target_stream_id: String,
    command_name: String,
}

impl ParkedIdentity {
    /// Whether this row is about `dispatch`.
    ///
    /// The command's *variant* and payload are not recorded and
    /// `Dispatch::to::<A>` records `A::Command` — the command enum, not the
    /// variant — so this is in practice "the same command type at the same
    /// instance": any two dispatches of one reaction to one aggregate instance
    /// are indistinguishable here. What settles a row when they are is
    /// [`Replay::settlement_for`].
    fn names(&self, dispatch: &DispatchIdentity) -> bool {
        self.aggregate_name == dispatch.aggregate_name
            && self.target_stream_id == dispatch.target_stream_id
            && self.command_name == dispatch.command_name
    }
}

/// One dispatch a replay ran, and what it concluded.
struct ReplayedDispatch {
    identity: DispatchIdentity,
    /// `None` when it succeeded or was declined — both resolve the row.
    outcome: Option<Settlement>,
    /// Whether a row has already been settled from it, so a reaction emitting
    /// two identical dispatches settles two rows rather than one twice.
    claimed: bool,
}

/// The dispatches a replay has concluded, held until it settles.
///
/// Outside the `catch_unwind` in [`PolicyRunner::retry_reaction`] for the reason
/// the drain's [`PendingFailures`] is (ADR-0016): a panicking command handler
/// settles the replay by unwinding, and what its siblings concluded first must
/// settle their own rows instead of vanishing with the stack.
///
/// Bounded by the number of commands one reaction returns — the vector
/// `react_erased` already materialises.
#[derive(Default)]
struct ReplayedDispatches(Mutex<ReplayInProgress>);

/// What the replay has concluded, and what it is in the middle of.
#[derive(Default)]
struct ReplayInProgress {
    concluded: Vec<ReplayedDispatch>,
    /// The dispatch being awaited right now. What names the panic when the
    /// replay ends by unwinding out of a command handler, exactly as the
    /// drain's [`Attempt::in_flight`] does.
    in_flight: Option<DispatchIdentity>,
}

impl ReplayedDispatches {
    /// Record the dispatch about to be awaited, so a panic in its handler is a
    /// dispatch that concluded *with* that panic rather than one the replay
    /// never reached.
    fn dispatching(&self, identity: DispatchIdentity) {
        self.lock().in_flight = Some(identity);
    }

    fn push(&self, identity: DispatchIdentity, outcome: Option<Settlement>) {
        let mut replay = self.lock();
        replay.in_flight = None;
        replay.concluded.push(ReplayedDispatch {
            identity,
            outcome,
            claimed: false,
        });
    }

    /// Everything the replay concluded, the dispatch it unwound out of
    /// included, settled by `panic`.
    fn take(&self, panic: Option<&str>) -> Vec<ReplayedDispatch> {
        let mut replay = self.lock();
        let mut concluded = std::mem::take(&mut replay.concluded);
        if let (Some(identity), Some(message)) = (replay.in_flight.take(), panic) {
            concluded.push(ReplayedDispatch {
                identity,
                outcome: Some(Settlement::panicked(message)),
                claimed: false,
            });
        }
        concluded
    }

    /// A poisoned lock carries the dispatches concluded before the panic that
    /// poisoned it, which are exactly the ones that must still settle rows.
    fn lock(&self) -> MutexGuard<'_, ReplayInProgress> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// What a replay of a parked reaction concluded.
enum Replay {
    /// The reaction ran to completion: every dispatch it produced, in order.
    Ran(Vec<ReplayedDispatch>),
    /// It unwound. The dispatches concluded before the panic still settle their
    /// own rows; about the rest the replay says only that it panicked.
    Panicked {
        concluded: Vec<ReplayedDispatch>,
        message: String,
    },
}

impl Replay {
    /// What settles a row naming `identity`, given how many rows of the group
    /// name it.
    ///
    /// Matching a row to a dispatch by position is only honest when the two
    /// counts line up. They do not when a dispatch of that identity concluded
    /// without ever parking a row — the reaction sent two commands to one
    /// instance and only the later failed — or when a redelivery parked a
    /// second copy of one row (funkode-io/replay#220). Taking position on faith
    /// there archives a row whose command has just failed again, which is the
    /// one outcome a retry must never produce.
    ///
    /// So: counts aligned, settle in production order; counts apart, every row
    /// of that identity takes the same verdict — a failure among those
    /// dispatches re-parks it, all of them resolving archives it. The rows are
    /// indistinguishable by construction, so a shared error is what the table
    /// can honestly say about them.
    ///
    /// A panicked replay is matched the same way: the dispatch it unwound out
    /// of is one of its conclusions, carrying the panic, so only the dispatches
    /// it never reached are missing — and a row naming one of those falls
    /// through to [`unmatched`](Self::unmatched).
    fn settlement_for(
        &mut self,
        identity: &ParkedIdentity,
        rows_naming_it: usize,
    ) -> Option<Settlement> {
        if self.aligns_with(identity, rows_naming_it) {
            if let Some(outcome) = self.claim(identity) {
                return outcome;
            }
        } else if let Some(outcome) = self.verdict_on(identity) {
            return outcome;
        }
        self.unmatched()
    }

    /// Whether this replay ran exactly as many dispatches of `identity` as the
    /// group has rows naming it.
    fn aligns_with(&self, identity: &ParkedIdentity, rows_naming_it: usize) -> bool {
        self.concluded()
            .iter()
            .filter(|dispatch| identity.names(&dispatch.identity))
            .count()
            == rows_naming_it
    }

    /// Claim the dispatch `identity` names, if this replay ran one: `Some(None)`
    /// when it resolved, `Some(Some(_))` when it failed again, `None` when the
    /// replay produced no such dispatch, or none whose turn is still to come.
    fn claim(&mut self, identity: &ParkedIdentity) -> Option<Option<Settlement>> {
        let dispatch = self
            .concluded_mut()
            .iter_mut()
            .find(|dispatch| !dispatch.claimed && identity.names(&dispatch.identity))?;
        dispatch.claimed = true;
        Some(dispatch.outcome.clone())
    }

    /// What every row naming `identity` concluded together: the first failure
    /// among the replay's dispatches of that identity, or resolution when none
    /// of them failed. `None` when the replay produced no such dispatch — there
    /// is nothing to share a verdict from.
    ///
    /// Every dispatch of that identity is marked claimed: a row has spoken for
    /// all of them together, so none is left over to be parked anew.
    fn verdict_on(&mut self, identity: &ParkedIdentity) -> Option<Option<Settlement>> {
        let mut verdict = None;
        let mut matched = false;
        for dispatch in self.concluded_mut() {
            if !identity.names(&dispatch.identity) {
                continue;
            }
            matched = true;
            dispatch.claimed = true;
            verdict = verdict.or_else(|| dispatch.outcome.clone());
        }
        matched.then_some(verdict)
    }

    /// What settles a row naming a command this replay did not produce.
    ///
    /// The reaction no longer emits it, which resolves the row the way a
    /// declined command does — unless the reaction never got far enough to say,
    /// which only a panic does.
    fn unmatched(&self) -> Option<Settlement> {
        match self {
            Self::Panicked { message, .. } => Some(Settlement::panicked(message)),
            Self::Ran(_) => None,
        }
    }

    /// What settles a row that names no command at all: the replay as a whole,
    /// which is the only thing such a row can be judged by.
    ///
    /// The **first** failure the replay concluded, as the all-or-nothing retry
    /// this row was parked under would have stopped at; the panic only when the
    /// replay concluded nothing at all, which is a panic in `react` itself.
    /// Every dispatch is marked claimed: the row stands for the whole reaction,
    /// so it has already spoken for all of them.
    fn verdict(&mut self) -> Option<Settlement> {
        let panicked = match self {
            Self::Panicked { message, .. } => Some(Settlement::panicked(message)),
            Self::Ran(_) => None,
        };
        let mut verdict = None;
        for dispatch in self.concluded_mut() {
            dispatch.claimed = true;
            verdict = verdict.or_else(|| dispatch.outcome.clone());
        }
        verdict.or(panicked)
    }

    /// The failures no row spoke for: commands this reaction did not park
    /// before, which the retry has just watched fail.
    ///
    /// Borrowed rather than consumed, so a settlement that rolled back can be
    /// attempted again from the same replay. Bounded by the dispatch vector, of
    /// which this is the part nothing has settled.
    fn unclaimed_failures(&self) -> Vec<UnclaimedFailure> {
        self.concluded()
            .iter()
            .filter(|dispatch| !dispatch.claimed)
            .filter_map(|dispatch| {
                Some(UnclaimedFailure {
                    identity: dispatch.identity.clone(),
                    settlement: dispatch.outcome.clone()?,
                })
            })
            .collect()
    }

    /// Forget what has been claimed: the transaction that claimed it rolled
    /// back, so no row in the table speaks for any of these dispatches.
    fn forget_claims(&mut self) {
        for dispatch in self.concluded_mut() {
            dispatch.claimed = false;
        }
    }

    fn concluded(&self) -> &[ReplayedDispatch] {
        match self {
            Self::Ran(concluded) | Self::Panicked { concluded, .. } => concluded,
        }
    }

    fn concluded_mut(&mut self) -> &mut Vec<ReplayedDispatch> {
        match self {
            Self::Ran(concluded) | Self::Panicked { concluded, .. } => concluded,
        }
    }
}

/// A dispatch that failed and that no row of the reaction spoke for.
struct UnclaimedFailure {
    identity: DispatchIdentity,
    settlement: Settlement,
}

/// What a row that is still failing is re-parked with.
#[derive(Clone)]
struct Settlement {
    error_kind: String,
    error_message: String,
}

impl Settlement {
    fn of(failure: &DispatchFailure) -> Self {
        Self {
            error_kind: failure.error_kind(),
            error_message: failure.to_string(),
        }
    }

    fn panicked(message: &str) -> Self {
        Self {
            error_kind: PANIC_ERROR_KIND.to_string(),
            error_message: message.to_string(),
        }
    }
}

/// The failures of the attempt in progress, held until the delivery settles.
///
/// Lives outside the `catch_unwind` in [`Delivery::react_to_event`] rather than
/// on the attempt loop's stack: a panicking command handler settles the delivery
/// by unwinding, and the failures its siblings produced first are parked with
/// the panic instead of vanishing with the stack (ADR-0016).
///
/// `failures` is bounded by the number of commands one reaction returns for an
/// event — the vector `react_erased` already materialises — and is emptied at
/// the start of every attempt, so what is parked is the settling attempt's
/// outcome and not a tally across attempts (funkode-io/replay#209).
///
/// The `Mutex` is what makes the type usable across the unwind boundary, not
/// concurrency: one task pushes, and never across an `await`.
#[derive(Default)]
struct PendingFailures(Mutex<Attempt>);

impl PendingFailures {
    /// Start attempt `number`, discarding what the previous one failed on — a
    /// retry re-executes the same commands and produces its own failures.
    ///
    /// Called before the reaction runs, not before the first dispatch: `react`
    /// is user code and can panic, and the panic path must not park an earlier
    /// attempt's failures as if this one had produced them.
    fn begin(&self, number: u32) {
        let mut attempt = self.lock();
        attempt.number = number;
        attempt.failures.clear();
        attempt.in_flight = None;
    }

    /// Record the dispatch about to be awaited, so a panic in its handler is
    /// parked naming it. Cleared by [`Self::returned`] whatever the outcome, so
    /// what an unwind finds here is the dispatch it unwound out of.
    fn dispatching(&self, identity: DispatchIdentity) {
        self.lock().in_flight = Some(identity);
    }

    fn returned(&self) {
        self.lock().in_flight = None;
    }

    fn push(&self, identity: DispatchIdentity, failure: DispatchFailure) {
        self.lock()
            .failures
            .push(FailedDispatch { identity, failure });
    }

    /// Take what the attempt in progress has failed on, leaving it empty, so a
    /// second call (the panic path after the settling path) parks nothing twice.
    fn take(&self) -> Attempt {
        std::mem::take(&mut *self.lock())
    }

    /// A poisoned lock carries the failures of the panic that poisoned it, which
    /// are exactly what must still be parked.
    fn lock(&self) -> MutexGuard<'_, Attempt> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

/// Write the record a [`Narration`] decided on.
///
/// `info`, because these are the lines an operator reads to see work start and
/// finish: two per burst, plus one while it lasts per [`PROGRESS_EVERY`], and
/// none at all while a Policy is idle. A dispatch that commits is a `debug`
/// record ([`Delivery::execute_dispatch_within`]) and stays off in production;
/// one that is declined, retried or parked already says so at its own level.
fn narrate(policy: &str, record: Record) {
    match record {
        Record::Working => tracing::info!(
            policy = %policy,
            "policy has work to do"
        ),
        Record::Progress { events, elapsed } => tracing::info!(
            policy = %policy,
            events,
            elapsed_ms = elapsed.as_millis(),
            "policy is working through its backlog"
        ),
        Record::CaughtUp { events, elapsed } => tracing::info!(
            policy = %policy,
            events,
            elapsed_ms = elapsed.as_millis(),
            "policy is caught up"
        ),
    }
}

/// Where one poll reports what it saw.
///
/// The two axes a poll feeds, carried together because a poll speaks to both at
/// the same moments: what stopped the feed ([Progress]) and what the worker is
/// doing about it ([Narration]).
///
/// [Progress]: https://github.com/funkode-io/replay/blob/main/CONTEXT.md#progress
/// [Narration]: https://github.com/funkode-io/replay/blob/main/CONTEXT.md#narration
struct Reporting<'a> {
    /// The bracket around a burst of work. `None` for [`PolicyRunner::drain`],
    /// which polls once on the caller's command: a manual drain that opened a
    /// burst would leave a bracket nothing ever closes.
    narration: Option<&'a mut Narration>,
}

impl Reporting<'_> {
    /// Tell the narration what a poll found, and write whatever record it earns.
    fn tell(&mut self, policy: &str, poll: Poll) {
        if let Some(narration) = self.narration.as_deref_mut() {
            if let Some(record) = narration.polled(poll) {
                narrate(policy, record);
            }
        }
    }
}

async fn drain_policy_once(
    cqrs: &Cqrs<PostgresEventStore>,
    pool: &Pool<Postgres>,
    executors: &HashMap<TypeId, Arc<dyn AggregateExecutor>>,
    policy: &RegisteredPolicy,
    progress: &mut PolicyProgress,
    max_depth: u32,
    reporting: &mut Reporting<'_>,
) -> Result<usize, replay::Error> {
    let started = Instant::now();
    let name = policy.name().to_string();
    let checkpoint_size = resolve_checkpoint_batch_size(policy.settings());
    let read_batch = resolve_read_batch_size(policy.settings(), checkpoint_size);
    let dispatch_timeout = resolve_dispatch_timeout(policy.settings());
    let delivery = Delivery {
        cqrs,
        pool,
        executors,
        policy_name: &name,
        dispatch_timeout,
    };

    // Asked once per poll, not once per stream: it is the Policy's own code, and the
    // number of times a library calls back into it should not depend on how the log is
    // laid out.
    let filter = policy.stream_filter();

    // Three sources: what the last poll could not finish, what the sweep just found, and
    // — on its own cadence — what the sweep has missed.
    let discovered = sweep_for_streams(pool, progress.swept_through, read_batch).await?;
    let reconciling = progress.reconcile_is_due();
    let examined = if reconciling {
        streams_behind(pool, &name, &progress.reconciled_through, read_batch).await?
    } else {
        Vec::new()
    };

    // Everything this poll decides is decided in one place: which streams it reads and in
    // what order, how far its budget gets, where that leaves the rotation, and what the
    // next poll starts from. What is left here is the I/O those decisions are about
    // (funkode-io/replay#243).
    //
    // The carried queue is copied rather than taken, and `progress` keeps the old one
    // until the settle below replaces it. Taking it would mean every way out of this
    // function between here and there loses it — a `?` on either query, and a caller who
    // drops the future: `PolicyRunner::drain` is public and can be timed out or aborted
    // half way (funkode-io/replay#246 review). A poll that never settles costs the next
    // one a re-read of streams this one may already have caught up, which the places
    // decide when it gets there.
    let mut plan = PollPlan::plan(Nominations {
        carried: progress.unfinished.clone(),
        swept: discovered.streams,
        examined,
        reconciling,
        read_batch,
        share_from: progress.share_from,
    });
    progress.share_from += 1;

    // Whether the poll found nothing to read. Narrated once its rotation is written, so
    // an operator is never told a Policy is caught up by a poll that then failed.
    let mut exhausted = false;

    // The places this poll has moved and not yet written. One entry per stream advanced
    // since the last flush, so the batch bounds it. Declared out here because a poll that
    // fails still has to say what it was holding: these places are in memory only.
    let mut advanced: Vec<(String, i64)> = Vec::new();

    // Everything from here to the settle is I/O against what the plan decided, and every
    // way out of it — including a failed statement — goes through the settle below.
    let drained: Result<usize, replay::Error> = async {
        // The sweep has read this stretch of log whatever the streams in it turn out to
        // owe, and a stream left unfinished is remembered rather than re-swept for.
        //
        // Written before it is taken in memory, for the reason the rotation is: a write
        // that fails, or a future dropped waiting on it, must leave this runner searching
        // from where the database says it got to. Advancing in memory first would step
        // this runner over a stretch nothing recorded, and the streams that stretch
        // nominated would be nominated again by nothing but the reconciliation.
        if discovered.swept_through > progress.swept_through {
            write_sweep(pool, &name, discovered.swept_through).await?;
            progress.swept_through = discovered.swept_through;
        }

        if plan.streams().is_empty() {
            // Nothing was nominated, so the page was empty — a source with anything to
            // offer always wins a slot. The poll is settled all the same, which is what
            // wraps the rotation: an empty page is the end of a pass, and a cursor that
            // has reached the last stream id queries past the end for ever until
            // something records that (funkode-io/replay#231 review).
            //
            // Narrated below rather than here: "caught up" is a statement about a poll
            // that finished, and this one still has its rotation to write.
            exhausted = true;
            return Ok(0);
        }

        let places = places_of(pool, &name, plan.streams()).await?;

        // The window is work, before any of it is done: a first reaction that takes
        // minutes must run inside the bracket rather than before it.
        reporting.tell(&name, Poll::Found { at: started });

        let mut executed = 0;
        let mut events_since_checkpoint = 0u32;
        // What the poll observed each place to be, which is what its checkpoints are
        // written against: a place that has moved underneath this poll belongs to an
        // operator or to another runner, and this one's arithmetic about it is stale.
        let mut observed = places.clone();

        // `read_batch_size` is a budget for the drain, not for each stream: the plan hands
        // out what is left of it a stream at a time, and stops when it is spent. What the
        // budget does not reach is carried, not lost.
        while let Some(turn) = plan.turn() {
            let stream_id = &turn.stream_id;
            let place = places.get(stream_id).copied().unwrap_or_default().seq;
            let events = read_stream(pool, filter.clone(), stream_id, place, turn.budget).await?;
            plan.read(&turn, events.len() as u32);

            let mut reached = place;
            let mut superseded = false;
            for event in events {
                if let Some(raw) = event.delivered {
                    let depth = event_causation_depth(&raw);
                    if depth >= max_depth {
                        // Circuit breaker: the event's causation chain is too deep.
                        // Skip reactions but keep advancing so the policy is not wedged.
                        let (_, limit_source) = resolve_max_depth_with_source(policy.settings());
                        tracing::warn!(
                            policy        = %name,
                            event_id      = %raw.id,
                            stream_id     = %raw.stream_id,
                            global_position = event.global_position,
                            stream_seq    = event.stream_seq,
                            depth,
                            max_depth,
                            limit_source,
                            causation_chain = ?parse_causation_info(&raw),
                            "causation depth limit reached; skipping reaction to prevent runaway cascade"
                        );
                    } else {
                        // Real event within depth budget: deliver to the policy with
                        // the full resilience policy (BRV advance, retry, dead-letter),
                        // and with a panic in the reaction contained to this event.
                        executed += delivery
                            .react_to_event(policy, event.global_position, &raw)
                            .await?;
                    }
                }
                // Always track in-memory place.
                reached = event.stream_seq;
                events_since_checkpoint += 1;
                // Told as the Policy moves rather than when the poll returns: one poll's
                // batch is dispatched event by event, each bounded only by the dispatch
                // timeout and its retries, so a poll can outlast the progress cadence
                // several times over.
                reporting.tell(
                    &name,
                    Poll::Advanced {
                        events: 1,
                        at: Instant::now(),
                    },
                );
                // Write the persistent places every `checkpoint_size` events so that
                // a crash re-processes at most `checkpoint_size - 1` events rather
                // than the full drain batch (skip-safety: a place only advances past
                // events whose reactions are already durably committed).
                if events_since_checkpoint >= checkpoint_size {
                    // Flushed out of `advanced` rather than taken from it: a checkpoint
                    // that fails leaves these places where the poll's failure path can
                    // find them, which is the only record that they moved at all.
                    advanced.push((stream_id.clone(), reached));
                    let kept = checkpoint_places(pool, &name, &advanced, &observed).await?;
                    for (stream, seq) in &advanced {
                        if let Some(written_by) = kept.get(stream) {
                            observed.insert(
                                stream.clone(),
                                Place {
                                    seq: *seq,
                                    written_by: Some(*written_by),
                                },
                            );
                        }
                    }
                    advanced.clear();
                    events_since_checkpoint = 0;
                    if !kept.contains_key(stream_id) {
                        superseded = true;
                        break;
                    }
                }
            }

            // A stream whose place moved under the poll is left where its new owner put
            // it: nothing is written for it, and it is not carried, because the next poll
            // reads the place afresh and resumes from there.
            if superseded {
                tracing::info!(
                    policy    = %name,
                    stream_id = %stream_id,
                    "policy place moved underneath this poll; abandoning the stream and \
                     resuming from the place that is stored"
                );
                plan.abandoned(&turn);
                continue;
            }

            plan.delivered(&turn);
            if reached > place {
                advanced.push((turn.stream_id, reached));
            }
        }

        // Final checkpoint: flush any stream advanced since the last periodic save.
        checkpoint_places(pool, &name, &advanced, &observed).await?;

        Ok(executed)
    }
    .await;

    let settled = plan.settle();

    // A poll that failed hands on the places it had moved and not written, and hands them
    // on *first*: those events were delivered, their places are in memory only, and the
    // sweep has passed the positions that would nominate those streams again. Anything
    // the cap has to drop should be a stream that costs a re-read, not one that costs a
    // redelivery.
    let carrying = if drained.is_err() {
        let mut recovering: Vec<String> = Vec::new();
        for (stream, _) in advanced {
            if !recovering.contains(&stream) {
                recovering.push(stream);
            }
        }
        for stream in settled.carried {
            if !recovering.contains(&stream) {
                recovering.push(stream);
            }
        }
        recovering.truncate(read_batch as usize);
        recovering
    } else {
        settled.carried
    };

    // The queue the next poll starts from, on every path out of this one — the failing
    // one included. `progress` has held the copy this poll planned from all along, so
    // this is the only place the queue changes: what a poll that got somewhere says is
    // still owed, replacing what it was given.
    progress.unfinished = carrying;

    // A poll that stopped on an error has not finished reading, so the cadence is not
    // stamped and the rotation is not moved: the next poll reconciles again, over a page
    // that no longer holds whatever this one did manage to read.
    let executed = drained?;

    // One call site for the rotation, taken on every path a poll finishes by — the one
    // that read nothing included, which is exactly the one a finished pass ends on
    // (funkode-io/replay#231 review).
    if settled.reconciled {
        // Written first, and taken in memory only once it is written: a runner whose
        // rotation had moved past a page the database still has it before would examine
        // that page again only after a restart.
        let rotation = settled
            .rotation
            .unwrap_or_else(|| progress.reconciled_through.clone());
        write_reconciled(pool, &name, &rotation).await?;
        progress.reconciled_through = rotation;
        progress.reconciled_at = Some(Instant::now());
    }

    if exhausted {
        reporting.tell(&name, Poll::Exhausted);
    }

    Ok(executed)
}

/// The runner's machinery for delivering events to **one** policy: the
/// execution path, the tables, and the time each dispatch is allowed.
///
/// One value because the per-event path threads all of it unchanged through
/// three layers (containment → retry → one dispatch), and because
/// [`PolicyRunner::retry_dead_letter`] must reproduce it to re-run a parked row
/// the way the drain ran it.
struct Delivery<'a> {
    cqrs: &'a Cqrs<PostgresEventStore>,
    pool: &'a Pool<Postgres>,
    executors: &'a HashMap<TypeId, Arc<dyn AggregateExecutor>>,
    policy_name: &'a str,
    /// How long one dispatch may run before it is abandoned
    /// ([`resolve_dispatch_timeout`]).
    dispatch_timeout: Duration,
}

impl Delivery<'_> {
    /// Deliver one event to a policy — the runner's **per-event containment
    /// boundary**.
    ///
    /// [`Self::execute_event_reactions`] absorbs every *returned* failure; this
    /// absorbs the one failure it cannot see. A reaction is arbitrary user code
    /// called on the worker's own task, so a panic in it unwinds the worker: the
    /// policy would stop reacting for the rest of the process's life, silently.
    /// Catching here — at the event rather than at the worker — is what stops one
    /// bad event from consuming a worker's restart budget.
    ///
    /// A panic is classified **permanent on first occurrence** and parked
    /// immediately, never retried: re-running a reaction that panicked
    /// deterministically panics again. The parked row records
    /// [`PANIC_ERROR_KIND`] so an operator can tell a defect in the reaction from
    /// a command the domain refused.
    ///
    /// **Not contained**: a panic inside a task the reaction spawns itself (or
    /// hands to a blocking pool). It unwinds in its own task, outside this
    /// boundary and outside the worker, so nothing parks a dead letter for it.
    /// Nor is anything contained in a binary built with `panic = "abort"`, where
    /// a panic ends the process before any catch runs.
    ///
    /// [`AssertUnwindSafe`] is the honest claim here: what survives the catch is
    /// the database and the runner's own bookkeeping (both untouched by the
    /// unwind) plus the policy object itself, whose interior state — if it has
    /// any — is the reaction's own to keep consistent.
    async fn react_to_event(
        &self,
        policy: &RegisteredPolicy,
        global_position: i64,
        raw: &PersistedEvent<Value>,
    ) -> Result<usize, replay::Error> {
        let policy_name = self.policy_name;
        let pending = PendingFailures::default();
        let reactions = self.execute_event_reactions(policy, global_position, raw, &pending);

        match AssertUnwindSafe(reactions).catch_unwind().await {
            Ok(result) => result,
            Err(payload) => {
                let message = panic_message(&*payload);
                tracing::error!(
                    policy          = %policy_name,
                    event_id        = %raw.id,
                    stream_id       = %raw.stream_id,
                    global_position,
                    panic           = %message,
                    "policy reaction panicked; writing dead-letter and advancing cursor"
                );
                // A panic settles the delivery, so the dispatches that had
                // already failed in this attempt are parked with it: the unwind
                // crossed the buffer rather than carrying it off.
                let attempt = pending.take();
                // The dispatch the unwind came out of, when it came out of one:
                // a panic in `react` itself has none, and its row says so with
                // null identity columns rather than with a guess.
                let in_flight = attempt.in_flight.clone();
                self.park(global_position, raw, attempt).await?;
                write_dead_letter(
                    self.pool,
                    DeadLetterWrite {
                        policy_name,
                        global_position,
                        event_id: raw.id,
                        identity: in_flight.as_ref(),
                        error_kind: PANIC_ERROR_KIND,
                        error_message: &message,
                        parking: Parking::Delivery,
                    },
                )
                .await?;
                Ok(0)
            }
        }
    }

    /// Execute all reactions for one event, applying the resilience policy:
    ///
    /// | Outcome                             | Action                                |
    /// |-------------------------------------|---------------------------------------|
    /// | `Ok`                                | count as executed, continue           |
    /// | `BusinessRuleViolation`             | log + advance (aggregate said no)     |
    /// | Retryable (`Unavailable`, `Conflict`, `RateLimited`, or a timeout) within retry budget | back-off + retry |
    /// | Permanent or retries exhausted      | write `policy_dead_letters`, advance  |
    ///
    /// The function always returns `Ok`; failures are absorbed here so the caller's
    /// cursor always advances (a circuit-breaker, never a poison pill). A reaction
    /// that *panics* is absorbed one level out, in [`Self::react_to_event`], which
    /// is the only failure this function cannot observe.
    ///
    /// Parking happens once the delivery settles, not as each attempt produces a
    /// failure: a retryable command forces another attempt for its siblings too,
    /// and a permanently failing sibling would otherwise be parked once per
    /// attempt (funkode-io/replay#209). What is parked is the settling attempt's outcome.
    /// The attempt's failures are buffered in `pending`, which belongs to
    /// [`Self::react_to_event`]: a panic in a later dispatch settles the delivery
    /// from out there, and the failures before it are parked rather than lost to
    /// the unwind.
    ///
    /// Each dispatch is awaited for at most [`Delivery::dispatch_timeout`], so
    /// an event costs at most one timeout per dispatch per attempt.
    ///
    /// **Re-react safety**: on retry the policy's `react` is called again for the
    /// same event.  `react` is pure, and the same event yields the same dispatches —
    /// including the idempotency key the command carries — so a target that absorbs a
    /// repeated key sees an earlier successful dispatch as a no-op (ADR-0027).
    async fn execute_event_reactions(
        &self,
        policy: &RegisteredPolicy,
        global_position: i64,
        raw: &PersistedEvent<Value>,
        pending: &PendingFailures,
    ) -> Result<usize, replay::Error> {
        let policy_name = self.policy_name;
        for attempt in 0..=MAX_DISPATCH_RETRIES {
            pending.begin(attempt);
            let dispatches = policy.react_erased(raw);
            let mut executed = 0usize;
            let mut need_retry = false;

            for (ordinal, dispatch) in dispatches.into_iter().enumerate() {
                let identity = DispatchIdentity::of(ordinal, &dispatch);
                pending.dispatching(identity.clone());
                let outcome = self
                    .execute_dispatch_within(global_position, raw, dispatch)
                    .await;
                pending.returned();

                match outcome {
                    Ok(()) => {
                        executed += 1;
                    }
                    Err(failure) if failure.declined() => {
                        tracing::info!(
                            policy          = %policy_name,
                            event_id        = %raw.id,
                            global_position,
                            aggregate       = identity.aggregate_name,
                            target          = %identity.target_stream_id,
                            error           = %failure,
                            "policy dispatch declined by aggregate business rule; advancing cursor"
                        );
                    }
                    Err(failure) if failure.retryable() && attempt < MAX_DISPATCH_RETRIES => {
                        tracing::warn!(
                            policy          = %policy_name,
                            event_id        = %raw.id,
                            global_position,
                            attempt,
                            aggregate       = identity.aggregate_name,
                            target          = %identity.target_stream_id,
                            error           = %failure,
                            "policy dispatch failed with retryable error; backing off before retry"
                        );
                        need_retry = true;
                        break; // skip remaining dispatches for this attempt
                    }
                    Err(failure) => {
                        // Permanent error, or retryable (including a timeout) but
                        // retries exhausted.
                        pending.push(identity, failure);
                    }
                }
            }

            if !need_retry {
                self.park(global_position, raw, pending.take()).await?;
                return Ok(executed);
            }

            // Exponential back-off: 100 ms, 200 ms, 400 ms, …
            let backoff = Duration::from_millis(100 * (1u64 << attempt.min(5)));
            tokio::time::sleep(backoff).await;
        }

        Ok(0)
    }

    /// Write a dead letter for everything an attempt failed on, each row naming
    /// the dispatch it is about.
    async fn park(
        &self,
        global_position: i64,
        raw: &PersistedEvent<Value>,
        attempt: Attempt,
    ) -> Result<(), replay::Error> {
        let Attempt {
            number, failures, ..
        } = attempt;
        for FailedDispatch { identity, failure } in failures {
            tracing::error!(
                policy          = %self.policy_name,
                event_id        = %raw.id,
                global_position,
                attempt         = number,
                aggregate       = identity.aggregate_name,
                target          = %identity.target_stream_id,
                command         = identity.command_name,
                error           = %failure,
                "policy dispatch failed permanently; writing dead-letter and advancing cursor"
            );
            write_dead_letter(
                self.pool,
                DeadLetterWrite {
                    policy_name: self.policy_name,
                    global_position,
                    event_id: raw.id,
                    identity: Some(&identity),
                    error_kind: &failure.error_kind(),
                    error_message: &failure.to_string(),
                    parking: Parking::Delivery,
                },
            )
            .await?;
        }
        Ok(())
    }

    /// Execute one dispatch, bounded in time.
    ///
    /// Execute one dispatch, abandoning it after [`Self::dispatch_timeout`].
    ///
    /// Cancelling the future is all a timeout can do: work the reaction moved
    /// onto another task keeps running, unobserved (ADR-0018).
    async fn execute_dispatch_within(
        &self,
        global_position: i64,
        raw: &PersistedEvent<Value>,
        dispatch: Dispatch,
    ) -> Result<(), DispatchFailure> {
        let policy_name = self.policy_name;
        let limit = self.dispatch_timeout;
        // Read before the dispatch is moved into the call; on a timeout it is the
        // only thing that names what the worker was waiting for.
        let aggregate = dispatch.aggregate_name();
        let started = Instant::now();

        match tokio::time::timeout(
            limit,
            execute_dispatch(
                self.cqrs,
                self.executors,
                policy_name,
                global_position,
                raw,
                dispatch,
            ),
        )
        .await
        {
            Ok(Ok(())) => {
                // The only per-dispatch record, and the reason the edge-triggered
                // lines above can stay two per burst: an operator who needs to see
                // every command a Policy sent turns this on for as long as they are
                // looking. A ten-thousand-row import fans out to six figures of
                // these, which is why it is never on by default.
                tracing::debug!(
                    policy          = %policy_name,
                    event_id        = %raw.id,
                    stream_id       = %raw.stream_id,
                    global_position,
                    aggregate,
                    elapsed_ms      = started.elapsed().as_millis(),
                    "policy dispatch committed"
                );
                Ok(())
            }
            Ok(Err(error)) => Err(DispatchFailure::Returned(error)),
            Err(_) => {
                tracing::warn!(
                    policy          = %policy_name,
                    event_id        = %raw.id,
                    stream_id       = %raw.stream_id,
                    global_position,
                    aggregate,
                    timeout_ms      = limit.as_millis(),
                    elapsed_ms      = started.elapsed().as_millis(),
                    "policy dispatch exceeded its dispatch timeout; abandoning it"
                );
                Err(DispatchFailure::TimedOut { limit })
            }
        }
    }
}

/// Read the message out of a caught panic's payload.
///
/// `panic!` carries a `&'static str` when its argument is a literal and a
/// `String` when it is formatted; anything else reached `panic_any` and has no
/// message to report.
fn panic_message(payload: &(dyn Any + Send)) -> String {
    if let Some(message) = payload.downcast_ref::<&'static str>() {
        (*message).to_string()
    } else if let Some(message) = payload.downcast_ref::<String>() {
        message.clone()
    } else {
        "reaction panicked with a payload that is neither &str nor String".to_string()
    }
}

/// One parked command, as [`write_dead_letter`] writes it: which reaction, which
/// dispatch of it, and what that dispatch failed with.
struct DeadLetterWrite<'a> {
    policy_name: &'a str,
    global_position: i64,
    event_id: uuid::Uuid,
    /// The dispatch the row names. `None` only where there is none to name: a
    /// panic in `react` itself, which fails before it has built one.
    identity: Option<&'a DispatchIdentity>,
    error_kind: &'a str,
    error_message: &'a str,
    parking: Parking,
}

/// Why a row is being parked: which of the two writers is at the keyboard.
///
/// Both go through [`write_dead_letter`], and both may find the row already
/// there — but only one of them is a delivery of the event. A retry that parks a
/// command the reaction had not parked is racing another retry of the same
/// reaction, not watching the event arrive again, and must not say it was
/// delivered again (funkode-io/replay#227).
#[derive(Clone, Copy, PartialEq, Eq)]
enum Parking {
    /// The drain parked it, for the delivery it is settling.
    Delivery,
    /// A retry parked it, for a command the reaction had not parked before.
    Retry,
}

/// Write a dead-letter record for a reaction that could not be completed.
///
/// `error_kind` is the [`replay::ErrorKind`] of a returned error, or
/// [`PANIC_ERROR_KIND`] when the reaction panicked.
///
/// `identity` names the dispatch that failed. `None` only where there is no
/// dispatch to name — a panic in `react` itself, which fails before it has built
/// one — and the row's identity columns stay null.
///
/// **One row per parked command per reaction.** The park is written before the
/// batched cursor checkpoint, so a crash in between — or an operator rewinding
/// the cursor — delivers the event again and parks the same command again. The
/// `ON CONFLICT` refreshes the row that command already has with the error this
/// delivery produced rather than leaving a second generation of rows behind
/// (ADR-0024). It does **not** touch `created_at` (when the command first
/// failed) or the retry bookkeeping: a redelivery is not a [`retry_dead_letter`]
/// — nobody invoked the control surface.
///
/// Only a [`Parking::Delivery`] writes anything to a row that already exists. A
/// [`Parking::Retry`] parks a command the reaction had not parked, so a row
/// under that key means another writer got there first — a delivery of the
/// event, or another retry of the same reaction (ADR-0021) — and this retry's
/// error is not newer than theirs. It leaves the row untouched and says so in
/// [`Parked::conflicted`], which the caller reports as
/// [`DeadLetterRetry::Superseded`]. The failure is not lost either way: the row
/// is a row for that command, parked and active.
///
/// `clock_timestamp()`, not `now()`, and never backwards: `now()` is fixed at
/// transaction start, and a retry parks inside one, so a settlement that began
/// earlier can commit later and would otherwise stamp the recency signal a live
/// delivery has already moved.
///
/// [`retry_dead_letter`]: PolicyRunner::retry_dead_letter
async fn write_dead_letter(
    executor: impl sqlx::PgExecutor<'_>,
    park: DeadLetterWrite<'_>,
) -> Result<Parked, replay::Error> {
    let DeadLetterWrite {
        policy_name,
        global_position,
        event_id,
        identity,
        error_kind,
        error_message,
        parking,
    } = park;
    let row = sqlx::query(
        "INSERT INTO policy_dead_letters \
         (policy_name, global_position, event_id, error_kind, error_message, \
          aggregate_name, target_stream_id, command_name, dispatch_ordinal, \
          last_parked_at) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, clock_timestamp()) \
         ON CONFLICT (policy_name, event_id, aggregate_name, target_stream_id, \
                      command_name, dispatch_ordinal) \
         DO UPDATE SET error_kind = CASE WHEN $10 THEN EXCLUDED.error_kind \
                           ELSE policy_dead_letters.error_kind END, \
                       error_message = CASE WHEN $10 THEN EXCLUDED.error_message \
                           ELSE policy_dead_letters.error_message END, \
                       deliveries = policy_dead_letters.deliveries \
                           + CASE WHEN $10 THEN 1 ELSE 0 END, \
                       last_parked_at = CASE WHEN $10 \
                           THEN GREATEST(policy_dead_letters.last_parked_at, \
                                         clock_timestamp()) \
                           ELSE policy_dead_letters.last_parked_at END \
         RETURNING id, xmax <> 0 AS conflicted",
    )
    .bind(policy_name)
    .bind(global_position)
    .bind(event_id)
    .bind(error_kind)
    .bind(error_message)
    .bind(identity.map(|i| i.aggregate_name))
    .bind(identity.map(|i| i.target_stream_id.as_str()))
    .bind(identity.map(|i| i.command_name))
    .bind(identity.map(|i| i.ordinal))
    .bind(parking == Parking::Delivery)
    .fetch_one(executor)
    .await
    .map_err(crate::db_error)?;

    Ok(Parked {
        id: row.get("id"),
        conflicted: row.get("conflicted"),
    })
}

/// What a park wrote.
///
/// `conflicted` is read off `xmax`, which an `ON CONFLICT` insert leaves at 0 on
/// the row it inserted and at the updating transaction's id on a row it found:
/// the one thing `RETURNING` can say about which of the two happened.
struct Parked {
    id: i64,
    conflicted: bool,
}

/// Update a parked dead letter in place with the failure a retry just produced,
/// so a row that keeps failing is never duplicated — and stamp what has been
/// tried on it.
///
/// The row stays **retryable**: what makes another retry worth making is a
/// change outside the library, which the library cannot observe.
/// [`PolicyRunner::discard_dead_letter`] is what takes a row out of play.
///
/// Returns whether a row was still there to re-park. Under the group's locks
/// (see [`GroupDigest`]) that is always true; a `false` would mean a row left a
/// locked group, which is reported rather than assumed away.
async fn re_park_dead_letter(
    executor: impl sqlx::PgExecutor<'_>,
    id: i64,
    error_kind: &str,
    error_message: &str,
) -> Result<bool, replay::Error> {
    let result = sqlx::query(
        "UPDATE policy_dead_letters \
         SET error_kind = $2, error_message = $3, \
             retry_count = retry_count + 1, last_retried_at = now() \
         WHERE id = $1",
    )
    .bind(id)
    .bind(error_kind)
    .bind(error_message)
    .execute(executor)
    .await
    .map_err(crate::db_error)?;
    Ok(result.rows_affected() > 0)
}

/// What a settlement that matched no row means: the row is gone (a concurrent
/// discard), or it is still parked but is no longer the row the replay read (a
/// delivery re-parked its command, or another retry settled it).
///
/// Asked only on that path, so a settlement that lands stays one statement.
async fn unsettled(
    executor: impl sqlx::PgExecutor<'_>,
    id: i64,
) -> Result<DeadLetterRetry, replay::Error> {
    let still_parked: bool =
        sqlx::query_scalar("SELECT EXISTS (SELECT 1 FROM policy_dead_letters WHERE id = $1)")
            .bind(id)
            .fetch_one(executor)
            .await
            .map_err(crate::db_error)?;

    Ok(if still_parked {
        DeadLetterRetry::Superseded
    } else {
        DeadLetterRetry::NotFound
    })
}

/// One page of the reactions a policy has parked, in the order they happened,
/// starting after `after` and stopping at `backlog`.
///
/// Bounded by [`RETRY_PAGE_SIZE`]. An outage parks one reaction per event it
/// refused, so the set this walks is the size of the outage: reading it whole
/// would hold a backlog's worth of rows for as long as the bulk retry runs,
/// which is the shape that OOM-killed a consumer in funkode-io/replay#146.
async fn load_parked_reactions(
    pool: &Pool<Postgres>,
    policy_name: &str,
    after: ReactionKeyset,
    backlog: i64,
) -> Result<Vec<ParkedReaction>, replay::Error> {
    let rows = sqlx::query(
        "SELECT global_position, event_id \
         FROM policy_dead_letters \
         WHERE policy_name = $1 AND (global_position, event_id) > ($2, $3) \
           AND global_position <= $4 \
         GROUP BY global_position, event_id \
         ORDER BY global_position ASC, event_id ASC \
         LIMIT $5",
    )
    .bind(policy_name)
    .bind(after.global_position)
    .bind(after.event_id)
    .bind(backlog)
    .bind(RETRY_PAGE_SIZE)
    .fetch_all(pool)
    .await
    .map_err(crate::db_error)?;

    Ok(rows
        .into_iter()
        .map(|row| ParkedReaction {
            policy_name: policy_name.to_string(),
            global_position: row.get("global_position"),
            event_id: row.get("event_id"),
        })
        .collect())
}

/// The furthest position this policy has parked a reaction at, or `None` when it
/// has parked none.
///
/// What a bulk retry treats as the backlog it was asked to drain. A worker drains
/// the feed forward, so anything it parks while the retry runs is at a greater
/// position and is this call's business no more than an event that has not
/// happened yet.
async fn last_parked_position(
    pool: &Pool<Postgres>,
    policy_name: &str,
) -> Result<Option<i64>, replay::Error> {
    sqlx::query_scalar(
        "SELECT MAX(global_position) FROM policy_dead_letters WHERE policy_name = $1",
    )
    .bind(policy_name)
    .fetch_one(pool)
    .await
    .map_err(crate::db_error)
}

/// What a reaction's group adds up to right now, read without locking anything.
///
/// Taken before the replay, and compared with [`lock_group`]'s reading after it:
/// the group's stand-in for a per-row version ([`GroupDigest`]).
///
/// Bounded by its shape: one row of four scalars, whatever the group holds.
///
/// `global_position` is redundant with `event_id` — one event has one position —
/// and is in the filter to make it a prefix match on
/// `idx_dead_letters_policy_reaction`.
async fn group_digest(
    executor: impl sqlx::PgExecutor<'_>,
    reaction: &ParkedReaction,
) -> Result<GroupDigest, replay::Error> {
    let row = sqlx::query(
        "SELECT count(*) AS rows, \
                coalesce(sum(deliveries), 0) AS deliveries, \
                coalesce(sum(retry_count), 0) AS retries, \
                max(last_parked_at) AS last_parked_at \
         FROM policy_dead_letters \
         WHERE policy_name = $1 AND global_position = $2 AND event_id = $3",
    )
    .bind(&reaction.policy_name)
    .bind(reaction.global_position)
    .bind(reaction.event_id)
    .fetch_one(executor)
    .await
    .map_err(crate::db_error)?;

    Ok(GroupDigest::of(&row))
}

/// The same four aggregates, over a group every row of which is locked until the
/// transaction ends.
///
/// The lock is what makes the comparison decisive rather than advisory: a writer
/// that had already moved a row shows up in the numbers, and one that has not yet
/// cannot move it behind the settlement's back — it waits, and applies its
/// fresher failure on top of what this retry concluded.
///
/// The aggregates sit outside the locking sub-select because `FOR UPDATE` and
/// aggregation cannot share a query level; the rows it locks are read but never
/// returned, so this stays one row of four scalars however large the group is.
async fn lock_group(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    reaction: &ParkedReaction,
) -> Result<GroupDigest, replay::Error> {
    let row = sqlx::query(
        "SELECT count(*) AS rows, \
                coalesce(sum(deliveries), 0) AS deliveries, \
                coalesce(sum(retry_count), 0) AS retries, \
                max(last_parked_at) AS last_parked_at \
         FROM (SELECT deliveries, retry_count, last_parked_at \
               FROM policy_dead_letters \
               WHERE policy_name = $1 AND global_position = $2 AND event_id = $3 \
               ORDER BY id ASC \
               FOR UPDATE) g",
    )
    .bind(&reaction.policy_name)
    .bind(reaction.global_position)
    .bind(reaction.event_id)
    .fetch_one(&mut **tx)
    .await
    .map_err(crate::db_error)?;

    Ok(GroupDigest::of(&row))
}

/// Open the transaction a settlement runs in: one snapshot for every page it will
/// read, every row of the group locked, and the digest that says whether this is
/// still the group the replay ran against.
///
/// The snapshot is the half of the guard the locks cannot give. A command parked
/// while the settlement walks the group is a row nobody could have locked, and
/// under the default READ COMMITTED every page takes a fresh snapshot — so a
/// later page would read that row and settle it from a replay that ran before it
/// existed, which is the chunked-rebuild problem
/// (`PostgresEventStoreBuilder::build`) in another table. Postgres requires the
/// isolation level before the transaction's first query, and the lock is that
/// query.
async fn begin_settlement<'a>(
    pool: &'a Pool<Postgres>,
    reaction: &ParkedReaction,
) -> Result<(sqlx::Transaction<'a, Postgres>, GroupDigest), replay::Error> {
    let mut tx = pool.begin().await.map_err(crate::db_error)?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *tx)
        .await
        .map_err(crate::db_error)?;

    let digest = lock_group(&mut tx, reaction).await?;
    Ok((tx, digest))
}

/// One page of a reaction's rows, oldest first, from one half of the group.
///
/// Bounded by [`RETRY_ROW_PAGE_SIZE`]. A reaction's rows are one per command the
/// current code dispatches — plus the tail an older version of the policy parked
/// and the dedupe could not prove duplicate, which is the old code's commands
/// times the deliveries it saw and is a number nothing in the code bounds
/// (funkode-io/replay#228). Reading the group whole therefore held a table's
/// worth of strings for a reaction an upgrade had inherited.
///
/// Filter and order are the index's own (`idx_dead_letters_policy_reaction`, on
/// `(policy_name, global_position, event_id, id)`), so a page is a range scan
/// that resumes where the last one stopped rather than a sort of the group: a
/// walk over a long tail stays linear in it.
///
/// `global_position` is redundant with `event_id` — one event has one position —
/// and is in the filter to make that prefix match.
async fn load_parked_page(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    reaction: &ParkedReaction,
    phase: GroupPhase,
    after: i64,
) -> Result<Vec<ParkedRow>, replay::Error> {
    let rows = sqlx::query(phase.page())
        .bind(&reaction.policy_name)
        .bind(reaction.global_position)
        .bind(reaction.event_id)
        .bind(after)
        .bind(RETRY_ROW_PAGE_SIZE)
        .fetch_all(&mut **tx)
        .await
        .map_err(crate::db_error)?;

    Ok(rows
        .into_iter()
        .map(|row| {
            let aggregate_name: Option<String> = row.get("aggregate_name");
            let target_stream_id: Option<String> = row.get("target_stream_id");
            let command_name: Option<String> = row.get("command_name");
            ParkedRow {
                id: row.get("id"),
                identity: match (aggregate_name, target_stream_id, command_name) {
                    (Some(aggregate_name), Some(target_stream_id), Some(command_name)) => {
                        Some(ParkedIdentity {
                            aggregate_name,
                            target_stream_id,
                            command_name,
                        })
                    }
                    _ => None,
                },
            }
        })
        .collect())
}

/// How many rows of the group name each dispatch this replay ran.
///
/// Whether a row is settled in production order or shares a verdict with its
/// indistinguishable siblings is a question about the whole reaction
/// ([`Replay::settlement_for`]), so the count cannot be taken over a page. It is
/// taken **once**, before the walk, and only for the identities the replay
/// produced: a row naming a command this replay did not run is settled by
/// [`Replay::unmatched`], which never asks.
///
/// One index range scan over the group, where counting per page cost one for
/// each page — quadratic in the tail this change exists to make readable.
///
/// The arrays it binds borrow from `dispatched`, the vector `react_erased`
/// already materialised, and the rows it reads back are one per distinct
/// identity among them: bounded by what one reaction dispatches, never by the
/// group it counts. Duplicates are left in rather than deduplicated into a
/// fourth buffer — `IN` does not care.
async fn count_rows_naming(
    tx: &mut sqlx::Transaction<'_, Postgres>,
    reaction: &ParkedReaction,
    dispatched: &[ReplayedDispatch],
) -> Result<Vec<(ParkedIdentity, usize)>, replay::Error> {
    if dispatched.is_empty() {
        return Ok(Vec::new());
    }

    let aggregates: Vec<&str> = dispatched
        .iter()
        .map(|it| it.identity.aggregate_name)
        .collect();
    let targets: Vec<&str> = dispatched
        .iter()
        .map(|it| it.identity.target_stream_id.as_str())
        .collect();
    let commands: Vec<&str> = dispatched
        .iter()
        .map(|it| it.identity.command_name)
        .collect();

    let rows = sqlx::query(
        "SELECT aggregate_name, target_stream_id, command_name, count(*) AS naming \
         FROM policy_dead_letters \
         WHERE policy_name = $1 AND global_position = $2 AND event_id = $3 \
           AND (aggregate_name, target_stream_id, command_name) \
               IN (SELECT * FROM unnest($4::text[], $5::text[], $6::text[])) \
         GROUP BY aggregate_name, target_stream_id, command_name",
    )
    .bind(&reaction.policy_name)
    .bind(reaction.global_position)
    .bind(reaction.event_id)
    .bind(&aggregates)
    .bind(&targets)
    .bind(&commands)
    .fetch_all(&mut **tx)
    .await
    .map_err(crate::db_error)?;

    Ok(rows
        .into_iter()
        .map(|row| {
            let naming: i64 = row.get("naming");
            (
                ParkedIdentity {
                    aggregate_name: row.get("aggregate_name"),
                    target_stream_id: row.get("target_stream_id"),
                    command_name: row.get("command_name"),
                },
                naming as usize,
            )
        })
        .collect())
}

/// Move a dead letter out of the active `policy_dead_letters` table into the
/// `discarded_dead_letters` archive in a single statement, recording why it
/// left (`reason`: `retried`, `discarded`).
///
/// The `DELETE ... RETURNING` feeds the `INSERT` so the row is removed from the
/// active set and preserved for audit atomically. Returns `true` when a row was
/// moved, `false` when no active row matched.
///
/// What keeps a retry from archiving a row a delivery re-parked while the replay
/// ran is the group's lock and digest ([`GroupDigest`]), taken before any row of
/// it is settled — so this statement matches on `id` and nothing else, and a
/// discard, which re-runs nothing and holds no group, uses the same one.
///
/// A move with reason `retried` is a retry settling the row, so it stamps the
/// retry bookkeeping the same way [`re_park_dead_letter`] does; a discard
/// re-runs nothing and stamps nothing.
async fn move_dead_letter_to_archive(
    executor: impl sqlx::PgExecutor<'_>,
    id: i64,
    reason: &str,
) -> Result<bool, replay::Error> {
    let result = sqlx::query(
        "WITH moved AS ( \
             DELETE FROM policy_dead_letters \
             WHERE id = $1 \
             RETURNING id, policy_name, global_position, event_id, error_kind, \
                       error_message, created_at, aggregate_name, target_stream_id, \
                       command_name, retry_count, last_retried_at, dispatch_ordinal, \
                       deliveries, last_parked_at \
         ) \
         INSERT INTO discarded_dead_letters \
             (dead_letter_id, policy_name, global_position, event_id, error_kind, \
              error_message, created_at, reason, aggregate_name, target_stream_id, \
              command_name, retry_count, last_retried_at, dispatch_ordinal, \
              deliveries, last_parked_at) \
         SELECT id, policy_name, global_position, event_id, error_kind, \
                error_message, created_at, $2, aggregate_name, target_stream_id, \
                command_name, \
                retry_count + (CASE WHEN $2 = 'retried' THEN 1 ELSE 0 END), \
                CASE WHEN $2 = 'retried' THEN now() ELSE last_retried_at END, \
                dispatch_ordinal, deliveries, last_parked_at \
         FROM moved",
    )
    .bind(id)
    .bind(reason)
    .execute(executor)
    .await
    .map_err(crate::db_error)?;

    Ok(result.rows_affected() > 0)
}

/// Load a single event by its primary key, shaped exactly like [`read_stream`] delivers
/// one, so it can be fed back into a policy's erased reaction during retry.
async fn load_event_by_id(
    pool: &Pool<Postgres>,
    event_id: uuid::Uuid,
) -> Result<Option<PersistedEvent<Value>>, replay::Error> {
    let row = sqlx::query(
        "SELECT id, data, metadata, stream_id, type, version, created, aggregate_version, \
         global_position, compacted_snapshot FROM events WHERE id = $1",
    )
    .bind(event_id)
    .fetch_optional(pool)
    .await
    .map_err(crate::db_error)?;

    match row {
        Some(row) => Ok(Some(PersistedEvent::<Value>::try_from(row)?)),
        None => Ok(None),
    }
}

/// Sweep the log past `from` for streams with new events.
///
/// Two columns and an indexed range scan, bounded by `limit`: this runs on every poll, so
/// it is the query the design is paid for. It reads no cursors and waits for no position
/// to fill — a missing `global_position` names no stream, so there is nothing to stop at.
async fn sweep_for_streams(
    pool: &Pool<Postgres>,
    from: i64,
    limit: u32,
) -> Result<Discovered, replay::Error> {
    let rows = sqlx::query(
        "SELECT global_position, stream_id FROM events WHERE global_position > $1 \
         ORDER BY global_position ASC LIMIT $2",
    )
    .bind(from)
    .bind(limit as i64)
    .fetch_all(pool)
    .await
    .map_err(crate::db_error)?;

    Ok(discovered_from_sweep(
        from,
        rows.into_iter()
            .map(|row| (row.get("global_position"), row.get("stream_id")))
            .collect(),
    ))
}

/// The streams this Policy is behind on, compared head to cursor, taking the batch that
/// sorts after `after`.
///
/// This is the correctness half: it finds what the sweep missed, which is any write that
/// committed below a position the sweep had already passed. No index answers a comparison
/// between two tables' columns, so it scans one row per stream and is bounded by `limit`
/// rather than by an index — which is why it runs on a cadence and the sweep runs on every
/// poll (ADR-0026).
///
/// It resumes after the last id it examined instead of restarting at the lowest, because
/// `limit` is a batch and not a snapshot: a Policy with `limit` permanently-behind streams
/// low in the sort order would otherwise return those same ids every time, and a quiet
/// stream sorting after them — one whose only write the sweep passed, so no future event
/// will nominate it — would never be examined again. Rotating bounds that at one pass over
/// the streams: `ceil(streams / limit)` cadences while the Policy is keeping up, and one
/// stream a cadence at worst, because the poll it runs on gives it the first slot
/// ([ADR-0026](../../docs/adr/0026-a-policy-tracks-its-position-per-stream.md) owns the
/// bound).
async fn streams_behind(
    pool: &Pool<Postgres>,
    name: &str,
    after: &str,
    limit: u32,
) -> Result<Vec<String>, replay::Error> {
    let rows = sqlx::query(
        "SELECT s.id FROM streams s \
         LEFT JOIN policy_stream_cursors c ON c.policy = $1 AND c.stream_id = s.id \
         WHERE s.id > $2 AND s.stream_seq > COALESCE(c.stream_seq, 0) \
         ORDER BY s.id LIMIT $3",
    )
    .bind(name)
    .bind(after)
    .bind(limit as i64)
    .fetch_all(pool)
    .await
    .map_err(crate::db_error)?;

    Ok(rows.into_iter().map(|row| row.get("id")).collect())
}

/// A Policy's place in one stream, as a poll read it.
///
/// `written_by` is the transaction that last wrote the row — PostgreSQL's `xmin`, which
/// every write to the row bumps whoever issues it. It is what a checkpoint compares, so
/// that "nobody has written this since I read it" is a different fact from "somebody wrote
/// it and it holds the value I read" (funkode-io/replay#234). `None` means the poll found
/// no row, which is a stream at the beginning of itself.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
struct Place {
    seq: i64,
    written_by: Option<i64>,
}

/// Where the Policy has got to in each of `streams`, and the row version it read that
/// from. A stream with no row is at 0.
async fn places_of(
    pool: &Pool<Postgres>,
    name: &str,
    streams: &[String],
) -> Result<HashMap<String, Place>, replay::Error> {
    // `xid` is 32 bits unsigned and has no `sqlx` decoding of its own, so it is read as
    // the number it is. Nothing is done with the value but compare it for equality.
    let rows = sqlx::query(
        "SELECT stream_id, stream_seq, xmin::text::bigint AS written_by \
           FROM policy_stream_cursors \
          WHERE policy = $1 AND stream_id = ANY($2)",
    )
    .bind(name)
    .bind(streams)
    .fetch_all(pool)
    .await
    .map_err(crate::db_error)?;

    Ok(rows
        .into_iter()
        .map(|row| {
            (
                row.get("stream_id"),
                Place {
                    seq: row.get("stream_seq"),
                    written_by: Some(row.get("written_by")),
                },
            )
        })
        .collect())
}

/// One stream's events past `place`, in the order they were written to it.
///
/// No contiguity to check: appends to a stream serialise on its row, so its places arrive
/// in order and a missing one cannot be an append in flight — it is the operator's doing.
/// The filter decides delivery only; an excluded row advances the place like a compaction
/// snapshot does (ADR-0013, ADR-0004).
async fn read_stream(
    pool: &Pool<Postgres>,
    filter: StreamFilter,
    stream_id: &str,
    place: i64,
    limit: u32,
) -> Result<Vec<StreamEvent>, replay::Error> {
    let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
        "SELECT id, data, metadata, stream_id, type, version, created, aggregate_version, \
         global_position, stream_seq, compacted_snapshot, COALESCE((",
    );
    // As a predicate NULL means no match; read as a value it must be collapsed.
    PostgresEventStore::add_filters(&mut qb, filter);
    qb.push("), FALSE) AS matches_filter FROM events WHERE stream_id = ");
    qb.push_bind(stream_id.to_string());
    qb.push(" AND stream_seq > ");
    qb.push_bind(place);
    qb.push(" ORDER BY stream_seq ASC LIMIT ");
    qb.push_bind(limit as i64);

    let rows = qb.build().fetch_all(pool).await.map_err(crate::db_error)?;

    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        let stream_seq: i64 = row.get("stream_seq");
        let global_position: i64 = row.get("global_position");
        let is_snapshot: bool = row.get("compacted_snapshot");
        let matches_filter: bool = row.get("matches_filter");

        // Only delivered rows are parsed; a skipped row's bytes are still fetched.
        let delivered = if is_snapshot || !matches_filter {
            None
        } else {
            Some(PersistedEvent::<Value>::try_from(row)?)
        };

        events.push(StreamEvent {
            stream_seq,
            global_position,
            delivered,
        });
    }

    Ok(events)
}

/// One event of one stream, and whether this Policy reacts to it.
struct StreamEvent {
    stream_seq: i64,
    /// Carried for the causation the runner stamps and for what an operator reads in a
    /// trace: the Policy's order does not use it.
    global_position: i64,
    delivered: Option<PersistedEvent<Value>>,
}

/// Record how far the Policy has got in each stream it advanced, and report back which
/// of those writes the Policy still owned — and at what row version, so the poll can go
/// on checkpointing the streams it kept.
///
/// One statement for the batch, and a compare-and-set per stream: a place is written only
/// where the row is still the one this poll read, compared by the transaction that wrote
/// it rather than by the place it holds. Comparing the place would not do, and
/// monotonicity would do even less:
///
/// - the write to refuse is *lower* than the one this poll carries — an operator rewinding
///   a stream to force a redelivery (ADR-0012) against a poll that is mid-batch, whose
///   higher place would otherwise reinstate itself and undo the rewind silently;
/// - and it may hold the very value this poll read, when the operator rewinds to just
///   before the event being delivered right now. A compared place cannot see that one at
///   all (funkode-io/replay#234); a compared row version can, because PostgreSQL bumps
///   `xmin` on every write whether or not the value changes, and whether or not the writer
///   knew it had to.
///
/// The same set-and-check tells a runner that has lost its leadership that it has, which
/// is the only signal it gets.
///
/// Deleting the row is the other half of that control surface, and what protects it is
/// that a place the poll read is only ever *updated*: an update matches nothing where the
/// row has gone. What decides which half a stream is in is whether the poll saw a *row*,
/// which a place of 0 does not tell you — a stream at the beginning and a stream with no
/// row are the same number and different things (funkode-io/replay#231), and only the
/// second may be created here. An upsert would not do, even guarded on the row existing,
/// because the guard reads the snapshot the statement opened on: against a delete that
/// had not committed when the statement started, the guard sees the row, the insert waits
/// on the primary key, and once the delete commits there is nothing left to conflict
/// with, so the operator's delete is undone by an insert (funkode-io/replay#236 review).
/// A place the poll found no row for is only ever inserted, and loses to whoever created
/// one in the meantime.
async fn checkpoint_places(
    pool: &Pool<Postgres>,
    name: &str,
    places: &[(String, i64)],
    observed: &HashMap<String, Place>,
) -> Result<HashMap<String, i64>, replay::Error> {
    if places.is_empty() {
        return Ok(HashMap::new());
    }

    let streams: Vec<String> = places.iter().map(|(stream, _)| stream.clone()).collect();
    let seqs: Vec<i64> = places.iter().map(|(_, seq)| *seq).collect();
    // A row the poll saw carries the version it saw; a stream it found no row for carries
    // nothing, which is what puts it in the half of the statement that may create one. A
    // place of 0 does not tell you which — a stream at the beginning and a stream with no
    // row are both 0 (funkode-io/replay#231).
    let from: Vec<Option<i64>> = places
        .iter()
        .map(|(stream, _)| observed.get(stream).and_then(|place| place.written_by))
        .collect();

    let kept = sqlx::query(
        "WITH incoming AS ( \
             SELECT * FROM UNNEST($2::text[], $3::bigint[], $4::bigint[]) \
                       AS i(stream_id, stream_seq, observed)), \
         advanced AS ( \
             UPDATE policy_stream_cursors c \
                SET stream_seq = i.stream_seq, updated_at = now() \
               FROM incoming i \
              WHERE c.policy = $1 AND c.stream_id = i.stream_id \
                AND i.observed IS NOT NULL \
                AND c.xmin::text::bigint = i.observed \
          RETURNING c.stream_id, c.xmin::text::bigint AS written_by), \
         created AS ( \
             INSERT INTO policy_stream_cursors (policy, stream_id, stream_seq) \
             SELECT $1, i.stream_id, i.stream_seq FROM incoming i \
              WHERE i.observed IS NULL \
             ON CONFLICT (policy, stream_id) DO NOTHING \
          RETURNING stream_id, xmin::text::bigint AS written_by) \
         SELECT stream_id, written_by FROM advanced \
          UNION ALL \
         SELECT stream_id, written_by FROM created",
    )
    .bind(name)
    .bind(&streams)
    .bind(&seqs)
    .bind(&from)
    .fetch_all(pool)
    .await
    .map_err(crate::db_error)?;

    // The version this statement has just left on each row it kept: what the poll must
    // compare against next time, since its own write moved the row on.
    let kept: HashMap<String, i64> = kept
        .into_iter()
        .map(|row| (row.get("stream_id"), row.get("written_by")))
        .collect();

    // What `PolicyStatus::last_checkpoint_at` is measured from: the Policy processed
    // something, whichever stream it was. A batch whose every write lost its
    // compare-and-set processed nothing that stands, so it leaves the stamp alone rather
    // than reporting an outage as fresh progress.
    if !kept.is_empty() {
        sqlx::query("UPDATE policy_cursors SET updated_at = now() WHERE name = $1")
            .bind(name)
            .execute(pool)
            .await
            .map_err(crate::db_error)?;
    }

    Ok(kept)
}

async fn execute_dispatch(
    cqrs: &Cqrs<PostgresEventStore>,
    executors: &HashMap<TypeId, Arc<dyn AggregateExecutor>>,
    policy_name: &str,
    global_position: i64,
    raw: &PersistedEvent<Value>,
    dispatch: Dispatch,
) -> Result<(), replay::Error> {
    let executor = executors.get(&dispatch.target()).ok_or_else(|| {
        replay::Error::invalid_input(
            "no services registered for the aggregate targeted by a policy dispatch",
        )
        .with_operation("policy_drain")
        .with_context("policy", policy_name)
        .with_context("aggregate", dispatch.aggregate_name())
    })?;

    let aggregate_name = dispatch.aggregate_name();
    let dispatch_metadata = dispatch.metadata().cloned();

    let metadata = merge_dispatch_metadata(
        causation_metadata(policy_name, global_position, raw),
        dispatch_metadata,
    )
    .map_err(|err| {
        err.with_operation("policy_drain")
            .with_context("policy", policy_name)
            .with_context("aggregate", aggregate_name)
    })?;

    executor.execute(cqrs, dispatch, metadata).await
}

/// Where a Policy is: a place per stream, and how far discovery has swept the log.
///
/// The per-stream places are the progress. The sweep position is a hint that says where
/// to look next — never what has been delivered — so losing it, resetting it or running
/// past an uncommitted write costs a search, not an event
/// ([ADR-0026](../../docs/adr/0026-a-policy-tracks-its-position-per-stream.md)).
///
/// The places are not held in memory between polls. Each poll loads the places of the
/// streams it is about to read, which bounds what this struct holds by the poll's own
/// limit rather than by how many streams the Policy has ever seen — and means an operator
/// editing `policy_stream_cursors` is adopted on the next poll, with no machinery for it
/// (ADR-0012).
struct PolicyProgress {
    /// The position discovery has swept to, as last written to `policy_cursors`.
    swept_through: i64,
    /// Streams this Policy is behind on that the last poll could not finish, so the next
    /// one looks at them whatever the sweep finds. Bounded by the poll's stream limit.
    unfinished: Vec<String>,
    /// Which source takes the first slot of the next poll. Rotating it is what keeps a
    /// batch too small to divide between the three sources from shutting one out.
    share_from: usize,
    /// The stream id the last reconciliation admitted, so the next one resumes after it.
    /// Persisted, because a runner that restarts often would otherwise rotate from the
    /// start every time and never reach the end of the sort order.
    reconciled_through: String,
    /// When the frontier was last compared with the places. `None` until the first
    /// reconciliation, so a worker that has just been elected does one straight away —
    /// which is what makes a crash mid-backlog cost a cadence rather than a deployment.
    reconciled_at: Option<Instant>,
    /// How long the sweep is trusted on its own. This is the whole exposure of the
    /// design: a write that commits below the sweep is delivered within a bounded number
    /// of these, which
    /// [ADR-0026](../../docs/adr/0026-a-policy-tracks-its-position-per-stream.md) states
    /// and this is the unit of.
    reconcile_every: Duration,
}

impl PolicyProgress {
    /// Load the Policy's sweep position, creating its row the first time it runs.
    async fn load(
        pool: &Pool<Postgres>,
        name: &str,
        start_at: StartAt,
    ) -> Result<Self, replay::Error> {
        let (swept_through, reconciled_through) = match read_sweep(pool, name).await? {
            Some(progress) => progress,
            None => bootstrap(pool, name, start_at).await?,
        };

        Ok(Self {
            swept_through,
            unfinished: Vec::new(),
            share_from: 0,
            reconciled_through,
            reconciled_at: None,
            reconcile_every: resolve_reconcile_cadence(),
        })
    }

    /// Make the next poll pay for the frontier scan whatever the cadence says.
    fn reconcile_now(&mut self) {
        self.reconciled_at = None;
    }

    /// Whether this poll pays for the frontier scan.
    fn reconcile_is_due(&self) -> bool {
        self.reconciled_at
            .is_none_or(|last| last.elapsed() >= self.reconcile_every)
    }
}

/// Built-in default for [`resolve_reconcile_cadence`].
///
/// Five seconds is a latency bound, not a correctness one: it is how long a Policy can
/// take to notice a write that committed below its sweep. Lower it for a deployment that
/// cares more about that tail than about scanning one row per stream; raise it for one
/// with millions of streams and no long transactions.
const DEFAULT_RECONCILE_CADENCE: Duration = Duration::from_secs(5);

/// Environment variable overriding the built-in cadence, in seconds.
const RECONCILE_CADENCE_ENV_VAR: &str = "REPLAY_POLICY_RECONCILE_SECS";

/// How often a Policy compares every stream's head with its own place.
///
/// Precedence: `REPLAY_POLICY_RECONCILE_SECS` → 5s. `0` and unparseable values fall back
/// to the default rather than turning every poll into a full scan.
fn resolve_reconcile_cadence() -> Duration {
    std::env::var(RECONCILE_CADENCE_ENV_VAR)
        .ok()
        .and_then(|raw| raw.parse::<u64>().ok())
        .filter(|secs| *secs > 0)
        .map_or(DEFAULT_RECONCILE_CADENCE, Duration::from_secs)
}

/// Read how far a Policy has swept and where its rotation left off, or `None` when it has
/// no row yet.
async fn read_sweep(
    pool: &Pool<Postgres>,
    name: &str,
) -> Result<Option<(i64, String)>, replay::Error> {
    let row = sqlx::query(
        "SELECT discovered_through, reconciled_through FROM policy_cursors WHERE name = $1",
    )
    .bind(name)
    .fetch_optional(pool)
    .await
    .map_err(crate::db_error)?;

    Ok(row.map(|row| (row.get("discovered_through"), row.get("reconciled_through"))))
}

/// Create a Policy's row, and for [`StartAt::Now`] the places that mean "from here".
///
/// `Now` is the one case that writes a place per existing stream: every stream is at its
/// current head, so nothing already written is delivered. It is a single statement and it
/// happens once in a Policy's life, but it is one row per stream, which is worth knowing
/// before pointing a new Policy at a database with a million of them.
///
/// A stream created *after* this has no row and starts at 0 — correct without a special
/// case, because everything in it was written after the Policy started.
///
/// A write in flight while this runs is not counted, so its events are delivered when it
/// commits. That is the at-least-once side of the trade: a `Now` Policy may see an event
/// from just before it started, and never misses one from just after.
///
/// The two reads it takes — where the log ends, and where each stream ends — have to be
/// one snapshot, which is why this runs in a `REPEATABLE READ` transaction. Read
/// separately, a write committing between them is seeded as processed while sitting above
/// the position the search starts from: nominated by no sweep, owed by no place, and lost
/// for good.
///
/// The policy's row is claimed *before* anything is seeded, and a runner that loses the
/// claim seeds nothing. Seeding first would let two runners bootstrapping at once combine
/// one's sweep position with the other's places: the loser's seed for a stream created
/// after the winner's snapshot conflicts with nothing, so it stands, and says a stream was
/// processed through an event above where the winner's search starts
/// (funkode-io/replay#231 review).
async fn bootstrap(
    pool: &Pool<Postgres>,
    name: &str,
    start_at: StartAt,
) -> Result<(i64, String), replay::Error> {
    let mut tx = pool.begin().await.map_err(crate::db_error)?;
    sqlx::query("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ")
        .execute(&mut *tx)
        .await
        .map_err(crate::db_error)?;

    // The claim, and the snapshot everything below is read in. A concurrent bootstrap
    // queues here on the primary key and comes back empty-handed.
    let claimed = sqlx::query(
        "INSERT INTO policy_cursors (name, discovered_through, updated_at) \
         VALUES ($1, 0, now()) ON CONFLICT (name) DO NOTHING RETURNING name",
    )
    .bind(name)
    .fetch_optional(&mut *tx)
    .await
    .map_err(crate::db_error)?;

    if claimed.is_none() {
        tx.rollback().await.map_err(crate::db_error)?;

        // Both halves come from the row, which is the Policy's and not this process's
        // opinion of it. The rotation of a row that has just been claimed is empty, so
        // reading it rather than assuming it changes nothing today — but a value that is
        // right because of when it is read is one edit from being wrong
        // (funkode-io/replay#231 review).
        return Ok(read_sweep(pool, name).await?.unwrap_or_default());
    }

    let swept_through = match start_at {
        StartAt::Beginning => 0,
        StartAt::Now => {
            sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(global_position) FROM events")
                .fetch_one(&mut *tx)
                .await
                .map_err(crate::db_error)?
                .unwrap_or_default()
        }
    };

    if matches!(start_at, StartAt::Now) {
        sqlx::query(
            "INSERT INTO policy_stream_cursors (policy, stream_id, stream_seq) \
             SELECT $1, s.id, s.stream_seq FROM streams s WHERE s.stream_seq > 0 \
             ON CONFLICT DO NOTHING",
        )
        .bind(name)
        .execute(&mut *tx)
        .await
        .map_err(crate::db_error)?;
    }

    // The claim went in at 0 so that it could be made before the log was read; nobody has
    // seen it yet, because it is this transaction's own uncommitted row.
    sqlx::query("UPDATE policy_cursors SET discovered_through = $2 WHERE name = $1")
        .bind(name)
        .bind(swept_through)
        .execute(&mut *tx)
        .await
        .map_err(crate::db_error)?;

    tx.commit().await.map_err(crate::db_error)?;

    // The rotation starts at the beginning of the stream ids, which is what the row this
    // transaction just inserted carries.
    Ok((swept_through, String::new()))
}

/// Record how far discovery has swept.
///
/// Monotonic and unconditional: two runners for one Policy cannot make this wrong, since
/// the worst a stale value does is sweep a stretch of log again. `updated_at` is left
/// alone — it records when the Policy last *processed* something, which a search does not.
async fn write_sweep(
    pool: &Pool<Postgres>,
    name: &str,
    swept_through: i64,
) -> Result<(), replay::Error> {
    sqlx::query(
        "UPDATE policy_cursors SET discovered_through = $2 \
         WHERE name = $1 AND discovered_through < $2",
    )
    .bind(name)
    .bind(swept_through)
    .execute(pool)
    .await
    .map_err(crate::db_error)?;
    Ok(())
}

/// Record where the rotation left off, so a restart resumes the pass rather than
/// restarting it. Unconditional, unlike the sweep: the rotation wraps, so "backwards" is
/// where it is meant to go once per pass.
async fn write_reconciled(
    pool: &Pool<Postgres>,
    name: &str,
    reconciled_through: &str,
) -> Result<(), replay::Error> {
    sqlx::query("UPDATE policy_cursors SET reconciled_through = $2 WHERE name = $1")
        .bind(name)
        .bind(reconciled_through)
        .execute(pool)
        .await
        .map_err(crate::db_error)?;
    Ok(())
}

/// Typed representation of the `causation` block stamped in event metadata by
/// every policy reaction.  Using a struct instead of manual `Value` navigation
/// ensures the write (serialization) and read (deserialization) paths stay in sync.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CausationInfo {
    policy: String,
    event_id: String,
    stream_id: String,
    global_position: i64,
    /// How many policy hops deep this event is.  Root events (from normal
    /// `append`) carry no causation block and are treated as depth 0.
    #[serde(default)]
    depth: u32,
}

/// Top-level metadata payload written by the runner for each policy-issued command.
#[derive(Debug, Serialize)]
struct CausationPayload {
    causation: CausationInfo,
}

/// Causation metadata stamped on every command a policy issues.
///
/// Records which policy reacted and which event triggered it, and increments the
/// causation depth so the runner can detect runaway event→command→event cascades.
fn causation_metadata(
    policy_name: &str,
    global_position: i64,
    raw: &PersistedEvent<Value>,
) -> Metadata {
    Metadata::new(CausationPayload {
        causation: CausationInfo {
            policy: policy_name.to_string(),
            event_id: raw.id.to_string(),
            stream_id: raw.stream_id.to_string(),
            global_position,
            depth: event_causation_depth(raw) + 1,
        },
    })
}

/// Extract the causation depth from an event's metadata.
///
/// Events written by normal `append` calls carry no `causation` block and are
/// treated as depth 0 (the root of a potential chain).  Events emitted by policy
/// reactions carry the depth stamped in [`causation_metadata`].
fn event_causation_depth(raw: &PersistedEvent<Value>) -> u32 {
    parse_causation_info(raw).map(|c| c.depth).unwrap_or(0)
}

/// Deserialize the `causation` block from an event's metadata, if present.
fn parse_causation_info(raw: &PersistedEvent<Value>) -> Option<CausationInfo> {
    raw.metadata
        .to_json()
        .get("causation")
        .cloned()
        .and_then(|v| serde_json::from_value(v).ok())
}

/// Built-in default maximum causation depth (circuit-breaker for loops).
const DEFAULT_MAX_CAUSATION_DEPTH: u32 = 10;

/// Environment variable that overrides the built-in depth limit.
const CAUSATION_DEPTH_ENV_VAR: &str = "REPLAY_MAX_CAUSATION_DEPTH";

/// Built-in default for the number of events fetched per drain call.
const DEFAULT_READ_BATCH_SIZE: u32 = 100;

/// Built-in default for the number of events between cursor persistence writes.
const DEFAULT_CHECKPOINT_BATCH_SIZE: u32 = 100;

/// Reactions read per page by [`PolicyRunner::retry_policy_dead_letters`].
///
/// Not a tunable: it bounds a buffer, and the buffer is two scalars per entry.
/// Raising it would buy nothing an operator can measure, and the round trip it
/// saves is dwarfed by the replays each page performs.
const RETRY_PAGE_SIZE: i64 = 100;

/// Rows of one reaction's group read per page while it is settled.
///
/// Not a tunable either, and the same shape of bound as [`RETRY_PAGE_SIZE`]: it
/// caps what a settlement holds, which is a row's identity strings. A retry
/// settles every row of the reaction whatever this is (ADR-0025); the number
/// only decides how many round trips that takes.
const RETRY_ROW_PAGE_SIZE: i64 = 100;

/// Environment variable that overrides the read-batch default.
const READ_BATCH_SIZE_ENV_VAR: &str = "REPLAY_READ_BATCH_SIZE";

/// Environment variable that overrides the checkpoint-batch default.
const CHECKPOINT_BATCH_SIZE_ENV_VAR: &str = "REPLAY_CHECKPOINT_BATCH_SIZE";

/// Built-in default for how long one dispatch may run before it is abandoned.
///
/// Generous on purpose: it exists to cut loose a reaction that has *stopped*,
/// not to enforce a latency budget, so it must not park a reaction that is
/// merely slow. A reaction with a legitimately longer ceiling raises it with
/// [`PolicySettings::with_dispatch_timeout`].
const DEFAULT_DISPATCH_TIMEOUT: Duration = Duration::from_secs(30);

/// Environment variable that overrides the dispatch-timeout default, in
/// milliseconds.
const DISPATCH_TIMEOUT_ENV_VAR: &str = "REPLAY_DISPATCH_TIMEOUT_MS";

/// Resolve the effective causation depth limit for a policy.
///
/// Precedence (most-specific wins):
///   1. Per-policy override via [`PolicySettings::with_max_causation_depth`]
///   2. `REPLAY_MAX_CAUSATION_DEPTH` environment variable
///   3. Built-in default (10)
fn resolve_max_depth(settings: &PolicySettings) -> u32 {
    resolve_max_depth_with_source(settings).0
}

/// Like [`resolve_max_depth`] but also returns the source for diagnostic logging.
fn resolve_max_depth_with_source(settings: &PolicySettings) -> (u32, &'static str) {
    if let Some(d) = settings.max_causation_depth() {
        return (d, "policy override");
    }
    if let Ok(s) = std::env::var(CAUSATION_DEPTH_ENV_VAR) {
        if let Ok(d) = s.parse::<u32>() {
            return (d, CAUSATION_DEPTH_ENV_VAR);
        }
    }
    (DEFAULT_MAX_CAUSATION_DEPTH, "built-in default")
}

/// Resolve the effective checkpoint batch size (events between cursor saves).
///
/// Precedence: per-policy override → `REPLAY_CHECKPOINT_BATCH_SIZE` env var → default 100.
fn resolve_checkpoint_batch_size(settings: &PolicySettings) -> u32 {
    if let Some(n) = settings.checkpoint_batch_size() {
        return n.max(1);
    }
    if let Ok(s) = std::env::var(CHECKPOINT_BATCH_SIZE_ENV_VAR) {
        if let Ok(n) = s.parse::<u32>() {
            return n.max(1);
        }
    }
    DEFAULT_CHECKPOINT_BATCH_SIZE
}

/// Resolve the effective read-batch size: the most one poll reads of any one stream, and
/// the most streams one poll looks at.
///
/// Precedence: per-policy override → `REPLAY_READ_BATCH_SIZE` env var → default 100.
/// Enforces the invariant `read_batch_size ≥ checkpoint_batch_size`.
fn resolve_read_batch_size(settings: &PolicySettings, checkpoint_size: u32) -> u32 {
    let raw = if let Some(n) = settings.read_batch_size() {
        n.max(1)
    } else if let Ok(s) = std::env::var(READ_BATCH_SIZE_ENV_VAR) {
        s.parse::<u32>().unwrap_or(DEFAULT_READ_BATCH_SIZE).max(1)
    } else {
        DEFAULT_READ_BATCH_SIZE
    };
    // Invariant: read_batch_size ≥ checkpoint_batch_size.
    raw.max(checkpoint_size)
}

/// Resolve the effective dispatch timeout (how long one dispatch may run).
///
/// Precedence: per-policy override → `REPLAY_DISPATCH_TIMEOUT_MS` env var →
/// default 30s. `0` and unparseable values fall back to the default rather than
/// abandoning every dispatch the moment it starts.
fn resolve_dispatch_timeout(settings: &PolicySettings) -> Duration {
    dispatch_timeout_or_default(
        settings.dispatch_timeout(),
        std::env::var(DISPATCH_TIMEOUT_ENV_VAR).ok(),
    )
}

/// The precedence itself, over values rather than the process environment, so it
/// is testable without mutating global state.
fn dispatch_timeout_or_default(declared: Option<Duration>, env: Option<String>) -> Duration {
    if let Some(timeout) = declared {
        if !timeout.is_zero() {
            return timeout;
        }
    }
    env.and_then(|raw| raw.parse::<u64>().ok())
        .filter(|millis| *millis > 0)
        .map_or(DEFAULT_DISPATCH_TIMEOUT, Duration::from_millis)
}

fn merge_dispatch_metadata(
    causation: Metadata,
    dispatch: Option<Metadata>,
) -> Result<Metadata, replay::Error> {
    let Some(dispatch) = dispatch else {
        return Ok(causation);
    };

    let Value::Object(mut merged) = causation.to_json() else {
        return Err(replay::Error::internal(
            "policy causation metadata must be a JSON object",
        ));
    };

    let Value::Object(extra) = dispatch.to_json() else {
        return Err(replay::Error::invalid_input(
            "policy dispatch metadata must be a JSON object",
        ));
    };

    merge_no_collisions(&mut merged, extra)?;
    Ok(Metadata::from_json(Value::Object(merged)))
}

fn merge_no_collisions(
    destination: &mut Map<String, Value>,
    source: Map<String, Value>,
) -> Result<(), replay::Error> {
    for (key, value) in source {
        if destination.contains_key(&key) {
            return Err(replay::Error::invalid_input(
                "policy dispatch metadata contains a key that collides with causation metadata",
            )
            .with_context("key", key));
        }
        destination.insert(key, value);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use replay::Metadata;
    use serde_json::json;

    use super::{
        dispatch_timeout_or_default, merge_dispatch_metadata, panic_message, DispatchFailure,
        DEFAULT_DISPATCH_TIMEOUT, TIMEOUT_ERROR_KIND,
    };

    #[test]
    fn a_policy_that_declares_a_timeout_gets_it() {
        let declared = Duration::from_millis(250);

        assert_eq!(
            dispatch_timeout_or_default(Some(declared), Some("900".into())),
            declared,
            "a declared override beats the environment"
        );
    }

    #[test]
    fn the_env_var_is_read_when_no_policy_declares_a_timeout() {
        assert_eq!(
            dispatch_timeout_or_default(None, Some("900".into())),
            Duration::from_millis(900)
        );
    }

    /// A zero timeout would abandon every dispatch the moment it started, so it
    /// is read as "unset" rather than obeyed — from either source.
    #[test]
    fn a_zero_timeout_is_not_a_timeout() {
        assert_eq!(
            dispatch_timeout_or_default(Some(Duration::ZERO), None),
            DEFAULT_DISPATCH_TIMEOUT
        );
        assert_eq!(
            dispatch_timeout_or_default(None, Some("0".into())),
            DEFAULT_DISPATCH_TIMEOUT
        );
    }

    #[test]
    fn an_unparseable_env_var_falls_back_to_the_default() {
        assert_eq!(
            dispatch_timeout_or_default(None, Some("30s".into())),
            DEFAULT_DISPATCH_TIMEOUT
        );
        assert_eq!(
            dispatch_timeout_or_default(None, None),
            DEFAULT_DISPATCH_TIMEOUT
        );
    }

    /// A timeout is parked under its own kind, and says what it exceeded: the
    /// hung command returned nothing to describe itself with.
    #[test]
    fn a_timed_out_dispatch_parks_as_a_timeout() {
        let failure = DispatchFailure::TimedOut {
            limit: Duration::from_millis(1_500),
        };

        assert_eq!(failure.error_kind(), TIMEOUT_ERROR_KIND);
        assert!(failure.retryable(), "a hang may still be a passing outage");
        assert!(!failure.declined());
        assert!(
            failure.to_string().contains("1500 ms"),
            "the parked message must name the limit it blew, got {failure}"
        );
    }

    /// The classification a returned error carries is untouched by the timeout.
    #[test]
    fn a_returned_error_keeps_its_own_kind() {
        let declined = DispatchFailure::Returned(replay::Error::business_rule_violation(
            "the account is frozen",
        ));
        let transient = DispatchFailure::Returned(replay::Error::unavailable("the ledger is down"));
        let permanent = DispatchFailure::Returned(replay::Error::invalid_input("no such account"));

        assert!(declined.declined());
        assert!(transient.retryable());
        assert!(!permanent.retryable());
        assert_eq!(
            permanent.error_kind(),
            replay::ErrorKind::InvalidInput.to_string()
        );
    }

    #[test]
    fn reads_the_message_of_a_literal_panic() {
        let payload = std::panic::catch_unwind(|| panic!("reaction exploded"))
            .expect_err("must have panicked");

        assert_eq!(panic_message(&*payload), "reaction exploded");
    }

    #[test]
    fn reads_the_message_of_a_formatted_panic() {
        let event = "evt-7";
        let payload = std::panic::catch_unwind(|| panic!("reaction exploded on {event}"))
            .expect_err("must have panicked");

        assert_eq!(panic_message(&*payload), "reaction exploded on evt-7");
    }

    #[test]
    fn reports_a_panic_payload_that_carries_no_message() {
        let payload = std::panic::catch_unwind(|| std::panic::panic_any(42u8))
            .expect_err("must have panicked");

        assert_eq!(
            panic_message(&*payload),
            "reaction panicked with a payload that is neither &str nor String"
        );
    }

    #[test]
    fn merges_dispatch_metadata_without_collisions() {
        let causation = Metadata::new(json!({
            "causation": { "policy": "p", "global_position": 1 }
        }));
        let dispatch = Metadata::new(json!({
            "user_id": "u-1",
            "related_aggregate_id": "urn:catalog:1"
        }));

        let merged = merge_dispatch_metadata(causation, Some(dispatch)).expect("must merge");
        let value = merged.to_json();

        assert_eq!(value["causation"]["policy"], "p");
        assert_eq!(value["user_id"], "u-1");
        assert_eq!(value["related_aggregate_id"], "urn:catalog:1");
    }

    #[test]
    fn errors_on_metadata_key_collision() {
        let causation = Metadata::new(json!({ "causation": { "policy": "p" } }));
        let dispatch = Metadata::new(json!({ "causation": { "override": true } }));

        let err = merge_dispatch_metadata(causation, Some(dispatch)).expect_err("must fail");

        assert_eq!(err.kind(), replay::ErrorKind::InvalidInput);
        assert!(err
            .to_string()
            .contains("policy dispatch metadata contains a key that collides"));
    }
}

#[cfg(test)]
mod restart_budget_tests {
    use std::time::{Duration, Instant};

    use super::{RestartBudget, RestartDecision, WorkerSupervision};

    /// Three restarts a minute, 100 ms doubling to a 400 ms ceiling.
    fn supervision() -> WorkerSupervision {
        WorkerSupervision::default()
            .max_restarts(3)
            .restart_window(Duration::from_secs(60))
            .initial_backoff(Duration::from_millis(100))
            .max_backoff(Duration::from_millis(400))
    }

    fn restart(decision: RestartDecision) -> (Duration, u32) {
        match decision {
            RestartDecision::Restart { backoff, restarts } => (backoff, restarts),
            RestartDecision::Exhausted { restarts } => {
                panic!("expected a restart, got exhaustion after {restarts}")
            }
        }
    }

    /// A worker failing against a struggling database must not spin: each
    /// restart in the window waits twice as long as the one before it.
    #[test]
    fn each_restart_in_a_window_waits_twice_as_long() {
        let now = Instant::now();
        let mut budget = RestartBudget::new(supervision());

        assert_eq!(
            restart(budget.record_death(now)),
            (Duration::from_millis(100), 1)
        );
        assert_eq!(
            restart(budget.record_death(now + Duration::from_millis(1))),
            (Duration::from_millis(200), 2)
        );
        assert_eq!(
            restart(budget.record_death(now + Duration::from_millis(2))),
            (Duration::from_millis(400), 3)
        );
    }

    /// The doubling stops at the configured ceiling rather than growing until a
    /// restart is indistinguishable from never happening.
    #[test]
    fn the_backoff_stops_doubling_at_the_ceiling() {
        let supervision = supervision().max_restarts(10);
        let now = Instant::now();
        let mut budget = RestartBudget::new(supervision);

        let backoffs: Vec<Duration> = (0..6)
            .map(|nth| restart(budget.record_death(now + Duration::from_millis(nth))).0)
            .collect();

        assert_eq!(
            backoffs,
            vec![
                Duration::from_millis(100),
                Duration::from_millis(200),
                Duration::from_millis(400),
                Duration::from_millis(400),
                Duration::from_millis(400),
                Duration::from_millis(400),
            ]
        );
    }

    /// "Restarting forever" must never pass for "running": once the window's
    /// restarts are spent the worker stays down.
    #[test]
    fn a_fourth_death_inside_the_window_exhausts_a_budget_of_three() {
        let now = Instant::now();
        let mut budget = RestartBudget::new(supervision());

        for nth in 0..3 {
            restart(budget.record_death(now + Duration::from_millis(nth)));
        }

        assert_eq!(
            budget.record_death(now + Duration::from_secs(59)),
            RestartDecision::Exhausted { restarts: 3 }
        );
    }

    /// The budget is a window, not a lifetime total: a worker that dies once a
    /// day is restarted every day.
    #[test]
    fn a_death_past_the_window_restarts_with_a_fresh_budget() {
        let now = Instant::now();
        let mut budget = RestartBudget::new(supervision());

        for nth in 0..3 {
            restart(budget.record_death(now + Duration::from_millis(nth)));
        }

        // Far enough out that every earlier restart has aged out of the window.
        let later = now + Duration::from_secs(61);
        assert_eq!(
            restart(budget.record_death(later)),
            (Duration::from_millis(100), 1),
            "the first restart of a new window backs off from the start again"
        );
    }

    /// A budget of zero is how a consumer says "never restart this": the first
    /// death is terminal, and nothing waits for a backoff that will not be used.
    #[test]
    fn a_budget_of_zero_makes_the_first_death_terminal() {
        let mut budget = RestartBudget::new(supervision().max_restarts(0));

        assert_eq!(
            budget.record_death(Instant::now()),
            RestartDecision::Exhausted { restarts: 0 }
        );
    }

    /// A zero-length window is "no window", not "everything has already expired":
    /// the budget still bounds the restarts.
    #[test]
    fn a_window_of_zero_still_bounds_the_restarts() {
        let now = Instant::now();
        let mut budget = RestartBudget::new(supervision().restart_window(Duration::ZERO));

        for nth in 0..3 {
            restart(budget.record_death(now + Duration::from_millis(nth)));
        }

        assert_eq!(
            budget.record_death(now + Duration::from_secs(3_600)),
            RestartDecision::Exhausted { restarts: 3 },
            "no age makes a restart stop counting against a zero window"
        );
    }

    /// The bookkeeping is bounded by the budget, not by how long the process has
    /// been up or by how often the worker has died in it.
    #[test]
    fn the_recorded_deaths_never_outgrow_the_budget() {
        let now = Instant::now();
        let mut budget = RestartBudget::new(supervision());

        for nth in 0..1_000 {
            let _ = budget.record_death(now + Duration::from_millis(nth));
        }

        assert!(
            budget.spent.len() <= 3,
            "the window holds at most the budget, got {}",
            budget.spent.len()
        );
    }
}

#[cfg(test)]
mod supervisor_tests {
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex};
    use std::time::Duration;

    use tokio::sync::watch;
    use tracing_test::traced_test;

    use super::{
        supervise, Escalation, EscalationReason, LivenessRegistry, RevokeLeadership, Stop,
        StoppedWorkers, SupervisedTask, WorkerSupervision,
    };
    use crate::Liveness;

    /// Backoffs short enough that a test waits on outcomes rather than on time.
    fn supervision() -> WorkerSupervision {
        WorkerSupervision::default()
            .max_restarts(2)
            .initial_backoff(Duration::from_millis(1))
            .max_backoff(Duration::from_millis(2))
    }

    /// An escalation hook that records rather than exits — the only kind a test
    /// can install, since the default one ends the test process.
    #[derive(Clone, Default)]
    struct Escalations(Arc<Mutex<Vec<Escalation>>>);

    impl Escalations {
        fn hook(&self) -> impl Fn(&Escalation) + Send + Sync + 'static {
            let recorded = Arc::clone(&self.0);
            move |escalation: &Escalation| recorded.lock().unwrap().push(escalation.clone())
        }

        fn recorded(&self) -> Vec<Escalation> {
            self.0.lock().unwrap().clone()
        }
    }

    fn a_policy_worker(stopped: &StoppedWorkers, escalations: &Escalations) -> SupervisedTask {
        a_policy_worker_reporting(stopped, escalations, &LivenessRegistry::default())
    }

    /// The same, with its liveness published where a test can read it.
    fn a_policy_worker_reporting(
        stopped: &StoppedWorkers,
        escalations: &Escalations,
        liveness: &LivenessRegistry,
    ) -> SupervisedTask {
        SupervisedTask {
            kind: "policy worker",
            policy: Some("supervised".to_string()),
            stopped: stopped.clone(),
            liveness: Some(liveness.register("supervised")),
            on_escalation: Arc::new(escalations.hook()),
        }
    }

    /// A task that stops because it was asked to is not a fault: it is not
    /// restarted, and nothing is reported.
    #[tokio::test]
    async fn a_task_that_shuts_down_is_not_restarted() {
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let stopped = StoppedWorkers::default();
        let escalations = Escalations::default();
        let attempts = Arc::new(AtomicUsize::new(0));

        let spawned = Arc::clone(&attempts);
        supervise(
            a_policy_worker(&stopped, &escalations),
            supervision(),
            shutdown_rx,
            move || {
                spawned.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async { Stop::Shutdown })
            },
        )
        .await;

        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert!(stopped.snapshot().is_empty());
        assert!(escalations.recorded().is_empty());
    }

    /// A task that keeps dying is restarted its budget's worth of times and then
    /// reported, rather than restarted forever or dropped in silence.
    ///
    /// The `warn` is asserted here because it is what an operator sees first: it
    /// has to name the policy and how many restarts the window has seen
    /// (funkode-io/replay#185).
    #[tokio::test]
    #[traced_test]
    async fn a_task_that_keeps_dying_is_restarted_then_reported() {
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let stopped = StoppedWorkers::default();
        let escalations = Escalations::default();
        let attempts = Arc::new(AtomicUsize::new(0));

        let spawned = Arc::clone(&attempts);
        supervise(
            a_policy_worker(&stopped, &escalations),
            supervision(),
            shutdown_rx,
            move || {
                spawned.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async { panic!("died in the night") })
            },
        )
        .await;

        assert_eq!(
            attempts.load(Ordering::SeqCst),
            3,
            "the first run plus two restarts"
        );
        let stopped = stopped.snapshot();
        assert_eq!(stopped.len(), 1);
        assert_eq!(stopped[0].policy, "supervised");
        assert_eq!(stopped[0].restarts, 2);

        assert_eq!(
            escalations.recorded(),
            vec![Escalation {
                policy: "supervised".to_string(),
                reason: EscalationReason::BudgetExhausted {
                    restarts: 2,
                    cause: "died in the night".to_string(),
                },
            }],
            "the hook fires once, naming the policy and why it is down"
        );

        logs_assert(|lines| {
            let restarts: Vec<_> = lines
                .iter()
                .filter(|line| line.contains("restarting after backoff"))
                .collect();
            if restarts.len() != 2 {
                return Err(format!("one line per restart, got {}", restarts.len()));
            }
            for (nth, line) in restarts.iter().enumerate() {
                for field in [
                    "WARN",
                    "policy=\"supervised\"",
                    &format!("restarts={}", nth + 1),
                ] {
                    if !line.contains(field) {
                        return Err(format!("the restart warning lacks {field}: {line}"));
                    }
                }
            }
            Ok(())
        });
    }

    /// A worker whose lock manager is gone cannot be helped by restarting it, so
    /// it is reported straight away instead of burning a budget first.
    #[tokio::test]
    async fn an_abandoned_task_is_reported_without_being_restarted() {
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let stopped = StoppedWorkers::default();
        let escalations = Escalations::default();
        let liveness = LivenessRegistry::default();
        let attempts = Arc::new(AtomicUsize::new(0));

        let spawned = Arc::clone(&attempts);
        supervise(
            a_policy_worker_reporting(&stopped, &escalations, &liveness),
            supervision(),
            shutdown_rx,
            move || {
                spawned.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async { Stop::Abandoned })
            },
        )
        .await;

        assert_eq!(attempts.load(Ordering::SeqCst), 1);
        assert_eq!(stopped.snapshot()[0].policy, "supervised");
        assert_eq!(
            liveness.snapshot()[0].liveness,
            Liveness::Stopped,
            "a worker nothing can elect again reads as stopped, not standing by"
        );
        assert_eq!(
            escalations.recorded(),
            vec![Escalation {
                policy: "supervised".to_string(),
                reason: EscalationReason::Abandoned,
            }],
            "a worker nothing can elect again is as absent as one that spent its budget"
        );
    }

    /// The two states supervision owns, in the order it produces them: a worker
    /// waiting out its backoff is restarting — down now, back shortly — and one
    /// whose budget then runs out is stopped.
    #[tokio::test]
    async fn a_worker_reports_restarting_inside_its_backoff_and_stopped_after_it() {
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let stopped = StoppedWorkers::default();
        let escalations = Escalations::default();
        let liveness = LivenessRegistry::default();

        // One restart, and a backoff long enough to be observed rather than
        // raced past.
        let supervision = supervision()
            .max_restarts(1)
            .initial_backoff(Duration::from_millis(200))
            .max_backoff(Duration::from_millis(200));

        let supervising = tokio::spawn(supervise(
            a_policy_worker_reporting(&stopped, &escalations, &liveness),
            supervision,
            shutdown_rx,
            || tokio::spawn(async { panic!("died in the night") }),
        ));

        await_liveness(&liveness, Liveness::Restarting).await;
        supervising.await.expect("supervision must end cleanly");

        assert_eq!(liveness.snapshot()[0].liveness, Liveness::Stopped);
        assert_eq!(stopped.snapshot()[0].restarts, 1);
    }

    /// Wait for the supervised worker to publish `expected`, failing rather than
    /// hanging if it never does.
    async fn await_liveness(liveness: &LivenessRegistry, expected: Liveness) {
        let published = tokio::time::timeout(Duration::from_secs(5), async {
            while liveness.snapshot()[0].liveness != expected {
                tokio::time::sleep(Duration::from_millis(1)).await;
            }
        })
        .await;

        assert!(
            published.is_ok(),
            "the worker never reported {expected}; it reported {}",
            liveness.snapshot()[0].liveness
        );
    }

    /// A hook that returns has declined to end the process. The library's own
    /// obligation is unchanged: the worker is recorded as stopped, so the policy
    /// is never silently absent.
    #[tokio::test]
    async fn a_hook_that_returns_leaves_the_worker_recorded_as_stopped() {
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let stopped = StoppedWorkers::default();
        let escalations = Escalations::default();

        supervise(
            a_policy_worker(&stopped, &escalations),
            supervision().max_restarts(0),
            shutdown_rx,
            || tokio::spawn(async { panic!("died once") }),
        )
        .await;

        assert_eq!(escalations.recorded().len(), 1);
        assert_eq!(stopped.snapshot()[0].policy, "supervised");

        // A budget of zero makes the first death terminal, so no restart was ever
        // made and the reason must not read as though one had been.
        assert_eq!(
            escalations.recorded()[0].reason,
            EscalationReason::BudgetExhausted {
                restarts: 0,
                cause: "died once".to_string(),
            }
        );
    }

    /// `restarts` counts restarts made, not deaths suffered — the death being
    /// escalated is the one no restart was left for, and the text an operator
    /// reads must not turn the two into each other.
    #[test]
    fn the_reason_reads_as_restarts_spent_not_deaths_counted() {
        let spent = EscalationReason::BudgetExhausted {
            restarts: 2,
            cause: "died in the night".to_string(),
        }
        .to_string();
        assert!(
            spent.contains("restart budget spent (2 used in the window)"),
            "a worker restarted twice and dead a third time reads: {spent}"
        );

        let never_restarted = EscalationReason::BudgetExhausted {
            restarts: 0,
            cause: "died once".to_string(),
        }
        .to_string();
        assert!(
            !never_restarted.contains("0 times"),
            "a budget of zero must not report zero deaths: {never_restarted}"
        );
    }

    /// A defective hook is the consumer's problem and must not become the
    /// library's: the worker is recorded stopped before the hook runs, so a hook
    /// that panics costs the process nothing it was not already losing.
    #[tokio::test]
    async fn a_panicking_hook_does_not_take_the_report_with_it() {
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let stopped = StoppedWorkers::default();

        supervise(
            SupervisedTask {
                kind: "policy worker",
                policy: Some("supervised".to_string()),
                stopped: stopped.clone(),
                liveness: None,
                on_escalation: Arc::new(|_| panic!("the consumer's hook is defective")),
            },
            supervision().max_restarts(0),
            shutdown_rx,
            || tokio::spawn(async { panic!("died once") }),
        )
        .await;

        assert_eq!(stopped.snapshot()[0].policy, "supervised");
    }

    /// A worker that stops while the daemon is shutting down has not failed at    /// anything: escalating there would exit a process that is already on its way
    /// out, and would do it on every clean shutdown.
    #[tokio::test]
    async fn a_stop_during_shutdown_escalates_nothing() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let stopped = StoppedWorkers::default();
        let escalations = Escalations::default();

        let shutting_down = shutdown_tx.clone();
        supervise(
            a_policy_worker(&stopped, &escalations),
            supervision(),
            shutdown_rx,
            move || {
                // The daemon is shutting down; a worker racing the lock manager's
                // dropped leadership channel reads it as abandonment.
                let _ = shutting_down.send(true);
                tokio::spawn(async { Stop::Abandoned })
            },
        )
        .await;

        assert!(escalations.recorded().is_empty());
        assert!(
            stopped.snapshot().is_empty(),
            "a shutdown is not a worker the runner gave up on"
        );
    }

    /// A shared task names no policy, so its stop is left to the policies it
    /// abandons to report — `stopped_workers` stays a list of policies, and no
    /// escalation fires for plumbing that owns no policy.
    #[tokio::test]
    async fn a_shared_task_that_stops_reports_no_policy() {
        let (_shutdown_tx, shutdown_rx) = watch::channel(false);
        let stopped = StoppedWorkers::default();
        let escalations = Escalations::default();

        supervise(
            SupervisedTask {
                kind: "lock manager",
                policy: None,
                stopped: stopped.clone(),
                liveness: None,
                on_escalation: Arc::new(escalations.hook()),
            },
            supervision(),
            shutdown_rx,
            || tokio::spawn(async { panic!("the lock manager died") }),
        )
        .await;

        assert!(stopped.snapshot().is_empty());
        assert!(escalations.recorded().is_empty());
    }

    /// Shutdown during a backoff ends supervision there and then: a daemon
    /// shutting down never waits out a restart it is not going to make.
    #[tokio::test]
    async fn shutdown_during_a_backoff_ends_supervision() {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let stopped = StoppedWorkers::default();
        let escalations = Escalations::default();
        let attempts = Arc::new(AtomicUsize::new(0));

        let spawned = Arc::clone(&attempts);
        let supervisor = tokio::spawn(supervise(
            a_policy_worker(&stopped, &escalations),
            // A backoff no test would wait out on purpose.
            supervision().initial_backoff(Duration::from_secs(30)),
            shutdown_rx,
            move || {
                spawned.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async { panic!("died once") })
            },
        ));

        // Let the first death happen, then shut down mid-backoff.
        while attempts.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
        let _ = shutdown_tx.send(true);

        tokio::time::timeout(Duration::from_secs(5), supervisor)
            .await
            .expect("supervision must end with the shutdown, not with the backoff")
            .expect("the supervisor task must not panic");

        assert_eq!(attempts.load(Ordering::SeqCst), 1, "no restart was made");
        assert!(stopped.snapshot().is_empty());
    }

    /// A lock manager that dies while leading releases its advisory locks with
    /// its session, so a standby may take them at once. Leadership must therefore
    /// be revoked on the unwind, not when the next attempt starts a backoff
    /// later, or this process's workers would drain a policy it no longer leads.
    #[tokio::test]
    async fn leadership_is_revoked_as_a_dying_manager_unwinds() {
        let (leader_tx, leader_rx) = watch::channel(false);
        let leadership = Arc::new(vec![("led".to_string(), leader_tx)]);

        let manager = tokio::spawn({
            let leadership = Arc::clone(&leadership);
            async move {
                let _revoke_on_exit = RevokeLeadership(Arc::clone(&leadership));
                let _ = leadership[0].1.send(true);
                panic!("died while leading");
            }
        });

        assert!(manager.await.is_err(), "the manager must have panicked");
        assert!(
            !*leader_rx.borrow(),
            "leadership must be false the moment the manager's task is gone"
        );
    }
}

#[cfg(test)]
mod pinned_session_tests {
    use std::time::{Duration, Instant};

    use sqlx::postgres::PgPoolOptions;
    use testcontainers_modules::postgres;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;

    use super::PinnedSession;

    /// Try to take `key` on `session`, reporting whether it was free.
    async fn try_lock(session: &mut sqlx::PgConnection, key: i64) -> bool {
        sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock($1)")
            .bind(key)
            .fetch_one(session)
            .await
            .expect("the lock attempt must reach the server")
    }

    /// How long the server is given to release the locks of a session that is on
    /// its way out: the close is sqlx's to schedule, so the release is awaited
    /// rather than assumed to have landed by the next statement.
    const RELEASE_TIMEOUT: Duration = Duration::from_secs(10);

    /// Postgres releases a session advisory lock when the *session* ends, and
    /// sqlx returns a dropped pool connection to the idle queue with its locks
    /// intact. The whole fix rests on `close_on_drop` ending the session
    /// instead; if that ever stops holding, it fails here rather than as a
    /// Policy nobody in the fleet can lead (funkode-io/replay#185).
    ///
    /// Deliberately not pinned to the suite's image tag: a session-scoped
    /// advisory lock has been released at session end on every PostgreSQL this
    /// crate supports, so the version is not what this test is about.
    #[tokio::test]
    async fn a_dropped_pinned_session_releases_its_advisory_locks_postgres_test() {
        let container = postgres::Postgres::default()
            .start()
            .await
            .expect("failed to start the postgres container");
        let host = container
            .get_host()
            .await
            .expect("failed to read the container host");
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .expect("failed to read the container port");
        let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .expect("failed to create the postgres pool");

        // The observer is its own session, never the pool's: a pooled
        // connection handed back after the drop may be the very session that
        // took the lock, and a session can always re-take a lock it holds.
        let mut observer = <sqlx::PgConnection as sqlx::Connection>::connect(&url)
            .await
            .expect("failed to connect the observing session");
        let key: i64 = 0x5eed;

        {
            let mut pinned = PinnedSession::pin(
                pool.acquire()
                    .await
                    .expect("failed to acquire the connection"),
            );
            sqlx::query("SELECT pg_advisory_lock($1)")
                .bind(key)
                .execute(&mut *pinned)
                .await
                .expect("the pinned session must take the lock");

            assert!(
                !try_lock(&mut observer, key).await,
                "nobody else can take a lock the pinned session holds"
            );
        }

        let deadline = Instant::now() + RELEASE_TIMEOUT;
        loop {
            if try_lock(&mut observer, key).await {
                break;
            }
            assert!(
                Instant::now() < deadline,
                "a dropped PinnedSession must end its session and release its \
                 locks; after {RELEASE_TIMEOUT:?} the lock was still held by a \
                 connection nobody is using"
            );
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    }
}

/// What a Policy's places are, before a worker is anywhere near them
/// (funkode-io/replay#195).
///
/// These drive the progress machinery against a real database rather than through a
/// daemon: bootstrapping and checkpointing are statements a test can reason about
/// directly, and one that has to catch a running worker between them is a test that fails
/// on a busy machine.
#[cfg(test)]
mod progress_tests {
    use std::collections::HashMap;

    use sqlx::postgres::PgPoolOptions;
    use sqlx::PgPool;
    use testcontainers_modules::postgres;
    use testcontainers_modules::testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt};

    use crate::policy_frontier::rotation_after;

    use super::{
        bootstrap, checkpoint_places, places_of, streams_behind, write_reconciled, Place,
        PolicyProgress, StartAt,
    };

    /// Move the rotation the way a poll that compared `page` and read `read` of it would,
    /// without a poll: these tests are about what the next `streams_behind` returns from
    /// where the rotation lands.
    fn rotate(progress: &mut PolicyProgress, page: &[String], read: usize, limit: u32) {
        if let Some(through) = rotation_after(page, read, limit) {
            progress.reconciled_through = through;
        }
    }

    const POLICY: &str = "progress_under_test";

    // The server the suite is verified against. Shared with
    // `tests/common/postgres_image.rs`, which this module cannot import, rather than
    // copied: the copy drifted two majors behind the floor (funkode-io/replay#226).
    include!("infrastructure/postgres_tag.rs");

    pub(super) async fn start_postgres() -> (PgPool, ContainerAsync<postgres::Postgres>) {
        let container = postgres::Postgres::default()
            .with_tag(POSTGRES_TAG)
            .start()
            .await
            .expect("failed to start the postgres container");
        let host = container
            .get_host()
            .await
            .expect("failed to read the container host");
        let port = container
            .get_host_port_ipv4(5432)
            .await
            .expect("failed to read the container port");
        let pool = PgPoolOptions::new()
            .max_connections(5)
            .connect(&format!(
                "postgres://postgres:postgres@{host}:{port}/postgres"
            ))
            .await
            .expect("failed to create the postgres pool");

        // A migration that fails here says which database it was talking to and
        // what both sides held. The failure this is for: a branch adding a
        // migration while another branch adds one under the same number, which
        // only the merge sees — the set embeds both, the second insert takes a
        // 23505 on `_sqlx_migrations`, and "migrations must succeed" alone sends
        // the reader looking for a broken migration rather than a collision.
        if let Err(error) = sqlx::migrate!("./tests/migrations").run(&pool).await {
            // `string_agg` rather than a `fetch_all`: `tests/bounded_queries.rs`
            // reviews every `fetch_all` call site in this file, and a diagnostic
            // is not worth an entry in that review.
            let applied: Option<String> = sqlx::query_scalar(
                "SELECT string_agg(version::text, ', ' ORDER BY version) FROM _sqlx_migrations",
            )
            .fetch_one(&pool)
            .await
            .unwrap_or_default();
            let database: String = sqlx::query_scalar("SELECT current_database()")
                .fetch_one(&pool)
                .await
                .unwrap_or_else(|_| "?".to_string());
            let embedded: Vec<String> = sqlx::migrate!("./tests/migrations")
                .iter()
                .map(|migration| format!("{}:{}", migration.version, migration.description))
                .collect();
            panic!(
                "migrations must succeed: {error}\n  port: {port}\n  database: {database}\n  \
                 applied: {}\n  embedded: {embedded:?}",
                applied.unwrap_or_else(|| "none".to_string())
            );
        }

        (pool, container)
    }

    /// Append `count` events to `stream` through the store's own writer. No runner is
    /// involved: these tests are about what a Policy makes of the log, not how it got
    /// there.
    async fn append_events(pool: &PgPool, stream: &str, count: usize) {
        for _ in 0..count {
            sqlx::query(
                "SELECT append_event(gen_random_uuid(), '{}'::jsonb, '{}'::jsonb, \
                 'Appended', $1, 'Probe', NULL)",
            )
            .bind(stream)
            .execute(pool)
            .await
            .expect("appending must succeed");
        }
    }

    /// What a poll reads before it delivers anything, and what its checkpoints are
    /// compared against.
    async fn observe(pool: &PgPool, streams: &[&str]) -> HashMap<String, Place> {
        let streams: Vec<String> = streams.iter().map(|s| (*s).to_string()).collect();
        places_of(pool, POLICY, &streams)
            .await
            .expect("reading the places must succeed")
    }

    /// The places, as a test asserts them: where each stream has got to, without the row
    /// version that only a checkpoint has any use for.
    async fn places(pool: &PgPool, streams: &[&str]) -> HashMap<String, i64> {
        observe(pool, streams)
            .await
            .into_iter()
            .map(|(stream, place)| (stream, place.seq))
            .collect()
    }

    /// A Policy that starts at the beginning owes every stream everything: it writes no
    /// places, because a stream with no place is at the beginning of itself.
    #[tokio::test]
    async fn a_policy_starting_at_the_beginning_is_owed_every_stream_postgres_test() {
        let (pool, _container) = start_postgres().await;
        append_events(&pool, "urn:probe:a", 2).await;
        append_events(&pool, "urn:probe:b", 1).await;

        let progress = PolicyProgress::load(&pool, POLICY, StartAt::Beginning)
            .await
            .expect("loading must succeed");

        assert_eq!(progress.swept_through, 0, "it has searched nothing yet");
        assert_eq!(
            places(&pool, &["urn:probe:a", "urn:probe:b"]).await,
            HashMap::new(),
            "no place is stored, and no place means the start of the stream"
        );
        assert_eq!(
            streams_behind(&pool, POLICY, "", 10).await.unwrap(),
            vec!["urn:probe:a".to_string(), "urn:probe:b".to_string()],
            "so both streams are behind"
        );
    }

    /// A Policy that starts now is owed nothing: every stream is recorded at its head, so
    /// the frontier finds nothing behind.
    #[tokio::test]
    async fn a_policy_starting_now_is_owed_nothing_postgres_test() {
        let (pool, _container) = start_postgres().await;
        append_events(&pool, "urn:probe:a", 3).await;

        let progress = PolicyProgress::load(&pool, POLICY, StartAt::Now)
            .await
            .expect("loading must succeed");

        assert_eq!(
            progress.swept_through, 3,
            "its search starts at the head of the log"
        );
        assert_eq!(
            places(&pool, &["urn:probe:a"]).await,
            HashMap::from([("urn:probe:a".to_string(), 3)]),
            "and the stream is recorded where it already is"
        );
        assert!(
            streams_behind(&pool, POLICY, "", 10)
                .await
                .unwrap()
                .is_empty(),
            "nothing written before it started is owed to it"
        );
    }

    /// A stream created after a `Now` Policy started has no place, and a missing place is
    /// the beginning of the stream — which is where that Policy should start in it.
    #[tokio::test]
    async fn a_stream_born_after_a_now_policy_started_is_delivered_whole_postgres_test() {
        let (pool, _container) = start_postgres().await;
        append_events(&pool, "urn:probe:before", 1).await;
        PolicyProgress::load(&pool, POLICY, StartAt::Now)
            .await
            .expect("loading must succeed");

        append_events(&pool, "urn:probe:after", 2).await;

        assert_eq!(
            streams_behind(&pool, POLICY, "", 10).await.unwrap(),
            vec!["urn:probe:after".to_string()],
            "the new stream is behind; the one that predates the Policy is not"
        );
    }

    /// Checkpoint one stream, the way a poll that has just read it would: against what is
    /// stored right now.
    async fn checkpoint(pool: &PgPool, stream: &str, to: i64) -> HashMap<String, i64> {
        let observed = observe(pool, &[stream]).await;
        checkpoint_from(pool, &observed, stream, to).await
    }

    /// Checkpoint one stream against a view the caller took earlier — a poll that has been
    /// mid-batch for a while, which is where every contested write happens.
    async fn checkpoint_from(
        pool: &PgPool,
        observed: &HashMap<String, Place>,
        stream: &str,
        to: i64,
    ) -> HashMap<String, i64> {
        checkpoint_places(pool, POLICY, &[(stream.to_string(), to)], observed)
            .await
            .expect("checkpointing must succeed")
    }

    /// A rotation that has reached the end wraps even on a poll that finds nothing.
    ///
    /// The cursor past the last stream id is the state every pass ends in, and the page
    /// it reads there is empty — which means no candidates, which means the poll returns
    /// before it does anything. Recording the reconciliation only on the path that reads
    /// a stream leaves the cursor there for ever, and a behind stream sorting before it
    /// is never compared again (funkode-io/replay#231 review).
    #[tokio::test]
    async fn a_pass_that_ends_on_an_empty_poll_still_wraps_postgres_test() {
        let (pool, _container) = start_postgres().await;
        append_events(&pool, "urn:probe:a", 1).await;
        let mut progress = PolicyProgress::load(&pool, POLICY, StartAt::Beginning)
            .await
            .expect("loading must succeed");

        // Where a finished pass leaves it, with the stream it is owed sorting before it.
        progress.reconciled_through = "urn:probe:z".to_string();
        write_reconciled(&pool, POLICY, &progress.reconciled_through)
            .await
            .expect("writing the rotation must succeed");

        assert!(
            streams_behind(&pool, POLICY, &progress.reconciled_through, 10)
                .await
                .unwrap()
                .is_empty(),
            "nothing sorts after the end, which is the state this is about"
        );

        rotate(&mut progress, &[], 0, 10);

        assert_eq!(
            progress.reconciled_through, "",
            "an empty page ends the pass, whatever the poll went on to do"
        );
        assert_eq!(
            streams_behind(&pool, POLICY, &progress.reconciled_through, 10)
                .await
                .unwrap(),
            vec!["urn:probe:a".to_string()],
            "and the next pass finds the stream that was behind the cursor"
        );
    }

    /// The reconciliation rotates, so a stream sorting after a batch that never catches
    /// up is still examined.
    ///
    /// Restarting at the lowest id every cadence is what made this a hole rather than a
    /// delay: `limit` permanently-behind streams below it would return the same ids for
    /// ever, and a quiet stream — one whose only write the sweep passed, so no future
    /// event will nominate it — would never be compared again
    /// (funkode-io/replay#231 review).
    #[tokio::test]
    async fn the_reconciliation_rotates_past_streams_that_stay_behind_postgres_test() {
        let (pool, _container) = start_postgres().await;
        for stream in ["urn:probe:a1", "urn:probe:a2", "urn:probe:z"] {
            append_events(&pool, stream, 1).await;
        }
        let mut progress = PolicyProgress::load(&pool, POLICY, StartAt::Beginning)
            .await
            .expect("loading must succeed");

        let first = streams_behind(&pool, POLICY, &progress.reconciled_through, 2)
            .await
            .unwrap();
        assert_eq!(
            first,
            vec!["urn:probe:a1".to_string(), "urn:probe:a2".to_string()],
            "a full batch of the streams that sort first"
        );
        rotate(&mut progress, &first, first.len(), 2);
        write_reconciled(&pool, POLICY, &progress.reconciled_through)
            .await
            .expect("writing the rotation must succeed");

        // A restart resumes the pass rather than starting it again, which is why the
        // rotation is persisted and not held in memory.
        let mut progress = PolicyProgress::load(&pool, POLICY, StartAt::Beginning)
            .await
            .expect("loading must succeed");
        assert_eq!(progress.reconciled_through, "urn:probe:a2");

        let second = streams_behind(&pool, POLICY, &progress.reconciled_through, 2)
            .await
            .unwrap();
        assert_eq!(
            second,
            vec!["urn:probe:z".to_string()],
            "the stream the first pass could not reach, with the first two still behind"
        );
        rotate(&mut progress, &second, second.len(), 2);
        assert_eq!(
            progress.reconciled_through, "",
            "a short batch is the end of the pass, and the next one starts over"
        );
    }

    /// A place records what has been processed, and the frontier is the difference
    /// between that and the stream's head.
    #[tokio::test]
    async fn a_checkpointed_place_takes_a_stream_off_the_frontier_postgres_test() {
        let (pool, _container) = start_postgres().await;
        append_events(&pool, "urn:probe:a", 3).await;
        PolicyProgress::load(&pool, POLICY, StartAt::Beginning)
            .await
            .expect("loading must succeed");

        checkpoint(&pool, "urn:probe:a", 2).await;

        assert_eq!(
            streams_behind(&pool, POLICY, "", 10).await.unwrap(),
            vec!["urn:probe:a".to_string()],
            "two of three places processed is still behind"
        );

        checkpoint(&pool, "urn:probe:a", 3).await;

        assert!(
            streams_behind(&pool, POLICY, "", 10)
                .await
                .unwrap()
                .is_empty(),
            "caught up with the last place written"
        );
    }

    /// A checkpoint is written only where the place still reads as the value its poll
    /// started from. Two runners for one Policy is a leadership fault, and the one whose
    /// view is stale is told so rather than allowed to write over the other.
    #[tokio::test]
    async fn a_checkpoint_from_a_stale_view_is_refused_postgres_test() {
        let (pool, _container) = start_postgres().await;
        append_events(&pool, "urn:probe:a", 5).await;
        PolicyProgress::load(&pool, POLICY, StartAt::Beginning)
            .await
            .expect("loading must succeed");

        // A poll that read the stream before anything was written there.
        let stale = observe(&pool, &["urn:probe:a"]).await;

        checkpoint(&pool, "urn:probe:a", 4).await;
        let kept = checkpoint_from(&pool, &stale, "urn:probe:a", 2).await;

        assert!(
            kept.is_empty(),
            "the write loses, and its runner is told it lost"
        );
        assert_eq!(
            places(&pool, &["urn:probe:a"]).await,
            HashMap::from([("urn:probe:a".to_string(), 4)]),
            "the stale write is dropped, not applied"
        );
    }

    /// The reason the write compares rather than only refusing to go backwards: an
    /// operator rewinding a stream mid-poll writes a place *below* the one the poll is
    /// carrying, so a monotonic write would reinstate the higher value and undo the
    /// rewind without a word (funkode-io/replay#231 review).
    #[tokio::test]
    async fn an_operator_rewinding_mid_poll_is_not_overwritten_postgres_test() {
        let (pool, _container) = start_postgres().await;
        append_events(&pool, "urn:probe:a", 20).await;
        PolicyProgress::load(&pool, POLICY, StartAt::Beginning)
            .await
            .expect("loading must succeed");
        checkpoint(&pool, "urn:probe:a", 10).await;

        // The poll is mid-batch: it read the place as 10 and has since delivered to 15.
        let observed = observe(&pool, &["urn:probe:a"]).await;

        sqlx::query(
            "UPDATE policy_stream_cursors SET stream_seq = 3 \
             WHERE policy = $1 AND stream_id = $2",
        )
        .bind(POLICY)
        .bind("urn:probe:a")
        .execute(&pool)
        .await
        .expect("the operator's move must succeed");

        let kept = checkpoint_from(&pool, &observed, "urn:probe:a", 15).await;

        assert!(kept.is_empty(), "the poll is told its view went stale");
        assert_eq!(
            places(&pool, &["urn:probe:a"]).await,
            HashMap::from([("urn:probe:a".to_string(), 3)]),
            "and the operator's rewind stands"
        );
    }

    /// The rewind an operator is most likely to make is the one a compared *place* cannot
    /// see: back to just before the event being delivered right now, which is exactly the
    /// place the poll started from (funkode-io/replay#234).
    ///
    /// "Nobody has written this since I read it" and "somebody wrote it and it holds the
    /// value I read" are the same value and different facts. The write that has to be
    /// refused here is the one that *agrees* with what the poll observed.
    #[tokio::test]
    async fn an_operator_rewinding_to_the_place_a_poll_started_from_is_not_overwritten_postgres_test(
    ) {
        let (pool, _container) = start_postgres().await;
        append_events(&pool, "urn:probe:a", 20).await;
        PolicyProgress::load(&pool, POLICY, StartAt::Beginning)
            .await
            .expect("loading must succeed");
        checkpoint(&pool, "urn:probe:a", 10).await;

        // The poll is mid-batch: it read the place as 10 and has since delivered to 15.
        let observed = observe(&pool, &["urn:probe:a"]).await;

        // The operator asks for event 11 to be delivered again — a rewind to 10, which is
        // where this poll found the place.
        sqlx::query(
            "UPDATE policy_stream_cursors SET stream_seq = 10 \
             WHERE policy = $1 AND stream_id = $2",
        )
        .bind(POLICY)
        .bind("urn:probe:a")
        .execute(&pool)
        .await
        .expect("the operator's move must succeed");

        let kept = checkpoint_from(&pool, &observed, "urn:probe:a", 15).await;

        assert!(kept.is_empty(), "the poll is told its view went stale");
        assert_eq!(
            places(&pool, &["urn:probe:a"]).await,
            HashMap::from([("urn:probe:a".to_string(), 10)]),
            "and the redelivery the operator asked for still happens"
        );
    }

    /// Deleting a place is the documented way to redeliver a stream from its first event
    /// (README, "Moving a policy on a running system"), so a poll mid-batch must not
    /// recreate the row it deleted — which is what an insert with nothing to conflict
    /// with would do.
    #[tokio::test]
    async fn an_operator_deleting_a_place_mid_poll_is_not_overwritten_postgres_test() {
        let (pool, _container) = start_postgres().await;
        append_events(&pool, "urn:probe:a", 20).await;
        PolicyProgress::load(&pool, POLICY, StartAt::Beginning)
            .await
            .expect("loading must succeed");
        checkpoint(&pool, "urn:probe:a", 10).await;

        // The poll is mid-batch, holding the place it read before the delete.
        let observed = observe(&pool, &["urn:probe:a"]).await;

        sqlx::query("DELETE FROM policy_stream_cursors WHERE policy = $1 AND stream_id = $2")
            .bind(POLICY)
            .bind("urn:probe:a")
            .execute(&pool)
            .await
            .expect("the operator's delete must succeed");

        let kept = checkpoint_from(&pool, &observed, "urn:probe:a", 15).await;

        assert!(kept.is_empty(), "the poll is told its view went stale");
        assert!(
            places(&pool, &["urn:probe:a"]).await.is_empty(),
            "and the stream is still at the beginning, where the delete left it"
        );
    }

    /// The same delete, still open when the checkpoint starts — which is the interleaving
    /// the committed one cannot reach (funkode-io/replay#236 review).
    ///
    /// A checkpoint that guarded an upsert on the row existing would read that guard from
    /// the snapshot it opened on, where the row is still there, and then wait on the
    /// primary key: by the time it goes in, the delete has committed and there is nothing
    /// to conflict with, so the operator's delete is undone by an insert. The window is
    /// forced rather than raced for — the checkpoint is made while the delete is open, and
    /// the delete commits while the checkpoint waits.
    #[tokio::test]
    async fn an_operator_deleting_a_place_during_a_checkpoint_is_not_overwritten_postgres_test() {
        let (pool, _container) = start_postgres().await;
        append_events(&pool, "urn:probe:a", 20).await;
        PolicyProgress::load(&pool, POLICY, StartAt::Beginning)
            .await
            .expect("loading must succeed");
        checkpoint(&pool, "urn:probe:a", 10).await;

        // The poll is mid-batch, holding the place it read before the delete.
        let observed = observe(&pool, &["urn:probe:a"]).await;

        let mut deleting = pool.begin().await.expect("beginning must succeed");
        let operator: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
            .fetch_one(&mut *deleting)
            .await
            .expect("the deleting backend must identify itself");
        sqlx::query("DELETE FROM policy_stream_cursors WHERE policy = $1 AND stream_id = $2")
            .bind(POLICY)
            .bind("urn:probe:a")
            .execute(&mut *deleting)
            .await
            .expect("the operator's delete must succeed");

        let checkpointing = tokio::spawn({
            let pool = pool.clone();
            let observed = observed.clone();
            async move {
                checkpoint_places(&pool, POLICY, &[("urn:probe:a".to_string(), 15)], &observed)
                    .await
            }
        });
        await_blocked_by(&pool, operator).await;
        deleting.commit().await.expect("committing must succeed");

        let kept = checkpointing
            .await
            .expect("the checkpoint must finish")
            .expect("checkpointing must succeed");

        assert!(kept.is_empty(), "the poll is told its view went stale");
        assert!(
            places(&pool, &["urn:probe:a"]).await.is_empty(),
            "and the stream is still at the beginning, where the delete left it"
        );
    }

    /// The same, for a row an operator had already rewound to the beginning.
    ///
    /// A place of 0 and no place at all are the same number, so a write allowed to create
    /// a row whenever it saw 0 recreates the one the operator has just deleted. What
    /// decides is whether the poll saw a *row* (funkode-io/replay#231 review).
    #[tokio::test]
    async fn an_operator_deleting_a_place_it_had_rewound_to_zero_is_not_overwritten_postgres_test()
    {
        let (pool, _container) = start_postgres().await;
        append_events(&pool, "urn:probe:a", 20).await;
        PolicyProgress::load(&pool, POLICY, StartAt::Beginning)
            .await
            .expect("loading must succeed");

        // Rewound to the beginning by hand, then thought better of it and deleted.
        sqlx::query(
            "INSERT INTO policy_stream_cursors (policy, stream_id, stream_seq) VALUES ($1, $2, 0)",
        )
        .bind(POLICY)
        .bind("urn:probe:a")
        .execute(&pool)
        .await
        .expect("the operator's rewind must succeed");

        // The poll that was mid-batch when both happened: it saw the row at 0, which is a
        // row, and not the absence of one that the same number would mean.
        let observed = observe(&pool, &["urn:probe:a"]).await;

        sqlx::query("DELETE FROM policy_stream_cursors WHERE policy = $1 AND stream_id = $2")
            .bind(POLICY)
            .bind("urn:probe:a")
            .execute(&pool)
            .await
            .expect("the operator's delete must succeed");

        let kept = checkpoint_from(&pool, &observed, "urn:probe:a", 15).await;

        assert!(kept.is_empty(), "the poll is told its view went stale");
        assert!(
            places(&pool, &["urn:probe:a"]).await.is_empty(),
            "and the row the operator deleted stays deleted"
        );
    }

    /// ADR-0012, moved to the table that now holds the position: an operator writes a
    /// place back and the Policy reads it on its next poll, because places are read fresh
    /// every poll rather than held in memory between them.
    #[tokio::test]
    async fn an_operator_can_move_a_place_back_to_force_a_redelivery_postgres_test() {
        let (pool, _container) = start_postgres().await;
        append_events(&pool, "urn:probe:a", 5).await;
        PolicyProgress::load(&pool, POLICY, StartAt::Beginning)
            .await
            .expect("loading must succeed");
        checkpoint(&pool, "urn:probe:a", 5).await;

        sqlx::query(
            "UPDATE policy_stream_cursors SET stream_seq = $1 \
             WHERE policy = $2 AND stream_id = $3",
        )
        .bind(2_i64)
        .bind(POLICY)
        .bind("urn:probe:a")
        .execute(&pool)
        .await
        .expect("the operator's move must succeed");

        assert_eq!(
            places(&pool, &["urn:probe:a"]).await,
            HashMap::from([("urn:probe:a".to_string(), 2)]),
            "the next poll reads what the operator wrote"
        );
        assert_eq!(
            streams_behind(&pool, POLICY, "", 10).await.unwrap(),
            vec!["urn:probe:a".to_string()],
            "and the stream is owed its last three events again"
        );
    }

    /// A bootstrap that loses the claim on the policy's row seeds no places.
    ///
    /// Seeding before claiming let two runners starting at once combine one's sweep
    /// position with the other's places: a stream created after the winner's snapshot has
    /// no seed from the winner, so the loser's — which says the stream is processed
    /// through an event above where the winner's search starts — conflicts with nothing
    /// and stands, and that event is delivered by nobody (funkode-io/replay#231 review).
    #[tokio::test]
    async fn a_bootstrap_that_loses_the_claim_seeds_nothing_postgres_test() {
        let (pool, _container) = start_postgres().await;
        append_events(&pool, "urn:probe:a", 3).await;

        // The winner, at a point that predates the stream below.
        sqlx::query(
            "INSERT INTO policy_cursors (name, discovered_through, updated_at) \
             VALUES ($1, 0, now())",
        )
        .bind(POLICY)
        .execute(&pool)
        .await
        .expect("the winning claim must succeed");

        // The winner has a rotation of its own by now, mid-pass.
        write_reconciled(&pool, POLICY, "urn:probe:m")
            .await
            .expect("the winner's rotation must write");

        // `bootstrap` rather than `load`, which would see the row and never get here:
        // this is the losing half of two runners that both found no row.
        let progress = bootstrap(&pool, POLICY, StartAt::Now)
            .await
            .expect("bootstrapping must succeed");

        assert_eq!(
            progress,
            (0, "urn:probe:m".to_string()),
            "the loser takes both halves from the winner's row, not its own opinion of \
             either: a rotation reset to the beginning would re-read the streams sorting \
             first and could starve one sorting last"
        );
        assert!(
            places(&pool, &["urn:probe:a"]).await.is_empty(),
            "and writes no places of its own, which would say this stream was processed"
        );
        assert_eq!(
            streams_behind(&pool, POLICY, "", 10).await.unwrap(),
            vec!["urn:probe:a".to_string()],
            "so the stream is owed, as the winner's own bootstrap decided"
        );
    }

    /// The two reads `StartAt::Now` takes are one snapshot, so a write that commits
    /// between them is owed rather than lost.
    ///
    /// The interleaving is forced rather than raced for: locking `policy_stream_cursors`
    /// holds the bootstrap between its read of the log's end and its read of each
    /// stream's end, which is the window the bug lived in. Under a snapshot the stream is
    /// seeded where it was, so the event that committed in the window sits above the
    /// place *and* above the sweep, and the Policy is owed it. Read separately, the seed
    /// would include that event while the search started below it: nominated by no sweep,
    /// owed by no place.
    #[tokio::test]
    async fn a_write_committing_during_bootstrap_is_owed_not_lost_postgres_test() {
        let (pool, _container) = start_postgres().await;
        append_events(&pool, "urn:probe:a", 1).await;

        // Hold the bootstrap at its second read.
        let mut blocker = pool.begin().await.expect("beginning must succeed");
        sqlx::query("LOCK TABLE policy_stream_cursors IN ACCESS EXCLUSIVE MODE")
            .execute(&mut *blocker)
            .await
            .expect("locking must succeed");

        let bootstrapping = tokio::spawn({
            let pool = pool.clone();
            async move { PolicyProgress::load(&pool, POLICY, StartAt::Now).await }
        });
        await_blocked_on(&pool, "policy_stream_cursors").await;

        // The write the old code lost: a stream that already existed, gaining an event
        // while the bootstrap is between its two reads.
        append_events(&pool, "urn:probe:a", 1).await;
        blocker.commit().await.expect("releasing must succeed");

        let progress = bootstrapping
            .await
            .expect("the bootstrap must finish")
            .expect("loading must succeed");

        assert_eq!(
            progress.swept_through, 1,
            "the search starts where the log ended when the snapshot was taken"
        );
        assert_eq!(
            places(&pool, &["urn:probe:a"]).await,
            HashMap::from([("urn:probe:a".to_string(), 1)]),
            "and the stream is seeded where it was in that same snapshot"
        );
        assert_eq!(
            streams_behind(&pool, POLICY, "", 10).await.unwrap(),
            vec!["urn:probe:a".to_string()],
            "so the event that committed in the window is owed"
        );
    }

    /// Wait until something is queued behind a lock on `relation`.
    async fn await_blocked_on(pool: &PgPool, relation: &str) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let blocked: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_locks l JOIN pg_class c ON c.oid = l.relation \
                  WHERE c.relname = $1 AND NOT l.granted",
            )
            .bind(relation)
            .fetch_one(pool)
            .await
            .expect("reading pg_locks must succeed");

            if blocked > 0 {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "nothing ever blocked on the lock on {relation}"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    /// Wait until something is queued behind whatever `holder` is holding.
    async fn await_blocked_by(pool: &PgPool, holder: i32) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let blocked: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_stat_activity \
                  WHERE wait_event_type = 'Lock' AND $1 = ANY(pg_blocking_pids(pid))",
            )
            .bind(holder)
            .fetch_one(pool)
            .await
            .expect("reading who is blocked must succeed");

            if blocked > 0 {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "nothing ever blocked on what backend {holder} holds"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    /// The sweep is a hint, so it is written unconditionally forwards and never
    /// backwards: a stale runner cannot make another one search the log twice.
    #[tokio::test]
    async fn the_sweep_position_only_moves_forwards_postgres_test() {
        let (pool, _container) = start_postgres().await;
        append_events(&pool, "urn:probe:a", 3).await;
        PolicyProgress::load(&pool, POLICY, StartAt::Beginning)
            .await
            .expect("loading must succeed");

        super::write_sweep(&pool, POLICY, 3).await.unwrap();
        super::write_sweep(&pool, POLICY, 1).await.unwrap();

        let reloaded = PolicyProgress::load(&pool, POLICY, StartAt::Beginning)
            .await
            .expect("loading must succeed");
        assert_eq!(reloaded.swept_through, 3);
    }
}

/// What a settlement can see while it settles (funkode-io/replay#228).
///
/// Driven against a real database rather than through a daemon, for the reason
/// [`progress_tests`] is: a settlement reads its pages several statements apart, and
/// a test that has to catch a running retry between two of them is a test that
/// fails on a busy machine.
#[cfg(test)]
mod settlement_tests {
    use sqlx::PgPool;

    use super::progress_tests::start_postgres;
    use super::{
        begin_settlement, group_digest, load_parked_page, Cqrs, DispatchIdentity, GroupPhase,
        ParkedReaction, PolicyRunner, Replay, ReplayedDispatch, Settlement,
    };

    const POLICY: &str = "settlement_under_test";
    const POSITION: i64 = 7;

    /// A row parked for `command`, as an old version of the policy would have left
    /// it. No event is involved: these tests are about what the settlement reads,
    /// not about what a reaction dispatches.
    async fn park(pool: &PgPool, reaction: &ParkedReaction, command: &str) {
        sqlx::query(
            "INSERT INTO policy_dead_letters \
             (policy_name, global_position, event_id, error_kind, error_message, \
              aggregate_name, target_stream_id, command_name, dispatch_ordinal, \
              last_parked_at) \
             VALUES ($1, $2, $3, 'Unavailable', 'parked', 'Probe', 'urn:probe:1', $4, \
                     (SELECT coalesce(max(dispatch_ordinal), 0) + 1 \
                      FROM policy_dead_letters WHERE policy_name = $1), now())",
        )
        .bind(&reaction.policy_name)
        .bind(reaction.global_position)
        .bind(reaction.event_id)
        .bind(command)
        .execute(pool)
        .await
        .expect("parking must succeed");
    }

    /// A writer that moves a row while the settlement is taking its locks is the
    /// same conclusion, reached by the server.
    ///
    /// `lock_group` waits for the row lock, and the update it was waiting for
    /// commits after the settlement's snapshot: Postgres cannot show this
    /// transaction a consistent row, so it refuses with a `40001`. Classified as
    /// a conflict (`db_error`), which is what [`PolicyRunner::settle`] reports as
    /// [`DeadLetterRetry::Superseded`] rather than letting a bulk retry's walk
    /// abort on it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_row_moved_while_the_settlement_locks_is_a_conflict_postgres_test() {
        let (pool, _container) = start_postgres().await;
        let reaction = ParkedReaction {
            policy_name: POLICY.to_string(),
            global_position: POSITION,
            event_id: uuid::Uuid::new_v4(),
        };
        park(&pool, &reaction, "First").await;

        // The delivery that re-parks the row: it holds the row's lock, so the
        // settlement's `FOR UPDATE` waits on it rather than reading around it.
        let mut delivery = pool.begin().await.expect("the delivery must begin");
        sqlx::query(
            "UPDATE policy_dead_letters SET deliveries = deliveries + 1, \
             last_parked_at = now() WHERE policy_name = $1",
        )
        .bind(&reaction.policy_name)
        .execute(&mut *delivery)
        .await
        .expect("the re-park must apply");

        let settling = {
            let pool = pool.clone();
            let reaction = ParkedReaction {
                policy_name: reaction.policy_name.clone(),
                ..reaction
            };
            tokio::spawn(async move { begin_settlement(&pool, &reaction).await.map(|_| ()) })
        };

        // The settlement has taken its snapshot and is queued behind the
        // delivery's row lock — read off `pg_locks` rather than slept for, so a
        // busy machine delays the test instead of failing it.
        await_blocked_on_a_row(&pool).await;
        delivery.commit().await.expect("the delivery must commit");

        let refused = settling
            .await
            .expect("the settling task must not panic")
            .expect_err("a settlement cannot lock a group that moved under it");

        assert_eq!(
            refused.kind(),
            replay::ErrorKind::Conflict,
            "a group moved under the snapshot is a conflict, not a database \
             failure: {refused}"
        );
    }

    /// A discard that takes the last row while the settlement waits for its lock
    /// does not swallow what the replay found.
    ///
    /// The refusal arrives as a `40001` rather than as a digest mismatch, so it
    /// takes the branch that reports a moved group — and a group emptied that way
    /// has nowhere left to record the failures this replay watched happen: the
    /// reaction has no rows, so nothing enumerates it and the drain is long past
    /// the event. The settlement is attempted once more against the empty group.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn a_discard_that_wins_the_lock_does_not_swallow_the_replay_postgres_test() {
        let (pool, _container) = start_postgres().await;
        let reaction = ParkedReaction {
            policy_name: POLICY.to_string(),
            global_position: POSITION,
            event_id: uuid::Uuid::new_v4(),
        };
        park(&pool, &reaction, "First").await;

        let runner =
            PolicyRunner::builder(Cqrs::new(crate::PostgresEventStore::new(pool.clone()))).build();
        let before = group_digest(&pool, &reaction)
            .await
            .expect("the digest must be readable");

        // The operator's discard, holding the row's lock: a transaction here
        // because the settlement has to be waiting on it when it commits, which
        // is what turns the refusal into a serialization failure.
        let mut discard = pool.begin().await.expect("the discard must begin");
        sqlx::query("DELETE FROM policy_dead_letters WHERE policy_name = $1")
            .bind(&reaction.policy_name)
            .execute(&mut *discard)
            .await
            .expect("the discard must apply");

        // What the replay found: a command the reaction now dispatches and no
        // row was ever parked for — the one row a retry inserts (ADR-0021).
        let replay = Replay::Ran(vec![ReplayedDispatch {
            identity: DispatchIdentity {
                aggregate_name: "Probe",
                target_stream_id: "urn:probe:2".to_string(),
                command_name: "Second",
                ordinal: 0,
            },
            outcome: Some(Settlement {
                error_kind: "Unavailable".to_string(),
                error_message: "the command the replay found failing".to_string(),
            }),
            claimed: false,
        }]);

        let settling = {
            let reaction = ParkedReaction {
                policy_name: reaction.policy_name.clone(),
                ..reaction
            };
            tokio::spawn(async move { runner.settle(&reaction, before, replay, None).await })
        };

        await_blocked_on_a_row(&pool).await;
        discard.commit().await.expect("the discard must commit");

        let settled = settling
            .await
            .expect("the settling task must not panic")
            .expect("a settlement refused its snapshot must still return an outcome");
        assert!(
            settled.any_still_failing,
            "the reaction is still failing: a command of it just did"
        );

        // `string_agg` rather than a `fetch_all`: `tests/bounded_queries.rs`
        // reviews every `fetch_all` call site in this file, and a test's
        // assertion is not worth an entry in that review.
        let parked: Option<String> = sqlx::query_scalar(
            "SELECT string_agg(command_name, ', ' ORDER BY id) \
             FROM policy_dead_letters WHERE policy_name = $1",
        )
        .bind(&reaction.policy_name)
        .fetch_one(&pool)
        .await
        .expect("the table must be readable");
        assert_eq!(
            parked.as_deref(),
            Some("Second"),
            "the failure the replay found is parked, and the discarded row stays gone"
        );
    }

    /// Wait until something is queued behind a row lock somebody else holds.
    ///
    /// A waiter for a locked row queues on the *holder's transaction id* rather
    /// than on the table (`tuple` while it takes its turn, `transactionid` while
    /// it waits for the holder to end), so neither shows as an ungranted lock on
    /// `policy_dead_letters` itself.
    async fn await_blocked_on_a_row(pool: &PgPool) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            let blocked: i64 = sqlx::query_scalar(
                "SELECT count(*) FROM pg_locks \
                  WHERE NOT granted AND locktype IN ('transactionid', 'tuple')",
            )
            .fetch_one(pool)
            .await
            .expect("reading pg_locks must succeed");

            if blocked > 0 {
                return;
            }
            assert!(
                std::time::Instant::now() < deadline,
                "the settlement never blocked on the delivery's row lock"
            );
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    /// A command parked after the settlement began is not settled by it.
    ///
    /// The group's locks cannot cover a row nobody has written yet, so what keeps
    /// it out of a later page is the settlement's snapshot. Under the default READ
    /// COMMITTED each page reads a fresh one, and this row — parked by a delivery
    /// with a failure of its own — would be settled from a replay that ran before
    /// it existed.
    #[tokio::test]
    async fn a_page_does_not_see_a_command_parked_after_the_settlement_began_postgres_test() {
        let (pool, _container) = start_postgres().await;
        let reaction = ParkedReaction {
            policy_name: POLICY.to_string(),
            global_position: POSITION,
            event_id: uuid::Uuid::new_v4(),
        };
        park(&pool, &reaction, "First").await;
        park(&pool, &reaction, "Second").await;

        let (mut tx, locked) = begin_settlement(&pool, &reaction)
            .await
            .expect("the settlement must open");
        assert_eq!(locked.rows, 2, "the group the settlement locked");

        // The delivery that arrives while the settlement is walking the group.
        // Its own connection, so it commits without waiting on the locks above —
        // which cover the rows that existed, and this is not one of them.
        park(&pool, &reaction, "Third").await;

        let page = load_parked_page(&mut tx, &reaction, GroupPhase::Naming, 0)
            .await
            .expect("the page must be readable");
        let commands: Vec<Option<&str>> = page
            .iter()
            .map(|row| {
                row.identity
                    .as_ref()
                    .map(|identity| identity.command_name.as_str())
            })
            .collect();

        assert_eq!(
            commands,
            vec![Some("First"), Some("Second")],
            "a settlement settles the group it locked, not what arrived after it"
        );

        drop(tx);
        assert_eq!(
            group_digest(&pool, &reaction)
                .await
                .expect("the digest must be readable")
                .rows,
            3,
            "and the command parked meanwhile is in the table, for the next retry"
        );
    }
}
