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
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use futures::FutureExt;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sqlx::{Pool, Postgres, QueryBuilder, Row};
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;

use replay::{Aggregate, Metadata};

use crate::burned_position::{
    resume_after_burned, sequence_holders, BurnedPositions, Permanence, Resume,
};
use crate::commit_stamp::CommitStamp;
use crate::policy::{Dispatch, ErasedPolicy, Policy, StartAt};
use crate::policy_blocked::{probe_blocked, resolve_blocked_warn_after, BlockedWatch};
use crate::policy_feed::{feed_from_window, Feed, Gap, WindowPosition};
use crate::policy_liveness::{
    Beat, HeartbeatColumns, HeartbeatWriter, LivenessHandle, LivenessRegistry, WorkerLiveness,
};
use crate::policy_narration::{Narration, Poll, Record, PROGRESS_EVERY};
use crate::{Cqrs, PersistedEvent, PostgresEventStore, StreamFilter};

/// Erased, services-bound execution path for one aggregate type.
///
/// Registered via [`PolicyRunnerBuilder::register_services`], which captures the
/// concrete aggregate `A` *and* its `Services`. At drain time the runner looks up
/// the executor by the [`Dispatch`]'s [`TypeId`], hands over the opaque payload,
/// and the executor downcasts it back to `(A::StreamId, A::Command)` and runs it
/// through [`Cqrs::execute`].
trait AggregateExecutor: Send + Sync {
    fn execute<'a>(
        &'a self,
        cqrs: &'a Cqrs<PostgresEventStore>,
        payload: Box<dyn Any + Send>,
        metadata: Metadata,
        expected_version: Option<i64>,
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
        payload: Box<dyn Any + Send>,
        metadata: Metadata,
        expected_version: Option<i64>,
    ) -> BoxFuture<'a, Result<(), replay::Error>> {
        Box::pin(async move {
            let (id, command) = *payload
                .downcast::<(A::StreamId, A::Command)>()
                .map_err(|_| {
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

// ── Closure-based policy adapter ─────────────────────────────────────────────

/// A [`Policy`] backed by a plain closure, created via
/// [`PolicyRunnerBuilder::register_policy_fn`].
struct ClosurePolicy<E, F> {
    name: String,
    start_at: StartAt,
    react: F,
    _phantom: std::marker::PhantomData<E>,
}

impl<E, F> Policy for ClosurePolicy<E, F>
where
    E: replay::Event + 'static,
    F: Fn(&PersistedEvent<E>) -> Vec<Dispatch> + Send + Sync + 'static,
{
    type Event = E;

    fn name(&self) -> &str {
        &self.name
    }

    fn start_at(&self) -> StartAt {
        self.start_at
    }

    fn react(&self, event: &PersistedEvent<Self::Event>) -> Vec<Dispatch> {
        (self.react)(event)
    }
}

// ─────────────────────────────────────────────────────────────────────────────

/// Builds a [`PolicyRunner`] by registering aggregate services and policies.
pub struct PolicyRunnerBuilder {
    cqrs: Cqrs<PostgresEventStore>,
    pool: Pool<Postgres>,
    policies: Vec<Arc<dyn ErasedPolicy>>,
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

    /// Register a policy. Its `name` becomes the stable cursor key.
    pub fn register_policy<P>(mut self, policy: P) -> Self
    where
        P: Policy + 'static,
    {
        self.policies.push(Arc::new(policy));
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
    ///         StartAt::Beginning,
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
        start_at: StartAt,
        react: F,
    ) -> Self
    where
        E: replay::Event + 'static,
        F: Fn(&PersistedEvent<E>) -> Vec<Dispatch> + Send + Sync + 'static,
    {
        self.register_policy(ClosurePolicy {
            name: name.into(),
            start_at,
            react,
            _phantom: std::marker::PhantomData,
        })
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
            stopped: StoppedPolicies::new(),
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
    policies: Vec<Arc<dyn ErasedPolicy>>,
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
    /// What this process remembers about the Policies that are stopped, shared by
    /// every drain path so a stop is judged the same way however it is driven.
    stopped: StoppedPolicies,
}

/// What a running process remembers about a Policy that is parked in front of a
/// hole: how long it has been there, and whether any transaction can still fill it.
///
/// The two are separate questions with separate answers — one is about elapsed time
/// and decides when to report, the other is about running transactions and decides
/// when to move — but they share a subject and a lifetime: the moment a Policy is no
/// longer parked where it was, both are stale ([`forget`](Self::forget)).
#[derive(Clone)]
struct StoppedPolicies {
    /// Rate gate for the blocked-policy warning.
    blocked: Arc<BlockedWatch>,
    /// Candidate transactions for each Policy's hole, as first observed.
    burned: Arc<BurnedPositions>,
}

impl StoppedPolicies {
    fn new() -> Self {
        Self {
            blocked: Arc::new(BlockedWatch::new(resolve_blocked_warn_after())),
            burned: Arc::new(BurnedPositions::new()),
        }
    }

    /// Note that `policy` is parked in front of `gap` without judging it: the poll
    /// that first sees a hole may still have work in front of it, and the hole is no
    /// younger for that.
    fn sighted(&self, policy: &str, gap: Gap) {
        self.blocked
            .sighted(policy, gap.expected, std::time::Instant::now());
    }

    /// Forget everything known about where `policy` was stopped: it is no longer
    /// parked there, so the next hole is a fresh wait and a fresh question.
    fn forget(&self, policy: &str) {
        self.blocked.cleared(policy);
        self.burned.cleared(policy);
    }
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
    /// For each policy: read the gap-free prefix of events past its cursor,
    /// `react`, execute the returned dispatches through [`Cqrs`], and advance the
    /// cursor — one event at a time, advancing only after that event's commands
    /// have committed (at-least-once delivery; reactions must be idempotent).
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
    /// already parked is updated, never duplicated — and it is the one settlement
    /// two concurrent retries of the same reaction can duplicate, since it is not
    /// primary-key-scoped like the rest (funkode-io/replay#220).
    ///
    /// Settling a row that already existed stamps its `retry_count` and
    /// `last_retried_at`, the archived copy included, so what has already been
    /// tried survives the error message being overwritten. A row this retry
    /// *parks* carries neither: it is a first parking, born untried exactly as
    /// the drain's rows are, and the column counts retries made on a row, not
    /// executions of a command.
    ///
    /// Re-execution safety comes from the causation guard (the command carries
    /// the triggering event's id) plus the optimistic-concurrency check in
    /// [`Cqrs::execute`], so retrying an already-applied reaction is a no-op.
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
        let settled = self.retry_reaction(reaction, OPERATION).await?;

        // The row `id` names is normally in the group this replay settled; it is
        // absent when a concurrent discard took it out between the SELECT above
        // and the group read, which is the same nothing-to-do as an absent id.
        Ok(settled
            .into_iter()
            .find_map(|(settled_id, outcome)| (settled_id == id).then_some(outcome))
            .unwrap_or(DeadLetterRetry::NotFound))
    }

    /// Replay one reaction and settle every row it parked.
    ///
    /// The group is read here rather than by the caller, so the by-id and the
    /// bulk path settle exactly the same set. Returns what each row of the group
    /// concluded, in the order the rows were parked.
    async fn retry_reaction(
        &self,
        reaction: ParkedReaction,
        operation: &'static str,
    ) -> Result<Vec<(i64, DeadLetterRetry)>, replay::Error> {
        // The group, before anything is re-executed: a reaction whose every row
        // was discarded between the caller's read and this one has nothing left
        // to settle, and replaying it would dispatch commands on behalf of rows
        // an operator has just retired.
        let rows = load_parked_reaction(&self.pool, &reaction).await?;
        if rows.is_empty() {
            return Ok(Vec::new());
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
                dispatch_timeout: resolve_dispatch_timeout(policy.as_ref()),
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

        self.settle(&reaction, rows, replay).await
    }

    /// Settle each row of a reaction's group with what the replay concluded for
    /// **its** command, and park what failed with no row to settle.
    ///
    /// One transaction, so a database error part-way through leaves the group as
    /// the replay found it. Without that, a row archived early and an insert that
    /// then failed would take a still-failing command out of the table with
    /// nothing put back: the drain is long past the event and would never park it
    /// again.
    async fn settle(
        &self,
        reaction: &ParkedReaction,
        rows: Vec<ParkedRow>,
        replay: Replay,
    ) -> Result<Vec<(i64, DeadLetterRetry)>, replay::Error> {
        let mut replay = replay;
        let mut settled = Vec::with_capacity(rows.len());
        let mut tx = self.pool.begin().await.map_err(crate::db_error)?;
        // How many rows name each row's identity, read before any of them is
        // settled: what tells a group of indistinguishable dispatches from a
        // dispatch that parked no row at all.
        let naming: Vec<usize> = rows
            .iter()
            .map(|row| match row.identity.as_ref() {
                Some(identity) => rows
                    .iter()
                    .filter(|other| other.identity.as_ref() == Some(identity))
                    .count(),
                None => 0,
            })
            .collect();

        // Rows that name a command are settled first, whatever their ids. A row
        // that names none is judged by the replay *as a whole* and speaks for
        // every dispatch of it, so letting one go first — an upgrade's row is
        // older than the rows a later delivery parked, hence lower-numbered —
        // would leave its neighbours nothing of their own to take.
        let mut ordered: Vec<(ParkedRow, usize)> = rows.into_iter().zip(naming).collect();
        ordered.sort_by_key(|(row, _)| row.identity.is_none());

        for (row, rows_naming_it) in ordered {
            let outcome = match row.identity.as_ref() {
                // The command this row was parked for ran again: its own
                // outcome settles it.
                Some(identity) => replay.settlement_for(identity, rows_naming_it),
                // The row names no command, so only the replay as a whole can
                // settle it.
                None => replay.verdict(),
            };

            let settlement = match outcome {
                None => {
                    if move_dead_letter_to_archive(&mut *tx, row.id, "retried").await? {
                        DeadLetterRetry::Resolved
                    } else {
                        // A concurrent discard removed the row between the group
                        // read and the archive move: nothing was archived with
                        // reason `retried`, so report it as NotFound.
                        DeadLetterRetry::NotFound
                    }
                }
                Some(Settlement {
                    error_kind,
                    error_message,
                }) => {
                    if re_park_dead_letter(&mut *tx, row.id, &error_kind, &error_message).await? {
                        DeadLetterRetry::StillFailing
                    } else {
                        // Same concurrent discard as the archive branch, from
                        // the other side: there is no row left to re-park, so
                        // there is no reaction still failing to report.
                        DeadLetterRetry::NotFound
                    }
                }
            };
            settled.push((row.id, settlement));
        }

        // A replay is the drain's equal, so it parks what failed: a dispatch
        // that failed and no row spoke for is a command this reaction did not
        // park before — the policy's code changed between the park and the
        // retry, which is the whole point of replaying the reaction as defined
        // now (ADR-0007). Without this it would be executed, fail, and leave
        // nothing behind while the retry reported the reaction resolved.
        for unclaimed in replay.unclaimed_failures() {
            let id = write_dead_letter(
                &mut *tx,
                &reaction.policy_name,
                reaction.global_position,
                reaction.event_id,
                Some(&unclaimed.identity),
                &unclaimed.settlement.error_kind,
                &unclaimed.settlement.error_message,
            )
            .await?;
            tracing::warn!(
                policy    = %reaction.policy_name,
                event_id  = %reaction.event_id,
                aggregate = unclaimed.identity.aggregate_name,
                target    = %unclaimed.identity.target_stream_id,
                command   = unclaimed.identity.command_name,
                error     = %unclaimed.settlement.error_message,
                dead_letter_id = id,
                "a retried reaction failed on a command it had not parked; parking it"
            );
            settled.push((id, DeadLetterRetry::StillFailing));
        }

        tx.commit().await.map_err(crate::db_error)?;

        // Back into the order the rows were parked in, which settling the
        // identity-less ones last has just disturbed.
        settled.sort_by_key(|(id, _)| *id);

        Ok(settled)
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
    /// Returns a [`DeadLetterRetrySummary`] counting **reactions**: one whose
    /// every row was archived is resolved, one with any row still parked is
    /// still failing. The policy has fully recovered when
    /// `reactions_still_failing == 0`. A policy with no parked dead letters is a
    /// clean no-op (a zero summary), and so is a reaction whose rows were all
    /// discarded concurrently: it is skipped without being replayed.
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
                let settled = self.retry_reaction(reaction, OPERATION).await?;

                if settled
                    .iter()
                    .any(|(_, outcome)| *outcome == DeadLetterRetry::StillFailing)
                {
                    summary.reactions_still_failing += 1;
                } else if settled
                    .iter()
                    .any(|(_, outcome)| *outcome == DeadLetterRetry::Resolved)
                {
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
                stopped: self.stopped.clone(),
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

    async fn drain_policy(&self, policy: &dyn ErasedPolicy) -> Result<usize, replay::Error> {
        let name = policy.name().to_string();
        let mut cursor = PolicyCursor::load(&self.pool, &name, policy.start_at()).await?;
        let max_depth = resolve_max_depth(policy);
        drain_policy_once(
            &self.cqrs,
            &self.pool,
            &self.executors,
            policy,
            &mut cursor,
            max_depth,
            &mut Reporting {
                stopped: &self.stopped,
                narration: None,
            },
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
    policy: Arc<dyn ErasedPolicy>,
    cqrs: Cqrs<PostgresEventStore>,
    pool: Pool<Postgres>,
    executors: HashMap<TypeId, Arc<dyn AggregateExecutor>>,
    stopped: StoppedPolicies,
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
            stopped,
            mut shutdown_rx,
            mut leader_rx,
            wake_tx,
            interval,
            liveness,
            name,
        } = self;
        let mut wake_rx = wake_tx.as_ref().map(broadcast::Sender::subscribe);

        let max_depth = resolve_max_depth(policy.as_ref());

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

            // Initialize cursor from the stored checkpoint (or bootstrap).
            let mut cursor = match PolicyCursor::load(&pool, &name, policy.start_at()).await {
                Ok(cursor) => cursor,
                Err(error) => {
                    tracing::error!(
                        policy = %name,
                        error = %error,
                        "leader failed to initialize cursor; retrying"
                    );
                    tokio::select! {
                        _ = shutdown_rx.changed() => return Stop::Shutdown,
                        _ = tokio::time::sleep(interval) => {}
                    }
                    continue 'lifetime;
                }
            };

            // Where this worker picks up, once per election. It is the only
            // line a process killed from outside leaves: the same
            // `next_position` on every restart names a poison event
            // (`SELECT * FROM events WHERE global_position = <next_position>`), a
            // position that advances means the process is leaking instead.
            tracing::info!(
                policy = %name,
                resuming_after = cursor.position(),
                next_position = cursor.position() + 1,
                "policy worker is leading; resuming after its last checkpoint"
            );

            // One election, one bracket: a burst this worker does not finish is
            // abandoned rather than closed by whoever leads next.
            let mut narration = Narration::new(PROGRESS_EVERY);

            // Leadership polling loop.
            loop {
                if *shutdown_rx.borrow() || !*leader_rx.borrow() {
                    break;
                }

                // Narrated from inside the drain, as the cursor moves: a batch
                // whose dispatches take minutes is working throughout, and a
                // record earned only when the poll returns would be paced by the
                // work rather than by the clock.
                if let Err(error) = drain_policy_once(
                    &cqrs,
                    &pool,
                    &executors,
                    policy.as_ref(),
                    &mut cursor,
                    max_depth,
                    &mut Reporting {
                        stopped: &stopped,
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
    fn unclaimed_failures(self) -> Vec<UnclaimedFailure> {
        let concluded = match self {
            Self::Ran(concluded) | Self::Panicked { concluded, .. } => concluded,
        };
        concluded
            .into_iter()
            .filter(|dispatch| !dispatch.claimed)
            .filter_map(|dispatch| {
                Some(UnclaimedFailure {
                    identity: dispatch.identity,
                    settlement: dispatch.outcome?,
                })
            })
            .collect()
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
    /// Where a feed that stops is remembered, and how often it is reported.
    stopped: &'a StoppedPolicies,
    /// The bracket around a burst of work. `None` for [`PolicyRunner::drain`],
    /// which polls once on the caller's command: a manual drain that opened a
    /// burst would leave a bracket nothing ever closes.
    narration: Option<&'a mut Narration>,
}

impl Reporting<'_> {
    /// Tell the narration what a read of the feed found, and write whatever
    /// record it earns.
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
    policy: &dyn ErasedPolicy,
    cursor: &mut PolicyCursor,
    max_depth: u32,
    reporting: &mut Reporting<'_>,
) -> Result<usize, replay::Error> {
    let started = Instant::now();
    let name = policy.name().to_string();
    let checkpoint_size = resolve_checkpoint_batch_size(policy);
    let read_batch = resolve_read_batch_size(policy, checkpoint_size);
    let dispatch_timeout = resolve_dispatch_timeout(policy);
    let delivery = Delivery {
        cqrs,
        pool,
        executors,
        policy_name: &name,
        dispatch_timeout,
    };
    let Feed { positions, gap } =
        read_feed(pool, policy.stream_filter(), cursor.position(), read_batch).await?;

    if positions.is_empty() {
        // Nothing to process: the policy is idle, or parked in front of a gap
        // that will never fill. Both are the states an operator corrects by hand
        // with an UPDATE on `policy_cursors`, and both are the only moments when
        // a re-read costs nothing, so this is where a *running* leader picks the
        // correction up — no restart, no leadership change.
        if cursor.refresh(pool, &name).await? {
            tracing::info!(
                policy = %name,
                position = cursor.position(),
                "persisted cursor was moved externally; adopting it"
            );
            // The gap was read from the position the operator has just replaced:
            // reporting it would name a stop that no longer exists.
            reporting.stopped.forget(&name);
            // Not a catch-up: this poll read nothing and proved nothing about
            // the feed, which now starts somewhere else entirely.
            reporting.tell(&name, Poll::Stalled);
            return Ok(0);
        }

        match gap {
            // Parked in front of a hole, and the cursor is where the read left it.
            Some(gap) => {
                trace_gap(&name, gap);
                // A hole no running transaction can fill is crossed here; anything
                // else is an append the feed is right to wait for, and waiting is
                // what gets reported.
                if !skip_burned_positions(pool, &name, cursor, gap, reporting.stopped).await? {
                    report_blocked(pool, &name, gap, &reporting.stopped.blocked).await?;
                }
                // A Policy stopped in front of a hole has not reached the end of
                // its feed, whatever this poll read. Reporting it as caught up
                // would say the opposite of what happened, and the block has a
                // record of its own.
                reporting.tell(&name, Poll::Stalled);
            }
            // Caught up: a healthy idle policy, and it stays silent unless this
            // is the poll that closes a burst.
            None => {
                reporting.stopped.forget(&name);
                reporting.tell(&name, Poll::Exhausted);
            }
        }
        return Ok(0);
    }

    // The window is work, before any of it is done: a first reaction that takes
    // minutes must run inside the bracket rather than before it.
    reporting.tell(&name, Poll::Found { at: started });

    match gap {
        // A truncated window: the policy advances over the prefix now and parks at
        // the hole. The hole is as old as this poll even though this poll had work,
        // so the clock starts here rather than on the first empty poll.
        //
        // Whether the hole can ever fill is asked on the *next* poll, once the
        // prefix has been delivered and the feed comes back empty: the answer costs
        // a query, and a poll with work in front of it has somewhere better to be.
        // The delay is one poll, and the positions are no less burned for it.
        Some(gap) => {
            trace_gap(&name, gap);
            reporting.stopped.sighted(&name, gap);
        }
        // Advancing with nothing in the way.
        None => reporting.stopped.forget(&name),
    }

    let mut executed = 0;
    let mut events_since_checkpoint = 0u32;
    for WindowPosition {
        commit_txid,
        global_position,
        delivered,
    } in positions
    {
        if let Some(raw) = delivered {
            let depth = event_causation_depth(&raw);
            if depth >= max_depth {
                // Circuit breaker: the event's causation chain is too deep.
                // Skip reactions but keep advancing so the policy is not wedged.
                let (_, limit_source) = resolve_max_depth_with_source(policy);
                tracing::warn!(
                    policy        = %name,
                    event_id      = %raw.id,
                    stream_id     = %raw.stream_id,
                    global_position,
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
                    .react_to_event(policy, global_position, &raw)
                    .await?;
            }
        }
        // Always track in-memory position.
        cursor.advance_to(commit_txid, global_position);
        events_since_checkpoint += 1;
        // Told as the cursor moves rather than when the poll returns: one poll's
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
        // Write the persistent cursor every `checkpoint_size` events so that
        // a crash re-processes at most `checkpoint_size - 1` events rather
        // than the full drain batch (skip-safety: the cursor only advances
        // past events whose reactions are already durably committed).
        if events_since_checkpoint >= checkpoint_size {
            if cursor.checkpoint(pool, &name).await? == Checkpoint::Superseded {
                log_superseded(&name, cursor);
                return Ok(executed);
            }
            events_since_checkpoint = 0;
        }
    }

    // Final checkpoint: flush any events processed since the last periodic save.
    if events_since_checkpoint > 0
        && cursor.checkpoint(pool, &name).await? == Checkpoint::Superseded
    {
        log_superseded(&name, cursor);
    }

    Ok(executed)
}

/// Trace the stop: the one fact the blocked deployment in funkode-io/replay#164
/// never had. Cheap enough to emit on every poll, so it needs no rate limit.
///
/// `cursor` is where the *feed* stops: the position before the hole, which is where
/// the poll leaves the cursor when it gets that far. It is a property of the read,
/// not of what the reactions then managed to do, so it is the same field whether the
/// window was empty or was truncated after a prefix.
fn trace_gap(name: &str, gap: Gap) {
    tracing::debug!(
        policy = %name,
        cursor = gap.expected - 1,
        expected = gap.expected,
        found = gap.found,
        "policy feed stops at a gap in global_position"
    );
}

/// Cross a hole no transaction can ever fill, returning whether the Policy moved.
///
/// The feed stops at a missing `global_position` because it cannot tell an append
/// still committing from a position burned by one that aborted — `nextval` is not
/// transactional, so an aborted append's positions are gone for good
/// (funkode-io/replay#164). The two are told apart by who holds the sequence:
/// only a transaction that has already taken the missing position can write it, and
/// it holds a lock on the sequence until it ends (see [`crate::burned_position`]).
///
/// The order of the two queries is the correctness argument and not an accident:
/// the lock is read first, and only a position still missing *after* that read can
/// never appear, because Postgres publishes a commit before releasing its locks.
///
/// Every burned position in front of the cursor is crossed in one move, so an
/// aborted batch that burned thousands costs one poll rather than thousands.
async fn skip_burned_positions(
    pool: &Pool<Postgres>,
    name: &str,
    cursor: &mut PolicyCursor,
    gap: Gap,
    stopped: &StoppedPolicies,
) -> Result<bool, replay::Error> {
    let Some(holders) = sequence_holders(pool).await? else {
        // More transactions hold the sequence than this process will track. An
        // incomplete candidate set can only produce a wrong "permanent", so the
        // poll declines to judge and looks again next time.
        return Ok(false);
    };
    if stopped.burned.verdict(name, gap.expected, holders) == Permanence::Fillable {
        return Ok(false);
    }

    let resume = resume_after_burned(pool, name, cursor.position()).await?;
    let Resume::Skip { next_position } = resume else {
        // The cursor moved under us, or the hole is gone: either way this verdict
        // is about a position the Policy is no longer parked in front of.
        stopped.forget(name);
        return Ok(false);
    };

    // The record is written after the checkpoint, not before: a cursor moved by an
    // operator between the read above and this write loses the compare-and-set, and
    // this process then crossed nothing. A `warn` saying otherwise would send
    // whoever reads it looking for a move that never happened.
    let parked_at = cursor.position();
    cursor.park_at(next_position - 1);
    match cursor.checkpoint(pool, name).await? {
        Checkpoint::Written => tracing::warn!(
            policy = %name,
            cursor = parked_at,
            skipped_from = gap.expected,
            skipped_to = next_position - 1,
            skipped = next_position - gap.expected,
            next_position,
            "policy feed skipped global_position values that can never appear: no \
             transaction still holds them, so they were burned by an append that \
             aborted. Advancing past them (funkode-io/replay#164)"
        ),
        Checkpoint::Superseded => log_superseded(name, cursor),
    }
    stopped.forget(name);

    Ok(true)
}

/// Report a Policy parked in front of a hole long enough for the hole to be
/// permanent rather than an append still landing.
///
/// The wait is by design (ADR-0003 skip-safety): a `global_position` is assigned at
/// `INSERT` and visible at `COMMIT`, so a missing one is normally about to land.
/// A hole that outlives the transactions that could fill it is crossed by
/// [`skip_burned_positions`] instead, so what reaches this warning is a wait that is
/// still legitimate and has lasted longer than an operator wants to be left guessing
/// about — a long-running append, or a Policy whose cursor an operator has parked in
/// front of a position that does not exist yet.
///
/// [`BlockedWatch`] decides which poll reports; the probe then runs at most once
/// per interval, to name the head and how long the cursor has been parked.
async fn report_blocked(
    pool: &Pool<Postgres>,
    name: &str,
    gap: Gap,
    blocked: &BlockedWatch,
) -> Result<(), replay::Error> {
    if !blocked.poll(name, gap.expected, std::time::Instant::now()) {
        return Ok(());
    }

    // Confirms the hole is still there and the cursor still in front of it: the read
    // that found it is a few statements old by now.
    let Some(parked) = probe_blocked(pool, name, gap.expected - 1, gap.expected).await? else {
        return Ok(());
    };

    tracing::warn!(
        policy = %name,
        // The feed was empty, so the cursor is parked immediately before the hole.
        cursor = gap.expected - 1,
        head = parked.head,
        missing_position = gap.expected,
        next_position = gap.found,
        blocked_for_secs = parked.elapsed.as_secs(),
        "policy is blocked: its feed stops at a global_position that does not exist \
         yet. A transaction still holds it, so this is an append that has not \
         committed, or a cursor parked in front of a position that was never \
         written. A position no transaction holds is crossed automatically \
         (funkode-io/replay#170), so do not move the cursor past this one until the \
         append it is waiting for is known to be gone"
    );

    Ok(())
}

/// Report a checkpoint that lost to a cursor moved outside this process. The
/// batch stops here; the cursor already holds the stored position.
fn log_superseded(name: &str, cursor: &PolicyCursor) {
    tracing::info!(
        policy = %name,
        position = cursor.position(),
        "persisted cursor was moved externally mid-batch; abandoning this batch at the stored position"
    );
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
        policy: &dyn ErasedPolicy,
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
                    policy_name,
                    global_position,
                    raw.id,
                    in_flight.as_ref(),
                    PANIC_ERROR_KIND,
                    &message,
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
    /// same event.  Because `react` is a pure function and the at-least-once +
    /// causation-guard contract already guarantees idempotency, re-executing an
    /// earlier dispatch that already succeeded is safe.
    async fn execute_event_reactions(
        &self,
        policy: &dyn ErasedPolicy,
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
                self.policy_name,
                global_position,
                raw.id,
                Some(&identity),
                &failure.error_kind(),
                &failure.to_string(),
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
/// delivery produced and counts the delivery, rather than leaving a second
/// generation of rows behind (funkode-io/replay#220). Two concurrent retries
/// that both park a command the reaction had not parked settle the same way: one
/// inserts, the other refreshes.
///
/// It does **not** touch `created_at` (when the command first failed) or the
/// retry bookkeeping: a redelivery is not a [`retry_dead_letter`] — nobody
/// invoked the control surface.
///
/// [`retry_dead_letter`]: PolicyRunner::retry_dead_letter
async fn write_dead_letter(
    executor: impl sqlx::PgExecutor<'_>,
    policy_name: &str,
    global_position: i64,
    event_id: uuid::Uuid,
    identity: Option<&DispatchIdentity>,
    error_kind: &str,
    error_message: &str,
) -> Result<i64, replay::Error> {
    sqlx::query_scalar(
        "INSERT INTO policy_dead_letters \
         (policy_name, global_position, event_id, error_kind, error_message, \
          aggregate_name, target_stream_id, command_name, dispatch_ordinal) \
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9) \
         ON CONFLICT (policy_name, event_id, aggregate_name, target_stream_id, \
                      command_name, dispatch_ordinal) \
         DO UPDATE SET error_kind = EXCLUDED.error_kind, \
                       error_message = EXCLUDED.error_message, \
                       deliveries = policy_dead_letters.deliveries + 1, \
                       last_parked_at = now() \
         RETURNING id",
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
    .fetch_one(executor)
    .await
    .map_err(crate::db_error)
}

/// Update a parked dead letter in place with the failure a retry just produced,
/// so a row that keeps failing is never duplicated — and stamp what has been
/// tried on it.
///
/// The row stays **retryable**: what makes another retry worth making is a
/// change outside the library, which the library cannot observe.
/// [`PolicyRunner::discard_dead_letter`] is what takes a row out of play.
///
/// Returns whether a row was still there to re-park: a concurrent discard
/// leaves nothing to update, which is not a reaction still failing.
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

/// The rows one reaction parked, oldest first.
///
/// Bounded by the commands one reaction dispatches — the vector `react_erased`
/// already materialises. No longer multiplied by the number of times the event
/// was delivered: a parked command is one row, and a redelivery refreshes it
/// (funkode-io/replay#220).
///
/// `global_position` is redundant with `event_id` — one event has one position —
/// and is in the filter to make it a prefix match on
/// `idx_dead_letters_policy_reaction`.
async fn load_parked_reaction(
    pool: &Pool<Postgres>,
    reaction: &ParkedReaction,
) -> Result<Vec<ParkedRow>, replay::Error> {
    let rows = sqlx::query(
        "SELECT id, aggregate_name, target_stream_id, command_name \
         FROM policy_dead_letters \
         WHERE policy_name = $1 AND global_position = $2 AND event_id = $3 \
         ORDER BY id ASC",
    )
    .bind(&reaction.policy_name)
    .bind(reaction.global_position)
    .bind(reaction.event_id)
    .fetch_all(pool)
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

/// Move a dead letter out of the active `policy_dead_letters` table into the
/// `discarded_dead_letters` archive in a single statement, recording why it
/// left (`reason`: `retried` or `discarded`).
///
/// The `DELETE ... RETURNING` feeds the `INSERT` so the row is removed from the
/// active set and preserved for audit atomically. Returns `true` when a row was
/// moved, `false` when no active row matched `id`.
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

/// Load a single event by its primary key, shaped exactly like [`read_feed`]
/// so it can be fed back into a policy's erased reaction during retry.
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

/// Read the window of positions past `cursor` the policy may advance over.
///
/// Unfiltered — every `global_position > cursor`, up to `limit` — because contiguity
/// belongs to the position stream, not to the rows the policy asked for (ADR-0013).
/// `filter` is evaluated per row as `matches_filter` and decides delivery only; an
/// excluded row advances the cursor like a compaction snapshot
/// (`compacted_snapshot = TRUE`, ADR-0004). [`feed_from_window`] then truncates the
/// window at the first hole, and names the hole it truncated at.
async fn read_feed(
    pool: &Pool<Postgres>,
    filter: StreamFilter,
    cursor: i64,
    limit: u32,
) -> Result<Feed<PersistedEvent<Value>>, replay::Error> {
    let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
        "SELECT id, data, metadata, stream_id, type, version, created, aggregate_version, \
         global_position, commit_txid::text AS commit_txid, compacted_snapshot, COALESCE((",
    );
    // As a predicate NULL means no match; read as a value it must be collapsed.
    PostgresEventStore::add_filters(&mut qb, filter);
    qb.push("), FALSE) AS matches_filter FROM events WHERE global_position > ");
    qb.push_bind(cursor);
    qb.push(" ORDER BY global_position ASC LIMIT ");
    qb.push_bind(limit as i64);

    let rows = qb.build().fetch_all(pool).await.map_err(crate::db_error)?;

    let mut window = Vec::with_capacity(rows.len());
    for row in rows {
        let global_position: i64 = row.get("global_position");
        let commit_txid = CommitStamp::from_row(&row, "commit_txid")?;
        let is_snapshot: bool = row.get("compacted_snapshot");
        let matches_filter: bool = row.get("matches_filter");

        // Only delivered rows are parsed; a skipped row's bytes are still fetched.
        let delivered = if is_snapshot || !matches_filter {
            None
        } else {
            Some(PersistedEvent::<Value>::try_from(row)?)
        };

        window.push(WindowPosition {
            commit_txid,
            global_position,
            delivered,
        });
    }

    Ok(feed_from_window(cursor, window))
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
    let dispatch_metadata = dispatch.metadata.clone();

    let metadata = merge_dispatch_metadata(
        causation_metadata(policy_name, global_position, raw),
        dispatch_metadata,
    )
    .map_err(|err| {
        err.with_operation("policy_drain")
            .with_context("policy", policy_name)
            .with_context("aggregate", aggregate_name)
    })?;

    executor
        .execute(cqrs, dispatch.payload, metadata, dispatch.expected_version)
        .await
}

/// A point in the Policy feed: the transaction a Policy stopped in and the position it
/// stopped at.
///
/// The pair is what a cursor records (funkode-io/replay#194). Only the position decides
/// delivery today; the transaction is recorded so the feed can be ordered by writes that
/// have finished rather than by the numbers those writes took (funkode-io/replay#171).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct CursorPoint {
    commit_txid: CommitStamp,
    position: i64,
}

/// A policy's point in the global feed, paired with the value this process
/// believes is stored in `policy_cursors`.
///
/// The stored value is not this process's private state: an operator moves a
/// stuck policy by updating the row directly (that is how the permanent-gap
/// incident in funkode-io/replay#164 was recovered). So the in-memory point
/// is treated as a *lease* on the stored one:
///
/// - [`refresh`](Self::refresh) re-reads the row and adopts whatever it finds.
///   The drain calls it when the feed comes back empty — an idle or wedged
///   policy — which is where the correction can land for free.
/// - [`checkpoint`](Self::checkpoint) is a compare-and-set against `persisted`,
///   so a write derived from a point that predates the operator's update
///   fails instead of silently reinstating it.
struct PolicyCursor {
    /// Last point handed to the policy in this process.
    point: CursorPoint,
    /// The value this process last observed in `policy_cursors`.
    persisted: CursorPoint,
}

/// Outcome of a [`PolicyCursor::checkpoint`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Checkpoint {
    /// The stored point now matches the in-memory one.
    Written,
    /// Someone else moved the row since this process last read it. The write was
    /// refused and the cursor has adopted the stored point instead.
    Superseded,
}

impl PolicyCursor {
    /// Where the policy is, for the feed and for the log.
    fn position(&self) -> i64 {
        self.point.position
    }

    /// Move onto the position just read, in the transaction that wrote it.
    fn advance_to(&mut self, commit_txid: CommitStamp, position: i64) {
        self.point = CursorPoint {
            commit_txid,
            position,
        };
    }

    /// Move onto a position no event carries, keeping the transaction half.
    ///
    /// A burned position was taken by a write that aborted, so there is no event there
    /// and no transaction to name: the last write the policy passed is still the one it
    /// stopped in.
    fn park_at(&mut self, position: i64) {
        self.point.position = position;
    }

    /// Load the stored checkpoint, bootstrapping the row from `start_at` the
    /// first time a policy runs.
    async fn load(
        pool: &Pool<Postgres>,
        name: &str,
        start_at: StartAt,
    ) -> Result<Self, replay::Error> {
        if let Some(stored) = read_point(pool, name).await? {
            let mut cursor = Self {
                point: stored,
                persisted: stored,
            };
            // Even a row this policy's last leader wrote goes through `adopt`: a fresh
            // process cannot tell that row from one an operator has since edited, and
            // the derivation costs one indexed read per election.
            cursor.adopt(pool, name, stored).await?;
            return Ok(cursor);
        }

        let bootstrap = bootstrap_point(pool, start_at).await?;
        let mut cursor = Self {
            point: bootstrap,
            persisted: bootstrap,
        };

        // `refresh` does the rest: it creates the row if it is still missing and
        // adopts the stored value, which is a concurrent runner's bootstrap when
        // that runner won the insert.
        cursor.refresh(pool, name).await?;
        Ok(cursor)
    }

    /// Re-read the stored point and adopt it, returning `true` when it moved
    /// the in-memory one (i.e. something outside this process wrote it).
    ///
    /// A deleted row is recreated at the in-memory point: dropping the row is
    /// not a documented way to rewind a policy, and recreating it keeps the
    /// policy from silently replaying its whole history. The recreate can lose
    /// to a concurrent writer, so it re-reads and adopts the winner rather than
    /// assuming its own value took.
    async fn refresh(&mut self, pool: &Pool<Postgres>, name: &str) -> Result<bool, replay::Error> {
        let stored = match read_point(pool, name).await? {
            Some(stored) => stored,
            None => {
                insert_point(pool, name, self.point).await?;
                read_point(pool, name).await?.ok_or_else(|| {
                    replay::Error::not_found("policy cursor row vanished immediately after insert")
                        .with_operation("policy_cursor_refresh")
                        .with_context("policy", name)
                })?
            }
        };

        // The row is where this process left it: nothing to adopt, and no read of the
        // log to pay for on an idle poll.
        if stored == self.persisted && stored == self.point {
            return Ok(false);
        }

        let before = self.point;
        self.adopt(pool, name, stored).await?;
        Ok(self.point != before)
    }

    /// Take on a stored point this process did not write, completing its transaction
    /// half from the log.
    ///
    /// The operator's instruction is a position — that is the control surface ADR-0012
    /// documents, and it stays one column wide. The transaction that belongs with a
    /// position is the one that wrote the last event at or before it, which is exactly
    /// what the runner itself stores, so a pair the runner wrote survives this untouched
    /// and a position written alone is completed rather than refused.
    ///
    /// The completion is written back, so the row shows the point the Policy resumes
    /// from rather than the half-instruction it was given — without disturbing
    /// `updated_at`, since nothing was processed. Losing that compare-and-set means the
    /// row moved again; the next poll reads it and adopts that instead.
    async fn adopt(
        &mut self,
        pool: &Pool<Postgres>,
        name: &str,
        stored: CursorPoint,
    ) -> Result<(), replay::Error> {
        self.persisted = stored;
        self.point = CursorPoint {
            commit_txid: commit_txid_at(pool, stored.position).await?,
            position: stored.position,
        };

        if self.point != self.persisted
            && complete_commit_txid(pool, name, self.persisted, self.point.commit_txid).await?
        {
            self.persisted = self.point;
        }
        Ok(())
    }

    /// Persist the in-memory point, but only if the stored one is still the
    /// value this process last saw.
    ///
    /// On [`Checkpoint::Superseded`] the cursor has already adopted the stored
    /// point, so the caller must stop draining from its own: everything after
    /// it belongs to the operator's correction, not to this batch.
    async fn checkpoint(
        &mut self,
        pool: &Pool<Postgres>,
        name: &str,
    ) -> Result<Checkpoint, replay::Error> {
        if write_point(pool, name, self.persisted, self.point).await? {
            self.persisted = self.point;
            return Ok(Checkpoint::Written);
        }

        self.refresh(pool, name).await?;
        Ok(Checkpoint::Superseded)
    }
}

/// Read a policy's stored point, or `None` when it has no row yet.
async fn read_point(
    pool: &Pool<Postgres>,
    name: &str,
) -> Result<Option<CursorPoint>, replay::Error> {
    let row = sqlx::query(
        "SELECT position, commit_txid::text AS commit_txid FROM policy_cursors WHERE name = $1",
    )
    .bind(name)
    .fetch_optional(pool)
    .await
    .map_err(crate::db_error)?;

    row.map(|row| {
        Ok(CursorPoint {
            commit_txid: CommitStamp::from_row(&row, "commit_txid")?,
            position: row.get("position"),
        })
    })
    .transpose()
}

/// Create a policy's cursor row, leaving an existing one untouched.
async fn insert_point(
    pool: &Pool<Postgres>,
    name: &str,
    point: CursorPoint,
) -> Result<(), replay::Error> {
    sqlx::query(
        "INSERT INTO policy_cursors (name, position, commit_txid, updated_at) \
         VALUES ($1, $2, $3::xid8, now()) ON CONFLICT (name) DO NOTHING",
    )
    .bind(name)
    .bind(point.position)
    .bind(point.commit_txid.to_string())
    .execute(pool)
    .await
    .map_err(crate::db_error)?;
    Ok(())
}

/// The compare-and-set every cursor write goes through: move the row from `from` to
/// `to`, and report whether it still held `from`.
///
/// Both halves are compared, so a write derived from either a stale position or a stale
/// transaction is refused rather than reinstating it.
async fn write_point(
    pool: &Pool<Postgres>,
    name: &str,
    from: CursorPoint,
    to: CursorPoint,
) -> Result<bool, replay::Error> {
    let updated = sqlx::query(
        "UPDATE policy_cursors SET position = $2, commit_txid = $3::xid8, updated_at = now() \
         WHERE name = $1 AND position = $4 AND commit_txid = $5::xid8",
    )
    .bind(name)
    .bind(to.position)
    .bind(to.commit_txid.to_string())
    .bind(from.position)
    .bind(from.commit_txid.to_string())
    .execute(pool)
    .await
    .map_err(crate::db_error)?;

    Ok(updated.rows_affected() > 0)
}

/// Fill in the transaction half of a row whose position stays where it is, under the
/// same compare-and-set.
///
/// `updated_at` is deliberately left alone: it records when the cursor last *advanced*,
/// and is what a Policy's `blocked_for_secs` ([`crate::policy_blocked`]) and its status's
/// `last_checkpoint_at` ([`crate::PolicyStatus`]) are measured from. Completing a
/// transaction half processes nothing, so touching it would erase a running outage.
async fn complete_commit_txid(
    pool: &Pool<Postgres>,
    name: &str,
    from: CursorPoint,
    to: CommitStamp,
) -> Result<bool, replay::Error> {
    let updated = sqlx::query(
        "UPDATE policy_cursors SET commit_txid = $2::xid8 \
         WHERE name = $1 AND position = $3 AND commit_txid = $4::xid8",
    )
    .bind(name)
    .bind(to.to_string())
    .bind(from.position)
    .bind(from.commit_txid.to_string())
    .execute(pool)
    .await
    .map_err(crate::db_error)?;

    Ok(updated.rows_affected() > 0)
}

/// The transaction that belongs with `position`: the one that wrote the last event at or
/// before it.
///
/// [`CommitStamp::SENTINEL`] when there is no such event — a cursor at 0, or a log with
/// nothing in it yet — which orders before every real transaction, exactly as a cursor
/// that has processed nothing should. A position past the head takes the head's
/// transaction: the events between are the ones the policy is being told it has passed.
async fn commit_txid_at(
    pool: &Pool<Postgres>,
    position: i64,
) -> Result<CommitStamp, replay::Error> {
    let stamp = sqlx::query_scalar::<_, String>(
        "SELECT commit_txid::text FROM events WHERE global_position <= $1 \
         ORDER BY global_position DESC LIMIT 1",
    )
    .bind(position)
    .fetch_optional(pool)
    .await
    .map_err(crate::db_error)?;

    stamp.map_or(Ok(CommitStamp::SENTINEL), |stamp| {
        CommitStamp::parse(stamp.as_str())
    })
}

async fn bootstrap_point(
    pool: &Pool<Postgres>,
    start_at: StartAt,
) -> Result<CursorPoint, replay::Error> {
    let position = match start_at {
        StartAt::Beginning => 0,
        StartAt::Now => {
            let head =
                sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(global_position) FROM events")
                    .fetch_one(pool)
                    .await
                    .map_err(crate::db_error)?;
            head.unwrap_or_default()
        }
    };

    Ok(CursorPoint {
        commit_txid: commit_txid_at(pool, position).await?,
        position,
    })
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

/// Environment variable that overrides the read-batch default.
const READ_BATCH_SIZE_ENV_VAR: &str = "REPLAY_READ_BATCH_SIZE";

/// Environment variable that overrides the checkpoint-batch default.
const CHECKPOINT_BATCH_SIZE_ENV_VAR: &str = "REPLAY_CHECKPOINT_BATCH_SIZE";

/// Built-in default for how long one dispatch may run before it is abandoned.
///
/// Generous on purpose: it exists to cut loose a reaction that has *stopped*,
/// not to enforce a latency budget, so it must not park a reaction that is
/// merely slow. A reaction with a legitimately longer ceiling raises it with
/// [`Policy::dispatch_timeout`].
const DEFAULT_DISPATCH_TIMEOUT: Duration = Duration::from_secs(30);

/// Environment variable that overrides the dispatch-timeout default, in
/// milliseconds.
const DISPATCH_TIMEOUT_ENV_VAR: &str = "REPLAY_DISPATCH_TIMEOUT_MS";

/// Resolve the effective causation depth limit for a policy.
///
/// Precedence (most-specific wins):
///   1. Per-policy override via [`Policy::max_causation_depth`]
///   2. `REPLAY_MAX_CAUSATION_DEPTH` environment variable
///   3. Built-in default (10)
fn resolve_max_depth(policy: &dyn ErasedPolicy) -> u32 {
    resolve_max_depth_with_source(policy).0
}

/// Like [`resolve_max_depth`] but also returns the source for diagnostic logging.
fn resolve_max_depth_with_source(policy: &dyn ErasedPolicy) -> (u32, &'static str) {
    if let Some(d) = policy.max_causation_depth_erased() {
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
fn resolve_checkpoint_batch_size(policy: &dyn ErasedPolicy) -> u32 {
    if let Some(n) = policy.checkpoint_batch_size_erased() {
        return n.max(1);
    }
    if let Ok(s) = std::env::var(CHECKPOINT_BATCH_SIZE_ENV_VAR) {
        if let Ok(n) = s.parse::<u32>() {
            return n.max(1);
        }
    }
    DEFAULT_CHECKPOINT_BATCH_SIZE
}

/// Resolve the effective read-batch size (events fetched in a single `read_feed` call).
///
/// Precedence: per-policy override → `REPLAY_READ_BATCH_SIZE` env var → default 100.
/// Enforces the invariant `read_batch_size ≥ checkpoint_batch_size`.
fn resolve_read_batch_size(policy: &dyn ErasedPolicy, checkpoint_size: u32) -> u32 {
    let raw = if let Some(n) = policy.read_batch_size_erased() {
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
fn resolve_dispatch_timeout(policy: &dyn ErasedPolicy) -> Duration {
    dispatch_timeout_or_default(
        policy.dispatch_timeout_erased(),
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

/// What a [`PolicyCursor`] records and how it reads a row it did not write
/// (funkode-io/replay#194).
///
/// These drive the cursor against a real database rather than through a daemon: the
/// compare-and-set and the derivation are two statements apart, and a test that has to
/// catch a running worker between them is a test that fails on a busy machine.
#[cfg(test)]
mod cursor_tests {
    use sqlx::postgres::PgPoolOptions;
    use sqlx::{PgPool, Row};
    use testcontainers_modules::postgres;
    use testcontainers_modules::testcontainers::{runners::AsyncRunner, ContainerAsync, ImageExt};

    use crate::commit_stamp::CommitStamp;

    use super::{read_point, Checkpoint, CursorPoint, PolicyCursor, StartAt};

    const POLICY: &str = "cursor_under_test";

    /// The server the suite is verified against, pinned as
    /// `tests/common/postgres_image.rs` pins it — this module cannot reach that file,
    /// and the migration set needs `NULLS NOT DISTINCT`, which does not exist before
    /// PostgreSQL 15, so an unpinned default would fail here rather than run somewhere
    /// else. The two pins move together with the crate's floor (README "Requirements").
    const POSTGRES_TAG: &str = "15-alpine";

    async fn start_postgres() -> (PgPool, ContainerAsync<postgres::Postgres>) {
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

        sqlx::migrate!("./tests/migrations")
            .run(&pool)
            .await
            .expect("migrations must succeed");

        (pool, container)
    }

    /// Append `count` events through the store's own insert path, one transaction each,
    /// and report the point each one landed at. No runner is involved: these tests are
    /// about what a cursor makes of the log, not about how it got there.
    async fn append_events(pool: &PgPool, count: usize) -> Vec<CursorPoint> {
        let mut points = Vec::with_capacity(count);
        for index in 0..count {
            sqlx::query(
                "SELECT append_event(gen_random_uuid(), '{}'::jsonb, '{}'::jsonb, \
                 'Appended', $1, 'Probe', NULL)",
            )
            .bind(format!("urn:probe:{index}"))
            .execute(pool)
            .await
            .expect("appending must succeed");

            let row = sqlx::query(
                "SELECT global_position, commit_txid::text AS commit_txid FROM events \
                 ORDER BY global_position DESC LIMIT 1",
            )
            .fetch_one(pool)
            .await
            .expect("reading the appended event must succeed");

            points.push(CursorPoint {
                commit_txid: CommitStamp::from_row(&row, "commit_txid")
                    .expect("an appended event carries a readable stamp"),
                position: row.get("global_position"),
            });
        }
        points
    }

    /// The row as it stands, which is what a restarted process and an operator both read.
    async fn stored(pool: &PgPool) -> CursorPoint {
        read_point(pool, POLICY)
            .await
            .expect("reading the cursor must succeed")
            .expect("the cursor row exists")
    }

    /// The operator's instruction from ADR-0012, unchanged by this ticket: a position,
    /// written alone.
    async fn move_position(pool: &PgPool, position: i64) {
        sqlx::query("UPDATE policy_cursors SET position = $2, updated_at = now() WHERE name = $1")
            .bind(POLICY)
            .bind(position)
            .execute(pool)
            .await
            .expect("the operator's move must succeed");
    }

    /// When the cursor last advanced — the column `blocked_for_secs` and
    /// `PolicyStatus::last_checkpoint_at` are read from.
    async fn last_advanced(pool: &PgPool) -> chrono::DateTime<chrono::Utc> {
        sqlx::query_scalar("SELECT updated_at FROM policy_cursors WHERE name = $1")
            .bind(POLICY)
            .fetch_one(pool)
            .await
            .expect("reading the cursor's timestamp must succeed")
    }

    #[tokio::test]
    async fn a_checkpoint_records_the_transaction_the_policy_stopped_in_postgres_test() {
        let (pool, _container) = start_postgres().await;
        let events = append_events(&pool, 2).await;

        let mut cursor = PolicyCursor::load(&pool, POLICY, StartAt::Beginning)
            .await
            .expect("loading must succeed");
        cursor.advance_to(events[1].commit_txid, events[1].position);

        assert_eq!(
            cursor.checkpoint(&pool, POLICY).await.unwrap(),
            Checkpoint::Written
        );
        assert_eq!(
            stored(&pool).await,
            events[1],
            "the row records both halves of the point the policy stopped at"
        );
    }

    /// A Policy that has processed nothing sits at the sentinel: it is behind every
    /// transaction, which is what `StartAt::Beginning` means in the pair order.
    #[tokio::test]
    async fn a_policy_that_has_processed_nothing_sits_at_the_sentinel_postgres_test() {
        let (pool, _container) = start_postgres().await;
        append_events(&pool, 1).await;

        PolicyCursor::load(&pool, POLICY, StartAt::Beginning)
            .await
            .expect("loading must succeed");

        assert_eq!(
            stored(&pool).await,
            CursorPoint {
                commit_txid: CommitStamp::SENTINEL,
                position: 0,
            }
        );
    }

    /// `StartAt::Now` starts at the head, and the head is a pair.
    #[tokio::test]
    async fn a_cursor_bootstrapped_at_the_head_names_the_head_transaction_postgres_test() {
        let (pool, _container) = start_postgres().await;
        let events = append_events(&pool, 3).await;

        PolicyCursor::load(&pool, POLICY, StartAt::Now)
            .await
            .expect("loading must succeed");

        assert_eq!(stored(&pool).await, events[2]);
    }

    /// The compare-and-set guards the pair, not half of it: a process whose transaction
    /// half is stale loses, exactly as one whose position is stale does.
    #[tokio::test]
    async fn a_checkpoint_derived_from_a_stale_transaction_is_refused_postgres_test() {
        let (pool, _container) = start_postgres().await;
        let events = append_events(&pool, 2).await;

        let mut cursor = PolicyCursor::load(&pool, POLICY, StartAt::Beginning)
            .await
            .expect("loading must succeed");
        cursor.advance_to(events[0].commit_txid, events[0].position);
        cursor.checkpoint(&pool, POLICY).await.unwrap();

        // Someone else moves the row's transaction half only. The position this process
        // holds is still the stored one, so a guard on the position alone would let the
        // next checkpoint through.
        sqlx::query("UPDATE policy_cursors SET commit_txid = $2::xid8 WHERE name = $1")
            .bind(POLICY)
            .bind(events[1].commit_txid.to_string())
            .execute(&pool)
            .await
            .expect("the outside write must succeed");

        cursor.advance_to(events[1].commit_txid, events[1].position);
        assert_eq!(
            cursor.checkpoint(&pool, POLICY).await.unwrap(),
            Checkpoint::Superseded,
            "a write derived from a stale transaction is refused"
        );
        assert_eq!(
            cursor.position(),
            events[0].position,
            "the refused cursor is back where the row says it is, not where it wanted to be"
        );
    }

    /// ADR-0012's control surface stays one column wide: the operator writes a position
    /// and the runner supplies the transaction that belongs with it — the one that wrote
    /// the event there, which is what the runner would have stored itself.
    #[tokio::test]
    async fn an_operator_may_move_the_position_alone_postgres_test() {
        let (pool, _container) = start_postgres().await;
        let events = append_events(&pool, 3).await;

        let mut cursor = PolicyCursor::load(&pool, POLICY, StartAt::Beginning)
            .await
            .expect("loading must succeed");
        cursor.advance_to(events[0].commit_txid, events[0].position);
        cursor.checkpoint(&pool, POLICY).await.unwrap();

        move_position(&pool, events[2].position).await;

        assert!(
            cursor.refresh(&pool, POLICY).await.unwrap(),
            "the running leader adopts a cursor moved underneath it"
        );
        assert_eq!(
            cursor.point, events[2],
            "the adopted point carries the transaction that wrote the event there"
        );
        assert_eq!(
            stored(&pool).await,
            events[2],
            "and the row is completed, so it shows the point the policy resumes from"
        );
    }

    /// The same instruction aimed at a position no event carries — the #164 recovery,
    /// where the operator moves past a burned position. There is no transaction to name
    /// there, so the cursor takes the last one it has passed.
    #[tokio::test]
    async fn a_position_no_event_carries_takes_the_transaction_before_it_postgres_test() {
        let (pool, _container) = start_postgres().await;
        let events = append_events(&pool, 2).await;

        let mut cursor = PolicyCursor::load(&pool, POLICY, StartAt::Beginning)
            .await
            .expect("loading must succeed");
        move_position(&pool, events[1].position + 10).await;
        cursor.refresh(&pool, POLICY).await.unwrap();

        assert_eq!(
            cursor.point,
            CursorPoint {
                commit_txid: events[1].commit_txid,
                position: events[1].position + 10,
            }
        );
    }

    /// Completing a transaction half is bookkeeping, not progress: a Policy that has
    /// been parked for ten minutes still reads as parked for ten minutes afterwards.
    ///
    /// The case is a database upgraded through 0022 whose cursor sits on an event
    /// appended after 0018: the row takes the sentinel, and the first leader to load it
    /// derives the real id.
    #[tokio::test]
    async fn completing_the_transaction_half_reports_no_progress_postgres_test() {
        let (pool, _container) = start_postgres().await;
        let events = append_events(&pool, 2).await;

        sqlx::query(
            "INSERT INTO policy_cursors (name, position, updated_at) \
             VALUES ($1, $2, now() - interval '10 minutes')",
        )
        .bind(POLICY)
        .bind(events[1].position)
        .execute(&pool)
        .await
        .expect("staging the migrated cursor must succeed");
        let parked_since = last_advanced(&pool).await;

        let mut cursor = PolicyCursor::load(&pool, POLICY, StartAt::Now)
            .await
            .expect("loading must succeed");

        assert_eq!(
            stored(&pool).await,
            events[1],
            "the load completed the transaction half from the log"
        );
        assert_eq!(
            last_advanced(&pool).await,
            parked_since,
            "and reported no progress: nothing was processed"
        );

        // A checkpoint that does move the policy is progress, and says so.
        cursor.advance_to(events[1].commit_txid, events[1].position + 1);
        cursor.checkpoint(&pool, POLICY).await.unwrap();
        assert!(
            last_advanced(&pool).await > parked_since,
            "a cursor that advanced reports when it did"
        );
    }

    /// A cursor that predates 0022 carries the sentinel and the position it had. It
    /// resumes exactly there: the sentinel is the transaction every event it has already
    /// processed was stamped with, because they all predate 0018 too.
    #[tokio::test]
    async fn a_cursor_written_before_the_stamp_resumes_where_it_was_postgres_test() {
        let (pool, _container) = start_postgres().await;
        let events = append_events(&pool, 3).await;

        // The log a deployment upgrades with: events already there when 0018 arrived
        // carry the sentinel, and the ones appended since carry a real id.
        sqlx::query("UPDATE events SET commit_txid = '0'::xid8 WHERE global_position <= $1")
            .bind(events[1].position)
            .execute(&pool)
            .await
            .expect("staging the pre-stamp events must succeed");

        // The row 0022 leaves behind: a position, and the sentinel.
        sqlx::query("INSERT INTO policy_cursors (name, position) VALUES ($1, $2)")
            .bind(POLICY)
            .bind(events[1].position)
            .execute(&pool)
            .await
            .expect("staging the migrated cursor must succeed");

        let expected = CursorPoint {
            commit_txid: CommitStamp::SENTINEL,
            position: events[1].position,
        };
        let cursor = PolicyCursor::load(&pool, POLICY, StartAt::Now)
            .await
            .expect("loading must succeed");

        assert_eq!(
            cursor.point, expected,
            "a migrated cursor resumes at its position, behind every real transaction"
        );
        assert_eq!(
            stored(&pool).await,
            expected,
            "and the row is left alone: there is nothing to complete"
        );
    }
}
