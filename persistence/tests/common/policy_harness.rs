//! A live policy daemon under test: a real Postgres, a real
//! [`PolicyRunner`] polling in the background, and the handful of things an
//! operator can see about it.
//!
//! ## Why this exists
//!
//! The crate's other policy tests call `react` directly and inspect the
//! [`Dispatch`](replay_persistence::Dispatch)es it returns. That seam cannot see
//! a worker, a cursor, a restart or a timeout — everything the supervision work
//! in funkode-io/replay#180 is about. This harness moves the assertion boundary
//! out to where an operator stands: events land in the log, and what the policy
//! did is read back out of the database.
//!
//! ## What it exposes
//!
//! Only observations an operator could make without attaching a debugger:
//!
//! - [`PolicyDaemonHarness::dispatches`] — the commands the policy issued, seen
//!   as the events they wrote, each carrying the causation the runner stamps.
//! - [`PolicyDaemonHarness::cursor`] — the policy's persisted position.
//! - [`PolicyDaemonHarness::dead_letters`] — the reactions it parked.
//! - [`PolicyDaemonHarness::stopped_workers`] — the workers the runner gave up
//!   on, which is what an operator reads off the daemon in their own service.
//! - [`PolicyDaemonHarness::escalations`] — what the consumer's escalation hook
//!   was told. The harness installs a recording hook in place of the default,
//!   which exits the process: in a test that is the test runner.
//!
//! And two things an operator can *do*:
//!
//! - [`PolicyDaemonHarness::restart`] — stop the daemon and start an identical
//!   one against the same database, so a test can ask what survived in memory
//!   (nothing) and what survived in the tables (everything that matters).
//! - [`PolicyDaemonHarness::retry_parked`] — the bulk retry of everything the
//!   policy parked, run out of band while the daemon keeps polling.
//!
//! Tasks, channels and in-process state are deliberately absent.
//!
//! ## Waiting
//!
//! A daemon is asynchronous, so every observation is awaited rather than read
//! once. The `await_*` methods return the moment the observation holds and
//! panic at [`OBSERVE_TIMEOUT`] if it never does; no test ever sleeps for a
//! fixed duration and then asserts, which is what makes them fast when they pass
//! and honest when they fail. The panic message carries the full observation set
//! so a timeout reads as a diagnosis rather than "assertion failed".
//!
//! ## Isolation
//!
//! Each harness owns its own container, hence its own database, and names its
//! policy with a process-unique suffix. Cursors, dead letters and the advisory
//! locks that elect a leader are therefore private to one test even if it runs
//! concurrently with every other one.

// Each test binary uses a subset of the observations; an unused one here is not
// a defect.
#![allow(dead_code)]

use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use testcontainers_modules::postgres;
use testcontainers_modules::testcontainers::{runners::AsyncRunner, ContainerAsync};
use uuid::Uuid;

use super::postgres_image::{postgres_container, POSTGRES_PORT};

use replay_macros::define_aggregate;
use replay_persistence::{
    Cqrs, DeadLetterRetrySummary, Escalation, PolicyRunner, PolicyRunnerBuilder,
    PolicyRunnerDaemon, PostgresEventStore, StoppedWorker,
};

/// How often the daemon under test polls the feed. Short: these tests wait on
/// outcomes, so the interval only bounds how long an idle poll loop dawdles.
const DAEMON_POLL_INTERVAL: Duration = Duration::from_millis(25);

/// How long an `await_*` observation may go unsatisfied before it is a failure.
pub const OBSERVE_TIMEOUT: Duration = Duration::from_secs(10);

/// How often an unsatisfied observation is re-read. Not a delay before
/// asserting: the wait ends as soon as the observation holds.
const OBSERVE_RECHECK: Duration = Duration::from_millis(20);

/// Upper bound on rows any observation query returns. A test that produces more
/// than this is testing something this harness was not built for, and reading
/// the lot into memory would violate the crate's bounded-memory rule.
const OBSERVATION_LIMIT: i64 = 1_000;

/// Metadata key carrying the marker that identifies an event appended by
/// [`PolicyDaemonHarness::ping`]. Only the harness writes it, so an event
/// carrying a given marker is the one a given `ping` call wrote.
const PING_MARKER_KEY: &str = "harness_ping";

// ── The aggregate every harness test drives ──────────────────────────────────

define_aggregate! {
    Probe {
        namespace: "probe",
        state: {
            echoes: i64,
        },
        commands: {
            Ping { tag: String },
            Echo { tag: String },
            Refuse { reason: String },
            Explode { reason: String },
        },
        events: {
            Pinged { tag: String },
            Echoed { tag: String },
        }
    }
}

impl replay::EventStream for Probe {
    type Event = ProbeEvent;

    fn stream_type() -> String {
        "Probe".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            ProbeEvent::Pinged { .. } => {}
            ProbeEvent::Echoed { .. } => self.echoes += 1,
        }
    }
}

impl replay::Aggregate for Probe {
    type Command = ProbeCommand;
    type Error = replay::Error;
    type Services = ();

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            ProbeCommand::Ping { tag } => Ok(vec![ProbeEvent::Pinged { tag }]),
            ProbeCommand::Echo { tag } => Ok(vec![ProbeEvent::Echoed { tag }]),
            // A command that always fails permanently, so a test can drive a
            // reaction into the dead-letter path on purpose. `invalid_input`
            // rather than `internal` because `internal` hides its message from
            // `Display`, and the parked row is only worth reading if it says
            // what went wrong.
            ProbeCommand::Refuse { reason } => Err(replay::Error::invalid_input(format!(
                "probe refuses: {reason}"
            ))
            .with_operation("Refuse")),
            // A command that panics *inside the handler*, so a test can drive a
            // panic into the asynchronous half of the runner's per-event
            // boundary — the one a panic in `react` never reaches, because it
            // happens while awaiting the dispatch rather than before it.
            ProbeCommand::Explode { reason } => panic!("probe exploded: {reason}"),
        }
    }
}

// ── Observations ─────────────────────────────────────────────────────────────

/// An event appended by a test, identified the way the runner identifies it.
#[derive(Debug, Clone)]
pub struct AppendedEvent {
    pub event_id: Uuid,
    pub global_position: i64,
    pub stream_id: String,
}

/// A command the policy dispatched, observed as the event it wrote.
///
/// The runner stamps every policy-issued command with causation metadata naming
/// the policy and the event it reacted to, so this is readable from the log
/// alone — no in-process recording.
#[derive(Debug, Clone)]
pub struct DispatchedCommand {
    /// Position of the event the command wrote.
    pub global_position: i64,
    /// Stream the command was addressed to.
    pub stream_id: String,
    /// Type of the event the command produced, e.g. `"Echoed"`.
    pub event_type: String,
    /// Position of the event the policy reacted to.
    pub caused_by_position: i64,
    /// Id of the event the policy reacted to.
    pub caused_by_event_id: Uuid,
}

/// A reaction the runner gave up on and parked — the glossary's
/// [Dead letter], as an operator reads it out of `policy_dead_letters`.
#[derive(Debug, Clone)]
pub struct DeadLetter {
    pub global_position: i64,
    pub event_id: Uuid,
    pub error_kind: String,
    pub error_message: String,
}

/// What the consumer's escalation hook was told, recorded instead of acted on.
///
/// Installed by the harness on every daemon it starts, because the default hook
/// exits the process. A test that wants its own hook registers one in
/// `configure`, which runs afterwards and therefore wins. Scoped to the daemon
/// that fired them: [`PolicyDaemonHarness::restart`] clears the record, as the
/// daemon's own `stopped_workers` list is cleared by being replaced.
#[derive(Clone, Default)]
pub struct Escalations(Arc<std::sync::Mutex<Vec<Escalation>>>);

impl Escalations {
    fn hook(&self) -> impl Fn(&Escalation) + Send + Sync + 'static {
        let recorded = Arc::clone(&self.0);
        move |escalation: &Escalation| recorded.lock().unwrap().push(escalation.clone())
    }

    fn recorded(&self) -> Vec<Escalation> {
        self.0.lock().unwrap().clone()
    }

    /// Forget everything the previous daemon's hook was told. Escalations belong
    /// to the daemon that fired them, like the stopped workers they accompany;
    /// keeping them across a restart would let an `await_escalation` be satisfied
    /// by the daemon before it.
    fn cleared(&self) {
        self.0.lock().unwrap().clear();
    }
}

// ── The harness ──────────────────────────────────────────────────────────────

/// A running daemon, its database, and the observations an operator has.
///
/// Build one with [`start`](Self::start) and end every test with
/// [`shutdown`](Self::shutdown).
pub struct PolicyDaemonHarness {
    /// Dropping this stops the database; the field is never read.
    _container: ContainerAsync<postgres::Postgres>,
    pool: PgPool,
    cqrs: Cqrs<PostgresEventStore>,
    policy_name: String,
    /// How the policy under test is registered. Kept so [`restart`](Self::restart)
    /// can build the same daemon again against the same database.
    configure: Arc<Configure>,
    escalations: Escalations,
    daemon: Option<PolicyRunnerDaemon>,
}

/// Registers the policy under test on a fresh runner builder, under the name the
/// harness minted for it.
type Configure = dyn Fn(PolicyRunnerBuilder, &str) -> PolicyRunnerBuilder + Send + Sync;

impl PolicyDaemonHarness {
    /// Start a database, register the policy `configure` builds, and begin
    /// polling — all before the test appends anything.
    ///
    /// `configure` receives the runner builder (with `Probe`'s services already
    /// registered) and the unique name the policy must take, so the cursor key
    /// and advisory-lock key cannot collide with another test's. `label` is
    /// woven into that name to keep it legible in logs and in a failed
    /// assertion.
    pub async fn start<F>(label: &str, configure: F) -> Self
    where
        F: Fn(PolicyRunnerBuilder, &str) -> PolicyRunnerBuilder + Send + Sync + 'static,
    {
        let container = postgres_container()
            .start()
            .await
            .expect("failed to start the postgres container");
        let host = container
            .get_host()
            .await
            .expect("failed to read the container host")
            .to_string();
        let port = container
            .get_host_port_ipv4(POSTGRES_PORT)
            .await
            .expect("failed to read the container port");

        let pool = PgPoolOptions::new()
            .max_connections(20)
            .idle_timeout(Duration::from_secs(5))
            .connect(&format!(
                "postgres://postgres:postgres@{host}:{port}/postgres"
            ))
            .await
            .expect("failed to create the postgres pool");

        sqlx::migrate!("./tests/migrations")
            .run(&pool)
            .await
            .expect("failed to run migrations");

        let cqrs = Cqrs::new(PostgresEventStore::new(pool.clone()));
        let policy_name = unique_policy_name(label);
        let configure: Arc<Configure> = Arc::new(configure);
        let escalations = Escalations::default();

        let daemon = spawn_daemon(&cqrs, configure.as_ref(), &policy_name, &escalations);

        Self {
            _container: container,
            pool,
            cqrs,
            policy_name,
            configure,
            escalations,
            daemon: Some(daemon),
        }
    }

    /// Stop the daemon and start an identically configured one against the same
    /// database — the process restart an operator would perform, minus the
    /// process.
    ///
    /// Everything in memory (cursor position, in-flight work, what the escalation
    /// hook was told) is discarded; only what the first daemon made durable
    /// survives. That is what makes this the way to ask whether an event is
    /// re-delivered after a restart.
    pub async fn restart(&mut self) {
        if let Some(daemon) = self.daemon.take() {
            daemon.shutdown().await;
        }
        // After the shutdown has joined every task, so nothing can record into
        // the cleared recorder afterwards.
        self.escalations.cleared();
        self.daemon = Some(spawn_daemon(
            &self.cqrs,
            self.configure.as_ref(),
            &self.policy_name,
            &self.escalations,
        ));
    }

    /// Retry every dead letter this policy has parked, oldest-first — the
    /// operator's bulk recovery.
    ///
    /// Runs on a runner built exactly like the daemon's but never started: a
    /// retry takes no advisory lock and never touches the cursor, so it is the
    /// out-of-band call an operator makes against a system that is still
    /// running, which is how it is made here.
    pub async fn retry_parked(&self) -> DeadLetterRetrySummary {
        let runner = (self.configure)(
            PolicyRunner::builder(self.cqrs.clone()).register_services::<Probe>(()),
            &self.policy_name,
        )
        .build();

        runner
            .retry_policy_dead_letters(&self.policy_name)
            .await
            .expect("a bulk retry must return a summary rather than fail")
    }

    /// The name the policy under test was registered under.
    pub fn policy_name(&self) -> &str {
        &self.policy_name
    }

    /// Append a `Pinged` event for the policy to react to.
    ///
    /// The daemon is already running, so by the time the append returns the
    /// policy may have reacted — possibly into this very stream. The event is
    /// therefore identified by a marker minted here and stamped in its metadata,
    /// not by "the newest row on the stream", which a reaction can win.
    pub async fn ping(&self, stream: &str, tag: &str) -> AppendedEvent {
        let id = ProbeUrn::new(stream).expect("stream name must be a valid URN NSS");
        let marker = Uuid::new_v4();

        self.cqrs
            .execute::<Probe>(
                &id,
                replay::Metadata::new(serde_json::json!({ PING_MARKER_KEY: marker })),
                ProbeCommand::Ping {
                    tag: tag.to_string(),
                },
                &(),
                None,
            )
            .await
            .expect("append must succeed");

        let stream_id = id.to_urn().to_string();
        let row =
            sqlx::query("SELECT id, global_position FROM events WHERE metadata->>($1::text) = $2")
                .bind(PING_MARKER_KEY)
                .bind(marker.to_string())
                .fetch_one(&self.pool)
                .await
                .expect("the appended event must be readable");

        AppendedEvent {
            event_id: row.get("id"),
            global_position: row.get("global_position"),
            stream_id,
        }
    }

    /// Every command the policy has dispatched so far, in the order the events
    /// they wrote landed.
    pub async fn dispatches(&self) -> Vec<DispatchedCommand> {
        self.dispatches_for(&self.policy_name).await
    }

    /// The same, for any policy registered through this harness — a test that
    /// registers a second one (to prove one policy's trouble leaves the other
    /// alone) observes it here.
    pub async fn dispatches_for(&self, policy: &str) -> Vec<DispatchedCommand> {
        let rows = sqlx::query(
            "SELECT id, global_position, stream_id, type, \
                    (metadata->'causation'->>'global_position')::bigint AS caused_by_position, \
                    (metadata->'causation'->>'event_id')::uuid            AS caused_by_event_id \
             FROM events \
             WHERE metadata->'causation'->>'policy' = $1 \
             ORDER BY global_position ASC LIMIT $2",
        )
        .bind(policy)
        .bind(OBSERVATION_LIMIT)
        .fetch_all(&self.pool)
        .await
        .expect("dispatch observation must be readable");

        rows.into_iter()
            .map(|row| DispatchedCommand {
                global_position: row.get("global_position"),
                stream_id: row.get("stream_id"),
                event_type: row.get("type"),
                caused_by_position: row.get("caused_by_position"),
                caused_by_event_id: row.get("caused_by_event_id"),
            })
            .collect()
    }

    /// The policy's persisted cursor, or `None` before it has one.
    pub async fn cursor(&self) -> Option<i64> {
        self.cursor_for(&self.policy_name).await
    }

    /// The same, for any policy registered through this harness.
    pub async fn cursor_for(&self, policy: &str) -> Option<i64> {
        sqlx::query_scalar::<_, i64>("SELECT position FROM policy_cursors WHERE name = $1")
            .bind(policy)
            .fetch_optional(&self.pool)
            .await
            .expect("cursor observation must be readable")
    }

    /// The workers the runner has given up on — what an operator sees when a
    /// policy has stopped for good rather than merely paused.
    pub fn stopped_workers(&self) -> Vec<StoppedWorker> {
        self.daemon
            .as_ref()
            .expect("the daemon is only taken by shutdown")
            .stopped_workers()
    }

    /// The policy's dead letters — the reactions it parked — oldest first.
    pub async fn dead_letters(&self) -> Vec<DeadLetter> {
        let rows = sqlx::query(
            "SELECT global_position, event_id, error_kind, error_message \
             FROM policy_dead_letters WHERE policy_name = $1 \
             ORDER BY id ASC LIMIT $2",
        )
        .bind(&self.policy_name)
        .bind(OBSERVATION_LIMIT)
        .fetch_all(&self.pool)
        .await
        .expect("parked observation must be readable");

        rows.into_iter()
            .map(|row| DeadLetter {
                global_position: row.get("global_position"),
                event_id: row.get("event_id"),
                error_kind: row.get("error_kind"),
                error_message: row.get("error_message"),
            })
            .collect()
    }

    /// Wait until the policy has dispatched a command caused by the event at
    /// `position`, and return it.
    pub async fn await_dispatch_caused_by(&self, position: i64) -> DispatchedCommand {
        self.await_dispatch_caused_by_for(&self.policy_name, position)
            .await
    }

    /// The same, for any policy registered through this harness.
    pub async fn await_dispatch_caused_by_for(
        &self,
        policy: &str,
        position: i64,
    ) -> DispatchedCommand {
        self.observe(
            &format!("a dispatch by {policy} caused by the event at position {position}"),
            || async {
                self.dispatches_for(policy)
                    .await
                    .into_iter()
                    .find(|d| d.caused_by_position == position)
            },
        )
        .await
    }

    /// Wait until the runner has given up on `policy`'s worker, and return what
    /// it recorded about it.
    pub async fn await_stopped_worker(&self, policy: &str) -> StoppedWorker {
        self.observe(&format!("{policy}'s worker to stop for good"), || async {
            self.stopped_workers()
                .into_iter()
                .find(|stopped| stopped.policy == policy)
        })
        .await
    }

    /// Everything the escalation hook has been told, in the order it was told.
    pub fn escalations(&self) -> Vec<Escalation> {
        self.escalations.recorded()
    }

    /// Wait until the runner has escalated `policy`, and return what it handed
    /// the hook.
    pub async fn await_escalation(&self, policy: &str) -> Escalation {
        self.observe(&format!("{policy} to be escalated"), || async {
            self.escalations()
                .into_iter()
                .find(|escalated| escalated.policy == policy)
        })
        .await
    }

    /// Wait until the policy's persisted cursor has reached `position`, and
    /// return where it actually sits.
    pub async fn await_cursor_at_least(&self, position: i64) -> i64 {
        self.observe(
            &format!("the cursor to reach position {position}"),
            || async { self.cursor().await.filter(|stored| *stored >= position) },
        )
        .await
    }

    /// Wait until the policy has parked at least `count` reactions, and return
    /// every dead letter it has.
    pub async fn await_dead_letters(&self, count: usize) -> Vec<DeadLetter> {
        self.observe(&format!("{count} parked reaction(s)"), || async {
            let parked = self.dead_letters().await;
            (parked.len() >= count).then_some(parked)
        })
        .await
    }

    /// Re-read `observation` until it holds, or fail with everything this
    /// harness can see about the policy.
    ///
    /// The deadline bounds the *observation*, not just the gaps between reads:
    /// each attempt runs under whatever time is left, so a query that never
    /// comes back fails as a timeout here rather than hanging the test.
    ///
    /// Generic on purpose: a test with a question this harness does not answer
    /// directly can still ask it without inventing its own timeout, its own
    /// recheck interval, and its own diagnosis on failure.
    /// [`common::wait::wait_for_count`](super::wait::wait_for_count) is the same
    /// idea for a bare count; this one yields the observation itself and reports
    /// the whole set when it never arrives.
    pub async fn observe<T, F, Fut>(&self, what: &str, mut observation: F) -> T
    where
        F: FnMut() -> Fut,
        Fut: Future<Output = Option<T>>,
    {
        let deadline = Instant::now() + OBSERVE_TIMEOUT;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if let Ok(Some(observed)) = tokio::time::timeout(remaining, observation()).await {
                return observed;
            }
            if Instant::now() >= deadline {
                // Past the deadline the diagnosis is what the test is for, so it
                // gets its own budget rather than the exhausted one.
                let diagnosis = tokio::time::timeout(OBSERVE_TIMEOUT, async {
                    format!(
                        "  policy:     {}\n  cursor:     {:?}\n  dispatched: {:#?}\n  parked:     {:#?}",
                        self.policy_name,
                        self.cursor().await,
                        self.dispatches().await,
                        self.dead_letters().await,
                    )
                })
                .await
                .unwrap_or_else(|_| "  (the database stopped answering too)".to_string());

                panic!("timed out after {OBSERVE_TIMEOUT:?} waiting for {what}\n{diagnosis}");
            }
            tokio::time::sleep(OBSERVE_RECHECK).await;
        }
    }

    /// Stop the daemon and await every task it spawned.
    ///
    /// The runner's shutdown joins each worker, the shared listener and the lock
    /// manager, so on the path a passing test takes nothing it started is still
    /// running when this returns, and the advisory locks are released rather
    /// than left to a session timeout.
    ///
    /// A test that panics before reaching this line never gets that ordering —
    /// `Drop` cannot await. It cannot leak into another test either: the tasks
    /// belong to that test's own `#[tokio::test]` runtime, which is dropped (and
    /// aborts them) as the test unwinds, and they hold locks in a database no
    /// other test can see, which the container takes with it.
    pub async fn shutdown(mut self) {
        if let Some(daemon) = self.daemon.take() {
            daemon.shutdown().await;
        }
    }
}

/// A policy name no other harness in this process can be using.
fn unique_policy_name(label: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!("harness_{label}_{}", NEXT.fetch_add(1, Ordering::Relaxed))
}

/// Build the configured runner and start it polling.
///
/// The daemon's tasks own everything they need, so the runner itself is free to
/// drop here: a test can only reach the daemon through the harness, which is the
/// point.
///
/// The recording escalation hook is installed *before* `configure` runs, so a
/// test that supplies its own replaces it, and one that does not never has a
/// stray escalation exit the test runner.
fn spawn_daemon(
    cqrs: &Cqrs<PostgresEventStore>,
    configure: &Configure,
    policy_name: &str,
    escalations: &Escalations,
) -> PolicyRunnerDaemon {
    configure(
        PolicyRunner::builder(cqrs.clone())
            .register_services::<Probe>(())
            .on_escalation(escalations.hook()),
        policy_name,
    )
    .build()
    .start_polling(DAEMON_POLL_INTERVAL)
}
