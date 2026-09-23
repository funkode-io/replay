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
//! - [`PolicyDaemonHarness::status`] — the policy as the status read model
//!   reports it, which is what a consumer's health endpoint shows.
//! - [`PolicyDaemonHarness::archived_dead_letters`] — the parked reactions that
//!   have left the active set, and what settled them.
//! - [`PolicyDaemonHarness::stopped_workers`] — the workers the runner gave up
//!   on, which is what an operator reads off the daemon in their own service.
//! - [`PolicyDaemonHarness::liveness`] — what each worker in this process is
//!   doing, read off the daemon the same way.
//! - [`PolicyDaemonHarness::heartbeat`] — the row the Leader's beat writes,
//!   which is what a consumer reads from outside the process.
//! - [`PolicyDaemonHarness::escalations`] — what the consumer's escalation hook
//!   was told. The harness installs a recording hook in place of the default,
//!   which exits the process: in a test that is the test runner.
//!
//! And two things an operator can *do*:
//!
//! - [`PolicyDaemonHarness::restart`] — stop the daemon and start an identical
//!   one against the same database, so a test can ask what survived in memory
//!   (nothing) and what survived in the tables (everything that matters).
//! - [`PolicyDaemonHarness::start_replica`] — start a *second* runner against
//!   the same database, which is how a test reaches a Standby: whichever runner
//!   loses the advisory lock leads nothing and must say so.
//! - [`PolicyDaemonHarness::drop_heartbeat_column`] — take the heartbeat columns
//!   away, standing in for a consumer whose schema predates them.
//! - [`PolicyDaemonHarness::retry_parked`] — the bulk retry of everything the
//!   policy parked, run out of band while the daemon keeps polling.
//! - [`PolicyDaemonHarness::retry_parked_row`] /
//!   [`PolicyDaemonHarness::discard_parked_row`] — the same controls on one row,
//!   by the id an operator reads off the table.
//! - [`PolicyDaemonHarness::park_without_identity`] /
//!   [`PolicyDaemonHarness::park_panic_without_identity`] — a row as a release
//!   before the identity migration parked it, which running code can no longer
//!   write.
//! - [`PolicyDaemonHarness::redeliver`] — the event delivered to the policy
//!   again, by the cursor rewind that causes it in production.
//! - [`PolicyDaemonHarness::park_again`] — a second row for one parked command,
//!   as a release before the table had a key for one left behind.
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

use chrono::{DateTime, Utc};
use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use testcontainers_modules::postgres;
use testcontainers_modules::testcontainers::{runners::AsyncRunner, ContainerAsync};
use uuid::Uuid;

use super::postgres_image::{postgres_container, POSTGRES_PORT};

use replay_macros::define_aggregate;
use replay_persistence::{
    Cqrs, DeadLetterDiscard, DeadLetterRetry, DeadLetterRetrySummary, Escalation, Liveness,
    PolicyRunner, PolicyRunnerBuilder, PolicyRunnerDaemon, PolicyStatus, PolicyStatusStore,
    PostgresEventStore, StoppedWorker, WorkerLiveness,
};

/// How often the daemon under test polls the feed. Short: these tests wait on
/// outcomes, so the interval only bounds how long an idle poll loop dawdles.
const DAEMON_POLL_INTERVAL: Duration = Duration::from_millis(25);

/// How often the daemon under test writes the durable heartbeat: the floor the
/// crate allows, for the same reason the poll interval is short — these tests
/// wait on beats arriving, not on production's cadence.
pub const DAEMON_HEARTBEAT_CADENCE: Duration = replay_persistence::HEARTBEAT_MIN_CADENCE;

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
            Flake { reason: String },
            Explode { reason: String },
            Sleep { millis: u64 },
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
            // A command that always fails *retryably*, so a test can drive the
            // runner's back-off loop on purpose — the sibling that forces a
            // second attempt on a reaction whose other command is doomed.
            ProbeCommand::Flake { reason } => Err(replay::Error::unavailable(format!(
                "probe flakes: {reason}"
            ))
            .with_operation("Flake")),
            // A command that panics *inside the handler*, so a test can drive a
            // panic into the asynchronous half of the runner's per-event
            // boundary — the one a panic in `react` never reaches, because it
            // happens while awaiting the dispatch rather than before it.
            ProbeCommand::Explode { reason } => panic!("probe exploded: {reason}"),
            // A command that does not come back for `millis`: either side of a
            // dispatch timeout, depending on what a test passes. Sleeping rather
            // than blocking, because what the runner abandons is a future it is
            // awaiting.
            ProbeCommand::Sleep { millis } => {
                tokio::time::sleep(Duration::from_millis(millis)).await;
                Ok(vec![ProbeEvent::Echoed {
                    tag: format!("slept-{millis}ms"),
                }])
            }
        }
    }
}

// ── Observations ─────────────────────────────────────────────────────────────

/// An event appended by a test, identified the way the runner identifies it.
#[derive(Debug, Clone)]
pub struct AppendedEvent {
    pub event_id: Uuid,
    pub global_position: i64,
    /// The transaction that wrote it, as a Policy's cursor records it
    /// (funkode-io/replay#194). Text, because `xid8` is an unsigned 64-bit counter sqlx
    /// has no codec for.
    pub commit_txid: String,
    pub stream_id: String,
}

/// A Policy's persisted cursor: the transaction it stopped in and the position it
/// stopped at.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredCursor {
    pub commit_txid: String,
    pub position: i64,
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
    /// Row id — what an operator passes to a retry or a discard.
    pub id: i64,
    pub global_position: i64,
    pub event_id: Uuid,
    pub error_kind: String,
    pub error_message: String,
    /// Rust type name of the aggregate the failing command targeted. `None` for
    /// a row with no dispatch to name: parked before the identity migration, or
    /// parked for a panic in `react` itself.
    pub aggregate_name: Option<String>,
    /// URN of the aggregate instance the failing command was addressed to.
    pub target_stream_id: Option<String>,
    /// Rust type name of the failing command.
    pub command_name: Option<String>,
    /// The failing dispatch's index in the vector the reaction returned. `None`
    /// on the same rows the other identity columns are null on; **negative** on
    /// a row parked before the column existed, which names a command but not its
    /// place, and was numbered apart by the dedupe migration.
    pub dispatch_ordinal: Option<i32>,
    /// Deliveries of the triggering event that parked this command. 1 until the
    /// event is delivered again.
    pub deliveries: i32,
    /// When the command first failed — unmoved by a redelivery.
    pub created_at: DateTime<Utc>,
    /// When the last delivery parked it.
    pub last_parked_at: DateTime<Utc>,
    /// Settlements a retry has made on this row — what has already been tried.
    pub retry_count: i32,
    /// When the last of them was made. `None` until the row is first retried.
    pub last_retried_at: Option<DateTime<Utc>>,
}

/// A dead letter that has left the active set, as an operator reads it out of
/// `discarded_dead_letters`.
#[derive(Debug, Clone)]
pub struct ArchivedDeadLetter {
    /// Id the row had in `policy_dead_letters`.
    pub dead_letter_id: i64,
    /// Why it left: `retried`, `discarded` or `superseded` (a duplicate
    /// generation the dedupe migration retired).
    pub reason: String,
    pub aggregate_name: Option<String>,
    pub target_stream_id: Option<String>,
    pub command_name: Option<String>,
    pub dispatch_ordinal: Option<i32>,
    /// Deliveries that parked the command while the row was active.
    pub deliveries: i32,
    /// Retries made on the row, the settlement that archived it included.
    pub retry_count: i32,
    /// When the last of them was made. `None` for a row no retry ever settled.
    pub last_retried_at: Option<DateTime<Utc>>,
}

/// The durable liveness reading: one beat, as a consumer outside the process
/// sees it.
#[derive(Debug, Clone)]
pub struct Heartbeat {
    /// When the Leader's replica last beat. Stale means no live Leader.
    pub beat_at: DateTime<Utc>,
    /// What that replica's supervisor knows about the worker.
    pub liveness: String,
    /// When the worker last finished a poll. Old against a fresh beat means the
    /// worker is alive and not finishing polls.
    pub last_polled_at: Option<DateTime<Utc>>,
    /// Which replica wrote the beat.
    pub led_by: Option<String>,
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

        let daemon = spawn_daemon(
            &cqrs,
            configure.as_ref(),
            &policy_name,
            &escalations,
            PRIMARY_REPLICA,
        );

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
            PRIMARY_REPLICA,
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
        self.out_of_band_runner()
            .retry_policy_dead_letters(&self.policy_name)
            .await
            .expect("a bulk retry must return a summary rather than fail")
    }

    /// Retry one parked row, by the id an operator reads off the table.
    pub async fn retry_parked_row(&self, id: i64) -> DeadLetterRetry {
        self.out_of_band_runner()
            .retry_dead_letter(id)
            .await
            .expect("a retry must return an outcome rather than fail")
    }

    /// Discard one parked row, by the id an operator reads off the table.
    pub async fn discard_parked_row(&self, id: i64) -> DeadLetterDiscard {
        self.out_of_band_runner()
            .discard_dead_letter(id)
            .await
            .expect("a discard must return an outcome rather than fail")
    }

    /// A runner configured like the daemon's but never started, for the controls
    /// an operator invokes out of band.
    fn out_of_band_runner(&self) -> PolicyRunner {
        (self.configure)(
            PolicyRunner::builder(self.cqrs.clone()).register_services::<Probe>(()),
            &self.policy_name,
        )
        .build()
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
        let row = sqlx::query(
            "SELECT id, global_position, commit_txid::text AS commit_txid \
             FROM events WHERE metadata->>($1::text) = $2",
        )
        .bind(PING_MARKER_KEY)
        .bind(marker.to_string())
        .fetch_one(&self.pool)
        .await
        .expect("the appended event must be readable");

        AppendedEvent {
            event_id: row.get("id"),
            global_position: row.get("global_position"),
            commit_txid: row.get("commit_txid"),
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

    /// The policy's persisted cursor as the row holds it — both halves.
    pub async fn stored_cursor(&self) -> Option<StoredCursor> {
        sqlx::query(
            "SELECT position, commit_txid::text AS commit_txid \
             FROM policy_cursors WHERE name = $1",
        )
        .bind(&self.policy_name)
        .fetch_optional(&self.pool)
        .await
        .expect("cursor observation must be readable")
        .map(|row| StoredCursor {
            commit_txid: row.get("commit_txid"),
            position: row.get("position"),
        })
    }

    /// The operator's move from ADR-0012, as they make it: a position, written straight
    /// into the row against a running deployment, with no second column to remember.
    pub async fn move_cursor_to(&self, position: i64) {
        sqlx::query("UPDATE policy_cursors SET position = $2, updated_at = now() WHERE name = $1")
            .bind(&self.policy_name)
            .bind(position)
            .execute(&self.pool)
            .await
            .expect("the operator's move must succeed");
    }

    /// Deliver `event` to the policy again, by the move that causes a
    /// redelivery in production: the cursor rewound to just before it.
    ///
    /// The honest way to reach the window a park sits in. A dead letter is
    /// written before the batched cursor checkpoint, so a hard kill in between
    /// redelivers every event since the last checkpoint; an operator rewinding
    /// the cursor (ADR-0012) does the same deliberately, and is the half of it a
    /// test can perform.
    ///
    /// Waits for the cursor to reach `event` before moving it, because that same
    /// window is what a test races otherwise: a rewind written while the worker
    /// still has the event in flight is erased by the checkpoint that follows
    /// it — the compare-and-set sees the position it expects and advances — and
    /// the event is never delivered again.
    pub async fn redeliver(&self, event: &AppendedEvent) {
        self.await_cursor_at_least(event.global_position).await;
        self.move_cursor_to(event.global_position - 1).await;
    }

    /// The policy's status as a consumer reads it off [`PolicyStatusStore`] —
    /// the read model, not the tables it derives from.
    ///
    /// `None` before the policy has a cursor row, which is how the store reports
    /// a policy that has never run.
    pub async fn status(&self) -> Option<PolicyStatus> {
        PolicyStatusStore::new(self.pool.clone())
            .list()
            .await
            .expect("the status read model must be readable")
            .into_iter()
            .find(|status| status.name == self.policy_name)
    }

    /// The workers the runner has given up on — what an operator sees when a
    /// policy has stopped for good rather than merely paused.
    pub fn stopped_workers(&self) -> Vec<StoppedWorker> {
        self.daemon
            .as_ref()
            .expect("the daemon is only taken by shutdown")
            .stopped_workers()
    }

    /// What every worker in this daemon is doing, and when each last polled.
    pub fn liveness(&self) -> Vec<WorkerLiveness> {
        self.daemon
            .as_ref()
            .expect("the daemon is only taken by shutdown")
            .liveness()
    }

    /// Wait until this daemon reports `policy` as `expected`, and return the
    /// whole reading — the last poll included.
    pub async fn await_liveness(&self, policy: &str, expected: Liveness) -> WorkerLiveness {
        self.observe(&format!("{policy} to report {expected}"), || async {
            self.liveness()
                .into_iter()
                .find(|worker| worker.policy == policy && worker.liveness == expected)
        })
        .await
    }

    /// The durable stamp on the policy's cursor row: when its Leader last
    /// polled, as any replica can read it.
    pub async fn last_polled_at(&self) -> Option<DateTime<Utc>> {
        sqlx::query_scalar::<_, Option<DateTime<Utc>>>(
            "SELECT last_polled_at FROM policy_cursors WHERE name = $1",
        )
        .bind(&self.policy_name)
        .fetch_optional(&self.pool)
        .await
        .expect("the heartbeat observation must be readable")
        .flatten()
    }

    /// The heartbeat the Leader's replica writes for this policy — everything a
    /// consumer outside the process can see about it.
    pub async fn heartbeat(&self) -> Option<Heartbeat> {
        self.heartbeat_for(&self.policy_name).await
    }

    /// The same, for any policy registered through this harness.
    pub async fn heartbeat_for(&self, policy: &str) -> Option<Heartbeat> {
        let row = sqlx::query(
            "SELECT last_beat_at, liveness, last_polled_at, led_by \
             FROM policy_cursors WHERE name = $1",
        )
        .bind(policy)
        .fetch_optional(&self.pool)
        .await
        .expect("the heartbeat observation must be readable")?;

        let beat_at: Option<DateTime<Utc>> = row.get("last_beat_at");
        Some(Heartbeat {
            beat_at: beat_at?,
            liveness: row.get("liveness"),
            last_polled_at: row.get("last_polled_at"),
            led_by: row.get("led_by"),
        })
    }

    /// Wait until a beat arrives that satisfies `holds`, and return it.
    pub async fn await_heartbeat<F>(&self, what: &str, holds: F) -> Heartbeat
    where
        F: Fn(&Heartbeat) -> bool,
    {
        self.observe(what, || async { self.heartbeat().await.filter(&holds) })
            .await
    }

    /// Hold the row lock on `policy`'s cursor row until the returned guard is
    /// released — an operator part-way through a [Cursor move], sitting at a psql
    /// prompt they have not typed `COMMIT` into.
    ///
    /// The row is locked, not changed: what a test asks with it is what the lock
    /// alone costs the policies next door.
    ///
    /// [Cursor move]: ../../CONTEXT.md#cursor-move
    pub async fn hold_cursor_row(&self, policy: &str) -> HeldCursorRow {
        let mut tx = self
            .pool
            .begin()
            .await
            .expect("holding the cursor row must be possible");
        sqlx::query("SELECT name FROM policy_cursors WHERE name = $1 FOR UPDATE")
            .bind(policy)
            .fetch_one(&mut *tx)
            .await
            .expect("the cursor row must exist before it can be held");
        HeldCursorRow(tx)
    }

    /// Take the heartbeat columns away: the schema of a consumer who has not
    /// added them, which the runner must tolerate.
    ///
    /// They are the consumer's, not the crate's, so this is a schema a deployment
    /// can genuinely be in rather than a fault injected for the test. Call
    /// [`restart`](Self::restart) afterwards to run a daemon that never saw them.
    pub async fn drop_heartbeat_column(&self) {
        sqlx::query(
            "ALTER TABLE policy_cursors \
               DROP COLUMN last_beat_at, \
               DROP COLUMN liveness, \
               DROP COLUMN last_polled_at, \
               DROP COLUMN led_by",
        )
        .execute(&self.pool)
        .await
        .expect("dropping the heartbeat columns must succeed");
    }

    /// Start a second runner against this harness's database, registering the
    /// same policy under the same name — the other replica.
    ///
    /// Both compete for the one advisory lock that elects the policy's Leader,
    /// and the runner already polling holds it, so the replica stands by. It is
    /// a separate daemon with its own liveness, which is the point: a Standby
    /// must report itself from its own process.
    pub fn start_replica(&self) -> PolicyDaemonReplica {
        let escalations = Escalations::default();
        let daemon = spawn_daemon(
            &self.cqrs,
            self.configure.as_ref(),
            &self.policy_name,
            &escalations,
            SECOND_REPLICA,
        );
        PolicyDaemonReplica {
            escalations,
            daemon: Some(daemon),
        }
    }

    /// Park `count` rows for commands this Policy's registered code cannot
    /// dispatch, all for one reaction — the tail an upgrade inherits
    /// (funkode-io/replay#228).
    ///
    /// Written straight to the table because that is what they are: rows an
    /// older version of the reaction parked, which no `react` running here can
    /// reproduce. `target_bytes` pads the instance URN each row names, so a test
    /// can measure what holding the whole group at once would cost.
    pub async fn park_retired_commands(
        &self,
        event: &AppendedEvent,
        count: usize,
        target_bytes: usize,
    ) {
        sqlx::query(
            "INSERT INTO policy_dead_letters \
             (policy_name, global_position, event_id, error_kind, error_message, \
              aggregate_name, target_stream_id, command_name, dispatch_ordinal, \
              last_parked_at) \
             SELECT $1, $2, $3, 'Unavailable', 'parked by a version that is gone', \
                    'Probe', 'urn:probe:' || lpad(n::text, $5, 'x'), \
                    'RetiredCommand', n, now() \
             FROM generate_series(1, $4) AS n",
        )
        .bind(&self.policy_name)
        .bind(event.global_position)
        .bind(event.event_id)
        .bind(count as i32)
        .bind(target_bytes as i32)
        .execute(&self.pool)
        .await
        .expect("the retired commands must be parked");
    }

    /// How many rows this policy has parked, counted in the database rather than
    /// read into memory — what [`PolicyStatus::dead_letter_count`] reports.
    pub async fn parked_row_count(&self) -> i64 {
        sqlx::query_scalar("SELECT count(*) FROM policy_dead_letters WHERE policy_name = $1")
            .bind(&self.policy_name)
            .fetch_one(&self.pool)
            .await
            .expect("the parked rows must be countable")
    }

    /// How many of this policy's rows have been archived under `reason`.
    pub async fn archived_row_count(&self, reason: &str) -> i64 {
        sqlx::query_scalar(
            "SELECT count(*) FROM discarded_dead_letters \
             WHERE policy_name = $1 AND reason = $2",
        )
        .bind(&self.policy_name)
        .bind(reason)
        .fetch_one(&self.pool)
        .await
        .expect("the archived rows must be countable")
    }

    /// The policy's dead letters — the reactions it parked — oldest first.
    pub async fn dead_letters(&self) -> Vec<DeadLetter> {
        let rows = sqlx::query(
            "SELECT id, global_position, event_id, error_kind, error_message, \
                    aggregate_name, target_stream_id, command_name, dispatch_ordinal, \
                    deliveries, created_at, last_parked_at, retry_count, last_retried_at \
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
                id: row.get("id"),
                global_position: row.get("global_position"),
                event_id: row.get("event_id"),
                error_kind: row.get("error_kind"),
                error_message: row.get("error_message"),
                aggregate_name: row.get("aggregate_name"),
                target_stream_id: row.get("target_stream_id"),
                command_name: row.get("command_name"),
                dispatch_ordinal: row.get("dispatch_ordinal"),
                deliveries: row.get("deliveries"),
                created_at: row.get("created_at"),
                last_parked_at: row.get("last_parked_at"),
                retry_count: row.get("retry_count"),
                last_retried_at: row.get("last_retried_at"),
            })
            .collect()
    }

    /// Park a row the way a release before the identity migration did: the old
    /// columns only, the new ones left null.
    ///
    /// The only honest way to produce the backlog an upgrade inherits — the
    /// running code cannot write such a row any more.
    pub async fn park_without_identity(&self, event: &AppendedEvent, message: &str) -> i64 {
        sqlx::query_scalar(
            "INSERT INTO policy_dead_letters \
                 (policy_name, global_position, event_id, error_kind, error_message) \
             VALUES ($1, $2, $3, 'Invalid Input', $4) RETURNING id",
        )
        .bind(&self.policy_name)
        .bind(event.global_position)
        .bind(event.event_id)
        .bind(message)
        .fetch_one(&self.pool)
        .await
        .expect("a pre-migration row must still be insertable")
    }

    /// A row as an older release parked it for a reaction that **panicked**:
    /// no dispatch to name, and the kind that says the unwind settled the
    /// delivery.
    ///
    /// The shape an upgrade really inherits with a null ordinal. The dedupe
    /// migration numbers every other identity-less row apart, because only a
    /// panic parks exactly one row per delivery; a panic's row keeps the null
    /// ordinal the running code still writes, so it is the one an old release's
    /// row and a new delivery can share (funkode-io/replay#220).
    pub async fn park_panic_without_identity(&self, event: &AppendedEvent, message: &str) -> i64 {
        sqlx::query_scalar(
            "INSERT INTO policy_dead_letters \
                 (policy_name, global_position, event_id, error_kind, error_message) \
             VALUES ($1, $2, $3, 'Panic', $4) RETURNING id",
        )
        .bind(&self.policy_name)
        .bind(event.global_position)
        .bind(event.event_id)
        .bind(message)
        .fetch_one(&self.pool)
        .await
        .expect("a pre-migration row must still be insertable")
    }

    /// The same, but numbered **below** every row already parked — the real
    /// shape of an upgrade, where the identity-less row was parked first and the
    /// rows a later delivery parked come after it.
    pub async fn park_without_identity_first(&self, event: &AppendedEvent, message: &str) -> i64 {
        sqlx::query_scalar(
            "INSERT INTO policy_dead_letters \
                 (id, policy_name, global_position, event_id, error_kind, error_message) \
             VALUES ((SELECT COALESCE(MIN(id), 1) - 1 FROM policy_dead_letters), \
                     $1, $2, $3, 'Invalid Input', $4) RETURNING id",
        )
        .bind(&self.policy_name)
        .bind(event.global_position)
        .bind(event.event_id)
        .bind(message)
        .fetch_one(&self.pool)
        .await
        .expect("a pre-migration row must still be insertable")
    }

    /// Park a second copy of `id`, the way a redelivery did before the table
    /// had a key for a parked command (funkode-io/replay#220).
    ///
    /// The copy carries a **null** `dispatch_ordinal`, which is what makes it
    /// insertable at all now that `idx_dead_letters_parked_command` exists: it
    /// is a row of the shape a release before that migration wrote, next to the
    /// row running code writes for the same command. That pair is what an
    /// upgrade's backlog can still hold — the dedupe collapsed the duplicates
    /// that existed when it ran, not the ones a pre-ordinal row and a later
    /// delivery make afterwards — and a retry must still settle both.
    pub async fn park_again(&self, id: i64) -> i64 {
        sqlx::query_scalar(
            "INSERT INTO policy_dead_letters \
                 (policy_name, global_position, event_id, error_kind, error_message, \
                  aggregate_name, target_stream_id, command_name) \
             SELECT policy_name, global_position, event_id, error_kind, error_message, \
                    aggregate_name, target_stream_id, command_name \
             FROM policy_dead_letters WHERE id = $1 RETURNING id",
        )
        .bind(id)
        .fetch_one(&self.pool)
        .await
        .expect("a redelivery's duplicate must be insertable")
    }

    /// The policy's archived dead letters — what left the active set, and why.
    pub async fn archived_dead_letters(&self) -> Vec<ArchivedDeadLetter> {
        let rows = sqlx::query(
            "SELECT dead_letter_id, reason, aggregate_name, target_stream_id, command_name, \
                    dispatch_ordinal, deliveries, retry_count, last_retried_at \
             FROM discarded_dead_letters WHERE policy_name = $1 \
             ORDER BY id ASC LIMIT $2",
        )
        .bind(&self.policy_name)
        .bind(OBSERVATION_LIMIT)
        .fetch_all(&self.pool)
        .await
        .expect("archive observation must be readable");

        rows.into_iter()
            .map(|row| ArchivedDeadLetter {
                dead_letter_id: row.get("dead_letter_id"),
                reason: row.get("reason"),
                aggregate_name: row.get("aggregate_name"),
                target_stream_id: row.get("target_stream_id"),
                command_name: row.get("command_name"),
                dispatch_ordinal: row.get("dispatch_ordinal"),
                deliveries: row.get("deliveries"),
                retry_count: row.get("retry_count"),
                last_retried_at: row.get("last_retried_at"),
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

    /// End the test without awaiting the daemon's tasks.
    ///
    /// The only way to finish a test whose worker is inside a reaction that
    /// never returns: [`shutdown`](Self::shutdown) joins every task, and a
    /// wedged worker joins when its reaction does — which is exactly what such a
    /// test is proving does not happen. A real process ends the same way, by
    /// exiting with the worker still in there, and the same isolation applies:
    /// the tasks belong to this test's runtime and die with it, against a
    /// database no other test can see.
    pub fn abandon(self) {}
}

/// The second runner in a test: another process's daemon, against the same
/// database and the same policy name.
///
/// It exposes what a replica can be asked about itself — its own workers'
/// liveness — and nothing else. Everything in the database is already readable
/// through the harness that owns it, and reading it twice would only invite a
/// test to assert the same row from two places.
pub struct PolicyDaemonReplica {
    escalations: Escalations,
    daemon: Option<PolicyRunnerDaemon>,
}

impl PolicyDaemonReplica {
    /// What every worker in this replica is doing.
    pub fn liveness(&self) -> Vec<WorkerLiveness> {
        self.daemon
            .as_ref()
            .expect("the daemon is only taken by shutdown")
            .liveness()
    }

    /// What this replica's escalation hook was told — a Standby that escalates
    /// is the false alarm the liveness axis exists to prevent, so a test says so
    /// rather than only checking the state.
    pub fn escalations(&self) -> Vec<Escalation> {
        self.escalations.recorded()
    }

    /// Stop this replica and await every task it spawned, releasing any advisory
    /// lock it managed to take.
    pub async fn shutdown(mut self) {
        if let Some(daemon) = self.daemon.take() {
            daemon.shutdown().await;
        }
    }
}

/// Somebody else's open transaction on a cursor row. Dropping it rolls back, so
/// a test that panics releases the row with its runtime.
pub struct HeldCursorRow(sqlx::Transaction<'static, sqlx::Postgres>);

impl HeldCursorRow {
    /// Let the row go, without having changed it.
    pub async fn release(self) {
        let _ = self.0.rollback().await;
    }
}

/// A policy name no other harness in this process can be using.
fn unique_policy_name(label: &str) -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!("harness_{label}_{}", NEXT.fetch_add(1, Ordering::Relaxed))
}

/// What the harness's own daemon writes in `led_by`.
pub const PRIMARY_REPLICA: &str = "harness-primary";

/// What a [`PolicyDaemonHarness::start_replica`] daemon writes in `led_by`, so a
/// test can tell which of the two wrote a beat.
pub const SECOND_REPLICA: &str = "harness-replica";

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
    replica_id: &str,
) -> PolicyRunnerDaemon {
    configure(
        PolicyRunner::builder(cqrs.clone())
            .register_services::<Probe>(())
            .with_heartbeat(DAEMON_HEARTBEAT_CADENCE)
            .replica_id(replica_id)
            .on_escalation(escalations.hook()),
        policy_name,
    )
    .build()
    .start_polling(DAEMON_POLL_INTERVAL)
}
