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
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sqlx::{Pool, Postgres, QueryBuilder, Row};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use replay::{Aggregate, Metadata};

use crate::policy::{Dispatch, ErasedPolicy, Policy, StartAt};
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
        }
    }
}

/// Native runner that drives registered policies against the event feed.
pub struct PolicyRunner {
    cqrs: Cqrs<PostgresEventStore>,
    pool: Pool<Postgres>,
    policies: Vec<Arc<dyn ErasedPolicy>>,
    executors: HashMap<TypeId, Arc<dyn AggregateExecutor>>,
}

/// Handle for background policy tasks spawned by [`PolicyRunner::start_polling`].
pub struct PolicyRunnerDaemon {
    shutdown_tx: watch::Sender<bool>,
    tasks: Vec<JoinHandle<()>>,
}

impl PolicyRunnerDaemon {
    /// Signal all policy tasks to stop and await their completion.
    pub async fn shutdown(self) {
        let _ = self.shutdown_tx.send(true);
        for task in self.tasks {
            let _ = task.await;
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
        }
    }

    /// Manually drain every registered policy once.
    ///
    /// For each policy: read the gap-free prefix of events past its cursor,
    /// `react`, execute the returned dispatches through [`Cqrs`], and advance the
    /// cursor — one event at a time, advancing only after that event's commands
    /// have committed (at-least-once delivery; reactions must be idempotent).
    ///
    /// Returns the total number of dispatches executed across all policies.
    pub async fn drain(&self) -> Result<usize, replay::Error> {
        let mut total = 0;
        for policy in &self.policies {
            total += self.drain_policy(policy.as_ref()).await?;
        }
        Ok(total)
    }

    /// Start one long-lived polling task per registered policy.
    ///
    /// Each task competes for a per-policy `pg_advisory_lock` before entering its
    /// polling loop.  The lock key is derived from the policy name, so:
    ///
    /// - **Exactly one** service instance is the active leader for each policy at
    ///   any moment (single-consumer correctness for the ordered global feed).
    /// - **Different policies** may run on different instances concurrently.
    /// - **Leader failover is automatic**: the advisory lock is session-scoped, so
    ///   when the leader's task (or its host process) dies the lock is released and
    ///   a standby acquires it on the next poll and resumes from the stored cursor.
    ///
    /// Use [`PolicyRunnerDaemon::shutdown`] to stop all tasks cleanly.  Shutdown
    /// explicitly calls `pg_advisory_unlock` so the standby can take over without
    /// waiting for a TCP-level session timeout.
    pub fn start_polling(&self, interval: Duration) -> PolicyRunnerDaemon {
        let (shutdown_tx, shutdown_rx) = watch::channel(false);
        let mut tasks = Vec::with_capacity(self.policies.len());

        for policy in &self.policies {
            let policy = Arc::clone(policy);
            let cqrs = self.cqrs.clone();
            let pool = self.pool.clone();
            let executors = self.executors.clone();
            let mut policy_shutdown_rx = shutdown_rx.clone();

            tasks.push(tokio::spawn(async move {
                let name = policy.name().to_string();

                // Outer loop: repeatedly attempt to acquire the advisory lock.
                // A standby instance stays in this loop, sleeping between attempts.
                'acquire: loop {
                    if *policy_shutdown_rx.borrow() {
                        return;
                    }

                    // Hold a dedicated connection for the session-scoped lock.
                    // Keeping this connection alive for the full leadership tenure
                    // ensures the lock is not silently released between polls.
                    let mut lock_conn = match pool.acquire().await {
                        Ok(conn) => conn,
                        Err(error) => {
                            tracing::error!(
                                policy = %name,
                                error = %error,
                                "policy task could not acquire a connection for advisory lock"
                            );
                            tokio::select! {
                                _ = policy_shutdown_rx.changed() => {}
                                _ = tokio::time::sleep(interval) => {}
                            }
                            continue 'acquire;
                        }
                    };

                    // pg_try_advisory_lock is non-blocking: returns true only when
                    // this session exclusively holds the lock for `name`.
                    let acquired = match sqlx::query_scalar::<_, bool>(
                        "SELECT pg_try_advisory_lock(hashtext($1)::bigint)",
                    )
                    .bind(&name)
                    .fetch_one(&mut *lock_conn)
                    .await
                    {
                        Ok(v) => v,
                        Err(error) => {
                            tracing::error!(
                                policy = %name,
                                error = %error,
                                "advisory lock query failed"
                            );
                            tokio::select! {
                                _ = policy_shutdown_rx.changed() => {}
                                _ = tokio::time::sleep(interval) => {}
                            }
                            continue 'acquire;
                        }
                    };

                    if !acquired {
                        tracing::debug!(
                            policy = %name,
                            "advisory lock held by another instance; standing by"
                        );
                        drop(lock_conn);
                        tokio::select! {
                            _ = policy_shutdown_rx.changed() => {}
                            _ = tokio::time::sleep(interval) => {}
                        }
                        continue 'acquire;
                    }

                    tracing::info!(policy = %name, "acquired advisory lock; running as leader");

                    // Initialize cursor from the stored checkpoint (or bootstrap).
                    let mut cursor = match load_cursor(&pool, &name, policy.start_at()).await {
                        Ok(cursor) => cursor,
                        Err(error) => {
                            tracing::error!(
                                policy = %name,
                                error = %error,
                                "leader failed to initialize cursor; releasing lock"
                            );
                            let _ = sqlx::query("SELECT pg_advisory_unlock(hashtext($1)::bigint)")
                                .bind(&name)
                                .execute(&mut *lock_conn)
                                .await;
                            return;
                        }
                    };

                    // Leadership polling loop.
                    let max_depth = resolve_max_depth(policy.as_ref());
                    loop {
                        if *policy_shutdown_rx.borrow() {
                            break;
                        }

                        match drain_policy_once(
                            &cqrs,
                            &pool,
                            &executors,
                            policy.as_ref(),
                            &mut cursor,
                            max_depth,
                        )
                        .await
                        {
                            Ok(_) => {}
                            Err(error) => {
                                tracing::error!(
                                    policy = %name,
                                    error = %error,
                                    "policy polling iteration failed"
                                );
                            }
                        }

                        tokio::select! {
                            changed = policy_shutdown_rx.changed() => {
                                if changed.is_err() || *policy_shutdown_rx.borrow() {
                                    break;
                                }
                            }
                            _ = tokio::time::sleep(interval) => {}
                        }
                    }

                    // Shutdown: explicitly release the lock so a standby can take
                    // over immediately (without waiting for a TCP session timeout).
                    let _ = sqlx::query("SELECT pg_advisory_unlock(hashtext($1)::bigint)")
                        .bind(&name)
                        .execute(&mut *lock_conn)
                        .await;
                    return;
                }
            }));
        }

        PolicyRunnerDaemon { shutdown_tx, tasks }
    }

    async fn drain_policy(&self, policy: &dyn ErasedPolicy) -> Result<usize, replay::Error> {
        let name = policy.name().to_string();
        let mut cursor = load_cursor(&self.pool, &name, policy.start_at()).await?;
        let max_depth = resolve_max_depth(policy);
        drain_policy_once(
            &self.cqrs,
            &self.pool,
            &self.executors,
            policy,
            &mut cursor,
            max_depth,
        )
        .await
    }
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

async fn drain_policy_once(
    cqrs: &Cqrs<PostgresEventStore>,
    pool: &Pool<Postgres>,
    executors: &HashMap<TypeId, Arc<dyn AggregateExecutor>>,
    policy: &dyn ErasedPolicy,
    cursor: &mut i64,
    max_depth: u32,
) -> Result<usize, replay::Error> {
    let name = policy.name().to_string();
    let checkpoint_size = resolve_checkpoint_batch_size(policy);
    let read_batch = resolve_read_batch_size(policy, checkpoint_size);
    let feed = read_feed(pool, policy.stream_filter(), *cursor, read_batch).await?;

    let mut executed = 0;
    let mut events_since_checkpoint = 0u32;
    for (global_position, maybe_raw) in feed {
        if let Some(raw) = maybe_raw {
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
                // the full resilience policy (BRV advance, retry, dead-letter).
                executed += execute_event_reactions(
                    cqrs,
                    pool,
                    executors,
                    policy,
                    &name,
                    global_position,
                    &raw,
                )
                .await?;
            }
        }
        // Always track in-memory position.
        *cursor = global_position;
        events_since_checkpoint += 1;

        // Write the persistent cursor every `checkpoint_size` events so that
        // a crash re-processes at most `checkpoint_size - 1` events rather
        // than the full drain batch (skip-safety: the cursor only advances
        // past events whose reactions are already durably committed).
        if events_since_checkpoint >= checkpoint_size {
            save_cursor(pool, &name, global_position).await?;
            events_since_checkpoint = 0;
        }
    }

    // Final checkpoint: flush any events processed since the last periodic save.
    if events_since_checkpoint > 0 {
        save_cursor(pool, &name, *cursor).await?;
    }

    Ok(executed)
}

/// Execute all reactions for one event, applying the resilience policy:
///
/// | Outcome                             | Action                                |
/// |-------------------------------------|---------------------------------------|
/// | `Ok`                                | count as executed, continue           |
/// | `BusinessRuleViolation`             | log + advance (aggregate said no)     |
/// | Retryable (`Unavailable`, `Conflict`, `RateLimited`) within retry budget | back-off + retry |
/// | Permanent or retries exhausted      | write `policy_dead_letters`, advance  |
///
/// The function always returns `Ok`; failures are absorbed here so the caller's
/// cursor always advances (a circuit-breaker, never a poison pill).
///
/// **Re-react safety**: on retry the policy's `react` is called again for the
/// same event.  Because `react` is a pure function and the at-least-once +
/// causation-guard contract already guarantees idempotency, re-executing an
/// earlier dispatch that already succeeded is safe.
async fn execute_event_reactions(
    cqrs: &Cqrs<PostgresEventStore>,
    pool: &Pool<Postgres>,
    executors: &HashMap<TypeId, Arc<dyn AggregateExecutor>>,
    policy: &dyn ErasedPolicy,
    policy_name: &str,
    global_position: i64,
    raw: &PersistedEvent<Value>,
) -> Result<usize, replay::Error> {
    for attempt in 0..=MAX_DISPATCH_RETRIES {
        let dispatches = policy.react_erased(raw);
        let mut executed = 0usize;
        let mut need_retry = false;

        for dispatch in dispatches {
            match execute_dispatch(cqrs, executors, policy_name, global_position, raw, dispatch)
                .await
            {
                Ok(()) => {
                    executed += 1;
                }
                Err(e) if e.kind() == replay::ErrorKind::BusinessRuleViolation => {
                    tracing::info!(
                        policy          = %policy_name,
                        event_id        = %raw.id,
                        global_position,
                        error           = %e,
                        "policy dispatch declined by aggregate business rule; advancing cursor"
                    );
                }
                Err(e) if is_retryable(e.kind()) && attempt < MAX_DISPATCH_RETRIES => {
                    tracing::warn!(
                        policy          = %policy_name,
                        event_id        = %raw.id,
                        global_position,
                        attempt,
                        error           = %e,
                        "policy dispatch failed with retryable error; backing off before retry"
                    );
                    need_retry = true;
                    break; // skip remaining dispatches for this attempt
                }
                Err(e) => {
                    // Permanent error, or retryable but retries exhausted.
                    tracing::error!(
                        policy          = %policy_name,
                        event_id        = %raw.id,
                        global_position,
                        attempt,
                        error           = %e,
                        "policy dispatch failed permanently; writing dead-letter and advancing cursor"
                    );
                    write_dead_letter(pool, policy_name, global_position, raw, &e).await?;
                }
            }
        }

        if !need_retry {
            return Ok(executed);
        }

        // Exponential back-off: 100 ms, 200 ms, 400 ms, …
        let backoff = Duration::from_millis(100 * (1u64 << attempt.min(5)));
        tokio::time::sleep(backoff).await;
    }

    Ok(0)
}

/// Write a dead-letter record for a dispatch that could not be executed.
async fn write_dead_letter(
    pool: &Pool<Postgres>,
    policy_name: &str,
    global_position: i64,
    raw: &PersistedEvent<Value>,
    error: &replay::Error,
) -> Result<(), replay::Error> {
    sqlx::query(
        "INSERT INTO policy_dead_letters \
         (policy_name, global_position, event_id, error_kind, error_message) \
         VALUES ($1, $2, $3, $4, $5)",
    )
    .bind(policy_name)
    .bind(global_position)
    .bind(raw.id)
    .bind(error.kind().to_string())
    .bind(error.to_string())
    .execute(pool)
    .await
    .map_err(crate::db_error)?;
    Ok(())
}

/// Read the contiguous, gap-free prefix of events with `global_position >
/// cursor` matching `filter`, in global order.
///
/// BIGSERIAL positions are assigned at INSERT but become visible at COMMIT,
/// so a higher position can appear before a lower one fills in. Stopping at
/// the first gap guarantees we never skip an event that is still in flight.
///
/// Each entry is `(global_position, maybe_event)`.  When `maybe_event` is
/// `None` the row is a synthetic compaction snapshot (`compacted_snapshot =
/// TRUE`): the cursor must still advance past it, but no reaction is fired.
async fn read_feed(
    pool: &Pool<Postgres>,
    filter: StreamFilter,
    cursor: i64,
    limit: u32,
) -> Result<Vec<(i64, Option<PersistedEvent<Value>>)>, replay::Error> {
    let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
        "SELECT id, data, metadata, stream_id, type, version, created, aggregate_version, \
         global_position, compacted_snapshot FROM events WHERE global_position > ",
    );
    qb.push_bind(cursor);
    qb.push(" AND ");
    PostgresEventStore::add_filters(&mut qb, filter);
    qb.push(" ORDER BY global_position ASC LIMIT ");
    qb.push_bind(limit as i64);

    let rows = qb.build().fetch_all(pool).await.map_err(crate::db_error)?;

    let mut feed = Vec::with_capacity(rows.len());
    let mut expected = cursor + 1;
    for row in rows {
        let global_position: i64 = row.get("global_position");
        if global_position != expected {
            // Gap: stop here and let the hole fill on a later poll.
            break;
        }
        expected += 1;

        let is_snapshot: bool = row.get("compacted_snapshot");
        if is_snapshot {
            // Synthetic row: advance the cursor past it, but deliver nothing.
            feed.push((global_position, None));
        } else {
            let event = PersistedEvent::<Value>::try_from(row)?;
            feed.push((global_position, Some(event)));
        }
    }

    Ok(feed)
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

async fn load_cursor(
    pool: &Pool<Postgres>,
    name: &str,
    start_at: StartAt,
) -> Result<i64, replay::Error> {
    let position =
        sqlx::query_scalar::<_, i64>("SELECT position FROM policy_cursors WHERE name = $1")
            .bind(name)
            .fetch_optional(pool)
            .await
            .map_err(crate::db_error)?;

    if let Some(position) = position {
        return Ok(position);
    }

    let bootstrap_position = bootstrap_position(pool, start_at).await?;
    sqlx::query(
        "INSERT INTO policy_cursors (name, position, updated_at) VALUES ($1, $2, now()) \
         ON CONFLICT (name) DO NOTHING",
    )
    .bind(name)
    .bind(bootstrap_position)
    .execute(pool)
    .await
    .map_err(crate::db_error)?;

    let persisted =
        sqlx::query_scalar::<_, i64>("SELECT position FROM policy_cursors WHERE name = $1")
            .bind(name)
            .fetch_one(pool)
            .await
            .map_err(crate::db_error)?;

    Ok(persisted)
}

async fn bootstrap_position(
    pool: &Pool<Postgres>,
    start_at: StartAt,
) -> Result<i64, replay::Error> {
    match start_at {
        StartAt::Beginning => Ok(0),
        StartAt::Now => {
            let head =
                sqlx::query_scalar::<_, Option<i64>>("SELECT MAX(global_position) FROM events")
                    .fetch_one(pool)
                    .await
                    .map_err(crate::db_error)?;
            Ok(head.unwrap_or_default())
        }
    }
}

async fn save_cursor(
    pool: &Pool<Postgres>,
    name: &str,
    position: i64,
) -> Result<(), replay::Error> {
    sqlx::query(
        "INSERT INTO policy_cursors (name, position, updated_at) VALUES ($1, $2, now()) \
         ON CONFLICT (name) DO UPDATE SET position = EXCLUDED.position, updated_at = now()",
    )
    .bind(name)
    .bind(position)
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

/// Environment variable that overrides the read-batch default.
const READ_BATCH_SIZE_ENV_VAR: &str = "REPLAY_READ_BATCH_SIZE";

/// Environment variable that overrides the checkpoint-batch default.
const CHECKPOINT_BATCH_SIZE_ENV_VAR: &str = "REPLAY_CHECKPOINT_BATCH_SIZE";

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
    Ok(Metadata::new(Value::Object(merged)))
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
    use replay::Metadata;
    use serde_json::json;

    use super::merge_dispatch_metadata;

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
