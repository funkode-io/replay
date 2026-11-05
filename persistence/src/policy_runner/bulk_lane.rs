//! The bulk lane: how a bulk append reaches the Policies that consume it
//! ([ADR-0034](https://github.com/funkode-io/replay/blob/main/docs/adr/0034-a-bulk-append-reaches-policies-through-a-work-queue.md),
//! funkode-io/replay#302).
//!
//! [`Cqrs::execute_bulk`] writes, in the append's own transaction, one row of
//! `policy_bulk_work` per event per Policy registered here. A [`BulkWorker`] per bulk
//! Policy, running in the Policy's [Leader], claims the lowest batch of its rows with
//! `FOR UPDATE SKIP LOCKED`, reads those events, calls the Policy's unchanged `react` on
//! each, dispatches the commands grouped by target stream — one aggregate load per target
//! per batch — deletes the claimed rows and commits. Nothing here reads the online feed,
//! and the online feed never delivers a bulk event.
//!
//! [Leader]: https://github.com/funkode-io/replay/blob/main/CONTEXT.md#leader

use std::any::TypeId;
use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;
use std::time::{Duration, Instant};

use futures::future::BoxFuture;
use futures::FutureExt;
use serde_json::Value;
use sqlx::{Pool, Postgres, QueryBuilder, Row};
use tokio::sync::{broadcast, watch};

use replay::{Aggregate, Dispatch, Metadata};

use super::{
    causation_metadata, event_causation_depth, merge_dispatch_metadata, panic_message,
    parked_nothing, resolve_checkpoint_batch_size, resolve_dispatch_timeout, resolve_max_depth,
    resolve_read_batch_size, write_dead_letter, AggregateExecutor, DeadLetterWrite,
    DispatchFailure, DispatchIdentity, Parking, StandDown, Stop, TrackedStreams,
    MAX_DISPATCH_RETRIES, PANIC_ERROR_KIND,
};
use crate::policy::RegisteredPolicy;
use crate::policy_liveness::LivenessHandle;
use crate::{Cqrs, PersistedEvent, PostgresEventStore, StreamFilter};

/// The first wait before trying a Policy's claim lock again, while another claim of that
/// Policy is in flight; doubled up to [`CLAIM_LOCK_MAX_BACKOFF`].
const CLAIM_LOCK_INITIAL_BACKOFF: Duration = Duration::from_millis(10);

/// The longest wait between two tries of a Policy's claim lock.
const CLAIM_LOCK_MAX_BACKOFF: Duration = Duration::from_millis(500);

/// Record that `policy` consumes the bulk lane, and which stream types it tracks, so
/// every bulk append from now on queues work for it.
///
/// Every replica writes it as its worker starts, and a registration only ever **widens**:
/// the stored types become the union of what each replica tracks, and `NULL` — every
/// stream — absorbs any list. The fan-out happens at the append, so a replica that
/// narrowed the row (an older release in a rolling deploy, say) would lose bulk work for
/// the types it dropped; work queued for a type the Policy no longer tracks costs only its
/// row, because the worker's read applies the Policy's own filter and deletes what it
/// does not deliver. Narrowing a registration is the operator's call (release notes).
/// The types follow the Policy's filter as on the online lane (ADR-0031): only a
/// top-level `ForStreamTypes` narrows them.
pub(super) async fn register(
    pool: &Pool<Postgres>,
    policy: &RegisteredPolicy,
) -> Result<(), replay::Error> {
    let tracked = TrackedStreams::of(&policy.stream_filter());
    sqlx::query(
        "INSERT INTO policy_bulk_registrations AS r (policy, tracked_stream_types) \
         VALUES ($1, $2) \
         ON CONFLICT (policy) DO UPDATE \
            SET tracked_stream_types = CASE \
                    WHEN r.tracked_stream_types IS NULL \
                      OR EXCLUDED.tracked_stream_types IS NULL THEN NULL \
                    ELSE ARRAY(SELECT DISTINCT t FROM unnest(r.tracked_stream_types \
                               || EXCLUDED.tracked_stream_types) AS t ORDER BY t) \
                END \
          WHERE r.tracked_stream_types IS NOT NULL \
            AND (EXCLUDED.tracked_stream_types IS NULL \
                 OR NOT r.tracked_stream_types @> EXCLUDED.tracked_stream_types)",
    )
    .bind(policy.name())
    .bind(tracked.as_column())
    .execute(pool)
    .await
    .map_err(crate::db_error)?;
    Ok(())
}

// ── Grouped execution: one load per target stream ────────────────────────────

/// Commands executed one after another against one aggregate instance, which is loaded
/// once and kept between them.
///
/// Opened per target stream per batch by [`AggregateExecutor::session`], which erases
/// the aggregate the way [`AggregateExecutor::execute`] does.
pub(super) trait TargetSession: Send {
    fn execute<'s>(
        &'s mut self,
        dispatch: Dispatch,
        metadata: Metadata,
    ) -> BoxFuture<'s, Result<(), replay::Error>>;
}

/// The [`TargetSession`] of aggregate `A`.
pub(super) struct TypedSession<'a, A: Aggregate> {
    pub(super) cqrs: &'a Cqrs<PostgresEventStore>,
    pub(super) services: &'a A::Services,
    /// The aggregate as the last command that committed left it, with its stream
    /// version. Taken out while a command runs and put back only when it commits: a
    /// command that fails, or whose future is dropped on a timeout, may have folded in
    /// events whose append rolled back, so the next command loads the aggregate afresh.
    pub(super) loaded: Option<crate::cqrs::Loaded<A>>,
}

impl<A> TargetSession for TypedSession<'_, A>
where
    A: Aggregate + 'static,
    A::Error: Into<replay::Error>,
    A::StreamId: 'static,
    A::Command: 'static,
    A::Services: Send + Sync + 'static,
{
    fn execute<'s>(
        &'s mut self,
        dispatch: Dispatch,
        metadata: Metadata,
    ) -> BoxFuture<'s, Result<(), replay::Error>> {
        Box::pin(async move {
            let expected_version = dispatch.expected_version();
            let (id, command) = dispatch.into_parts::<A>().map_err(|_| {
                replay::Error::internal("policy dispatch payload type mismatch")
                    .with_operation("policy_bulk_execute")
            })?;

            // A command that names the version it expects is decided as `execute` decides
            // it — on the stream at that version — so it is never served from the cache,
            // and the cached instance is dropped: this command may move the stream.
            if expected_version.is_some() {
                self.loaded = None;
                self.cqrs
                    .execute::<A>(&id, metadata, command, self.services, expected_version)
                    .await
                    .map_err(Into::into)?;
                return Ok(());
            }

            self.cqrs
                .execute_cached(
                    &mut self.loaded,
                    &id,
                    metadata,
                    command,
                    self.services,
                    expected_version,
                )
                .await
                .map_err(Into::into)?;
            Ok(())
        })
    }
}

// ── One batch ────────────────────────────────────────────────────────────────

/// A command of the batch, and the event whose reaction returned it.
struct Pending {
    /// Index of the triggering event in the batch's events.
    event: usize,
    /// The command's index in the vector the reaction returned.
    ordinal: usize,
    dispatch: Dispatch,
}

/// Every command of a batch aimed at one target stream, in the order their events were
/// claimed — so a target receives a stream's commands in that stream's order.
struct TargetGroup {
    aggregate: TypeId,
    commands: Vec<Pending>,
}

/// Why one command of a batch did not commit.
enum CommandFailure {
    Dispatch(DispatchFailure),
    Panicked(String),
}

/// What one drained batch came to.
struct Batch {
    claimed: usize,
    executed: usize,
}

/// The runner's machinery for draining **one** Policy's bulk work queue.
pub(super) struct BulkDelivery<'a> {
    pub(super) cqrs: &'a Cqrs<PostgresEventStore>,
    pub(super) pool: &'a Pool<Postgres>,
    pub(super) executors: &'a HashMap<TypeId, Arc<dyn AggregateExecutor>>,
    pub(super) policy: &'a RegisteredPolicy,
}

impl BulkDelivery<'_> {
    /// Claim, react to and delete batches of this Policy's work until a claim comes back
    /// short — the queue is empty — or `stand_down` says the worker is to stop leading.
    /// A stand-down mid-batch abandons the batch: the claim rolls back, and whoever leads
    /// next claims the same rows.
    ///
    /// Returns how many dispatches committed.
    pub(super) async fn drain(&self, stand_down: &StandDown) -> Result<usize, replay::Error> {
        // The online lane's read batch is the bulk lane's claim: one setting bounds what a
        // Policy holds in memory whichever lane fed it.
        let checkpoint = resolve_checkpoint_batch_size(self.policy.settings());
        let batch = resolve_read_batch_size(self.policy.settings(), checkpoint);
        let mut executed = 0;
        loop {
            let Batch {
                claimed,
                executed: committed,
            } = self.deliver_batch(batch, stand_down).await?;
            executed += committed;
            if claimed < batch as usize || stand_down.now() {
                return Ok(executed);
            }
        }
    }

    /// One claim transaction: the lowest `batch` rows of this Policy's work, reacted to,
    /// dispatched and deleted.
    ///
    /// The claim stays open while the commands run, so a worker that dies releases its
    /// locks and the rows are there again for the next claim: at-least-once, as the
    /// online lane. Everything held here is bounded by `batch` — the claimed positions,
    /// their events, and the commands their reactions return.
    async fn deliver_batch(
        &self,
        batch: u32,
        stand_down: &StandDown,
    ) -> Result<Batch, replay::Error> {
        let name = self.policy.name();
        let mut claim = self.pool.begin().await.map_err(crate::db_error)?;
        // One claim at a time per Policy, whoever makes it: a daemon's worker, the one that
        // leads after a handoff while the old leader is finishing its batch, or a manual
        // drain. `SKIP LOCKED` alone would hand the next claimer the rows after a batch
        // still dispatching, and a stream's later events would overtake its earlier ones.
        // Waiting here instead, the next claim starts from what the last one left. The
        // two-key form is a lock space of its own, apart from the leadership lock that the
        // lock manager holds on `hashtext(policy)`. Tried rather than waited on, so a
        // stand-down ends the wait at once instead of when the other claim does.
        let mut backoff = CLAIM_LOCK_INITIAL_BACKOFF;
        loop {
            let taken: bool = sqlx::query_scalar(
                "SELECT pg_try_advisory_xact_lock(hashtext('replay.bulk_claim'), hashtext($1))",
            )
            .bind(name)
            .fetch_one(&mut *claim)
            .await
            .map_err(crate::db_error)?;
            if taken {
                break;
            }
            tokio::select! {
                _ = tokio::time::sleep(backoff) => {}
                _ = stand_down.signalled() => {
                    return Err(replay::Error::unavailable(
                        "bulk worker stopped leading while another claim was in flight",
                    )
                    .with_operation("policy_bulk_drain")
                    .with_context("policy", name));
                }
            }
            backoff = (backoff * 2).min(CLAIM_LOCK_MAX_BACKOFF);
        }
        let positions: Vec<i64> = sqlx::query_scalar(
            "SELECT global_position FROM policy_bulk_work WHERE policy = $1 \
             ORDER BY global_position LIMIT $2 FOR UPDATE SKIP LOCKED",
        )
        .bind(name)
        .bind(i64::from(batch))
        .fetch_all(&mut *claim)
        .await
        .map_err(crate::db_error)?;
        if positions.is_empty() {
            claim.commit().await.map_err(crate::db_error)?;
            return Ok(Batch {
                claimed: 0,
                executed: 0,
            });
        }

        let events = read_claimed(&mut claim, self.policy.stream_filter(), &positions).await?;
        let groups = self.react(&mut claim, &events).await?;

        // Raced against the stand-down, as the online lane races a hold: a worker that is
        // to stop leading abandons the batch — a dispatch in flight or a retry's backoff
        // included — and the claim rolls back with nothing deleted.
        let dispatched = async {
            let mut executed = 0;
            for group in groups {
                executed += self.dispatch_group(&mut claim, &events, group).await?;
            }
            Ok::<_, replay::Error>(executed)
        };
        let executed = tokio::select! {
            executed = dispatched => executed?,
            _ = stand_down.signalled() => {
                tracing::info!(
                    policy = %name,
                    claimed = positions.len(),
                    "bulk worker stopped leading mid-batch; the claim is released for \
                     whoever leads next"
                );
                return Err(replay::Error::unavailable(
                    "bulk worker stopped leading mid-batch",
                )
                .with_operation("policy_bulk_drain")
                .with_context("policy", name));
            }
        };

        sqlx::query("DELETE FROM policy_bulk_work WHERE policy = $1 AND global_position = ANY($2)")
            .bind(name)
            .bind(&positions)
            .execute(&mut *claim)
            .await
            .map_err(crate::db_error)?;
        claim.commit().await.map_err(crate::db_error)?;

        Ok(Batch {
            claimed: positions.len(),
            executed,
        })
    }

    /// Call `react` on each claimed event, in position order, and group what it returns
    /// by target stream, each group in the order its commands were returned.
    async fn react(
        &self,
        claim: &mut sqlx::PgConnection,
        events: &[(i64, PersistedEvent<Value>)],
    ) -> Result<Vec<TargetGroup>, replay::Error> {
        let name = self.policy.name();
        let max_depth = resolve_max_depth(self.policy.settings());
        let mut groups: Vec<TargetGroup> = Vec::new();
        // Bounded by the batch's commands, as `groups` is.
        let mut by_target: HashMap<(TypeId, String), usize> = HashMap::new();

        for (event, (global_position, raw)) in events.iter().enumerate() {
            let depth = event_causation_depth(raw);
            if depth >= max_depth {
                tracing::warn!(
                    policy          = %name,
                    event_id        = %raw.id,
                    stream_id       = %raw.stream_id,
                    global_position,
                    depth,
                    max_depth,
                    lane            = "bulk",
                    "causation depth limit reached; skipping reaction to prevent runaway cascade"
                );
                continue;
            }

            let dispatches = match std::panic::catch_unwind(AssertUnwindSafe(|| {
                self.policy.react_erased(raw)
            })) {
                Ok(dispatches) => dispatches,
                Err(payload) => {
                    let message = panic_message(&*payload);
                    tracing::error!(
                        policy          = %name,
                        event_id        = %raw.id,
                        global_position,
                        panic           = %message,
                        lane            = "bulk",
                        "policy reaction panicked; writing dead-letter"
                    );
                    self.park(
                        claim,
                        *global_position,
                        raw,
                        None,
                        PANIC_ERROR_KIND,
                        &message,
                    )
                    .await?;
                    continue;
                }
            };

            for (ordinal, dispatch) in dispatches.into_iter().enumerate() {
                let key = (dispatch.target(), dispatch.target_stream_id().to_string());
                let index = *by_target.entry(key).or_insert_with(|| {
                    groups.push(TargetGroup {
                        aggregate: dispatch.target(),
                        commands: Vec::new(),
                    });
                    groups.len() - 1
                });
                groups[index].commands.push(Pending {
                    event,
                    ordinal,
                    dispatch,
                });
            }
        }

        Ok(groups)
    }

    /// Run one target's commands in order against one load of its aggregate.
    ///
    /// Each command is settled as the online lane settles one: committed, declined by a
    /// business rule, retried while it fails retryably within the budget, and otherwise
    /// parked as a dead letter for its triggering event. A busy command (ADR-0036) ends
    /// the batch with an error instead: the claim rolls back, and the rows are claimed
    /// again on the next poll.
    async fn dispatch_group(
        &self,
        claim: &mut sqlx::PgConnection,
        events: &[(i64, PersistedEvent<Value>)],
        group: TargetGroup,
    ) -> Result<usize, replay::Error> {
        let name = self.policy.name();
        let Some(executor) = self.executors.get(&group.aggregate) else {
            for pending in group.commands {
                let (global_position, raw) = &events[pending.event];
                let identity = DispatchIdentity::of(pending.ordinal, &pending.dispatch);
                let error = replay::Error::invalid_input(
                    "no services registered for the aggregate targeted by a policy dispatch",
                )
                .with_operation("policy_bulk_drain")
                .with_context("policy", name)
                .with_context("aggregate", identity.aggregate_name);
                self.park(
                    claim,
                    *global_position,
                    raw,
                    Some(&identity),
                    &error.kind().to_string(),
                    &error.to_string(),
                )
                .await?;
            }
            return Ok(0);
        };

        let mut session = executor.session(self.cqrs);
        let dispatch_timeout = resolve_dispatch_timeout(self.policy.settings());
        let mut executed = 0;

        for pending in group.commands {
            let (global_position, raw) = &events[pending.event];
            let identity = DispatchIdentity::of(pending.ordinal, &pending.dispatch);
            let mut dispatch = Some(pending.dispatch);
            let mut attempt = 0;
            loop {
                // A retry asks the reaction again for the same command: `react` is pure,
                // so the same event yields it again, idempotency key included (ADR-0027).
                let Some(command) = dispatch.take().or_else(|| {
                    self.policy
                        .react_erased(raw)
                        .into_iter()
                        .nth(pending.ordinal)
                }) else {
                    break;
                };
                let outcome = self
                    .execute(
                        session.as_mut(),
                        *global_position,
                        raw,
                        command,
                        dispatch_timeout,
                    )
                    .await;
                match outcome {
                    Ok(()) => {
                        executed += 1;
                        break;
                    }
                    Err(CommandFailure::Panicked(message)) => {
                        tracing::error!(
                            policy          = %name,
                            event_id        = %raw.id,
                            global_position,
                            target          = %identity.target_stream_id,
                            panic           = %message,
                            lane            = "bulk",
                            "policy dispatch panicked; writing dead-letter"
                        );
                        self.park(
                            claim,
                            *global_position,
                            raw,
                            Some(&identity),
                            PANIC_ERROR_KIND,
                            &message,
                        )
                        .await?;
                        break;
                    }
                    Err(CommandFailure::Dispatch(failure)) if failure.busy() => {
                        return Err(replay::Error::busy(failure.to_string())
                            .with_operation("policy_bulk_drain")
                            .with_context("policy", name)
                            .with_context("target", identity.target_stream_id.clone()));
                    }
                    Err(CommandFailure::Dispatch(failure)) if failure.declined() => {
                        tracing::info!(
                            policy          = %name,
                            event_id        = %raw.id,
                            global_position,
                            target          = %identity.target_stream_id,
                            error           = %failure,
                            lane            = "bulk",
                            "policy dispatch declined by aggregate business rule"
                        );
                        break;
                    }
                    Err(CommandFailure::Dispatch(failure))
                        if failure.retryable() && attempt < MAX_DISPATCH_RETRIES =>
                    {
                        tracing::warn!(
                            policy          = %name,
                            event_id        = %raw.id,
                            global_position,
                            attempt,
                            target          = %identity.target_stream_id,
                            error           = %failure,
                            lane            = "bulk",
                            "policy dispatch failed with retryable error; backing off before retry"
                        );
                        tokio::time::sleep(Duration::from_millis(100 * (1u64 << attempt.min(5))))
                            .await;
                        attempt += 1;
                    }
                    Err(CommandFailure::Dispatch(failure)) => {
                        tracing::error!(
                            policy          = %name,
                            event_id        = %raw.id,
                            global_position,
                            attempt,
                            target          = %identity.target_stream_id,
                            error           = %failure,
                            lane            = "bulk",
                            "policy dispatch failed permanently; writing dead-letter"
                        );
                        self.park(
                            claim,
                            *global_position,
                            raw,
                            Some(&identity),
                            &failure.error_kind(),
                            &failure.to_string(),
                        )
                        .await?;
                        break;
                    }
                }
            }
        }

        Ok(executed)
    }

    /// Execute one command in the target's session, with the causation the runner
    /// stamps, bounded by the dispatch timeout, with a panic in it contained here.
    async fn execute(
        &self,
        session: &mut dyn TargetSession,
        global_position: i64,
        raw: &PersistedEvent<Value>,
        dispatch: Dispatch,
        limit: Duration,
    ) -> Result<(), CommandFailure> {
        let name = self.policy.name();
        let metadata = merge_dispatch_metadata(
            causation_metadata(name, global_position, raw),
            dispatch.metadata().cloned(),
        )
        .map_err(|error| {
            CommandFailure::Dispatch(DispatchFailure::Returned(
                error
                    .with_operation("policy_bulk_drain")
                    .with_context("policy", name),
            ))
        })?;

        let running = AssertUnwindSafe(session.execute(dispatch, metadata)).catch_unwind();
        match tokio::time::timeout(limit, running).await {
            Ok(Ok(Ok(()))) => Ok(()),
            Ok(Ok(Err(error))) => Err(CommandFailure::Dispatch(DispatchFailure::Returned(error))),
            Ok(Err(payload)) => Err(CommandFailure::Panicked(panic_message(&*payload))),
            Err(_) => Err(CommandFailure::Dispatch(DispatchFailure::TimedOut {
                limit,
            })),
        }
    }

    /// Write a dead letter for one command of the batch, or for a reaction that panicked
    /// before it returned any (`identity` `None`).
    ///
    /// In the claim transaction, with the deletion of the claimed rows: a batch that is
    /// abandoned — a busy command, a stand-down — rolls its parks back with its claim, so
    /// a row is parked once, by the delivery that settles it, and never left behind for a
    /// command a later delivery of the same work commits.
    async fn park(
        &self,
        claim: &mut sqlx::PgConnection,
        global_position: i64,
        raw: &PersistedEvent<Value>,
        identity: Option<&DispatchIdentity>,
        error_kind: &str,
        error_message: &str,
    ) -> Result<(), replay::Error> {
        let parked = write_dead_letter(
            claim,
            DeadLetterWrite {
                policy_name: self.policy.name(),
                global_position,
                event_id: raw.id,
                identity,
                error_kind,
                error_message,
                parking: Parking::Delivery,
            },
        )
        .await?;
        if parked.is_none() {
            parked_nothing(self.policy.name(), raw);
        }
        Ok(())
    }
}

/// The claimed events this Policy reacts to, in position order: those its filter
/// matches. A claimed position whose event is gone — its stream deleted while the work
/// was pending — or that the filter excludes is simply not returned, and its row is
/// deleted with the rest of the claim.
///
/// Bounded by the claim: `positions` is at most one batch.
async fn read_claimed(
    conn: &mut sqlx::PgConnection,
    filter: StreamFilter,
    positions: &[i64],
) -> Result<Vec<(i64, PersistedEvent<Value>)>, replay::Error> {
    let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
        "SELECT id, data, metadata, stream_id, type, version, created, aggregate_version, \
         global_position, stream_seq, compacted_snapshot, COALESCE((",
    );
    // As a predicate NULL means no match; read as a value it must be collapsed.
    PostgresEventStore::add_filters(&mut qb, filter);
    qb.push("), FALSE) AS matches_filter FROM events WHERE global_position = ANY(");
    qb.push_bind(positions.to_vec());
    qb.push(") AND lane = 'bulk' ORDER BY global_position ASC");

    let rows = qb.build().fetch_all(conn).await.map_err(crate::db_error)?;

    let mut events = Vec::with_capacity(rows.len());
    for row in rows {
        let matches_filter: bool = row.get("matches_filter");
        let is_snapshot: bool = row.get("compacted_snapshot");
        if !matches_filter || is_snapshot {
            continue;
        }
        let global_position: i64 = row.get("global_position");
        events.push((global_position, PersistedEvent::<Value>::try_from(row)?));
    }
    Ok(events)
}

// ── The worker ───────────────────────────────────────────────────────────────

/// Drives one Policy's bulk work queue for the life of the daemon: registers the Policy
/// on the bulk lane, waits to lead it, and drains its queue on every wakeup.
///
/// Elected by the Policy's own advisory lock, as its online worker is, so a Policy on
/// both lanes is led by one replica for both.
#[derive(Clone)]
pub(super) struct BulkWorker {
    pub(super) policy: Arc<RegisteredPolicy>,
    pub(super) cqrs: Cqrs<PostgresEventStore>,
    pub(super) pool: Pool<Postgres>,
    pub(super) executors: HashMap<TypeId, Arc<dyn AggregateExecutor>>,
    pub(super) shutdown_rx: watch::Receiver<bool>,
    pub(super) leader_rx: watch::Receiver<bool>,
    pub(super) wake_tx: Option<broadcast::Sender<()>>,
    pub(super) interval: Duration,
    /// Where this worker publishes what it is doing — for a Policy on the bulk lane only.
    /// A Policy on both lanes is reported by its online worker.
    pub(super) liveness: Option<LivenessHandle>,
    pub(super) name: String,
}

impl BulkWorker {
    pub(super) async fn run(self) -> Stop {
        let BulkWorker {
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

        // Registered before the election, by every replica: a bulk append queues work for
        // a Policy from the moment any process that runs it has started.
        while let Err(error) = register(&pool, &policy).await {
            tracing::error!(
                policy = %name,
                error = %error,
                "bulk worker failed to register its policy on the bulk lane; retrying"
            );
            tokio::select! {
                _ = shutdown_rx.changed() => return Stop::Shutdown,
                _ = tokio::time::sleep(interval) => {}
            }
        }

        'lifetime: loop {
            if *shutdown_rx.borrow() {
                return Stop::Shutdown;
            }

            if !*leader_rx.borrow() {
                if let Some(liveness) = &liveness {
                    liveness.standing_by();
                }
            }
            while !*leader_rx.borrow() {
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            return Stop::Shutdown;
                        }
                    }
                    changed = leader_rx.changed() => {
                        if changed.is_err() {
                            return Stop::Abandoned;
                        }
                    }
                }
            }
            if let Some(liveness) = &liveness {
                liveness.leading();
            }
            tracing::info!(policy = %name, "bulk worker is leading; draining its work queue");

            loop {
                if *shutdown_rx.borrow() || !*leader_rx.borrow() {
                    continue 'lifetime;
                }

                let delivery = BulkDelivery {
                    cqrs: &cqrs,
                    pool: &pool,
                    executors: &executors,
                    policy: policy.as_ref(),
                };
                let stand_down = StandDown::on(shutdown_rx.clone(), leader_rx.clone());
                if let Err(error) = delivery.drain(&stand_down).await {
                    if stand_down.now() {
                        tracing::debug!(
                            policy = %name,
                            error = %error,
                            "bulk drain ended as the worker stopped leading"
                        );
                    } else if error.kind() == replay::ErrorKind::Busy {
                        tracing::info!(
                            policy = %name,
                            error = %error,
                            "bulk batch met a busy command; its work is claimed again next poll"
                        );
                    } else {
                        tracing::error!(
                            policy = %name,
                            error = %error,
                            "bulk batch failed; its work is claimed again next poll"
                        );
                    }
                }
                if let Some(liveness) = &liveness {
                    liveness.polled(Instant::now());
                }

                // The next wakeup: an append's NOTIFY, the poll interval, a change of
                // leadership, or shutdown — whichever comes first.
                tokio::select! {
                    changed = shutdown_rx.changed() => {
                        if changed.is_err() || *shutdown_rx.borrow() {
                            return Stop::Shutdown;
                        }
                    }
                    changed = leader_rx.changed() => {
                        if changed.is_err() {
                            return Stop::Abandoned;
                        }
                    }
                    _ = tokio::time::sleep(interval) => {}
                    woken = async {
                        match wake_rx.as_mut() {
                            Some(wake) => wake.recv().await,
                            None => std::future::pending().await,
                        }
                    } => {
                        // Ok or Lagged both mean "drain now"; Closed means the listener
                        // stopped, so this worker falls back to polling.
                        if let Err(broadcast::error::RecvError::Closed) = woken {
                            wake_rx = None;
                        }
                    }
                }
            }
        }
    }
}
