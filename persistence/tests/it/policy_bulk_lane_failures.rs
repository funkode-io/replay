//! The bulk lane survives a failed reaction and a dead worker (funkode-io/replay#304,
//! ADR-0034).
//!
//! The bulk lane fails the way the online lane does: a recorded skip, never a silent one,
//! and never a wedged queue. A command that fails for good becomes a Dead letter for its
//! triggering event and its work row leaves the queue with the rest of the batch; a
//! worker that dies mid-batch releases its claim, and the next claim delivers every row
//! it held; the operator's retry and discard work on a bulk dead letter as on an online
//! one; and the Policy's status reports its bulk queue depth beside its online lag.

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use urn::Urn;

use crate::common;

use common::policy_harness::{PolicyDaemonHarness, Probe, ProbeCommand, ProbeEvent, ProbeUrn};
use replay_persistence::{
    CommandContextMut, CommandMiddleware, DeadLetterDiscard, DeadLetterRetry, Dispatch, Lanes,
    Next, ObservedEvent, PolicyCondition, PolicySettings, PolicyStatus, PolicyStatusStore,
};

fn bulk() -> PolicySettings {
    PolicySettings::new().on_lanes(Lanes::Bulk)
}

fn tags(prefix: &str, count: usize) -> Vec<String> {
    (0..count).map(|n| format!("{prefix}-{n}")).collect()
}

/// The stream a reaction to an event of `source` echoes into.
fn echo_stream(source: &str) -> ProbeUrn {
    let source: Urn = source.parse().expect("a stream id is a URN");
    ProbeUrn::new(format!("echo-{}", source.nss())).expect("a valid NSS")
}

/// Echo every ping into [`echo_stream`] of its source, except a ping tagged `refuse…`
/// while `refusing` holds, which is answered by a command the Probe refuses for good.
///
/// The refusal is aimed at the same echo stream, so it sits in one target group with the
/// echoes, in its event's place: what the group runs after a parked command is seen.
fn echo_unless_refusing(
    refusing: Arc<AtomicBool>,
) -> impl Fn(&ObservedEvent<ProbeEvent>) -> Vec<Dispatch> + Send + Sync + 'static {
    move |event| match &event.data {
        ProbeEvent::Pinged { tag }
            if tag.starts_with("refuse") && refusing.load(Ordering::SeqCst) =>
        {
            vec![Dispatch::to::<Probe>(
                echo_stream(event.stream_id.as_str()),
                ProbeCommand::Refuse {
                    reason: format!("{tag} is refused"),
                },
            )]
        }
        ProbeEvent::Pinged { tag } => vec![Dispatch::to::<Probe>(
            echo_stream(event.stream_id.as_str()),
            ProbeCommand::Echo { tag: tag.clone() },
        )],
        ProbeEvent::Echoed { .. } => vec![],
    }
}

/// The tags echoed into `stream`, in the stream's order.
async fn echoes(harness: &PolicyDaemonHarness, stream: &ProbeUrn) -> Vec<String> {
    sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT data FROM events WHERE stream_id = $1 ORDER BY stream_seq LIMIT 1000",
    )
    .bind(stream.to_urn().to_string())
    .fetch_all(harness.pool())
    .await
    .expect("the echo stream must be readable")
    .into_iter()
    .filter_map(|data| match serde_json::from_value(data) {
        Ok(ProbeEvent::Echoed { tag }) => Some(tag),
        _ => None,
    })
    .collect()
}

/// `policy`'s status as the read model reports it, asserting it reports it once.
async fn status_of(harness: &PolicyDaemonHarness, policy: &str) -> Option<PolicyStatus> {
    let mut reported: Vec<PolicyStatus> = PolicyStatusStore::new(harness.pool().clone())
        .list()
        .await
        .expect("the status read model must be readable")
        .into_iter()
        .filter(|status| status.name == policy)
        .collect();
    assert!(
        reported.len() <= 1,
        "one status per Policy, whatever its lanes: {reported:?}"
    );
    reported.pop()
}

async fn await_drained(harness: &PolicyDaemonHarness) {
    harness
        .observe("the work queue to drain", || async {
            (harness.all_bulk_work().await == 0).then_some(())
        })
        .await;
}

// ── A failed reaction ────────────────────────────────────────────────────────

/// A command that fails for good on the bulk lane is parked as a dead letter for its
/// triggering event, its row leaves the queue, and the rest of the batch is delivered.
#[tokio::test]
async fn a_failed_reaction_is_parked_and_the_batch_delivered_postgres_test() {
    let harness = PolicyDaemonHarness::start("bulk_failed_reaction", |builder, policy| {
        builder.register_policy_fn::<ProbeEvent, _>(
            policy,
            bulk(),
            echo_unless_refusing(Arc::new(AtomicBool::new(true))),
        )
    })
    .await;
    let policy = harness.policy_name().to_string();
    harness.await_bulk_registration(&policy).await;

    let batch = ["ok-0", "refuse", "ok-1"].map(String::from);
    let imported = harness.bulk_import("failing", &batch).await;
    await_drained(&harness).await;

    let parked = harness.dead_letters().await;
    assert_eq!(parked.len(), 1, "the refused command is parked: {parked:?}");
    assert_eq!(
        (parked[0].event_id, parked[0].global_position),
        (imported[1].event_id, imported[1].global_position),
        "for the event whose reaction failed"
    );
    assert!(
        parked[0].error_message.contains("refuse is refused"),
        "with the error that failed it: {}",
        parked[0].error_message
    );
    assert_eq!(
        echoes(&harness, &echo_stream(&imported[0].stream_id)).await,
        ["ok-0", "ok-1"],
        "the rest of the batch is delivered, in order"
    );

    let status = status_of(&harness, &policy)
        .await
        .expect("a bulk Policy has a status");
    assert_eq!(status.dead_letter_count, 1);
    assert_eq!(status.bulk_pending, Some(0), "nothing is left in the queue");
    assert_eq!(status.condition, PolicyCondition::Degraded);

    harness.shutdown().await;
}

// ── A dead worker ────────────────────────────────────────────────────────────

/// Holds the second command a Policy dispatches until it is let go, once: the batch is
/// mid-dispatch, its first command committed and its claim still open.
#[derive(Clone, Default)]
struct HoldSecondDispatch {
    dispatched: Arc<AtomicUsize>,
    holding: Arc<AtomicBool>,
    released: Arc<AtomicBool>,
}

impl CommandMiddleware for HoldSecondDispatch {
    fn before<'a>(&'a self, context: CommandContextMut<'a>) -> BoxFuture<'a, Next> {
        Box::pin(async move {
            if context.causation().is_none() || self.dispatched.fetch_add(1, Ordering::SeqCst) != 1
            {
                return Next::Continue;
            }
            self.holding.store(true, Ordering::SeqCst);
            while !self.released.load(Ordering::SeqCst) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Next::Continue
        })
    }
}

/// A worker whose claim transaction ends mid-batch — its connection killed, as a worker's
/// is when its process dies — leaves every row of the batch in the queue, and the next
/// claim delivers all of it: at-least-once, nothing lost.
#[tokio::test]
async fn a_worker_that_dies_mid_batch_leaves_its_claim_for_the_next_postgres_test() {
    let hold = HoldSecondDispatch::default();
    let (holding, released) = (Arc::clone(&hold.holding), Arc::clone(&hold.released));
    let harness =
        PolicyDaemonHarness::start_with_middleware("bulk_dead_worker", hold, |builder, policy| {
            builder.register_policy_fn::<ProbeEvent, _>(
                policy,
                bulk(),
                echo_unless_refusing(Arc::new(AtomicBool::new(false))),
            )
        })
        .await;
    let policy = harness.policy_name().to_string();
    harness.await_bulk_registration(&policy).await;

    let batch = tags("dies", 4);
    let imported = harness.bulk_import("dying", &batch).await;
    let echo = echo_stream(&imported[0].stream_id);
    harness
        .observe("the worker to be mid-batch", || async {
            holding.load(Ordering::SeqCst).then_some(())
        })
        .await;
    assert_eq!(
        echoes(&harness, &echo).await,
        ["dies-0"],
        "the batch's first command has committed"
    );

    // The worker dies: the backend holding the Policy's claim is killed, and its
    // transaction ends with it.
    let killed: Vec<bool> = sqlx::query_scalar(
        "SELECT pg_terminate_backend(pid) FROM pg_locks \
          WHERE locktype = 'advisory' AND objsubid = 2 \
            AND classid = hashtext('replay.bulk_claim')::oid \
            AND objid = hashtext($1)::oid \
            AND database = (SELECT oid FROM pg_database WHERE datname = current_database())",
    )
    .bind(&policy)
    .fetch_all(harness.pool())
    .await
    .expect("the claim's backend must be found and terminated");
    assert_eq!(killed, [true], "one claim, held by the worker mid-batch");
    harness
        .observe("the dead claim to release its lock", || async {
            sqlx::query_scalar::<_, bool>(
                "SELECT NOT EXISTS (SELECT 1 FROM pg_locks \
                  WHERE locktype = 'advisory' AND objsubid = 2 \
                    AND classid = hashtext('replay.bulk_claim')::oid \
                    AND objid = hashtext($1)::oid \
                    AND database = (SELECT oid FROM pg_database \
                                     WHERE datname = current_database()))",
            )
            .bind(&policy)
            .fetch_one(harness.pool())
            .await
            .expect("pg_locks must be readable")
            .then_some(())
        })
        .await;
    assert_eq!(
        harness.bulk_work(&policy).await,
        4,
        "every row of the dead batch is still in the queue, the delivered one included"
    );

    // The worker carries on as if it never knew; its batch can no longer commit, so the
    // next claim takes the whole of it again.
    released.store(true, Ordering::SeqCst);
    await_drained(&harness).await;
    let mut redelivered = batch.clone();
    redelivered.extend(batch.iter().cloned());
    assert_eq!(
        echoes(&harness, &echo).await,
        redelivered,
        "the dead batch's dispatches stand, and the next claim delivers every row again, \
         in order: at-least-once"
    );
    assert!(
        harness.dead_letters().await.is_empty(),
        "nothing was parked"
    );

    harness.shutdown().await;
}

// ── Retry and discard ────────────────────────────────────────────────────────

/// The operator's controls work on a bulk dead letter: a retry re-executes the reaction
/// from its triggering event, as the Policy's code defines it now, and archives the row;
/// a discard archives its row and executes nothing.
#[tokio::test]
async fn a_bulk_dead_letter_is_retried_from_its_event_or_discarded_postgres_test() {
    let refusing = Arc::new(AtomicBool::new(true));
    let reaction = Arc::clone(&refusing);
    let harness = PolicyDaemonHarness::start("bulk_retry_discard", move |builder, policy| {
        builder.register_policy_fn::<ProbeEvent, _>(
            policy,
            bulk(),
            echo_unless_refusing(Arc::clone(&reaction)),
        )
    })
    .await;
    let policy = harness.policy_name().to_string();
    harness.await_bulk_registration(&policy).await;

    let batch = ["refuse-retried", "refuse-discarded"].map(String::from);
    let imported = harness.bulk_import("parked", &batch).await;
    await_drained(&harness).await;
    let parked = harness.await_dead_letters(2).await;
    let row_for = |event: usize| {
        parked
            .iter()
            .find(|row| row.event_id == imported[event].event_id)
            .unwrap_or_else(|| panic!("a dead letter for {}: {parked:?}", batch[event]))
            .id
    };
    let (retried, discarded) = (row_for(0), row_for(1));
    let echo = echo_stream(&imported[0].stream_id);
    assert!(echoes(&harness, &echo).await.is_empty());

    // The cause is fixed: the reaction no longer refuses.
    refusing.store(false, Ordering::SeqCst);

    assert_eq!(
        harness.retry_parked_row(retried).await,
        DeadLetterRetry::Resolved
    );
    assert_eq!(
        echoes(&harness, &echo).await,
        ["refuse-retried"],
        "the retry re-executed the reaction to its triggering event"
    );
    let caused: Vec<_> = harness
        .dispatches_for(&policy)
        .await
        .into_iter()
        .filter(|dispatched| dispatched.caused_by_event_id == imported[0].event_id)
        .collect();
    assert_eq!(caused.len(), 1, "caused by the bulk event: {caused:?}");
    assert_eq!(caused[0].caused_by_position, imported[0].global_position);
    assert_eq!(harness.archived_row_count("retried").await, 1);

    assert_eq!(
        harness.discard_parked_row(discarded).await,
        DeadLetterDiscard::Discarded
    );
    assert_eq!(harness.archived_row_count("discarded").await, 1);
    assert_eq!(harness.parked_row_count().await, 0);
    assert_eq!(
        echoes(&harness, &echo).await,
        ["refuse-retried"],
        "a discard executes nothing"
    );

    let status = status_of(&harness, &policy)
        .await
        .expect("a bulk Policy has a status");
    assert_eq!(status.dead_letter_count, 0);
    assert_eq!(status.condition, PolicyCondition::CaughtUp);

    harness.shutdown().await;
}

// ── Status ───────────────────────────────────────────────────────────────────

/// A bulk Policy's status reports its queue depth: what an import has queued for it while
/// no worker runs, and zero once it has drained — `Working` until then.
#[tokio::test]
async fn status_reports_a_bulk_policys_pending_work_postgres_test() {
    let mut harness = PolicyDaemonHarness::start("bulk_status", |builder, policy| {
        builder.register_policy_fn::<ProbeEvent, _>(
            policy,
            bulk(),
            echo_unless_refusing(Arc::new(AtomicBool::new(false))),
        )
    })
    .await;
    let policy = harness.policy_name().to_string();
    harness.await_bulk_registration(&policy).await;
    harness.stop().await;

    let idle = status_of(&harness, &policy)
        .await
        .expect("a Policy only on the bulk lane has a status");
    assert_eq!(idle.bulk_pending, Some(0), "an empty queue, not no queue");
    assert_eq!(
        (idle.lag, idle.streams_behind),
        (0, 0),
        "no online view to lag"
    );
    assert_eq!(idle.condition, PolicyCondition::CaughtUp);

    harness.bulk_import("pending", &tags("pending", 5)).await;
    let queued = status_of(&harness, &policy).await.expect("a status");
    assert_eq!(queued.bulk_pending, Some(5), "one row per event owed");
    assert_eq!(queued.lag, 0, "the online view does not count bulk work");
    assert_eq!(queued.condition, PolicyCondition::Working);

    harness.resume();
    let drained = harness
        .observe("the status to report the queue drained", || async {
            status_of(&harness, &policy)
                .await
                .filter(|status| status.bulk_pending == Some(0))
        })
        .await;
    assert_eq!(drained.condition, PolicyCondition::CaughtUp);

    harness.shutdown().await;
}

/// A Policy on both lanes reports one status carrying both views — its online lag and its
/// bulk queue depth — and a Policy only online reports no bulk queue at all.
#[tokio::test]
async fn a_policy_on_both_lanes_reports_one_status_with_both_views_postgres_test() {
    let mut harness = PolicyDaemonHarness::start("bulk_status_both", |builder, policy| {
        builder
            .register_policy_fn::<ProbeEvent, _>(
                policy,
                PolicySettings::new().on_lanes(Lanes::Both),
                echo_unless_refusing(Arc::new(AtomicBool::new(false))),
            )
            .register_policy_fn::<ProbeEvent, _>(
                format!("{policy}_online"),
                PolicySettings::new(),
                echo_unless_refusing(Arc::new(AtomicBool::new(false))),
            )
    })
    .await;
    let policy = harness.policy_name().to_string();
    let online = format!("{policy}_online");
    harness.await_bulk_registration(&policy).await;
    for name in [&policy, &online] {
        harness
            .observe(&format!("{name} to have an online status"), || async {
                status_of(&harness, name).await
            })
            .await;
    }
    harness.stop().await;

    harness.ping("online-source", "online").await;
    harness.bulk_import("bulk-source", &tags("bulk", 3)).await;

    let both = status_of(&harness, &policy).await.expect("a status");
    assert!(
        both.lag > 0 && both.streams_behind > 0,
        "the online view owes the online ping: {both:?}"
    );
    assert_eq!(both.bulk_pending, Some(3), "the bulk view owes the import");
    assert_eq!(both.condition, PolicyCondition::Working);
    assert_eq!(
        status_of(&harness, &online)
            .await
            .expect("a status")
            .bulk_pending,
        None,
        "a Policy only online has no bulk queue"
    );

    harness.resume();
    harness
        .observe("the bulk view to drain", || async {
            status_of(&harness, &policy)
                .await
                .filter(|status| status.bulk_pending == Some(0))
        })
        .await;

    harness.shutdown().await;
}
