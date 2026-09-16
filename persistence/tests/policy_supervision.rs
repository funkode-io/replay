//! A worker that dies is restarted — supervision, driven against a real daemon
//! and a real database (funkode-io/replay#185).
//!
//! The death injected here is one no single event can be blamed for: a panic
//! raised while the worker prepares its read of the feed, the shape of a panic in
//! the lock manager, the listener or cursor I/O. A dispatch that *fails* is not
//! supervision's business — it is retried or parked and the cursor advances — so
//! no test here makes a failed dispatch restart a worker.
//!
//! Resuming from the last durable checkpoint is covered by
//! `policy_checkpoint_batch_crash_recovery_reprocesses_tail_postgres_test` in
//! `integration_tests.rs`. It cannot be re-asserted through a supervised restart:
//! the only per-event seam inside a batch belongs to the policy, so a death
//! landing mid-batch is a panic *inside* the reaction — funkode-io/replay#183's
//! boundary. Every death supervision can inject lands between batches, where the
//! cursor is already durable.
//!
//! Every assertion is something an operator could make: the policy reacted
//! again, the cursor moved, the daemon names a worker it gave up on.

mod common;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common::policy_harness::{PolicyDaemonHarness, Probe, ProbeCommand, ProbeEvent, ProbeUrn};
use replay_persistence::{
    Dispatch, PersistedEvent, Policy, StartAt, StreamFilter, WorkerSupervision,
};

/// Restarts fast enough that a test does not wait on production's caution, and
/// still exponential: 20 ms, 40 ms, 80 ms, capped at 80 ms.
fn quick_supervision() -> WorkerSupervision {
    WorkerSupervision::default()
        .initial_backoff(Duration::from_millis(20))
        .max_backoff(Duration::from_millis(80))
        .restart_window(Duration::from_secs(60))
}

/// A policy that kills its worker from *outside* the reaction.
///
/// `stream_filter` is called as the worker prepares its read of the feed, before
/// any event is delivered, so a panic there stands in for the panics this ticket
/// is about and has nothing to do with the event that happens to be next. The
/// reaction itself is ordinary — it echoes every ping — so a worker that came
/// back is visible by it reacting again.
struct DiesOutsideTheReaction {
    name: String,
    /// How many more times the worker should die. Decremented as it dies, so a
    /// test can ask for exactly one death, or for more than the budget allows.
    deaths_to_stage: Arc<AtomicUsize>,
    /// How many deaths actually happened, so a test can prove its fault fired
    /// rather than assume it.
    deaths: Arc<AtomicUsize>,
    /// Hold the fault back until the policy has reacted to something, so a test
    /// can tell what a restart lost from what the policy had never reached.
    only_after_reacting: bool,
    reacted: Arc<AtomicBool>,
}

impl Policy for DiesOutsideTheReaction {
    type Event = ProbeEvent;

    fn name(&self) -> &str {
        &self.name
    }

    fn start_at(&self) -> StartAt {
        StartAt::Beginning
    }

    fn stream_filter(&self) -> StreamFilter {
        if self.only_after_reacting && !self.reacted.load(Ordering::SeqCst) {
            return StreamFilter::all();
        }

        let staged = self
            .deaths_to_stage
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                left.checked_sub(1)
            })
            .is_ok();

        if staged {
            self.deaths.fetch_add(1, Ordering::SeqCst);
            panic!("the worker died preparing its read of the feed");
        }

        StreamFilter::all()
    }

    fn react(&self, event: &PersistedEvent<Self::Event>) -> Vec<Dispatch> {
        self.reacted.store(true, Ordering::SeqCst);
        echo(event)
    }
}

/// The reaction every policy in this file shares: echo a ping into its own
/// `-echo` stream.
fn echo(event: &PersistedEvent<ProbeEvent>) -> Vec<Dispatch> {
    match &event.data {
        ProbeEvent::Pinged { tag } => vec![Dispatch::to::<Probe>(
            ProbeUrn::new(format!("{tag}-echo")).unwrap(),
            ProbeCommand::Echo { tag: tag.clone() },
        )],
        _ => vec![],
    }
}

/// A worker that dies for a reason the per-event boundary cannot contain comes
/// back on its own, and the events that arrived while it was away are reacted to
/// rather than stepped over.
#[tokio::test]
async fn a_worker_that_dies_outside_the_reaction_is_restarted_postgres_test() {
    let deaths = Arc::new(AtomicUsize::new(0));
    let staged = Arc::new(AtomicUsize::new(1));

    let harness = {
        let deaths = Arc::clone(&deaths);
        let staged = Arc::clone(&staged);
        PolicyDaemonHarness::start("restarts", move |builder, policy| {
            builder
                .with_worker_supervision(quick_supervision())
                .register_policy(DiesOutsideTheReaction {
                    name: policy.to_string(),
                    deaths_to_stage: Arc::clone(&staged),
                    deaths: Arc::clone(&deaths),
                    only_after_reacting: true,
                    reacted: Arc::new(AtomicBool::new(false)),
                })
        })
        .await
    };

    // One event reacted to normally: the worker is alive and leading.
    let before = harness.ping("subject-1", "before").await;
    harness
        .await_dispatch_caused_by(before.global_position)
        .await;

    // The fault is now armed, and the worker dies on its next look at the feed.
    harness
        .observe("the worker to die", || async {
            (deaths.load(Ordering::SeqCst) == 1).then_some(())
        })
        .await;

    // Everything appended from here on is what a restart must not lose.
    let during = harness.ping("subject-2", "during").await;
    let after = harness.ping("subject-3", "after").await;

    let dispatched = harness
        .await_dispatch_caused_by(during.global_position)
        .await;
    assert_eq!(dispatched.event_type, "Echoed");
    harness
        .await_dispatch_caused_by(after.global_position)
        .await;
    let cursor = harness.await_cursor_at_least(after.global_position).await;
    assert!(
        cursor >= after.global_position,
        "cursor {cursor} must have passed every event the restarted worker read"
    );

    assert!(
        harness.dead_letters().await.is_empty(),
        "a worker death is a restart, not a parked reaction: {:?}",
        harness.dead_letters().await
    );
    assert!(
        harness.stopped_workers().is_empty(),
        "one death inside the budget must not stop the worker: {:?}",
        harness.stopped_workers()
    );

    harness.shutdown().await;
}

/// A worker that keeps dying is not restarted forever: the budget runs out, the
/// runner says so, and the policy next door is untouched by any of it.
#[tokio::test]
async fn a_worker_that_exhausts_its_budget_stops_and_is_reported_postgres_test() {
    let deaths = Arc::new(AtomicUsize::new(0));
    // Far more deaths than the budget allows, so the budget is what stops it.
    let staged = Arc::new(AtomicUsize::new(usize::MAX));

    let harness = {
        let deaths = Arc::clone(&deaths);
        let staged = Arc::clone(&staged);
        PolicyDaemonHarness::start("exhausts", move |builder, policy| {
            builder
                .with_worker_supervision(quick_supervision().max_restarts(2))
                .register_policy(DiesOutsideTheReaction {
                    name: policy.to_string(),
                    deaths_to_stage: Arc::clone(&staged),
                    deaths: Arc::clone(&deaths),
                    only_after_reacting: false,
                    reacted: Arc::new(AtomicBool::new(false)),
                })
                .register_policy_fn::<ProbeEvent, _>(neighbour_of(policy), StartAt::Beginning, echo)
        })
        .await
    };
    let neighbour = neighbour_of(harness.policy_name());

    let stopped = harness.await_stopped_worker(harness.policy_name()).await;
    assert_eq!(
        stopped.restarts, 2,
        "the worker must be restarted exactly the budget's worth of times"
    );

    // The neighbour leads, reacts and checkpoints as if nothing had happened.
    let ping = harness.ping("subject-1", "hello").await;
    let dispatched = harness
        .await_dispatch_caused_by_for(&neighbour, ping.global_position)
        .await;
    assert_eq!(dispatched.event_type, "Echoed");
    let neighbour_cursor = harness
        .observe("the neighbour's cursor to pass the ping", || async {
            harness
                .cursor_for(&neighbour)
                .await
                .filter(|stored| *stored >= ping.global_position)
        })
        .await;
    assert!(neighbour_cursor >= ping.global_position);

    assert_eq!(
        harness.stopped_workers().len(),
        1,
        "only the policy that kept dying stopped: {:?}",
        harness.stopped_workers()
    );
    // A stop is the absence of further deaths, and absence is the one thing no
    // `await_*` can observe: it has to be given time to be contradicted. Bounded
    // by several of this test's backoffs, so a worker still being restarted
    // would have died again inside it.
    let toll = deaths.load(Ordering::SeqCst);
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        deaths.load(Ordering::SeqCst),
        toll,
        "a stopped worker must not be restarted again"
    );

    harness.shutdown().await;
}

/// The name of the second policy a test registers alongside the one the harness
/// names, derived so the test can observe it after the fact.
fn neighbour_of(policy: &str) -> String {
    format!("{policy}_neighbour")
}
