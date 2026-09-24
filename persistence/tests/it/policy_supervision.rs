//! A worker that dies is restarted, and one that keeps dying is escalated —
//! supervision, driven against a real daemon and a real database
//! (funkode-io/replay#185, funkode-io/replay#186).
//!
//! The death injected here is one no single event can be blamed for: a panic
//! raised while the worker prepares its read of the feed, the shape of a panic in
//! the lock manager, the listener or cursor I/O. A panic *inside* the reaction is
//! parked as a dead letter and never reaches supervision
//! ([ADR-0016](../../docs/adr/0016-panicking-reaction-parked-as-a-permanent-failure.md),
//! `policy_panic.rs`), and a dispatch that merely fails is retried or parked — so
//! no test here restarts a worker for either.
//!
//! Resuming from the last durable checkpoint is covered by
//! `policy_checkpoint_batch_crash_recovery_reprocesses_tail_postgres_test` in
//! `integration_tests.rs`. It cannot be re-asserted through a supervised restart:
//! every death supervision can inject lands between batches, where the cursor is
//! already durable.
//!
//! Every assertion is something an operator could make: the policy reacted
//! again, the cursor moved, the daemon names a worker it gave up on, the
//! consumer's escalation hook was told about it. Escalation is asserted through
//! the hook and never by letting the default exit the test process.

use crate::common;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common::policy_harness::{PolicyDaemonHarness, Probe, ProbeCommand, ProbeEvent, ProbeUrn};
use replay_persistence::{
    Dispatch, EscalationReason, ObservedEvent, Policy, PolicySettings, StartAt, WorkerSupervision,
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
/// `name` is read as the worker prepares each drain, before any event is
/// delivered, so a panic there stands in for the panics this ticket is about and
/// has nothing to do with the event that happens to be next. The reaction itself
/// is ordinary — it echoes every ping — so a worker that came back is visible by
/// it reacting again.
///
/// The test arms the fault only once the daemon is up: `name` is read once more
/// while the daemon wires its workers together, and a death there would be a
/// daemon that never started rather than a worker that died.
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
        self.maybe_die();
        &self.name
    }

    fn react(&self, event: &ObservedEvent<Self::Event>) -> Vec<Dispatch> {
        self.reacted.store(true, Ordering::SeqCst);
        echo(event)
    }
}

impl DiesOutsideTheReaction {
    fn maybe_die(&self) {
        if self.only_after_reacting && !self.reacted.load(Ordering::SeqCst) {
            return;
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
    }
}

/// The reaction every policy in this file shares: echo a ping into its own
/// `-echo` stream.
fn echo(event: &ObservedEvent<ProbeEvent>) -> Vec<Dispatch> {
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
    let staged = Arc::new(AtomicUsize::new(0));

    let harness = {
        let deaths = Arc::clone(&deaths);
        let staged = Arc::clone(&staged);
        PolicyDaemonHarness::start("restarts", move |builder, policy| {
            builder
                .with_worker_supervision(quick_supervision())
                .register_policy(
                    DiesOutsideTheReaction {
                        name: policy.to_string(),
                        deaths_to_stage: Arc::clone(&staged),
                        deaths: Arc::clone(&deaths),
                        only_after_reacting: true,
                        reacted: Arc::new(AtomicBool::new(false)),
                    },
                    PolicySettings::new().starting_at(StartAt::Beginning),
                )
        })
        .await
    };

    // Armed now the daemon is up, so one death is a worker's and not the daemon's.
    staged.store(1, Ordering::SeqCst);

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
    harness.await_passed(after.global_position).await;

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
    assert!(
        harness.escalations().is_empty(),
        "a restart within the budget is not the consumer's business: {:?}",
        harness.escalations()
    );

    harness.shutdown().await;
}

/// A worker that keeps dying is not restarted forever: the budget runs out, the
/// runner hands the policy to the consumer's escalation hook, and the policy next
/// door is untouched by any of it.
///
/// The hook the harness installs records rather than exits, which is the only way
/// a test can watch the escalation path — the default would take the test runner
/// with it (funkode-io/replay#186).
#[tokio::test]
async fn a_worker_that_exhausts_its_budget_escalates_to_the_consumer_postgres_test() {
    let deaths = Arc::new(AtomicUsize::new(0));
    let staged = Arc::new(AtomicUsize::new(0));

    let harness = {
        let deaths = Arc::clone(&deaths);
        let staged = Arc::clone(&staged);
        PolicyDaemonHarness::start("exhausts", move |builder, policy| {
            builder
                .with_worker_supervision(quick_supervision().max_restarts(2))
                .register_policy(
                    DiesOutsideTheReaction {
                        name: policy.to_string(),
                        deaths_to_stage: Arc::clone(&staged),
                        deaths: Arc::clone(&deaths),
                        only_after_reacting: false,
                        reacted: Arc::new(AtomicBool::new(false)),
                    },
                    PolicySettings::new().starting_at(StartAt::Beginning),
                )
                .register_policy_fn::<ProbeEvent, _>(
                    neighbour_of(policy),
                    PolicySettings::new().starting_at(StartAt::Beginning),
                    echo,
                )
        })
        .await
    };
    let neighbour = neighbour_of(harness.policy_name());
    // Far more deaths than the budget allows, so the budget is what stops it.
    staged.store(usize::MAX, Ordering::SeqCst);

    let escalated = harness.await_escalation(harness.policy_name()).await;
    assert_eq!(
        escalated.reason,
        EscalationReason::BudgetExhausted {
            restarts: 2,
            cause: "the worker died preparing its read of the feed".to_string(),
        },
        "the hook is told which policy is down and why"
    );

    // A hook that returns does not absolve the runner of saying so.
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
    harness
        .observe("the neighbour to pass the ping", || async {
            harness
                .has_passed_for(&neighbour, ping.global_position)
                .await
                .then_some(())
        })
        .await;

    assert_eq!(
        harness.escalations().len(),
        1,
        "a hook that returns is called once and not again: {:?}",
        harness.escalations()
    );
    assert_eq!(
        harness.stopped_workers().len(),
        1,
        "only the policy that kept dying stopped: {:?}",
        harness.stopped_workers()
    );
    harness.shutdown().await;
}

/// The name of the second policy a test registers alongside the one the harness
/// names, derived so the test can observe it after the fact.
fn neighbour_of(policy: &str) -> String {
    format!("{policy}_neighbour")
}
