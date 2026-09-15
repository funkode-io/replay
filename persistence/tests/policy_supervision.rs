//! A worker that dies is restarted — supervision, driven against a real daemon
//! and a real database (funkode-io/replay#185).
//!
//! The deaths here are the ones the per-event boundary cannot contain: a panic
//! raised while the worker prepares its read of the feed, and a panic raised
//! from the command a reaction dispatched. Both unwind the worker task, which is
//! exactly how a panic in the lock manager, the listener or cursor I/O would end
//! it, and none of them is a panic inside `react` (that boundary is #183).
//!
//! Every assertion is something an operator could make: the policy reacted
//! again, the cursor moved, the daemon names a worker it gave up on. The only
//! in-process state a test touches is the fault it injected itself.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
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
/// `stream_filter` is called by the worker as it prepares its read of the feed,
/// before any event is delivered, so a panic there stands in for the panics this
/// ticket is about: the lock manager, the listener, cursor I/O. The reaction
/// itself is ordinary — it echoes every ping — so a restarted worker is visible
/// by it reacting again.
struct DiesBeforeReadingTheFeed {
    name: String,
    /// How many more times the worker should die. Decremented as it dies, so a
    /// test can ask for exactly one death, or for more than the budget allows.
    deaths_to_stage: Arc<AtomicUsize>,
    /// How many deaths actually happened, so a test can prove its fault fired
    /// rather than assume it.
    deaths: Arc<AtomicUsize>,
}

impl Policy for DiesBeforeReadingTheFeed {
    type Event = ProbeEvent;

    fn name(&self) -> &str {
        &self.name
    }

    fn start_at(&self) -> StartAt {
        StartAt::Beginning
    }

    fn stream_filter(&self) -> StreamFilter {
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
/// back on its own and carries on processing events.
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
                .register_policy(DiesBeforeReadingTheFeed {
                    name: policy.to_string(),
                    deaths_to_stage: staged,
                    deaths,
                })
        })
        .await
    };

    let ping = harness.ping("subject-1", "hello").await;

    let dispatched = harness.await_dispatch_caused_by(ping.global_position).await;
    assert_eq!(dispatched.event_type, "Echoed");
    harness.await_cursor_at_least(ping.global_position).await;

    assert_eq!(
        deaths.load(Ordering::SeqCst),
        1,
        "the test's fault must have fired: without a death there is no restart to observe"
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
                .register_policy(DiesBeforeReadingTheFeed {
                    name: policy.to_string(),
                    deaths_to_stage: staged,
                    deaths,
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

/// A worker that dies after one of an event's commands has committed, but
/// before its cursor is durable, resumes from the last checkpoint: the event it
/// died on is delivered again rather than stepped over.
#[tokio::test]
async fn a_restarted_worker_resumes_from_its_checkpoint_and_skips_nothing_postgres_test() {
    // The fault fires once: the second delivery of the same event must get
    // through, or the worker would simply die again.
    let explosions = Arc::new(AtomicUsize::new(1));

    let harness = {
        let explosions = Arc::clone(&explosions);
        PolicyDaemonHarness::start("resumes", move |builder, policy| {
            builder
                .with_worker_supervision(quick_supervision())
                .register_policy_fn::<ProbeEvent, _>(policy, StartAt::Beginning, move |event| {
                    match &event.data {
                        ProbeEvent::Pinged { tag } if tag == "boom" => {
                            let mut dispatches = echo(event);
                            let staged = explosions
                                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |left| {
                                    left.checked_sub(1)
                                })
                                .is_ok();
                            if staged {
                                // Runs *after* the echo has committed, so the
                                // worker dies mid-event with work already done
                                // and nothing checkpointed.
                                dispatches.push(Dispatch::to::<Probe>(
                                    ProbeUrn::new("fuse").unwrap(),
                                    ProbeCommand::Explode {
                                        message: "the worker died handling a dispatched command"
                                            .to_string(),
                                    },
                                ));
                            }
                            dispatches
                        }
                        _ => echo(event),
                    }
                })
        })
        .await
    };

    let boom = harness.ping("subject-1", "boom").await;

    // Two echoes caused by the same event: one from before the death, one from
    // the re-delivery a checkpoint-resumed worker performs. At-least-once is the
    // contract; skipping the event would be the bug.
    let echoes = harness
        .observe(
            "the event the worker died on to be delivered again",
            || async {
                let caused: Vec<_> = harness
                    .dispatches()
                    .await
                    .into_iter()
                    .filter(|d| d.caused_by_position == boom.global_position)
                    .collect();
                (caused.len() >= 2).then_some(caused)
            },
        )
        .await;
    assert!(echoes.iter().all(|d| d.event_type == "Echoed"));
    assert_eq!(
        explosions.load(Ordering::SeqCst),
        0,
        "the test's fault must have fired"
    );

    // And the policy is running normally afterwards: past the event that killed
    // it, with the next one reacted to and nothing parked.
    harness.await_cursor_at_least(boom.global_position).await;
    let next = harness.ping("subject-2", "hello").await;
    harness.await_dispatch_caused_by(next.global_position).await;
    assert!(
        harness.dead_letters().await.is_empty(),
        "a worker death is a restart, not a parked reaction: {:?}",
        harness.dead_letters().await
    );
    assert!(harness.stopped_workers().is_empty());

    harness.shutdown().await;
}

/// The name of the second policy a test registers alongside the one the harness
/// names, derived so the test can observe it after the fact.
fn neighbour_of(policy: &str) -> String {
    format!("{policy}_neighbour")
}
