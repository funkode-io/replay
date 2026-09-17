//! A consumer can read each worker's liveness from the daemon
//! (funkode-io/replay#187).
//!
//! Liveness answers "is this worker running"; [Policy status] answers "is this
//! Policy moving". These tests are about the first, so they assert nothing about
//! lag and nothing about cursors beyond what proves a worker is doing its job.
//!
//! The case worth the container is the Standby: two runners against one
//! database, one holding the advisory lock and one not. A replica that leads
//! nothing looks exactly like a fleet of dead policies to anyone inferring
//! liveness from cursor movement, which is the false alarm this axis exists to
//! prevent.
//!
//! [Policy status]: ../../CONTEXT.md#policy-status

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common::policy_harness::{PolicyDaemonHarness, Probe, ProbeCommand, ProbeEvent, ProbeUrn};
use replay_persistence::{
    Dispatch, Liveness, PersistedEvent, Policy, StartAt, StreamFilter, WorkerSupervision,
};

/// The reaction every policy in this file shares: echo a ping into its own
/// `-echo` stream, so a worker that is working is visible as an event.
fn echo(event: &PersistedEvent<ProbeEvent>) -> Vec<Dispatch> {
    match &event.data {
        ProbeEvent::Pinged { tag } => vec![Dispatch::to::<Probe>(
            ProbeUrn::new(format!("{tag}-echo")).unwrap(),
            ProbeCommand::Echo { tag: tag.clone() },
        )],
        _ => vec![],
    }
}

/// Two runners, one database: the one holding the advisory lock reports leading,
/// the one that does not reports standing by — and keeps reporting it, because a
/// Standby is healthy and deliberately idle rather than stopped.
#[tokio::test]
async fn one_replica_leads_and_the_other_stands_by_postgres_test() {
    let harness = PolicyDaemonHarness::start("liveness", |builder, policy| {
        builder.register_policy_fn::<ProbeEvent, _>(policy, StartAt::Beginning, echo)
    })
    .await;

    // The first runner takes the lock while it is the only one there.
    let leader = harness
        .await_liveness(harness.policy_name(), Liveness::Leading)
        .await;
    assert_eq!(leader.policy, harness.policy_name());
    harness
        .observe("the leader to report a poll of its feed", || async {
            harness
                .liveness()
                .into_iter()
                .find(|worker| worker.last_polled_at.is_some())
        })
        .await;

    let replica = harness.start_replica();
    let standby = harness
        .observe("the replica to stand by", || async {
            replica
                .liveness()
                .into_iter()
                .find(|worker| worker.liveness == Liveness::StandingBy)
        })
        .await;
    assert_eq!(standby.policy, harness.policy_name());
    assert_eq!(
        standby.last_polled_at, None,
        "a standby drives nothing, so it has no poll to report"
    );

    // The leader is still the leader, and the work goes to it.
    let ping = harness.ping("subject-1", "hello").await;
    harness.await_dispatch_caused_by(ping.global_position).await;

    assert_eq!(
        replica
            .liveness()
            .into_iter()
            .map(|worker| worker.liveness)
            .collect::<Vec<_>>(),
        vec![Liveness::StandingBy],
        "a replica that leads nothing stands by for as long as that lasts"
    );
    assert!(
        replica.escalations().is_empty(),
        "a standby is not a worker anybody has given up on"
    );
    assert_eq!(
        harness
            .liveness()
            .into_iter()
            .map(|worker| worker.liveness)
            .collect::<Vec<_>>(),
        vec![Liveness::Leading],
    );

    replica.shutdown().await;
    harness.shutdown().await;
}

/// A worker that keeps dying is restarting while it waits out its backoff and
/// stopped once the budget is spent — the two states supervision owns, read off
/// the daemon rather than inferred from the absence of work.
#[tokio::test]
async fn a_dying_worker_reports_restarting_and_then_stopped_postgres_test() {
    let deaths = Arc::new(AtomicUsize::new(0));
    // More deaths than the budget allows, so the budget is what stops it.
    let staged = Arc::new(AtomicUsize::new(usize::MAX));

    let harness = {
        let deaths = Arc::clone(&deaths);
        let staged = Arc::clone(&staged);
        PolicyDaemonHarness::start("dying", move |builder, policy| {
            builder
                .with_worker_supervision(
                    WorkerSupervision::default()
                        .max_restarts(1)
                        // Long enough that the backoff is observed rather than
                        // raced past, short enough that the test still waits on
                        // an outcome.
                        .initial_backoff(Duration::from_millis(500))
                        .max_backoff(Duration::from_millis(500)),
                )
                .register_policy(DiesOutsideTheReaction {
                    name: policy.to_string(),
                    deaths_to_stage: Arc::clone(&staged),
                    deaths: Arc::clone(&deaths),
                })
        })
        .await
    };

    harness
        .await_liveness(harness.policy_name(), Liveness::Restarting)
        .await;
    let stopped = harness
        .await_liveness(harness.policy_name(), Liveness::Stopped)
        .await;

    assert_eq!(
        stopped.policy,
        harness.policy_name(),
        "the worker that stopped is named, so an operator knows which Policy is down"
    );
    assert_eq!(
        harness.stopped_workers().len(),
        1,
        "the liveness reading and the stopped-worker record agree: {:?}",
        harness.stopped_workers()
    );

    harness.shutdown().await;
}

/// The Leader stamps its last poll on the cursor row, which is how a replica
/// that leads nothing — or a UI with only a connection string — reads liveness
/// it cannot hold in memory.
#[tokio::test]
async fn the_leader_stamps_its_last_poll_on_the_cursor_row_postgres_test() {
    let harness = PolicyDaemonHarness::start("heartbeat", |builder, policy| {
        builder.register_policy_fn::<ProbeEvent, _>(policy, StartAt::Beginning, echo)
    })
    .await;

    let first = harness
        .observe("the cursor row to carry a heartbeat", || async {
            harness.last_polled_at().await
        })
        .await;

    // It keeps moving: a stamp that never advances is a worker that stopped
    // polling, which is the whole signal.
    let later = harness
        .observe("the heartbeat to advance", || async {
            harness.last_polled_at().await.filter(|at| *at > first)
        })
        .await;
    assert!(later > first);

    harness.shutdown().await;
}

/// A database whose `policy_cursors` has no `last_polled_at` is a consumer who
/// has not run that migration, not a fault: the Policy reacts, the cursor
/// advances, and the daemon still reports the worker as leading.
#[tokio::test]
async fn a_schema_without_the_heartbeat_column_changes_nothing_postgres_test() {
    let mut harness = PolicyDaemonHarness::start("no_heartbeat", |builder, policy| {
        builder.register_policy_fn::<ProbeEvent, _>(policy, StartAt::Beginning, echo)
    })
    .await;

    harness.drop_heartbeat_column().await;
    // A daemon that never saw the column, which is what the consumer runs.
    harness.restart().await;

    let ping = harness.ping("subject-1", "hello").await;
    let dispatched = harness.await_dispatch_caused_by(ping.global_position).await;
    assert_eq!(dispatched.event_type, "Echoed");
    harness.await_cursor_at_least(ping.global_position).await;

    let leading = harness
        .await_liveness(harness.policy_name(), Liveness::Leading)
        .await;
    assert_eq!(leading.policy, harness.policy_name());
    harness
        .observe("the worker to report a poll of its feed", || async {
            harness
                .liveness()
                .into_iter()
                .find(|worker| worker.last_polled_at.is_some())
        })
        .await;
    // The in-memory poll instant is the daemon's own and owes the schema nothing.
    assert!(
        harness.dead_letters().await.is_empty(),
        "a missing heartbeat column parks nothing: {:?}",
        harness.dead_letters().await
    );
    assert!(
        harness.stopped_workers().is_empty(),
        "nor does it stop a worker: {:?}",
        harness.stopped_workers()
    );

    harness.shutdown().await;
}

/// A policy whose worker dies from *outside* the reaction — a panic as it
/// prepares its read of the feed, the shape of a panic in cursor I/O or the feed
/// read. The reaction itself is ordinary.
struct DiesOutsideTheReaction {
    name: String,
    /// How many more times the worker should die.
    deaths_to_stage: Arc<AtomicUsize>,
    /// How many deaths actually happened.
    deaths: Arc<AtomicUsize>,
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
