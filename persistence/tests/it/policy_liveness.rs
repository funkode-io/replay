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
//! [Policy status]: ../../../CONTEXT.md#policy-status

use crate::common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common::policy_harness::{
    PolicyDaemonHarness, Probe, ProbeCommand, ProbeEvent, ProbeUrn, DAEMON_HEARTBEAT_CADENCE,
    PRIMARY_REPLICA,
};
use replay_persistence::{
    Dispatch, Liveness, ObservedEvent, Policy, PolicySettings, StartAt, WorkerSupervision,
};

/// The reaction every policy in this file shares: echo a ping into its own
/// `-echo` stream, so a worker that is working is visible as an event.
fn echo(event: &ObservedEvent<ProbeEvent>) -> Vec<Dispatch> {
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
        builder.register_policy_fn::<ProbeEvent, _>(
            policy,
            PolicySettings::new().starting_at(StartAt::Beginning),
            echo,
        )
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
    let staged = Arc::new(AtomicUsize::new(0));

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
                .register_policy(
                    DiesOutsideTheReaction {
                        name: policy.to_string(),
                        deaths_to_stage: Arc::clone(&staged),
                        deaths: Arc::clone(&deaths),
                    },
                    PolicySettings::new().starting_at(StartAt::Beginning),
                )
        })
        .await
    };

    // More deaths than the budget allows, so the budget is what stops it. Armed now the
    // daemon is up, so the deaths counted are the worker's.
    staged.store(usize::MAX, Ordering::SeqCst);

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

/// The Leader beats on its own cadence: the row carries a fresh beat, the state
/// its supervisor knows, the last completed poll and the replica that wrote it —
/// which is everything a consumer outside the process needs.
#[tokio::test]
async fn the_leader_beats_on_the_cursor_row_postgres_test() {
    let harness = PolicyDaemonHarness::start("heartbeat", |builder, policy| {
        builder.register_policy_fn::<ProbeEvent, _>(
            policy,
            PolicySettings::new().starting_at(StartAt::Beginning),
            echo,
        )
    })
    .await;

    let first = harness
        .await_heartbeat("a beat carrying a completed poll", |beat| {
            beat.last_polled_at.is_some()
        })
        .await;
    assert_eq!(first.liveness, "Leading");
    assert_eq!(
        first.led_by.as_deref(),
        Some(PRIMARY_REPLICA),
        "the beat names the replica whose logs an operator would read"
    );

    // It keeps beating: a beat that never advances is a Leader that is gone,
    // which is the whole signal.
    harness
        .await_heartbeat("the beat to advance", |beat| beat.beat_at > first.beat_at)
        .await;

    harness.shutdown().await;
}

/// The beat does not stop for work. A reaction that hangs holds its worker, so a
/// stamp written on the poll path would go silent exactly here — and a consumer
/// would read a hang as a dead process. Instead the beat keeps arriving while
/// `last_polled_at` ages, which is the pair that tells wedged from gone.
#[tokio::test]
async fn the_beat_keeps_arriving_while_a_reaction_hangs_postgres_test() {
    let harness = PolicyDaemonHarness::start("hanging", |builder, policy| {
        builder.register_policy_fn::<ProbeEvent, _>(
            policy,
            PolicySettings::new().starting_at(StartAt::Beginning),
            |event| {
                match &event.data {
                    // Longer than this test needs: the reaction is still running when
                    // every assertion below is made.
                    ProbeEvent::Pinged { .. } => vec![Dispatch::to::<Probe>(
                        ProbeUrn::new("sleeper").unwrap(),
                        ProbeCommand::Sleep { millis: 60_000 },
                    )],
                    _ => vec![],
                }
            },
        )
    })
    .await;

    harness
        .await_heartbeat("a beat before the hang", |beat| {
            beat.last_polled_at.is_some()
        })
        .await;

    // From here the worker takes up a reaction that will not come back.
    harness.ping("subject-1", "hang").await;

    // The poll stamp standing still is how the hang is observable at all: beats
    // keep coming, and the poll they carry stops moving with them. It is
    // recomputed from an age on every beat, so it wobbles by the round trip
    // rather than being byte-identical — what matters is that it does not keep
    // pace with the beats.
    let window = DAEMON_HEARTBEAT_CADENCE * 5;
    let half_window = chrono::TimeDelta::from_std(window / 2).unwrap();
    let (before, after) = harness
        .observe("beats that carry no newer poll", || async {
            let before = harness.heartbeat().await?;
            tokio::time::sleep(window).await;
            let after = harness.heartbeat().await?;
            let polled_moved = after.last_polled_at? - before.last_polled_at?;
            (after.beat_at - before.beat_at >= half_window && polled_moved < half_window)
                .then_some((before, after))
        })
        .await;

    assert!(
        after.beat_at - before.beat_at >= half_window,
        "the beat kept its cadence through the hang"
    );
    assert_eq!(
        after.liveness, "Leading",
        "a wedged worker is leading, not stopped: nothing has given up on it"
    );
    assert_eq!(
        harness.places().await,
        Vec::new(),
        "and it really is wedged: the reaction never returned, so nothing was checkpointed"
    );

    // Nothing can join a worker that is still inside its reaction.
    harness.abandon();
}

/// A Leader whose worker is down for good keeps beating, saying so. The replica
/// still holds the advisory lock, so no standby takes over — `Stopped` against a
/// fresh beat is exactly the half-dead state that went unseen in
/// funkode-io/replay#164, and silence could not express it.
#[tokio::test]
async fn a_stopped_leader_keeps_beating_and_says_it_is_stopped_postgres_test() {
    let deaths = Arc::new(AtomicUsize::new(0));
    let staged = Arc::new(AtomicUsize::new(0));

    let harness = {
        let deaths = Arc::clone(&deaths);
        let staged = Arc::clone(&staged);
        PolicyDaemonHarness::start("stopped_beat", move |builder, policy| {
            builder
                .with_worker_supervision(
                    WorkerSupervision::default()
                        .max_restarts(1)
                        .initial_backoff(Duration::from_millis(20))
                        .max_backoff(Duration::from_millis(20)),
                )
                .register_policy(
                    DiesOutsideTheReaction {
                        name: policy.to_string(),
                        deaths_to_stage: Arc::clone(&staged),
                        deaths: Arc::clone(&deaths),
                    },
                    PolicySettings::new().starting_at(StartAt::Beginning),
                )
        })
        .await
    };

    staged.store(usize::MAX, Ordering::SeqCst);

    let stopped = harness
        .await_heartbeat("a beat reporting the worker stopped", |beat| {
            beat.liveness == "Stopped"
        })
        .await;

    harness
        .await_heartbeat("the beat to keep arriving after the stop", |beat| {
            beat.beat_at > stopped.beat_at && beat.liveness == "Stopped"
        })
        .await;

    harness.shutdown().await;
}

/// A standby writes nothing. One row per policy is shared by every replica, so a
/// standby that beat would overwrite the leader's beat with its own idleness.
#[tokio::test]
async fn a_standby_replica_does_not_write_the_beat_postgres_test() {
    let harness = PolicyDaemonHarness::start("standby_beat", |builder, policy| {
        builder.register_policy_fn::<ProbeEvent, _>(
            policy,
            PolicySettings::new().starting_at(StartAt::Beginning),
            echo,
        )
    })
    .await;

    harness
        .await_heartbeat("the leader's first beat", |beat| {
            beat.led_by.as_deref() == Some(PRIMARY_REPLICA)
        })
        .await;

    let replica = harness.start_replica();
    harness
        .observe("the replica to stand by", || async {
            replica
                .liveness()
                .into_iter()
                .find(|worker| worker.liveness == Liveness::StandingBy)
        })
        .await;

    // Several beats later, the row is still the leader's.
    let seen = harness.heartbeat().await.expect("a beat must exist");
    let after = harness
        .await_heartbeat("three more beats with the replica running", |beat| {
            beat.beat_at > seen.beat_at
        })
        .await;
    assert_eq!(
        after.led_by.as_deref(),
        Some(PRIMARY_REPLICA),
        "a standby must not claim a row it does not lead"
    );
    assert_eq!(after.liveness, "Leading");

    replica.shutdown().await;
    harness.shutdown().await;
}

/// A database whose `policy_cursors` has none of the heartbeat columns is a
/// consumer who has not run that migration, not a fault: the Policy reacts, the
/// cursor advances, and the daemon still reports the worker as leading.
#[tokio::test]
async fn a_schema_without_the_heartbeat_columns_changes_nothing_postgres_test() {
    let mut harness = PolicyDaemonHarness::start("no_heartbeat", |builder, policy| {
        builder.register_policy_fn::<ProbeEvent, _>(
            policy,
            PolicySettings::new().starting_at(StartAt::Beginning),
            echo,
        )
    })
    .await;

    harness.drop_heartbeat_column().await;
    // A daemon that never saw the columns, which is what the consumer runs.
    harness.restart().await;

    let ping = harness.ping("subject-1", "hello").await;
    let dispatched = harness.await_dispatch_caused_by(ping.global_position).await;
    assert_eq!(dispatched.event_type, "Echoed");
    harness.await_passed(ping.global_position).await;

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
    // The in-memory axis is the daemon's own and owes the schema nothing.
    assert!(
        harness.dead_letters().await.is_empty(),
        "missing heartbeat columns park nothing: {:?}",
        harness.dead_letters().await
    );
    assert!(
        harness.stopped_workers().is_empty(),
        "nor do they stop a worker: {:?}",
        harness.stopped_workers()
    );

    harness.shutdown().await;
}

/// One policy's contended row does not silence the rest. Every led policy is
/// beaten in one statement, so a row somebody else holds — an operator part-way
/// through a cursor move — would abort the lot and make this replica look
/// leaderless for every policy it runs. The held row is skipped instead.
#[tokio::test]
async fn a_row_somebody_else_holds_costs_only_that_policy_a_beat_postgres_test() {
    let harness = PolicyDaemonHarness::start("contended", |builder, policy| {
        builder
            .register_policy_fn::<ProbeEvent, _>(
                policy,
                PolicySettings::new().starting_at(StartAt::Beginning),
                echo,
            )
            .register_policy_fn::<ProbeEvent, _>(
                neighbour_of(policy),
                PolicySettings::new().starting_at(StartAt::Beginning),
                echo,
            )
    })
    .await;
    let neighbour = neighbour_of(harness.policy_name());

    // Both are beating before anything is held.
    harness
        .await_heartbeat("the contended policy's first beat", |beat| {
            beat.led_by.as_deref() == Some(PRIMARY_REPLICA)
        })
        .await;
    let neighbour_before = harness
        .observe("the neighbour's first beat", || async {
            harness.heartbeat_for(&neighbour).await
        })
        .await;

    let held = harness.hold_cursor_row(harness.policy_name()).await;

    let neighbour_after = harness
        .observe(
            "the neighbour to beat while the other row is held",
            || async {
                harness
                    .heartbeat_for(&neighbour)
                    .await
                    .filter(|beat| beat.beat_at > neighbour_before.beat_at)
            },
        )
        .await;
    assert_eq!(neighbour_after.liveness, "Leading");
    assert_eq!(neighbour_after.led_by.as_deref(), Some(PRIMARY_REPLICA));

    held.release().await;

    // And the skipped policy comes back on its own, with no restart and no
    // intervention.
    harness
        .await_heartbeat("the held policy to beat again once released", |beat| {
            beat.beat_at > neighbour_after.beat_at
        })
        .await;

    harness.shutdown().await;
}

/// A replica that takes over a policy does not inherit the previous leader's
/// poll. The row describes the worker that is leading now, so until that worker
/// completes a poll it reports none — "leading, nothing finished yet" rather than
/// a poll it never made.
#[tokio::test]
async fn a_new_leader_reports_no_poll_until_it_makes_one_postgres_test() {
    let mut harness = PolicyDaemonHarness::start("failover_poll", |builder, policy| {
        // Slow enough that a beat lands between taking leadership and finishing
        // the first poll, which is the window under test.
        builder
            .register_policy_fn::<ProbeEvent, _>(
                policy,
                PolicySettings::new().starting_at(StartAt::Beginning),
                echo,
            )
            .without_notifications()
    })
    .await;

    let led = harness
        .await_heartbeat("a beat carrying a completed poll", |beat| {
            beat.last_polled_at.is_some()
        })
        .await;
    assert_eq!(led.led_by.as_deref(), Some(PRIMARY_REPLICA));

    // A restart is a new process as far as the registry is concerned: the
    // worker that comes back has completed no poll of its own.
    harness.restart().await;

    let after = harness
        .observe("a beat from the daemon that took over", || async {
            harness
                .heartbeat()
                .await
                .filter(|beat| beat.beat_at > led.beat_at)
        })
        .await;
    assert!(
        after.last_polled_at.is_none() || after.last_polled_at > led.last_polled_at,
        "a new leader reports its own poll or none, never the one it inherited: {after:?}"
    );

    harness.shutdown().await;
}

/// The name of the second policy a test registers alongside the one the harness
/// names, derived so the test can observe it after the fact.
fn neighbour_of(policy: &str) -> String {
    format!("{policy}_neighbour")
}

/// A policy whose worker dies from *outside* the reaction — a panic as it
/// prepares its drain, the shape of a panic in cursor I/O or the feed read. The
/// reaction itself is ordinary.
///
/// `name` is the seam: the worker reads it on every drain, before any event is
/// delivered. The test arms the fault only once the daemon is up, since `name` is
/// read once more while the daemon wires its workers together, and a death there
/// would be a daemon that never started.
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

        &self.name
    }

    fn react(&self, event: &ObservedEvent<Self::Event>) -> Vec<Dispatch> {
        echo(event)
    }
}
