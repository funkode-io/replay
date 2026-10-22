//! A Policy finds a write the sweep passed from the transaction that committed it
//! (ADR-0030), rather than waiting for the full comparison.

use std::time::{Duration, Instant};

use crate::common;
use common::policy_harness::{PolicyDaemonHarness, ProbeEvent};
use replay_persistence::{PolicySettings, StartAt};

/// The production safety net: far enough away that it cannot be what finds a late write.
const SAFETY_NET: Duration = Duration::from_secs(300);

#[tokio::test]
async fn a_write_is_delivered_on_the_poll_after_its_transaction_commits_postgres_test() {
    let mut harness = PolicyDaemonHarness::start("late_write", |builder, policy| {
        builder
            .reconcile_every(SAFETY_NET)
            .register_policy_fn::<ProbeEvent, _>(
                policy,
                PolicySettings::new().starting_at(StartAt::Beginning),
                |_| Vec::new(),
            )
    })
    .await;

    let seed = harness.ping("seed", "seed").await;
    harness.await_passed(seed.global_position).await;
    harness.move_place_to(&seed.stream_id, 0).await;
    harness.restart().await;
    harness.await_passed(seed.global_position).await;

    let held = harness.hold_a_ping_open("slow", "held").await;
    let position = held.global_position;
    for n in 0..3 {
        let overtaking = harness
            .ping(&format!("quick-{n}"), &format!("overtook-{n}"))
            .await;
        harness.await_passed(overtaking.global_position).await;
        let swept_past = overtaking.global_position;
        harness
            .observe("the sweep to pass the held write", || async {
                (harness.swept_through().await? >= swept_past).then_some(())
            })
            .await;
    }

    assert!(
        !harness.has_passed(position).await,
        "the sweep has passed the held write without delivering it"
    );

    held.commit().await;
    harness
        .observe_within(
            "the late write to be delivered before the safety-net comparison",
            Duration::from_secs(1),
            || async { harness.has_passed(position).await.then_some(()) },
        )
        .await;

    harness.shutdown().await;
}

#[tokio::test]
async fn an_aborted_transaction_nominates_no_stream_and_does_not_delay_later_writes_postgres_test()
{
    let harness = PolicyDaemonHarness::start("aborted_write", |builder, policy| {
        builder
            .reconcile_every(SAFETY_NET)
            .register_policy_fn::<ProbeEvent, _>(
                policy,
                PolicySettings::new().starting_at(StartAt::Beginning),
                |_| Vec::new(),
            )
    })
    .await;

    let aborted = harness.hold_a_ping_open("aborted", "discarded").await;
    aborted.abort().await;
    let later = harness.ping("after-abort", "visible").await;

    harness.await_passed(later.global_position).await;
    assert!(
        harness
            .places()
            .await
            .iter()
            .all(|(stream, _)| stream != "urn:probe:aborted"),
        "an aborted append creates no committed stream nomination"
    );

    harness.shutdown().await;
}

/// A write in flight when its leader stops, and committed before the next one starts, is
/// below that leader's sweep from its first poll and in no snapshot it ever takes: the
/// late-write state that knew about it went with the old leader. The new leader's first
/// comparison, due within its start-up window, is what finds it (ADR-0030, #269).
#[tokio::test]
async fn a_write_in_flight_across_a_leader_change_is_delivered_postgres_test() {
    let mut harness = PolicyDaemonHarness::start("late_write_failover", |builder, policy| {
        builder
            .reconcile_every(SAFETY_NET)
            .register_policy_fn::<ProbeEvent, _>(
                policy,
                PolicySettings::new().starting_at(StartAt::Beginning),
                |_| Vec::new(),
            )
    })
    .await;

    let held = harness.hold_a_ping_open("in-flight", "held").await;
    let position = held.global_position;
    let overtaking = harness.ping("overtaking", "overtook").await;
    harness.await_passed(overtaking.global_position).await;
    let swept_past = overtaking.global_position;
    harness
        .observe("the sweep to pass the held write", || async {
            (harness.swept_through().await? >= swept_past).then_some(())
        })
        .await;

    harness.stop().await;
    held.commit().await;
    harness.resume();

    harness
        .observe_within(
            "the new leader to deliver the write in flight across the election",
            Duration::from_secs(2),
            || async { harness.has_passed(position).await.then_some(()) },
        )
        .await;

    harness.shutdown().await;
}

/// A poll that fails after writing its sweep keeps the late-write tail it computed
/// against that sweep. Every poll fails while the places are away. One sweeps past a
/// writer above its `xmax`, and is held at the write of its sweep while that writer
/// commits — so the next poll sees it ended, below a sweep that has passed it. Were the
/// failed poll to have kept an older tail, only the safety net — far away here — would
/// find the write.
#[tokio::test]
async fn a_poll_that_fails_after_its_sweep_keeps_its_tail_postgres_test() {
    let mut harness = PolicyDaemonHarness::start("late_write_failed_poll", |builder, policy| {
        builder
            .reconcile_every(SAFETY_NET)
            .register_policy_fn::<ProbeEvent, _>(
                policy,
                PolicySettings::new().starting_at(StartAt::Beginning),
                |_| Vec::new(),
            )
    })
    .await;

    // A place moved back is found only by a comparison. Moved while no leader runs, its
    // redelivery proves the new leader's start-up comparison has run: the next is the
    // safety net's, far away.
    let seed = harness.ping("seed", "seed").await;
    harness.await_passed(seed.global_position).await;
    harness.stop().await;
    harness.move_place_to(&seed.stream_id, 0).await;
    harness.resume();
    harness.await_passed(seed.global_position).await;
    // The comparison's poll is not over when the place is written: it still records its
    // rotation on the cursor row held below. Held mid-poll, it would wait there, and no
    // poll would come to sweep (funkode-io/replay#283).
    harness.await_a_poll_finished_after(Instant::now()).await;

    harness.break_places().await;
    let cursor_row = harness.hold_cursor_row(harness.policy_name()).await;
    let older = harness.take_an_id().await;
    let younger = harness.hold_a_ping_open("younger", "held").await;
    let older = harness.ping_in(older, "older", "overtook").await;
    older.commit().await;
    cursor_row.await_a_sweep_waiting().await;
    let position = younger.global_position;
    younger.commit().await;
    cursor_row.release().await;
    let swept_past = position;
    harness
        .observe("the failed poll to write its sweep", || async {
            (harness.swept_through().await? > swept_past).then_some(())
        })
        .await;
    harness.mend_places().await;

    harness
        .observe_within(
            "the write the failing polls swept past to be delivered",
            Duration::from_secs(2),
            || async { harness.has_passed(position).await.then_some(()) },
        )
        .await;

    harness.shutdown().await;
}
