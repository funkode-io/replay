//! A Policy finds a write the sweep passed from the transaction that committed it
//! (ADR-0030), rather than waiting for the full comparison.

use std::time::Duration;

use crate::common;
use common::policy_harness::{PolicyDaemonHarness, ProbeEvent};
use replay_persistence::{PolicySettings, StartAt};

#[tokio::test]
async fn a_write_is_delivered_on_the_poll_after_its_transaction_commits_postgres_test() {
    let mut harness = PolicyDaemonHarness::start("late_write", |builder, policy| {
        builder.register_policy_fn::<ProbeEvent, _>(
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
        builder.register_policy_fn::<ProbeEvent, _>(
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
