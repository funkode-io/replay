//! The online lane ignores a bulk-lane event (funkode-io/replay#301, ADR-0034).
//!
//! An event appended on the bulk lane reaches Policies through a work queue of its own;
//! the online feed must never deliver it. What is asserted is what a consumer and an
//! operator can see: the commands the Policy dispatched, the place it holds in each
//! stream, and whether its status reads as caught up. The per-source tests — sweep,
//! late-write lookup, stream read, reconciliation — are in `policy_runner::bulk_lane_tests`.

use crate::common;

use common::policy_harness::{
    reconciliation_timeout, AppendedEvent, PolicyDaemonHarness, Probe, ProbeCommand, ProbeEvent,
    ProbeUrn,
};
use replay_persistence::{Dispatch, ObservedEvent, PolicySettings, StartAt};

/// Echo every ping into a probe stream named after its tag, so what the Policy delivered
/// reads as the commands it dispatched.
fn echo_back(event: &ObservedEvent<ProbeEvent>) -> Vec<Dispatch> {
    match &event.data {
        ProbeEvent::Pinged { tag } => vec![Dispatch::to::<Probe>(
            ProbeUrn::new(format!("{tag}-echo")).expect("a valid NSS"),
            ProbeCommand::Echo { tag: tag.clone() },
        )],
        ProbeEvent::Echoed { .. } => vec![],
    }
}

async fn start(label: &str) -> PolicyDaemonHarness {
    PolicyDaemonHarness::start(label, |builder, policy| {
        builder.register_policy_fn::<ProbeEvent, _>(
            policy,
            PolicySettings::new().starting_at(StartAt::Beginning),
            echo_back,
        )
    })
    .await
}

/// Whether the Policy dispatched anything in reaction to `event`.
async fn reacted_to(harness: &PolicyDaemonHarness, event: &AppendedEvent) -> bool {
    harness
        .dispatches()
        .await
        .iter()
        .any(|dispatched| dispatched.caused_by_event_id == event.event_id)
}

/// A bulk event between two online ones in the same stream: the Policy reacts to both
/// online events, never to the bulk one, and its place in the stream passes all three —
/// as it passes a row its stream filter excludes (ADR-0013).
#[tokio::test]
async fn a_bulk_event_is_passed_and_never_delivered_postgres_test() {
    let harness = start("bulk_between_online").await;

    let first = harness.ping("mixed", "first").await;
    let bulk = harness.bulk_ping("mixed", "bulk").await;
    let last = harness.ping("mixed", "last").await;

    harness
        .await_dispatch_caused_by(first.global_position)
        .await;
    harness.await_dispatch_caused_by(last.global_position).await;
    harness.await_passed(last.global_position).await;

    assert!(
        harness.has_passed(bulk.global_position).await,
        "the place advances past the bulk event"
    );
    assert_eq!(harness.place_in(&last.stream_id).await, Some(3));
    assert!(
        !reacted_to(&harness, &bulk).await,
        "an online Policy never reacts to a bulk-lane event"
    );

    harness.shutdown().await;
}

/// A stream whose only new events are bulk: nothing online nominates it, the
/// reconciliation finds it behind, the poll that reads it passes every bulk event and
/// fires nothing, and the Policy then reads as caught up — the stream is not owed again.
#[tokio::test]
async fn a_caught_up_policy_settles_past_a_bulk_only_stream_postgres_test() {
    let harness = start("bulk_only_stream").await;

    // Caught up on an online stream first, so what follows is a Policy with nothing owed.
    let online = harness.ping("online", "online").await;
    harness
        .await_dispatch_caused_by(online.global_position)
        .await;
    harness.await_passed(online.global_position).await;

    let mut imported = Vec::new();
    for n in 0..3 {
        imported.push(harness.bulk_ping("imported", &format!("bulk-{n}")).await);
    }
    let stream_id = imported[0].stream_id.clone();

    harness
        .observe_within(
            "the Policy's place to pass the bulk-only stream",
            reconciliation_timeout(),
            || async {
                (harness.place_in(&stream_id).await == Some(imported.len() as i64)).then_some(())
            },
        )
        .await;
    let status = harness
        .observe("the Policy to read as caught up", || async {
            harness
                .status()
                .await
                .filter(|status| status.lag == 0 && status.streams_behind == 0)
        })
        .await;
    assert_eq!((status.lag, status.streams_behind), (0, 0));

    for event in &imported {
        assert!(
            !reacted_to(&harness, event).await,
            "no bulk event is delivered to the online Policy"
        );
    }
    assert_eq!(
        harness.dispatches().await.len(),
        1,
        "the only reaction is to the online ping"
    );

    harness.shutdown().await;
}
