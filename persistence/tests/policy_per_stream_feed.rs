//! A Policy tracks its position per stream, not over a global order
//! (funkode-io/replay#195).
//!
//! Every test here is a shape that a feed ordered by `global_position` gets wrong, and
//! that a feed ordered per stream gets right without having to detect anything: a write
//! held open in one stream, a position burned by a write that failed, a stream that goes
//! quiet. What they assert is delivery — what the Policy reacted to and in what order —
//! because that is the only thing a consumer of this library can see.

mod common;

use common::policy_harness::{PolicyDaemonHarness, Probe, ProbeCommand, ProbeEvent, ProbeUrn};
use replay_persistence::PersistedEvent;
use replay_persistence::{Dispatch, StartAt};

/// A Policy that echoes every ping, so what it has delivered is readable as the commands
/// it dispatched. Each echo goes to a stream of its own, named after the tag, so a
/// reaction never appends to a stream a test is holding a write open on.
fn echo_back(event: &PersistedEvent<ProbeEvent>) -> Vec<Dispatch> {
    match &event.data {
        ProbeEvent::Pinged { tag } => vec![Dispatch::to::<Probe>(
            ProbeUrn::new(format!("{tag}-echo")).expect("a valid NSS"),
            ProbeCommand::Echo { tag: tag.clone() },
        )],
        ProbeEvent::Echoed { .. } => vec![],
    }
}

/// The property the watermark could not give: a write that is slow, or stuck, or simply
/// large, delays the stream it is writing to and nothing else.
///
/// Under a global order this is the wedge of funkode-io/replay#164 — the held write owns
/// a position below the quiet stream's event, so every Policy stops at it until the write
/// ends. Per stream there is nothing in the way: the quiet stream's own sequence is
/// complete.
#[tokio::test]
async fn a_write_held_open_in_one_stream_does_not_delay_another_postgres_test() {
    let harness = PolicyDaemonHarness::start("held_write", |builder, policy| {
        builder.register_policy_fn::<ProbeEvent, _>(policy, StartAt::Beginning, echo_back)
    })
    .await;

    // A write that has taken its position and will not end for the rest of the test.
    let held = harness.hold_a_ping_open("slow-import", "held").await;

    // Appended after it, so it sits above the held position in the log.
    let after = harness.ping("elsewhere", "after").await;
    assert!(
        after.global_position > held.global_position,
        "the point of the test is an event the held write sits in front of"
    );

    harness
        .await_dispatch_caused_by(after.global_position)
        .await;

    held.abort().await;
    harness.shutdown().await;
}

/// The incident (funkode-io/replay#164): a write that failed took a position with it, and
/// every Policy stopped in front of the number it left behind.
///
/// `nextval` is not transactional, so the number is gone for good — nothing will ever
/// carry it. A Policy that reads no global order never looks at it.
#[tokio::test]
async fn a_position_burned_by_a_failed_write_delays_nobody_postgres_test() {
    let harness = PolicyDaemonHarness::start("burned", |builder, policy| {
        builder.register_policy_fn::<ProbeEvent, _>(policy, StartAt::Beginning, echo_back)
    })
    .await;

    // An append that started and failed: the place it took in its stream is handed back,
    // the position it drew from the sequence is not.
    harness
        .hold_a_ping_open("rolled-back", "never")
        .await
        .abort()
        .await;

    let after = harness.ping("carries-on", "after").await;
    harness
        .await_dispatch_caused_by(after.global_position)
        .await;

    harness.shutdown().await;
}

/// A write in flight is delivered when it commits, and not before: the events either side
/// of it do not wait for it, and it is not lost for having been overtaken.
///
/// The Policy's search sweeps past the held write's position while it is open — that is
/// the design, and it is what makes the event before this test's held one arrive
/// immediately. Finding it afterwards is the reconciliation's job, so this test waits for
/// one cadence of it.
#[tokio::test]
async fn an_overtaken_write_is_delivered_when_it_commits_postgres_test() {
    let harness = PolicyDaemonHarness::start("overtaken", |builder, policy| {
        builder.register_policy_fn::<ProbeEvent, _>(policy, StartAt::Beginning, echo_back)
    })
    .await;

    let held = harness.hold_a_ping_open("slow", "held").await;

    // Committed and delivered while the held write is still open, which is what leaves
    // the search swept past a position it never saw.
    let overtaking = harness.ping("quick", "overtook").await;
    harness
        .await_dispatch_caused_by(overtaking.global_position)
        .await;
    assert!(
        !harness.has_passed(held.global_position).await,
        "an uncommitted write has been passed by nobody"
    );

    let position = held.global_position;
    held.commit().await;

    harness.await_dispatch_caused_by(position).await;
    assert!(
        harness.has_passed(position).await,
        "and once it commits it is delivered like any other event"
    );

    harness.shutdown().await;
}

/// A stream that is written once and then falls silent is still delivered. Nothing about
/// it is ever written again, so a design that only noticed streams with *new* events
/// would leave it undelivered for good.
#[tokio::test]
async fn a_stream_that_falls_silent_is_still_delivered_postgres_test() {
    let harness = PolicyDaemonHarness::start("quiet", |builder, policy| {
        builder.register_policy_fn::<ProbeEvent, _>(policy, StartAt::Beginning, echo_back)
    })
    .await;

    let only = harness.ping("says-one-thing", "once").await;
    harness.await_dispatch_caused_by(only.global_position).await;

    // Nothing more is appended anywhere: the Policy's own record of the stream is what
    // keeps it caught up rather than any further traffic.
    harness.await_passed(only.global_position).await;
    assert_eq!(
        harness.place_in(&only.stream_id).await,
        Some(1),
        "the one event it has is the one place it holds"
    );

    harness.shutdown().await;
}

/// Every event of a stream is delivered in that stream's order, under concurrent appends
/// to it and to others — and each exactly once, which at-least-once delivery does not
/// promise but a quiet run should show.
#[tokio::test]
async fn a_streams_events_are_delivered_in_its_own_order_postgres_test() {
    const STREAMS: usize = 4;
    const EVENTS: usize = 5;

    let harness = PolicyDaemonHarness::start("ordered", |builder, policy| {
        builder.register_policy_fn::<ProbeEvent, _>(policy, StartAt::Beginning, echo_back)
    })
    .await;

    // Joined, not collected and awaited one at a time: each round appends to all four
    // streams at once, so the positions they take in the log are interleaved in an order
    // nobody chose. One append in flight per stream, because two concurrent *first*
    // appends to one stream race on creating its `streams` row (funkode-io/replay#232) —
    // a defect in the write path, and not what this test is about.
    for event in 0..EVENTS {
        let round: Vec<(String, String)> = (0..STREAMS)
            .map(|stream| (format!("ordered-{stream}"), format!("{stream}-{event}")))
            .collect();
        futures::future::join_all(round.iter().map(|(stream, tag)| harness.ping(stream, tag)))
            .await;
    }

    // The Policy also walks the streams its own echoes land in; what this test is about
    // is the four it was pinged on.
    harness
        .observe("every pinged stream to be delivered whole", || async {
            let places = harness.places().await;
            (0..STREAMS)
                .all(|stream| {
                    places.iter().any(|(id, seq)| {
                        id == &format!("urn:probe:ordered-{stream}") && *seq == EVENTS as i64
                    })
                })
                .then_some(())
        })
        .await;

    // Each echo goes to a stream named after the tag it echoed, so what the Policy was
    // given is readable from the log in the order it wrote them.
    let mut delivered: Vec<(String, Vec<String>)> = Vec::new();
    for dispatch in harness.dispatches().await {
        let Some(tag) = dispatch
            .stream_id
            .strip_prefix("urn:probe:")
            .and_then(|nss| nss.strip_suffix("-echo"))
            .map(str::to_string)
        else {
            continue;
        };
        let (stream, _) = tag.split_once('-').expect("tags are stream-event");
        match delivered.iter_mut().find(|(seen, _)| seen == stream) {
            Some((_, tags)) => tags.push(tag),
            None => delivered.push((stream.to_string(), vec![tag])),
        }
    }

    for (stream, tags) in delivered {
        let expected: Vec<String> = (0..EVENTS)
            .map(|event| format!("{stream}-{event}"))
            .collect();
        assert_eq!(
            tags, expected,
            "stream {stream} must be delivered in its own order, once each"
        );
    }

    harness.shutdown().await;
}

/// ADR-0012 on the table that now holds the position: an operator moves a place back
/// against a *running* daemon, and the Policy redelivers from there. No restart, no
/// leadership change — the property funkode-io/replay#168 asked for, on the new surface.
#[tokio::test]
async fn an_operator_moves_a_place_on_a_running_daemon_postgres_test() {
    let harness = PolicyDaemonHarness::start("operator", |builder, policy| {
        builder.register_policy_fn::<ProbeEvent, _>(policy, StartAt::Beginning, echo_back)
    })
    .await;

    let ping = harness.ping("redeliver-me", "again").await;
    harness.await_dispatch_caused_by(ping.global_position).await;
    let delivered_once = harness.dispatches().await.len();

    harness.move_place_to(&ping.stream_id, 0).await;

    harness
        .observe("the policy to react to the same event twice", || async {
            (harness.dispatches().await.len() > delivered_once).then_some(())
        })
        .await;

    harness.shutdown().await;
}
