//! Tests that drive a *real* policy daemon against a *real* database.
//!
//! Every assertion here is something an operator could have made from outside
//! the process: which commands a policy dispatched, where its cursor sits, and
//! what it parked. Nothing looks at tasks, channels or in-process bookkeeping.
//!
//! The harness they exercise lives in `tests/common/policy_harness.rs`. It is
//! the seam the supervision work (funkode-io/replay#180) asserts through, so
//! these two tests exist to prove the harness itself rather than any library
//! behaviour: the daemon reacts, and the daemon survives a reaction that fails.

mod common;

use common::policy_harness::{PolicyDaemonHarness, Probe, ProbeCommand, ProbeEvent, ProbeUrn};
use replay_persistence::{Dispatch, StartAt};

/// The harness works: a policy registered through it reacts to an appended
/// event, the reaction's command lands as an event carrying the policy's
/// causation, and the cursor moves past the event that triggered it.
#[tokio::test]
async fn a_registered_policy_reacts_to_an_appended_event_postgres_test() {
    let harness =
        PolicyDaemonHarness::start("reacts", |builder, policy| {
            builder.register_policy_fn::<ProbeEvent, _>(policy, StartAt::Beginning, |event| {
                match &event.data {
                    ProbeEvent::Pinged { tag } => vec![Dispatch::to::<Probe>(
                        ProbeUrn::new(format!("{tag}-echo")).unwrap(),
                        ProbeCommand::Echo { tag: tag.clone() },
                    )],
                    _ => vec![],
                }
            })
        })
        .await;

    let ping = harness.ping("subject-1", "hello").await;

    let dispatched = harness.await_dispatch_caused_by(ping.global_position).await;
    assert_eq!(dispatched.event_type, "Echoed");
    assert_eq!(dispatched.caused_by_event_id, ping.event_id);
    assert_eq!(dispatched.stream_id, "urn:probe:hello-echo");

    harness.await_passed(ping.global_position).await;
    assert!(
        harness.dead_letters().await.is_empty(),
        "a reaction that succeeded must park nothing"
    );

    harness.shutdown().await;
}

/// The harness can see a failure too: a reaction whose command is refused
/// permanently parks exactly one dispatch naming the triggering event, and the
/// daemon carries on — the next ping is still reacted to.
#[tokio::test]
async fn a_permanently_failing_reaction_is_parked_and_the_daemon_carries_on_postgres_test() {
    let harness =
        PolicyDaemonHarness::start("parks", |builder, policy| {
            builder.register_policy_fn::<ProbeEvent, _>(policy, StartAt::Beginning, |event| {
                match &event.data {
                    ProbeEvent::Pinged { tag } if tag == "poison" => vec![Dispatch::to::<Probe>(
                        ProbeUrn::new("refuser").unwrap(),
                        ProbeCommand::Refuse {
                            reason: "poison".to_string(),
                        },
                    )],
                    ProbeEvent::Pinged { tag } => vec![Dispatch::to::<Probe>(
                        ProbeUrn::new(format!("{tag}-echo")).unwrap(),
                        ProbeCommand::Echo { tag: tag.clone() },
                    )],
                    _ => vec![],
                }
            })
        })
        .await;

    let poison = harness.ping("subject-1", "poison").await;

    let parked = harness.await_dead_letters(1).await;
    assert_eq!(parked[0].global_position, poison.global_position);
    assert_eq!(parked[0].event_id, poison.event_id);
    assert!(
        parked[0].error_message.contains("probe refuses: poison"),
        "parked row must carry the failure's message, got {:?}",
        parked[0].error_message
    );

    // Still leading, still reading: the event after the poison one is reacted to.
    let next = harness.ping("subject-2", "hello").await;
    let dispatched = harness.await_dispatch_caused_by(next.global_position).await;
    assert_eq!(dispatched.event_type, "Echoed");
    assert_eq!(
        harness.dead_letters().await.len(),
        1,
        "no second parked row"
    );

    harness.shutdown().await;
}

/// A policy may dispatch back into the stream it just read — the shape that
/// makes "the newest event on the stream" a useless way to identify the one a
/// test appended, since the reaction can land first. The harness must still
/// hand back the ping, and the causation must still point at it.
#[tokio::test]
async fn a_reaction_into_the_same_stream_does_not_confuse_the_trigger_postgres_test() {
    let harness = PolicyDaemonHarness::start("same_stream", |builder, policy| {
        builder.register_policy_fn::<ProbeEvent, _>(policy, StartAt::Beginning, |event| {
            match &event.data {
                // Echo back into the stream the ping arrived on.
                ProbeEvent::Pinged { tag } => vec![Dispatch::to::<Probe>(
                    ProbeUrn::parse(&event.stream_id).unwrap(),
                    ProbeCommand::Echo { tag: tag.clone() },
                )],
                _ => vec![],
            }
        })
    })
    .await;

    let ping = harness.ping("subject-1", "hello").await;

    let dispatched = harness.await_dispatch_caused_by(ping.global_position).await;
    assert_eq!(dispatched.stream_id, ping.stream_id);
    assert_eq!(dispatched.caused_by_event_id, ping.event_id);
    assert!(
        dispatched.global_position > ping.global_position,
        "the reaction must land after the event it reacted to"
    );

    harness.shutdown().await;
}
