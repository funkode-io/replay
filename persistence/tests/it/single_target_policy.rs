//! A policy that names the aggregate it commands, driven by a real daemon.
//!
//! The unit half of this lives in `es-replay` (`es/tests/single_target_policy.rs`),
//! where a reaction is asserted as the pairs it returns. What only a database can show
//! is here: that the blanket impl's dispatches reach the runner, and that the metadata
//! hook lands on the event the command wrote (funkode-io/replay#239).

use crate::common;

use common::policy_harness::{PolicyDaemonHarness, Probe, ProbeCommand, ProbeEvent, ProbeUrn};
use replay::Metadata;
use replay_persistence::{AggregatePolicy, ObservedEvent, PolicySettings, StartAt};

/// Echoes every ping onto a stream of its own, stamping the source stream on each
/// command it issues.
///
/// Carries its name because the harness gives each run a unique one: a policy's name is
/// its cursor key, and two runs sharing one would share a position.
struct EchoEveryPing {
    name: String,
}

impl AggregatePolicy for EchoEveryPing {
    type Event = ProbeEvent;
    type Target = Probe;

    fn react(&self, event: &ObservedEvent<Self::Event>) -> Vec<(ProbeUrn, ProbeCommand)> {
        match &event.data {
            ProbeEvent::Pinged { tag } => vec![(
                ProbeUrn::new(format!("{tag}-echo")).unwrap(),
                ProbeCommand::Echo { tag: tag.clone() },
            )],
            _ => vec![],
        }
    }

    fn dispatch_metadata(&self, event: &ObservedEvent<Self::Event>) -> Option<Metadata> {
        Some(Metadata::new(
            serde_json::json!({ "pinged_stream": event.stream_id.to_string() }),
        ))
    }

    fn name(&self) -> &str {
        &self.name
    }
}

/// A single-target policy registered as itself — no `Dispatch` at the call site — reacts
/// end to end, and the hook's metadata rides alongside the runner's causation on the
/// event its command wrote.
#[tokio::test]
async fn a_single_target_policy_reacts_and_stamps_its_metadata_postgres_test() {
    let harness = PolicyDaemonHarness::start("typed-trait", |builder, policy| {
        builder.register_policy(
            EchoEveryPing {
                name: policy.to_string(),
            },
            PolicySettings::new().starting_at(StartAt::Beginning),
        )
    })
    .await;

    let ping = harness.ping("subject-1", "typed-hello").await;

    let dispatched = harness.await_dispatch_caused_by(ping.global_position).await;

    assert_eq!(dispatched.event_type, "Echoed");
    assert_eq!(dispatched.stream_id, "urn:probe:typed-hello-echo");
    assert_eq!(
        dispatched.metadata["pinged_stream"],
        serde_json::json!(ping.stream_id)
    );
    assert_eq!(
        dispatched.metadata["causation"]["global_position"],
        serde_json::json!(ping.global_position)
    );

    harness.shutdown().await;
}

/// The same rule as a closure registration that declares its target: the closure hands
/// back pairs, and the runner executes them exactly as it does the trait's.
#[tokio::test]
async fn a_typed_closure_policy_reacts_and_stamps_its_metadata_postgres_test() {
    let harness = PolicyDaemonHarness::start("typed-closure", |builder, policy| {
        builder.register_aggregate_policy_fn_with_metadata::<ProbeEvent, Probe, _, _>(
            policy,
            PolicySettings::new().starting_at(StartAt::Beginning),
            |event| {
                Some(Metadata::new(
                    serde_json::json!({ "pinged_stream": event.stream_id.to_string() }),
                ))
            },
            |event| match &event.data {
                ProbeEvent::Pinged { tag } => vec![(
                    ProbeUrn::new(format!("{tag}-echo")).unwrap(),
                    ProbeCommand::Echo { tag: tag.clone() },
                )],
                _ => vec![],
            },
        )
    })
    .await;

    let ping = harness.ping("subject-1", "closure-hello").await;

    let dispatched = harness.await_dispatch_caused_by(ping.global_position).await;

    assert_eq!(dispatched.event_type, "Echoed");
    assert_eq!(dispatched.stream_id, "urn:probe:closure-hello-echo");
    assert_eq!(
        dispatched.metadata["pinged_stream"],
        serde_json::json!(ping.stream_id)
    );

    harness.shutdown().await;
}
