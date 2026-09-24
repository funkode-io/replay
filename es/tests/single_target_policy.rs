//! A policy that targets one aggregate declares it, and its reaction is asserted as
//! the pairs it returns — no downcast, no `Dispatch`, no store
//! (funkode-io/replay#239).

use std::sync::atomic::{AtomicUsize, Ordering};

use replay::{
    Aggregate, AggregatePolicy, Error, Event, EventStream, Metadata, ObservedEvent, Policy, WithId,
};
use serde::{Deserialize, Serialize};
use urn::{Urn, UrnBuilder};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum AccountEvent {
    Frozen { reason: String },
    Thawed,
}

impl Event for AccountEvent {
    fn event_type(&self) -> String {
        match self {
            AccountEvent::Frozen { .. } => "Frozen".to_string(),
            AccountEvent::Thawed => "Thawed".to_string(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct CaseUrn(Urn);

impl From<CaseUrn> for Urn {
    fn from(id: CaseUrn) -> Self {
        id.0
    }
}

impl TryFrom<Urn> for CaseUrn {
    type Error = Error;

    fn try_from(urn: Urn) -> Result<Self, Self::Error> {
        Ok(CaseUrn(urn))
    }
}

#[derive(Debug, Clone, PartialEq)]
enum ComplianceCommand {
    OpenCase { reason: String },
}

struct ComplianceCase {
    id: CaseUrn,
}

impl WithId for ComplianceCase {
    type StreamId = CaseUrn;

    fn with_id(id: Self::StreamId) -> Self {
        ComplianceCase { id }
    }

    fn get_id(&self) -> &Self::StreamId {
        &self.id
    }
}

impl EventStream for ComplianceCase {
    type Event = AccountEvent;

    fn stream_type() -> String {
        "ComplianceCase".to_string()
    }

    fn apply(&mut self, _event: Self::Event) {}
}

impl Aggregate for ComplianceCase {
    type Command = ComplianceCommand;
    type Error = Error;
    type Services = ();

    async fn handle(
        &self,
        _command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        Ok(vec![])
    }
}

/// The rule: a freeze opens one case per reason, on the stream the event came from.
///
/// It counts the metadata calls so the test can pin *once per reaction*, which is what
/// lets a policy compute a correlation id it stamps on every command it issues.
#[derive(Default)]
struct OpenCaseOnFreeze {
    metadata_calls: AtomicUsize,
}

impl AggregatePolicy for OpenCaseOnFreeze {
    type Event = AccountEvent;
    type Target = ComplianceCase;

    fn name(&self) -> &str {
        "open_case_on_freeze"
    }

    fn react(
        &self,
        event: &ObservedEvent<Self::Event>,
    ) -> Vec<(CaseUrn, <ComplianceCase as Aggregate>::Command)> {
        match &event.data {
            AccountEvent::Frozen { reason } => vec![
                (
                    CaseUrn(event.stream_id.clone()),
                    ComplianceCommand::OpenCase {
                        reason: reason.clone(),
                    },
                ),
                (
                    CaseUrn(event.stream_id.clone()),
                    ComplianceCommand::OpenCase {
                        reason: "audit".to_string(),
                    },
                ),
            ],
            AccountEvent::Thawed => vec![],
        }
    }

    fn dispatch_metadata(&self, event: &ObservedEvent<Self::Event>) -> Option<Metadata> {
        self.metadata_calls.fetch_add(1, Ordering::SeqCst);
        Some(Metadata::from_json(
            serde_json::json!({ "frozen_stream": event.stream_id.to_string() }),
        ))
    }
}

fn account() -> Urn {
    UrnBuilder::new("account", "42").build().unwrap()
}

fn frozen() -> ObservedEvent<AccountEvent> {
    ObservedEvent::of(
        account(),
        AccountEvent::Frozen {
            reason: "fraud-review".to_string(),
        },
    )
}

/// The assertion the erasure used to cost: a plain equality on the pairs.
///
/// Spelled `AggregatePolicy::react` because this file also imports [`Policy`], which
/// the blanket impl gives the same type: with both traits in scope a method call is
/// ambiguous (`E0034`). A test that imports only `AggregatePolicy` writes
/// `policy.react(&event)`.
#[test]
fn the_reaction_is_the_assertion() {
    let reaction = AggregatePolicy::react(&OpenCaseOnFreeze::default(), &frozen());

    assert_eq!(
        reaction,
        vec![
            (
                CaseUrn(account()),
                ComplianceCommand::OpenCase {
                    reason: "fraud-review".to_string()
                }
            ),
            (
                CaseUrn(account()),
                ComplianceCommand::OpenCase {
                    reason: "audit".to_string()
                }
            ),
        ]
    );
}

/// The runner sees a `Policy`: every pair is a dispatch to the declared target.
#[test]
fn the_blanket_impl_addresses_the_declared_target() {
    let policy = OpenCaseOnFreeze::default();

    let dispatches = Policy::react(&policy, &frozen());

    assert_eq!(dispatches.len(), 2);
    let (id, command) = dispatches[0]
        .parts::<ComplianceCase>()
        .expect("the reaction targets the declared aggregate");
    assert_eq!(id, &CaseUrn(account()));
    assert_eq!(
        command,
        &ComplianceCommand::OpenCase {
            reason: "fraud-review".to_string()
        }
    );
    assert_eq!(Policy::name(&policy), "open_case_on_freeze");
}

/// Metadata is computed once for the reaction and carried by every dispatch in it.
#[test]
fn metadata_is_computed_once_and_attached_to_every_dispatch() {
    let policy = OpenCaseOnFreeze::default();

    let dispatches = Policy::react(&policy, &frozen());

    let expected =
        Metadata::from_json(serde_json::json!({ "frozen_stream": account().to_string() }));
    assert_eq!(dispatches[0].metadata(), Some(&expected));
    assert_eq!(dispatches[1].metadata(), Some(&expected));
    assert_eq!(
        policy.metadata_calls.load(Ordering::SeqCst),
        1,
        "the hook runs once per reaction, not once per dispatch"
    );
}

/// A reaction of no pairs asks for nothing, and does not pay for metadata either.
#[test]
fn an_empty_reaction_dispatches_nothing() {
    let policy = OpenCaseOnFreeze::default();

    let dispatches = Policy::react(&policy, &ObservedEvent::of(account(), AccountEvent::Thawed));

    assert!(dispatches.is_empty());
    assert_eq!(policy.metadata_calls.load(Ordering::SeqCst), 0);
}

/// The default hook attaches nothing, so a policy that says nothing about metadata
/// dispatches exactly what a raw `Policy` would.
#[test]
fn metadata_defaults_to_none() {
    struct Plain;

    impl AggregatePolicy for Plain {
        type Event = AccountEvent;
        type Target = ComplianceCase;

        fn name(&self) -> &str {
            "plain"
        }

        fn react(&self, event: &ObservedEvent<Self::Event>) -> Vec<(CaseUrn, ComplianceCommand)> {
            vec![(
                CaseUrn(event.stream_id.clone()),
                ComplianceCommand::OpenCase {
                    reason: "plain".to_string(),
                },
            )]
        }
    }

    let dispatches = Policy::react(&Plain, &frozen());

    assert!(dispatches[0].metadata().is_none());
}
