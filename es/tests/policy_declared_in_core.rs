//! A policy is declared, exercised and asserted against `es-replay` alone.
//!
//! The test compiles in a crate that cannot reach `Cqrs`, `PostgresEventStore` or the
//! runner — `es-replay` does not depend on `es-replay-persistence` — which is the whole
//! point of the contract living here (funkode-io/replay#237).

use replay::{Aggregate, Dispatch, Error, Event, EventStream, Metadata, ObservedEvent, Policy};
use serde::{Deserialize, Serialize};
use urn::{Urn, UrnBuilder};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
enum AccountEvent {
    Frozen { reason: String },
}

impl Event for AccountEvent {
    fn event_type(&self) -> String {
        "Frozen".to_string()
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
struct AccountUrn(Urn);

impl From<AccountUrn> for Urn {
    fn from(id: AccountUrn) -> Self {
        id.0
    }
}

impl TryFrom<Urn> for AccountUrn {
    type Error = Error;

    fn try_from(urn: Urn) -> Result<Self, Self::Error> {
        Ok(AccountUrn(urn))
    }
}

#[derive(Debug, Clone, PartialEq)]
enum ComplianceCommand {
    OpenCase { reason: String },
}

struct ComplianceCase {
    id: AccountUrn,
}

impl replay::WithId for ComplianceCase {
    type StreamId = AccountUrn;

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

/// The rule: when an account is frozen, open a compliance case on the same stream.
struct OpenCaseOnFreeze;

impl Policy for OpenCaseOnFreeze {
    type Event = AccountEvent;

    fn name(&self) -> &str {
        "open_case_on_freeze"
    }

    fn react(&self, event: &ObservedEvent<Self::Event>) -> Vec<Dispatch> {
        let AccountEvent::Frozen { reason } = &event.data;
        vec![Dispatch::to::<ComplianceCase>(
            AccountUrn(event.stream_id.clone()),
            ComplianceCommand::OpenCase {
                reason: reason.clone(),
            },
        )]
    }
}

fn account() -> Urn {
    UrnBuilder::new("account", "42").build().unwrap()
}

#[test]
fn a_rule_declared_in_core_is_asserted_in_core() {
    let event = ObservedEvent::of(
        account(),
        AccountEvent::Frozen {
            reason: "fraud-review".to_string(),
        },
    );

    let dispatches = OpenCaseOnFreeze.react(&event);

    let (id, command) = dispatches[0]
        .parts::<ComplianceCase>()
        .expect("the reaction targets ComplianceCase");
    assert_eq!(id, &AccountUrn(account()));
    assert_eq!(
        command,
        &ComplianceCommand::OpenCase {
            reason: "fraud-review".to_string()
        }
    );
}

/// The four fields a rule may read, and no more: the envelope a fixture builds is the
/// envelope `react` receives.
#[test]
fn the_envelope_carries_what_a_rule_reads() {
    let metadata = Metadata::from_json(serde_json::json!({ "correlation": "c-1" }));
    let event = ObservedEvent::of(
        account(),
        AccountEvent::Frozen {
            reason: "fraud-review".to_string(),
        },
    )
    .with_metadata(metadata.clone());

    assert_eq!(event.stream_id, account());
    assert_eq!(event.metadata, metadata);
    assert_eq!(event.created, chrono::DateTime::UNIX_EPOCH);
}
