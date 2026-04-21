#![cfg(not(target_arch = "wasm32"))]

use replay::Event;
use replay_macros::Event as DeriveEvent;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, DeriveEvent)]
struct TestEvent {
    pub id: i64,
    pub name: String,
}

#[test]
fn test_event() {
    let event = TestEvent {
        id: 1,
        name: "test".to_string(),
    };

    assert_eq!(event.event_type(), "TestEvent");
}

#[test]
fn test_serialize_deserialize_event() {
    let event = TestEvent {
        id: 1,
        name: "test".to_string(),
    };

    let serialized = serde_json::to_string(&event).unwrap();
    let deserialized: TestEvent = serde_json::from_str(&serialized).unwrap();

    assert_eq!(event, deserialized);
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, DeriveEvent)]
enum BankAccountEvent {
    Deposited { amount: f64 },
    Withdrawn { amount: f64 },
}

#[test]
fn test_bank_account_event() {
    let deposited = BankAccountEvent::Deposited { amount: 100.0 };
    let withdrawn = BankAccountEvent::Withdrawn { amount: 50.0 };

    assert_eq!(deposited.event_type(), "Deposited");
    assert_eq!(withdrawn.event_type(), "Withdrawn");
}

#[test]
fn test_serialize_deserialize_bank_account_event() {
    let deposited = BankAccountEvent::Deposited { amount: 100.0 };
    let withdrawn = BankAccountEvent::Withdrawn { amount: 50.0 };

    let deposited_serialized = serde_json::to_string(&deposited).unwrap();
    let deposited_deserialized: BankAccountEvent =
        serde_json::from_str(&deposited_serialized).unwrap();

    let withdrawn_serialized = serde_json::to_string(&withdrawn).unwrap();
    let withdrawn_deserialized: BankAccountEvent =
        serde_json::from_str(&withdrawn_serialized).unwrap();

    assert_eq!(deposited, deposited_deserialized);
    assert_eq!(withdrawn, withdrawn_deserialized);
}
