use std::fmt::{self};

use serde::{de::DeserializeOwned, Serialize};

/// Events are the source of truth in event sourcing.
///
/// An event has to be able to be serialized and deserialized and it should have a type.
/// You can derive `Event` on enums and structs that derive `Serialize`, `Deserialize`, `Clone`, `PartialEq`, `Debug`.
///
/// In the example of a bank account, the events could be: Deposited, Withdrawn, Transfer, etc.
///  
/// # Example
///
/// ```
/// use serde::{Deserialize, Serialize};
///
/// use replay::Event;
/// use replay_macros::Event;
///
/// #[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Event)]
/// enum BankAccountEvent {
///     Deposited { amount: f64 },
///     Withdrawn { amount: f64 },
/// }
///
/// assert_eq!(BankAccountEvent::Deposited { amount: 100.0 }.event_type(), "Deposited");
/// ```
///
pub trait Event:
    Serialize + DeserializeOwned + Clone + PartialEq + fmt::Debug + Sync + Send
{
    fn event_type(&self) -> String;
}

// tests
#[cfg(test)]
mod tests {
    use replay_macros::Event;
    use serde::Deserialize;

    use super::*;

    #[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Event)]
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

    // test serialize and deserialize event
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

    // create bank account events enum: Deposited and Withdrawn
    #[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Event)]
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

    // test serialize and deserialize bank account event
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
}
