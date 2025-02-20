use std::fmt::{self};

use serde::{de::DeserializeOwned, Serialize};

/// Events are the source of truth in event sourcing.
///
/// An event has to be able to be serialized and deserialized and it should have a type.
/// You can derive `Event` on enums and structs that derive `Serialize`, `Deserialize`, `Clone`, `PartialEq`, `Debug`.
///
/// In the example of a bank account, the events could be: Deposit, Withdraw, Transfer, etc.
///  
/// # Example
///
/// ```
/// use serde::{Deserialize, Serialize};
///
/// use replay_es::Event;
/// use replay_macros::Event;
///
/// #[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Event)]
/// enum BankAccountEvent {
///     Deposit { amount: f64 },
///     Withdraw { amount: f64 },
/// }
///
/// assert_eq!(BankAccountEvent::Deposit { amount: 100.0 }.event_type(), "Deposit");
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

    // create bank account events enum: Deposit and Withdraw
    #[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Event)]
    enum BankAccountEvent {
        Deposit { amount: f64 },
        Withdraw { amount: f64 },
    }

    #[test]
    fn test_bank_account_event() {
        let deposit = BankAccountEvent::Deposit { amount: 100.0 };
        let withdraw = BankAccountEvent::Withdraw { amount: 50.0 };

        assert_eq!(deposit.event_type(), "Deposit");
        assert_eq!(withdraw.event_type(), "Withdraw");
    }

    // test serialize and deserialize bank account event
    #[test]
    fn test_serialize_deserialize_bank_account_event() {
        let deposit = BankAccountEvent::Deposit { amount: 100.0 };
        let withdraw = BankAccountEvent::Withdraw { amount: 50.0 };

        let deposit_serialized = serde_json::to_string(&deposit).unwrap();
        let deposit_deserialized: BankAccountEvent =
            serde_json::from_str(&deposit_serialized).unwrap();

        let withdraw_serialized = serde_json::to_string(&withdraw).unwrap();
        let withdraw_deserialized: BankAccountEvent =
            serde_json::from_str(&withdraw_serialized).unwrap();

        assert_eq!(deposit, deposit_deserialized);
        assert_eq!(withdraw, withdraw_deserialized);
    }
}
