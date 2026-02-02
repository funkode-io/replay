use serde::{de::DeserializeOwned, Serialize};
use urn::Urn;

use crate::Error;

use super::Event;

/// A trait for types that have a stream identifier.
///
/// This trait provides constructor and accessor methods for working with stream identifiers.
pub trait WithId: Sized {
    type StreamId: Send
        + Sync
        + Into<Urn>
        + TryFrom<Urn, Error: std::fmt::Debug>
        + Clone
        + PartialEq
        + std::fmt::Debug
        + Serialize
        + DeserializeOwned;

    /// Creates a new instance with the given id.
    fn with_id(id: Self::StreamId) -> Self;

    /// Returns a reference to the identifier.
    fn get_id(&self) -> &Self::StreamId;

    fn with_string_id(id: impl Into<String>) -> crate::Result<Self> {
        use std::str::FromStr;
        let id_string = id.into();

        // Parse string as URN
        let urn = Urn::from_str(&id_string).map_err(|e| {
            Error::invalid_input("Invalid URN format")
                .with_operation("with_string_id")
                .with_context("id", id_string.clone())
                .with_context("error", format!("{:?}", e))
        })?;

        // Convert URN to aggregate StreamId type
        let aggregate_id: Self::StreamId = urn.try_into().map_err(|e| {
            Error::invalid_input("Failed to convert URN to StreamId type")
                .with_operation("with_string_id")
                .with_context("id", id_string.clone())
                .with_context("error", format!("{:?}", e))
        })?;

        Ok(Self::with_id(aggregate_id))
    }
}

/// In event sourcing a stream is a sequence of events that are related to a specific entity.
///
/// A stream is expected to be able to apply events and have a type.
/// A stream state is created only from its events, that's why you need to implement `Default` trait.
///
/// Streams are identified using a URN, recommendation is to use speficic types for that.
///
/// # Example
///
/// ```
/// use replay::{Event, EventStream, WithId};
/// use replay_macros::Event;
/// use serde::{Deserialize, Serialize};
/// use urn::Urn;
///
/// struct BankAccountStream {
///     pub id: BankAccountUrn,
///     pub balance: f64,
/// }
///
/// #[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Event)]
/// enum BankAccountEvent {
///     Deposited { amount: f64 },
///     Withdrawn { amount: f64 },
/// }
///
/// #[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
/// struct BankAccountUrn(Urn);
///
/// impl From<BankAccountUrn> for Urn {
///     fn from(urn: BankAccountUrn) -> Self {
///         urn.0
///     }
/// }
///
/// impl TryFrom<Urn> for BankAccountUrn {
///     type Error = String;
///
///     fn try_from(urn: Urn) -> Result<Self, Self::Error> {
///         Ok(BankAccountUrn(urn))
///     }
/// }
///
/// impl WithId for BankAccountStream {
///    type StreamId = BankAccountUrn;
///
///    fn with_id(id: Self::StreamId) -> Self {
///        Self { id, balance: 0.0 }
///    }
///
///    fn get_id(&self) -> &Self::StreamId {
///        &self.id
///    }
/// }
///
/// impl EventStream for BankAccountStream {
///    type Event = BankAccountEvent;
///
///    fn stream_type() -> String {
///        "BankAccount".to_string()
///    }
///
///    fn apply(&mut self, event: Self::Event) {
///        match event {
///            BankAccountEvent::Deposited { amount } => {
///                self.balance += amount;
///            }
///            BankAccountEvent::Withdrawn { amount } => {
///                self.balance -= amount;
///            }
///        }
///    }
///}
/// ```
pub trait EventStream: Sized + WithId {
    type Event: Event;

    fn stream_type() -> String;

    fn apply(&mut self, event: Self::Event);

    fn apply_all(&mut self, events: Vec<Self::Event>) {
        for event in events {
            self.apply(event);
        }
    }
}

/*
/// Stream state is a representation of the current state of a stream, every time an event is applied the state is updated and the version will increment.
///
/// The stream id is a URN that identifies the stream and it's not included in the model to avoid polluting the domain (not all domains have an id).
///
/// The stream type is the type of the stream, it's used to identify the stream.

#[derive(Debug)]
pub struct StreamState {
    pub stream_id: Urn,
    pub stream_type: String,
    pub version: i64,
}
impl Display for StreamState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Stream {} with id: {} and version: {}",
            self.stream_type, self.stream_id, self.version
        )
    }
}
*/

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    // hack to use macros inside this crate
    use crate as replay;
    use replay_macros::Event;
    use serde::{Deserialize, Serialize};
    use urn::Urn;

    use super::*;

    //  bank account stream (id of stream is not part of the model)
    struct BankAccountStream {
        pub id: BankAccountUrn,
        pub balance: f64,
    }

    // create bank account events enum: Deposited and Withdrawn
    #[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Event)]
    enum BankAccountEvent {
        Deposited { amount: f64 },
        Withdrawn { amount: f64 },
    }

    // bank account urn
    #[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
    struct BankAccountUrn(Urn);

    impl From<BankAccountUrn> for Urn {
        fn from(urn: BankAccountUrn) -> Self {
            urn.0
        }
    }

    impl TryFrom<Urn> for BankAccountUrn {
        type Error = String;

        fn try_from(urn: Urn) -> Result<Self, Self::Error> {
            Ok(BankAccountUrn(urn))
        }
    }

    impl WithId for BankAccountStream {
        type StreamId = BankAccountUrn;

        fn with_id(id: Self::StreamId) -> Self {
            BankAccountStream { id, balance: 0.0 }
        }

        fn get_id(&self) -> &Self::StreamId {
            &self.id
        }
    }

    // bank account stream
    impl EventStream for BankAccountStream {
        type Event = BankAccountEvent;

        fn stream_type() -> String {
            "BankAccount".to_string()
        }

        fn apply(&mut self, event: Self::Event) {
            match event {
                BankAccountEvent::Deposited { amount } => {
                    self.balance += amount;
                }
                BankAccountEvent::Withdrawn { amount } => {
                    self.balance -= amount;
                }
            }
        }
    }

    #[test]
    fn test_bank_account_stream() {
        let mut bank_account = BankAccountStream {
            id: BankAccountUrn(Urn::from_str("urn:bank-account:1").unwrap()),
            balance: 0.0,
        };
        let deposited_event = BankAccountEvent::Deposited { amount: 100.0 };
        let withdrawn_event = BankAccountEvent::Withdrawn { amount: 50.0 };

        // check initial state
        assert_eq!(bank_account.balance, 0.0);

        bank_account.apply(deposited_event);
        bank_account.apply(withdrawn_event);

        assert_eq!(bank_account.balance, 50.0);
    }
}
