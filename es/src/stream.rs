use std::fmt::{self, Display};

use urn::Urn;

use super::Event;

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
/// use replay::{Event, Stream};
/// use replay_macros::Event;
/// use serde::{Deserialize, Serialize};
/// use urn::Urn;
///
/// #[derive(Default)]
/// struct BankAccountStream {
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
/// impl Stream for BankAccountStream {
///    type Event = BankAccountEvent;
///    type StreamId = BankAccountUrn;
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
pub trait Stream: Default + Sized {
    type Event: Event;
    type StreamId: Into<Urn> + Clone + Sync + Send + PartialEq;

    fn stream_type() -> String;

    fn apply(&mut self, event: Self::Event);

    fn apply_all(&mut self, events: Vec<Self::Event>) {
        for event in events {
            self.apply(event);
        }
    }
}

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

#[cfg(test)]
mod tests {
    use replay_macros::Event;
    use serde::{Deserialize, Serialize};
    use urn::Urn;

    use super::*;

    //  bank account stream (id of stream is not part of the model)
    #[derive(Default)]
    struct BankAccountStream {
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

    // bank account stream
    impl Stream for BankAccountStream {
        type Event = BankAccountEvent;
        type StreamId = BankAccountUrn;

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
        let mut bank_account = BankAccountStream::default();
        let deposited_event = BankAccountEvent::Deposited { amount: 100.0 };
        let withdrawn_event = BankAccountEvent::Withdrawn { amount: 50.0 };

        // check initial state
        assert_eq!(bank_account.balance, 0.0);

        bank_account.apply(deposited_event);
        bank_account.apply(withdrawn_event);

        assert_eq!(bank_account.balance, 50.0);
    }
}
