# Replay

Event Sourcing and CQRS library

In event sourcing events are the source of truth, they are organized in streams that have an id and a version.
Everytime an event is applied into a stream its state changes and its version is incremented.

You can replay all events of a stream to reproduce previous states (hence the name of this library).

## Example

Let's assume we have a bank account with a balance that is updated when there is a deposit or withdrawal.

Our domain would look something like this:

```rust
use replay_es::Stream;
use replay_macros::Event;
use serde::{Deserialize, Serialize};
use urn::Urn;

#[derive(Default)]
struct BankAccountStream {
    pub balance: f64,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Event)]
enum BankAccountEvent {
    Deposit { amount: f64 },
    Withdraw { amount: f64 },
}

// recommended to create an specific type for the stream urn
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
struct BankAccountUrn(Urn);

impl From<BankAccountUrn> for Urn {
    fn from(urn: BankAccountUrn) -> Self {
        urn.0
    }
}

impl Stream for BankAccountStream {
   type Event = BankAccountEvent;
   type StreamId = BankAccountUrn;

   fn stream_type() -> String {
       "BankAccount".to_string()
   }

   fn apply(&mut self, event: Self::Event) {
       match event {
           BankAccountEvent::Deposit { amount } => {
               self.balance += amount;
           }
           BankAccountEvent::Withdraw { amount } => {
               self.balance -= amount;
           }
       }
   }
}
```
