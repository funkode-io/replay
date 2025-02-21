# Replay

Event Sourcing and CQRS library

In event sourcing events are the source of truth, they are organized in streams that have an id and a version.
Everytime an event is applied into a stream its state changes and its version is incremented.

You can replay all events of a stream to reproduce previous states (hence the name of this library).

## Example

Let's assume we have a bank account with a balance that is updated when there is a deposit or withdrawal.

Our domain would look something like this:

```rust
use replay::Stream;
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

Now we can create a Postgres store and start creating streams:

```rust
// connect to Postgres
let pg_pool = PgPoolOptions::new()
    .max_connections(50)
    .idle_timeout(std::time::Duration::from_secs(5))
    .connect(&format!(
        "postgres://postgres:postgres@{}:{}/postgres",
        host, port
    ))
    .await
    .expect("Failed to create postgres pool");

// create an event store
let store = replay::persistence::PostgresEventStore::new(pg_pool);

// store events for your stream
let stream_id = UrnBuilder::new("bank-account", "1").build().unwrap();

let events = vec![
    BankAccountEvent::Deposit { amount: 100.0 },
    BankAccountEvent::Withdraw { amount: 40.0 },
];

store
    .store_events(
        &stream_id,
        "BankAccount".to_string(),
        replay::Metadata::default(),
        &events,
        None,
    )
    .await
    .unwrap();

// now you can recover your stream from the store
let stream_events = store
    .stream_events::<BankAccountEvent>(StreamFilter::WithStreamId(stream_id))
    .map_ok(|persisted_event| persisted_event.data)
    .try_collect::<Vec<_>>()
    .await
    .unwrap();

assert_eq!(stream_events.len(), 2);

let mut stream = BankAccountStream::default();
stream.apply_all(stream_events);

assert_eq!(stream.balance, 60.0);
```
