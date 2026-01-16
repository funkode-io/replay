# Replay

Event Sourcing and CQRS library

In event sourcing events are the source of truth, they are organized in streams that have an id and a version.
Everytime an event is applied into a stream its state changes and its version is incremented.

You can replay all events of a stream to reproduce previous states (hence the name of this library).

Then from DDD we have aggregates that are implemented like a stream that accepts commands.
You can chose you implement just `Stream` (state will be built from events) or `Aggregate` (stream that accepts commands).

> Important to note `Streams` never fails as the are built from events that happened in the past (so there are no side effects, error handling, etc.). All of these concerns are managed in the aggregate.

## Example

Let's assume we have a bank account with a balance that is updated when there is a deposit or withdrawal.

Our domain would look something like this:

```rust
use replay::Stream;
use replay_macros::Event;
use serde::{Deserialize, Serialize};
use urn::Urn;

//  bank account is an aggregate (id of aggregate is not part of the model)
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Debug)]
struct BankAccountAggregate {
    pub balance: f64,
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
enum BankAccountCommand {
    Deposit { amount: f64 },
    Withdraw { amount: f64 },
}

#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Event)]
enum BankAccountEvent {
    Deposited { amount: f64 },
    Withdrawn { amount: f64 },
}

// recommended to create an specific type for the aggregate id
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
struct BankAccountUrn(Urn);

impl From<BankAccountUrn> for Urn {
    fn from(urn: BankAccountUrn) -> Self {
        urn.0
    }
}

#[derive(Debug, Error)]
enum BankAccountError {
    #[error("Insufficient funds")]
    InsufficientFunds,
    #[error("Persistence error: {source}")]
    PersistenceError {
        #[from]
        source: replay::persistence::EventStoreError,
    },
}

// to create an aggregate we need first to implement Stream trait
impl replay::Stream for BankAccountAggregate {
    type Event = BankAccountEvent;
    type StreamId = BankAccountUrn;

    fn stream_type() -> String {
        "BankAccount".to_string()
    }

    // as you can see this method can't fail and has no side effects
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

// the aggregate trait handles the commands / business logic rules / error management etc.
impl replay::Aggregate for BankAccountAggregate {
    type Command = BankAccountCommand;
    type Error = BankAccountError;

    type Services = ();

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            BankAccountCommand::Deposit { amount } => {
                let event = BankAccountEvent::Deposited { amount };
                Ok(vec![event])
            }
            BankAccountCommand::Withdraw { amount } => {
                if self.balance < amount {
                    return Err(BankAccountError::InsufficientFunds);
                }

                let event = BankAccountEvent::Withdrawn { amount };
                Ok(vec![event])
            }
        }
    }
}
```

Now we can create a Postgres store and start storing aggregates:

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

let bank_id = BankAccountUrn(UrnBuilder::new("bank-account", "1").build().unwrap());

let commands = vec![
    BankAccountCommand::Deposit { amount: 100.0 },
    BankAccountCommand::Withdraw { amount: 40.0 },
];

let mut bank_account: BankAccountAggregate = BankAccountAggregate::default();

let services = &();
let expected_version = None;

for command in commands {
    bank_account = store
        .apply_command_and_store_events(
            &bank_id,
            replay::Metadata::default(),
            command,
            services,
            expected_version,
        )
        .await
        .unwrap();
}

assert_eq!(bank_account.balance, 60.0);

// if we try to withdraw again we will get a business error (InsufficientFunds)
let result = store
    .apply_command_and_store_events::<BankAccountAggregate>(
        &bank_id,
        replay::Metadata::default(),
        BankAccountCommand::Withdraw { amount: 100f64 },
        services,
        expected_version,
    )
    .await;

assert_err!(result, "Insufficient funds");
```

## Using Macros

You can simplify the aggregate definition using the `define_aggregate!` macro. Here's the same bank account example using the macro:

```rust
use replay_macros::define_aggregate;
use replay::{Aggregate, EventStream};
use thiserror::Error;

// Define the aggregate structure with the macro
define_aggregate! {
    BankAccountAggregate {
        state: {
            balance: f64
        },
        commands: {
            Deposit { amount: f64 },
            Withdraw { amount: f64 }
        },
        events: {
            Deposited { amount: f64 },
            Withdrawn { amount: f64 }
        }
    }
}

#[derive(Debug, Error)]
enum BankAccountError {
    #[error("Insufficient funds")]
    InsufficientFunds,
    #[error("Persistence error: {source}")]
    PersistenceError {
        #[from]
        source: replay::persistence::EventStoreError,
    },
}

// Implement the EventStream trait
impl replay::EventStream for BankAccountAggregate {
    type Event = BankAccountAggregateEvent;
    type StreamId = BankAccountAggregateUrn;

    fn stream_type() -> String {
        "BankAccount".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            BankAccountAggregateEvent::Deposited { amount } => {
                self.balance += amount;
            }
            BankAccountAggregateEvent::Withdrawn { amount } => {
                self.balance -= amount;
            }
        }
    }
}

// Implement the Aggregate trait
impl replay::Aggregate for BankAccountAggregate {
    type Command = BankAccountAggregateCommand;
    type Error = BankAccountError;
    type Services = BankAccountAggregateServices;

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            BankAccountAggregateCommand::Deposit { amount } => {
                Ok(vec![BankAccountAggregateEvent::Deposited { amount }])
            }
            BankAccountAggregateCommand::Withdraw { amount } => {
                if self.balance < amount {
                    return Err(BankAccountError::InsufficientFunds);
                }
                Ok(vec![BankAccountAggregateEvent::Withdrawn { amount }])
            }
        }
    }
}
```

The macro automatically generates:

- The aggregate state struct (`BankAccountAggregate`)
- The command enum (`BankAccountAggregateCommand`)
- The event enum with `Event` trait (`BankAccountAggregateEvent`)
- The URN type (`BankAccountAggregateUrn`)
- A services placeholder struct (`BankAccountAggregateServices`)

This reduces boilerplate while keeping the same functionality. You still need to implement the `EventStream` and `Aggregate` traits to define the behavior.
