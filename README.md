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

//  bank account is an aggregate (id of aggregate is now part of the model)
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
struct BankAccountAggregate {
    pub id: BankAccountUrn,
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

impl TryFrom<Urn> for BankAccountUrn {
    type Error = String;

    fn try_from(urn: Urn) -> Result<Self, Self::Error> {
        if urn.nid() == "bank-account" {
            Ok(BankAccountUrn(urn))
        } else {
            Err(format!("Invalid namespace: expected 'bank-account', got '{}'", urn.nid()))
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
    ) -> replay::Result<Vec<Self::Event>> {
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

    fn with_id(id: Self::StreamId) -> Self {
        Self { 
            id,
            balance: 0.0 
        }
    }

    fn id(&self) -> &Self::StreamId {
        &self.id
    }
}
```

### Aggregate Identity Pattern

Aggregates in DDD must always have an identity. The `Aggregate` trait enforces this through:

- **`StreamId`**: Inherited from `EventStream`, a strongly-typed identifier that must implement `Into<Urn>`, `TryFrom<Urn>`, `Clone`, and `PartialEq`
- **`with_id(id)`**: Required constructor that creates an aggregate with an identity
- **`with_string_id(urn_string)`**: Convenience constructor that parses a URN string
- **`id()`**: Returns a reference to the aggregate's identity

This design ensures aggregates are always created with a valid URN-based identity, following DDD principles.

```rust
// Create aggregate with typed id
let bank_id = BankAccountUrn(UrnBuilder::new("bank-account", "123").build().unwrap());
let aggregate = BankAccountAggregate::with_id(bank_id);

// Create aggregate from URN string
let aggregate = BankAccountAggregate::with_string_id("urn:bank-account:456").unwrap();

// Access the aggregate's id
println!("Aggregate ID: {}", aggregate.id());
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

// Create aggregate with id
let mut bank_account = BankAccountAggregate::with_id(bank_id.clone());

let services = &();
let expected_version = None;

for command in commands {
    let events = bank_account.handle(command, services).await.unwrap();
    bank_account.apply_all(events);
}

assert_eq!(bank_account.balance, 60.0);

// if we try to withdraw again we will get a business error (InsufficientFunds)
let result = bank_account.handle(
    BankAccountCommand::Withdraw { amount: 100.0 },
    services
).await;

assert!(result.is_err());
```

## Using Macros

You can simplify the aggregate definition using the `define_aggregate!` macro. Here's the same bank account example using the macro:

```rust
use replay_macros::define_aggregate;
use replay::{Aggregate, EventStream};
use thiserror::Error;

// Define the aggregate structure with the macro
define_aggregate! {
    BankAccount {
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
impl replay::EventStream for BankAccount {
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

// Implement the Aggregate trait
impl replay::Aggregate for BankAccount {
    type Command = BankAccountCommand;
    type Error = BankAccountError;
    type Services = BankAccountServices;

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> replay::Result<Vec<Self::Event>> {
        match command {
            BankAccountCommand::Deposit { amount } => {
                Ok(vec![BankAccountEvent::Deposited { amount }])
            }
            BankAccountCommand::Withdraw { amount } => {
                if self.balance < amount {
                    return Err(BankAccountError::InsufficientFunds);
                }
                Ok(vec![BankAccountEvent::Withdrawn { amount }])
            }
        }
    }

    fn with_id(id: Self::StreamId) -> Self {
        Self {
            id,
            balance: 0.0,
        }
    }

    fn id(&self) -> &Self::StreamId {
        &self.id
    }
}
```

The macro automatically generates:

- The aggregate state struct (`BankAccount`) with an `id` field of type `StreamId`
- The command enum (`BankAccountCommand`)
- The event enum with `Event` trait (`BankAccountEvent`)
- The URN type (`BankAccountUrn`) with helper methods:
  - `YourTypeUrn::new(id)` - Creates a URN with the configured namespace
  - `YourTypeUrn::parse(input)` - Parses a URN string and validates the namespace
  - `YourTypeUrn::namespace()` - Returns the namespace identifier as a static string
  - `Display` implementation for easy string conversion
  - `TryFrom<Urn>` implementation for converting URNs to the typed wrapper
- A services placeholder struct (`BankAccountServices`)

This reduces boilerplate while keeping the same functionality. You still need to implement the `EventStream` and `Aggregate` traits (including `with_id` and `id` methods) to define the behavior.

### URN Namespace Configuration

The URN namespace identifier (NID) is automatically derived from the aggregate name by converting CamelCase to kebab-case (e.g., `BankAccount` becomes `bank-account`, `HTTPConnection` becomes `http-connection`).

You can optionally specify a custom `namespace` to override this default behavior:

```rust
define_aggregate! {
    Customer {
        namespace: "customer",
        state: {
            email: String,
            name: String
        },
        commands: {
            RegisterCustomer { email: String, name: String }
        },
        events: {
            CustomerRegistered { email: String, name: String }
        }
    }
}

// The URN helper methods are always available:
let customer_urn = CustomerUrn::new("peter@example.com").unwrap();
assert_eq!(customer_urn.to_string(), "urn:customer:peter@example.com");

// Get the namespace identifier
assert_eq!(CustomerUrn::namespace(), "customer");
```

If no custom namespace is specified, the namespace will be automatically derived from the aggregate name (e.g., `BankAccount` → `bank-account`).
