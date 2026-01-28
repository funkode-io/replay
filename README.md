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
  - `your_urn.to_urn()` - Returns a reference to the inner URN
  - `your_urn.nid()` - Returns the namespace identifier (NID)
  - `your_urn.nss()` - Returns the namespace specific string (NSS) - the ID part
  - `Display` implementation for easy string conversion
  - `TryFrom<Urn>` implementation for converting URNs to the typed wrapper
- A services trait (`BankAccountServices`) if service functions are defined, or a placeholder struct if not

This reduces boilerplate while keeping the same functionality. You still need to implement the `EventStream` and `Aggregate` traits (including `with_id` and `id` methods) to define the behavior.

### Using Services for External Dependencies

When your aggregate needs to interact with external services (e.g., authentication, validation, external APIs), you can define a service trait using the `service` section in the macro. The macro generates a **trait** (not a struct) that you implement with your own service logic.

```rust
use replay_macros::define_aggregate;
use replay::{Aggregate, EventStream, WithId};
use std::sync::Arc;

// The macro generates the BankAccountServices trait
define_aggregate! {
    BankAccount {
        state: {
            account_number: String,
            balance: f64
        },
        commands: {
            OpenAccount { account_number: String },
            Deposit { amount: f64 }
        },
        events: {
            AccountOpened { account_number: String },
            Deposited { amount: f64 }
        },
        service: {
            fn validate_account_number(account_number: &str) -> bool;
        }
    }
}

// This generates:
// pub trait BankAccountServices: Send + Sync {
//     fn validate_account_number(&self, account_number: &str) -> bool;
// }

// Now you implement the generated trait with your own struct
#[derive(Clone)]
pub struct MyBankServices;

impl BankAccountServices for MyBankServices {
    fn validate_account_number(&self, account_number: &str) -> bool {
        // Your validation logic
        account_number.len() >= 5
    }
}

impl EventStream for BankAccount {
    type Event = BankAccountEvent;

    fn stream_type() -> String {
        "BankAccount".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            BankAccountEvent::AccountOpened { account_number } => {
                self.account_number = account_number;
            }
            BankAccountEvent::Deposited { amount } => {
                self.balance += amount;
            }
        }
    }
}

impl Aggregate for BankAccount {
    type Command = BankAccountCommand;
    type Error = replay::Error;
    // Use Arc<dyn Trait> to accept any implementation
    type Services = Arc<dyn BankAccountServices>;

    async fn handle(
        &self,
        command: Self::Command,
        services: &Self::Services,
    ) -> replay::Result<Vec<Self::Event>> {
        match command {
            BankAccountCommand::OpenAccount { account_number } => {
                // Use the service to validate
                if !services.validate_account_number(&account_number) {
                    return Err(replay::Error::business_rule_violation(
                        "Invalid account number: must be at least 5 characters"
                    )
                    .with_operation("OpenAccount")
                    .with_context("account_number", account_number));
                }
                Ok(vec![BankAccountEvent::AccountOpened { account_number }])
            }
            BankAccountCommand::Deposit { amount } => {
                Ok(vec![BankAccountEvent::Deposited { amount }])
            }
        }
    }
}

// Usage example
#[tokio::main]
async fn main() {
    // Create service implementation wrapped in Arc
    let services: Arc<dyn BankAccountServices> = Arc::new(MyBankServices);
    
    let id = BankAccountUrn::new("acc-123").unwrap();
    let account = BankAccount::with_id(id);
    
    // Valid account number
    let cmd = BankAccountCommand::OpenAccount {
        account_number: "12345".to_string(),
    };
    let events = account.handle(cmd, &services).await.unwrap();
    println!("Account opened successfully");
    
    // Invalid account number (too short)
    let cmd = BankAccountCommand::OpenAccount {
        account_number: "123".to_string(),
    };
    let result = account.handle(cmd, &services).await;
    assert!(result.is_err());
    println!("Validation failed as expected");
}
```

**Key points about services:**

- The macro generates a **trait** (e.g., `BankAccountServices`), not a struct
- Service functions are defined without `&self` in the macro - it's added automatically
- The generated trait is `Send + Sync` compatible for async contexts
- You implement the trait with your own struct containing dependencies
- Use `Arc<dyn YourServices>` as the `Services` type in your aggregate
- Services allow dependency injection, making aggregates easier to test
- The aggregate struct has no type parameters - it stays simple

#### Async Services

Services can define async functions using the `async fn` syntax:

```rust
define_aggregate! {
    BankAccount {
        state: {
            account_number: String,
            balance: f64
        },
        commands: {
            OpenAccount { account_number: String }
        },
        events: {
            AccountOpened { account_number: String }
        },
        service: {
            // Async service function
            async fn validate_account_number(account_number: &str) -> bool;
        }
    }
}

// Implement with async_trait
#[async_trait::async_trait]
impl BankAccountServices for MyBankServices {
    async fn validate_account_number(&self, account_number: &str) -> bool {
        // Can call async APIs, databases, etc.
        external_api::validate(account_number).await
    }
}

// Use in handle method with .await
impl Aggregate for BankAccount {
    type Command = BankAccountCommand;
    type Error = replay::Error;
    type Services = Arc<dyn BankAccountServices>;

    async fn handle(
        &self,
        command: Self::Command,
        services: &Self::Services,
    ) -> replay::Result<Vec<Self::Event>> {
        match command {
            BankAccountCommand::OpenAccount { account_number } => {
                // Await the async service call
                if !services.validate_account_number(&account_number).await {
                    return Err(replay::Error::business_rule_violation(
                        "Invalid account number"
                    ));
                }
                Ok(vec![BankAccountEvent::AccountOpened { account_number }])
            }
        }
    }
}
```

**WASM Compatibility**: The generated service trait uses `#[cfg_attr(target_arch = "wasm32", async_trait(?Send))]` to automatically support WASM targets, where futures cannot be `Send`. For non-WASM targets, regular `async_trait` is used to enable multi-threaded execution.

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

// Access URN components
assert_eq!(customer_urn.nid(), "customer");
assert_eq!(customer_urn.nss(), "peter@example.com");

// Get reference to inner URN
let inner_urn: &Urn = customer_urn.to_urn();
```

If no custom namespace is specified, the namespace will be automatically derived from the aggregate name (e.g., `BankAccount` → `bank-account`).

### Using URN Helper Methods

The generated URN types provide convenient methods for working with identifiers:

```rust
use replay_macros::define_aggregate;

define_aggregate! {
    Order {
        state: {
            total: f64,
            status: String
        },
        commands: {
            PlaceOrder { total: f64 }
        },
        events: {
            OrderPlaced { total: f64 }
        }
    }
}

// Create a new URN
let order_id = OrderUrn::new("12345").unwrap();
println!("Full URN: {}", order_id);  // urn:order:12345

// Extract components
println!("Namespace: {}", order_id.nid());  // order
println!("ID: {}", order_id.nss());         // 12345

// Parse from string
let parsed = OrderUrn::parse("urn:order:67890").unwrap();
assert_eq!(parsed.nss(), "67890");

// Use in aggregates
let order = Order::with_id(order_id);
println!("Created: {}", order);  // Order(id: urn:order:12345, total: 0, status: )
```

### URNs in Collections

The generated URN types implement `Hash` and `Eq`, making them suitable for use as keys in `HashMap` and elements in `HashSet`:

```rust
use std::collections::{HashMap, HashSet};
use replay_macros::define_aggregate;

define_aggregate! {
    Product {
        state: { name: String, price: f64 },
        commands: { UpdatePrice { price: f64 } },
        events: { PriceUpdated { price: f64 } }
    }
}

// Use URNs as HashMap keys
let mut inventory = HashMap::new();
let product1 = ProductUrn::new("laptop-001").unwrap();
let product2 = ProductUrn::new("mouse-002").unwrap();

inventory.insert(product1.clone(), 50);
inventory.insert(product2.clone(), 200);

if let Some(stock) = inventory.get(&product1) {
    println!("Stock for {}: {}", product1.nss(), stock);  // Stock for laptop-001: 50
}

// Use URNs in HashSet for unique collections
let mut active_products = HashSet::new();
active_products.insert(product1);
active_products.insert(product2);

assert_eq!(active_products.len(), 2);
```

## WASM Support

The library supports WebAssembly (WASM) targets with automatic adjustments for single-threaded environments:

### Aggregate Trait

The `Aggregate` trait has two variants:

- **Non-WASM targets**: Includes `Send` bounds on aggregates, commands, services, and futures to enable multi-threaded async runtimes (Tokio, async-std)
- **WASM targets**: Omits `Send` bounds since WASM runs in a single-threaded environment

This is handled automatically - you don't need to change your code.

### Async Services in WASM

When defining async service functions, the generated trait automatically uses the appropriate async_trait configuration:

```rust
service: {
    async fn validate_data(data: &str) -> bool;
}
```

Generated trait (automatically adjusted per target):

```rust
// For WASM (wasm32):
#[cfg_attr(target_arch = "wasm32", async_trait::async_trait(?Send))]
// For servers (non-WASM):
#[cfg_attr(not(target_arch = "wasm32"), async_trait::async_trait)]
pub trait YourServices: Send + Sync {
    async fn validate_data(&self, data: &str) -> bool;
}
```

### Testing WASM

Run WASM tests using `wasm-pack`:

```bash
# Test in headless browser
wasm-pack test --headless --firefox es

# Or using the Makefile
make wasm-test
```

Tests should be placed in files with `#![cfg(target_arch = "wasm32")]` to only compile for WASM:

```rust
#![cfg(target_arch = "wasm32")]
use wasm_bindgen_test::*;

wasm_bindgen_test_configure!(run_in_browser);

#[wasm_bindgen_test]
async fn test_aggregate_in_wasm() {
    // Your test code
}
```

## Querying Events from Multiple Aggregates

When building CQRS queries, you often need to process events from multiple aggregate types together. The `query_events!` macro simplifies creating a wrapper enum that can hold events from different aggregates:

```rust
use replay_macros::query_events;
use replay::Event;
use serde::{Deserialize, Serialize};

// Define your individual event types
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum UserEvent {
    UserCreated { user_id: String, name: String },
    UserUpdated { user_id: String, name: String },
}

impl Event for UserEvent {
    fn event_type(&self) -> String {
        match self {
            UserEvent::UserCreated { .. } => "UserCreated".to_string(),
            UserEvent::UserUpdated { .. } => "UserUpdated".to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CatalogEvent {
    ProductAdded { product_id: String, name: String },
    ProductUpdated { product_id: String, name: String },
}

impl Event for CatalogEvent {
    fn event_type(&self) -> String {
        match self {
            CatalogEvent::ProductAdded { .. } => "ProductAdded".to_string(),
            CatalogEvent::ProductUpdated { .. } => "ProductUpdated".to_string(),
        }
    }
}

// Create a merged event type for queries
query_events!(UserHistoryEvent => [UserEvent, CatalogEvent]);

// Now you can use UserHistoryEvent in your queries
fn process_user_history(events: Vec<UserHistoryEvent>) {
    for event in events {
        match event {
            UserHistoryEvent::UserEvent(user_evt) => {
                println!("User event: {}", user_evt.event_type());
            }
            UserHistoryEvent::CatalogEvent(catalog_evt) => {
                println!("Catalog event: {}", catalog_evt.event_type());
            }
        }
    }
}

// Use From trait for easy conversion
let user_evt = UserEvent::UserCreated {
    user_id: "user-1".to_string(),
    name: "Alice".to_string(),
};
let merged: UserHistoryEvent = user_evt.into();

// Works with collections
let mut events: Vec<UserHistoryEvent> = vec![];
events.push(UserEvent::UserCreated {
    user_id: "user-1".to_string(),
    name: "Alice".to_string(),
}.into());
events.push(CatalogEvent::ProductAdded {
    product_id: "prod-1".to_string(),
    name: "Laptop".to_string(),
}.into());
```

### What the Macro Generates

The `query_events!` macro automatically generates:

1. **Enum with variants** for each event type:

   ```rust
   pub enum UserHistoryEvent {
       UserEvent(UserEvent),
       CatalogEvent(CatalogEvent),
   }
   ```

2. **From trait implementations** for easy conversion:

   ```rust
   impl From<UserEvent> for UserHistoryEvent { ... }
   impl From<CatalogEvent> for UserHistoryEvent { ... }
   ```

3. **Serialize/Deserialize** implementations that delegate to the inner event types

4. **Event trait implementation** that delegates `event_type()` to the wrapped event

5. **PartialEq** implementation for comparing wrapped events

6. **Display** implementation for formatting

7. **Clone and Debug** derived traits

### Use Cases

The merged event type is useful for:

- **Cross-aggregate queries**: Building read models that need data from multiple aggregates
- **User activity logs**: Tracking all actions across different parts of the system
- **Audit trails**: Recording events from various domains in a unified format
- **Event processing**: Handling events from multiple sources in a single stream processor
- **Projections**: Creating views that span multiple aggregate types

### Example: Building a User Activity Log

```rust
use replay_macros::query_events;

query_events!(ActivityEvent => [UserEvent, OrderEvent, PaymentEvent]);

// Query function that fetches events from multiple streams
async fn get_user_activity(user_id: &str) -> Vec<ActivityEvent> {
    let mut activity = Vec::new();
    
    // Fetch user events
    let user_events = fetch_user_events(user_id).await;
    activity.extend(user_events.into_iter().map(ActivityEvent::from));
    
    // Fetch order events
    let order_events = fetch_user_orders(user_id).await;
    activity.extend(order_events.into_iter().map(ActivityEvent::from));
    
    // Fetch payment events
    let payment_events = fetch_user_payments(user_id).await;
    activity.extend(payment_events.into_iter().map(ActivityEvent::from));
    
    // Sort by timestamp, filter, etc.
    activity.sort_by_key(|e| e.timestamp());
    activity
}

// Build a projection from the merged events
fn build_activity_summary(events: Vec<ActivityEvent>) -> ActivitySummary {
    let mut summary = ActivitySummary::default();
    
    for event in events {
        match event {
            ActivityEvent::UserEvent(evt) => summary.process_user_event(evt),
            ActivityEvent::OrderEvent(evt) => summary.process_order_event(evt),
            ActivityEvent::PaymentEvent(evt) => summary.process_payment_event(evt),
        }
    }
    
    summary
}
```
