# Replay

Event Sourcing and CQRS library

In event sourcing events are the source of truth, they are organized in streams that have an id and a version.
Everytime an event is applied into a stream its state changes and its version is incremented.

You can replay all events of a stream to reproduce previous states (hence the name of this library).

Then from DDD we have aggregates that are implemented like a stream that accepts commands.
You can chose you implement just `Stream` (state will be built from events) or `Aggregate` (stream that accepts commands).

> Important to note `Streams` never fails as the are built from events that happened in the past (so there are no side effects, error handling, etc.). All of these concerns are managed in the aggregate.

## Example

Let's model a small banking domain with two aggregate roots:

- **`User`** — a person who can register and own accounts.
- **`BankAccount`** — an account opened *for* a user that money flows in and out of.

From those events we build the same read model — a user's **global position**
(their name plus the summed balance of every account they own) — in two
different ways so the trade-offs are visible side by side:

- a **live** [`Query`] folded in memory on every read, and
- an **inline** [`InlineProjection`] materialised into Postgres inside the append transaction.

> The full, compilable source for everything below lives in
> [persistence/examples/global_position.rs](persistence/examples/global_position.rs)
> and is exercised by the integration tests, so the README stays in lock-step with
> working code. Run the live half (no database required) with
> `cargo run -p es-replay-persistence --example global_position`.

We glob-import `replay::prelude::*` to bring the core traits (`Aggregate`,
`EventStream`, `Compactable`, …) into scope, but import from `replay_persistence`
explicitly: its prelude re-exports `Result`/`Error`, which would shadow the
`std`/`serde` names the derive macros expand to.

```rust
use std::collections::HashSet;

use replay::prelude::*;
use replay_macros::{define_aggregate, query_events};
use replay_persistence::{db_error, InlineProjection, PersistedEvent, Query, StreamFilter};
```

### Defining the aggregates

The `define_aggregate!` macro generates the aggregate struct, its strongly-typed
URN (`UserUrn`, `BankAccountUrn`), and the command/event enums (`UserCommand`,
`UserEvent`, …). You provide the `EventStream` (how events fold into state) and
`Aggregate` (how commands produce events) implementations.

The URN namespace auto-derives from the type name (`User` → `"user"`), so it only
needs to be set explicitly when you want something other than the default — as
`BankAccount` does below.

```rust
define_aggregate! {
    User {
        // namespace auto-derives from the type name: `User` -> "user".
        state: {
            name: String,
        },
        commands: {
            Register { name: String },
        },
        events: {
            Registered { name: String },
        }
    }
}

impl EventStream for User {
    type Event = UserEvent;

    fn stream_type() -> String {
        "User".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            UserEvent::Registered { name } => self.name = name,
        }
    }
}

impl Aggregate for User {
    type Command = UserCommand;
    type Error = replay::Error;
    type Services = ();

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> replay::Result<Vec<Self::Event>> {
        match command {
            UserCommand::Register { name } => Ok(vec![UserEvent::Registered { name }]),
        }
    }
}
```

A `BankAccount` is opened *for* a user: the `OpenAccount` command only references
the owning root by its URN, it never reaches into the `User` aggregate. Only
`AccountOpened` carries the owner; movements stay lean and the read models resolve
account → owner from that event.

```rust
define_aggregate! {
    BankAccount {
        // Override the default "bank-account" with the shorter "account",
        // so URNs read `urn:account:alice-checking`.
        namespace: "account",
        state: {
            owner: Option<UserUrn>,
            balance: f64,
        },
        commands: {
            OpenAccount { owner: UserUrn },
            Deposit { amount: f64 },
            Withdraw { amount: f64 },
            CloseMonth { month: chrono::NaiveDate },
        },
        events: {
            AccountOpened { owner: UserUrn },
            Deposited { amount: f64 },
            Withdrawn { amount: f64 },
            MonthlyClosed { month: chrono::NaiveDate, closing_balance: f64 },
        }
    }
}

impl EventStream for BankAccount {
    type Event = BankAccountEvent;

    fn stream_type() -> String {
        "BankAccount".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            BankAccountEvent::AccountOpened { owner } => self.owner = Some(owner),
            BankAccountEvent::Deposited { amount } => self.balance += amount,
            BankAccountEvent::Withdrawn { amount } => self.balance -= amount,
            // A checkpoint replaces the running balance with the closing one, so a
            // compacted stream rehydrates to exactly the same state.
            BankAccountEvent::MonthlyClosed { closing_balance, .. } => {
                self.balance = closing_balance
            }
        }
    }
}

impl Aggregate for BankAccount {
    type Command = BankAccountCommand;
    type Error = replay::Error;
    type Services = ();

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> replay::Result<Vec<Self::Event>> {
        match command {
            BankAccountCommand::OpenAccount { owner } => {
                Ok(vec![BankAccountEvent::AccountOpened { owner }])
            }
            BankAccountCommand::Deposit { amount } => {
                Ok(vec![BankAccountEvent::Deposited { amount }])
            }
            BankAccountCommand::Withdraw { amount } => {
                if self.balance < amount {
                    return Err(replay::Error::business_rule_violation("Insufficient funds")
                        .with_operation("Withdraw")
                        .with_context("amount_tried", amount));
                }
                Ok(vec![BankAccountEvent::Withdrawn { amount }])
            }
            BankAccountCommand::CloseMonth { month } => Ok(vec![BankAccountEvent::MonthlyClosed {
                month,
                closing_balance: self.balance,
            }]),
        }
    }
}
```

### Compaction

Implement `Compactable` to keep streams short. Each `MonthlyClosed` snapshots the
balance, so everything before the most recent checkpoint is redundant once the
balance is replaced on replay — a compacted stream rehydrates to exactly the same
state.

```rust
impl Compactable for BankAccount {
    async fn compacted_events(
        &self,
        events: impl futures::TryStream<Ok = BankAccountEvent, Error = replay::Error> + Send,
    ) -> replay::Result<Vec<BankAccountEvent>> {
        use futures::TryStreamExt;
        events
            .try_fold(Vec::new(), |mut tail, event| async move {
                if matches!(event, BankAccountEvent::MonthlyClosed { .. }) {
                    tail.clear();
                }
                tail.push(event);
                Ok(tail)
            })
            .await
    }
}
```

### A cross-aggregate read model

`query_events!` builds one merged event type so a single reader can consume both
streams. Its `Deserialize` impl tries each underlying type in turn
(deserialize-or-skip), which is how unrelated events are filtered out during a
fold or while routing to an inline projection.

```rust
query_events!(GlobalPositionEvent => [UserEvent, BankAccountEvent]);

/// A user's name together with the summed balance of every account they own.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct GlobalPosition {
    pub name: String,
    pub total_balance: f64,
}
```

**Strategy 1 — a live query.** `GlobalPositionQuery` folds history on demand.
Because deposits and withdrawals do not carry the owner, it first learns which
accounts belong to the user (from `AccountOpened`) and then applies only the
movements on those accounts. The `stream_filter` is a hint the store pushes down
where it can; nothing is stored, so every read folds the matched log from the
start.

```rust
pub struct GlobalPositionQuery {
    user: UserUrn,
    owned_accounts: HashSet<urn::Urn>,
    position: GlobalPosition,
}

impl GlobalPositionQuery {
    pub fn for_user(user: UserUrn) -> Self {
        Self {
            user,
            owned_accounts: HashSet::new(),
            position: GlobalPosition::default(),
        }
    }

    pub fn position(&self) -> &GlobalPosition {
        &self.position
    }
}

impl Query for GlobalPositionQuery {
    type Event = GlobalPositionEvent;

    fn stream_filter(&self) -> StreamFilter {
        StreamFilter::with_stream_id::<User>(&self.user)
            .or(StreamFilter::for_stream_type::<BankAccount>())
    }

    fn update(&mut self, event: PersistedEvent<Self::Event>) {
        match event.data {
            GlobalPositionEvent::UserEvent(UserEvent::Registered { name }) => {
                self.position.name = name;
            }
            GlobalPositionEvent::BankAccountEvent(BankAccountEvent::AccountOpened { owner }) => {
                if owner == self.user {
                    self.owned_accounts.insert(event.stream_id);
                }
            }
            GlobalPositionEvent::BankAccountEvent(BankAccountEvent::Deposited { amount }) => {
                if self.owned_accounts.contains(&event.stream_id) {
                    self.position.total_balance += amount;
                }
            }
            GlobalPositionEvent::BankAccountEvent(BankAccountEvent::Withdrawn { amount }) => {
                if self.owned_accounts.contains(&event.stream_id) {
                    self.position.total_balance -= amount;
                }
            }
            GlobalPositionEvent::BankAccountEvent(BankAccountEvent::MonthlyClosed { .. }) => {}
        }
    }
}
```

**Strategy 2 — a materialised inline projection.** `GlobalPositionProjection`
maintains the same read model in two Postgres tables, written inside the very same
transaction that appends the events. Reads become a single indexed `SELECT`, paid
for with schema, versioning, and write-time cost. (See the full `handle`
implementation in
[persistence/examples/global_position.rs](persistence/examples/global_position.rs).)

The view tables are owned by a migration, not created from `init`. Prefer driving
schema from your migration history over running DDL in `init` — it keeps table
creation and evolution auditable instead of coupling it to registration. So `init`
stays a no-op here, and the tables come from
[persistence/tests/migrations/0006_global_position_projection.sql](persistence/tests/migrations/0006_global_position_projection.sql).

```rust
pub struct GlobalPositionProjection;

impl InlineProjection for GlobalPositionProjection {
    type Exec = sqlx::PgConnection;
    type Event = GlobalPositionEvent;

    fn name(&self) -> &str {
        "global_position"
    }

    fn version(&self) -> i32 {
        1
    }

    async fn init(&mut self, _conn: &mut Self::Exec) -> replay::Result<()> {
        // The view tables are created by a SQL migration, not here. Running DDL
        // from `init` is discouraged because it bypasses your migration history.
        Ok(())
    }

    async fn handle(
        &mut self,
        conn: &mut Self::Exec,
        events: &[PersistedEvent<Self::Event>],
    ) -> replay::Result<()> {
        // Upsert names from Registered, account→owner from AccountOpened, and add
        // each Deposited/Withdrawn delta to the owner's running total.
        # let _ = (conn, events);
        Ok(())
    }
}
```

### Driving it with CQRS

`Cqrs` wraps an event store. `execute` runs a command (load → handle → append),
`fetch_aggregate` rehydrates a single aggregate, and `run_query` folds a live
query across the streams its filter matches. The live half needs no database, so
it runs against the in-memory store:

```rust
use replay_persistence::{Cqrs, InMemoryEventStore};

let cqrs = Cqrs::new(InMemoryEventStore::new());

// Register a user.
let alice = UserUrn::new("alice").unwrap();
cqrs.execute::<User>(
    &alice,
    Default::default(),
    UserCommand::Register { name: "Alice".to_string() },
    &(),
    None,
)
.await?;

// Open two accounts for Alice and move some money around.
let checking = BankAccountUrn::new("alice-checking").unwrap();
let savings = BankAccountUrn::new("alice-savings").unwrap();

for account in [&checking, &savings] {
    cqrs.execute::<BankAccount>(
        account,
        Default::default(),
        BankAccountCommand::OpenAccount { owner: alice.clone() },
        &(),
        None,
    )
    .await?;
}

cqrs.execute::<BankAccount>(&checking, Default::default(),
    BankAccountCommand::Deposit { amount: 1_000.0 }, &(), None).await?;
cqrs.execute::<BankAccount>(&checking, Default::default(),
    BankAccountCommand::Withdraw { amount: 250.0 }, &(), None).await?;
cqrs.execute::<BankAccount>(&savings, Default::default(),
    BankAccountCommand::Deposit { amount: 500.0 }, &(), None).await?;

// A single account always knows its own balance straight from the aggregate.
let checking_account = cqrs.fetch_aggregate::<BankAccount>(&checking).await?;
assert_eq!(checking_account.balance, 750.0);

// The global position spans every account Alice owns. Here it is folded live.
let mut position = GlobalPositionQuery::for_user(alice.clone());
cqrs.run_query::<_, GlobalPositionEvent>(&mut position).await?;

assert_eq!(position.position().name, "Alice");
assert_eq!(position.position().total_balance, 1_250.0); // 1000 - 250 + 500
```

Swap `InMemoryEventStore` for `PostgresEventStore` and register
`GlobalPositionProjection` to have the same numbers materialised inside the append
transaction — the integration test
`global_position_live_query_and_inline_projection_agree_postgres_test` proves both
strategies produce an identical `GlobalPosition`.

## Using Macros

### `#[derive(Urn)]`

The `Urn` derive macro generates the boilerplate needed to use a newtype wrapper around `urn::Urn`
as a strongly-typed stream identifier. Given a struct with a single `Urn` field it generates:

| What is generated | Description |
| --- | --- |
| `impl From<MyUrn> for Urn` | Unwrap to the raw `urn::Urn` |
| `impl Display for MyUrn` | Delegates to the inner `Urn` |
| `impl FromStr for MyUrn` | Parses a URN string, returns `urn::Error` on failure |
| `impl PartialEq / Eq` | Equality based on the inner `Urn` value |
| `impl Hash` | Hash based on the inner `Urn` value — safe to use as `HashMap`/`HashSet` key |
| `impl TryFrom<Urn>` | Validates the NID against the namespace, returns `Err(String)` on mismatch |
| `new(id)`, `new_random()`, `parse(s)` | Constructors with namespace validation |
| `namespace()`, `nid()`, `nss()`, `to_urn()` | Namespace and accessor helpers |

**Namespace** is determined in order:

1. Explicit `#[urn(namespace = "your-nid")]` attribute — use this when the type name doesn’t match the desired NID (e.g. `FileManagerUrn` with `namespace = "file"` gives `urn:file:123`).
2. Auto-derived from the type name: strips a trailing `Urn` suffix, then converts `CamelCase` → `kebab-case` (e.g. `BankAccountUrn` → `"bank-account"`).

```rust
use replay_macros::Urn;
use serde::{Serialize, Deserialize};
use urn::Urn;

// Namespace auto-derived: BankAccountUrn → strip Urn → BankAccount → "bank-account"
#[derive(Clone, Debug, Serialize, Deserialize, Urn)]
pub struct BankAccountUrn(Urn);

// Namespace pinned explicitly — auto-derive would give "file-manager",
// but the domain calls for the shorter "file".
#[derive(Clone, Debug, Serialize, Deserialize, Urn)]
#[urn(namespace = "file")]
pub struct FileManagerUrn(Urn);

// Constructors
let id = BankAccountUrn::new("acct-1").unwrap();
assert_eq!(id.to_string(), "urn:bank-account:acct-1");

let file = FileManagerUrn::new("123").unwrap();
assert_eq!(file.to_string(), "urn:file:123");

// Parse from a string
let id: BankAccountUrn = "urn:bank-account:acct-1".parse().unwrap();

// Convert to raw Urn
let raw: Urn = id.clone().into();
assert_eq!(raw.nid(), "bank-account");

// Equality and hashing
let same = BankAccountUrn::new("acct-1").unwrap();
assert_eq!(id, same);

use std::collections::HashMap;
let mut map: HashMap<BankAccountUrn, f64> = HashMap::new();
map.insert(id.clone(), 100.0);
assert_eq!(map[&id], 100.0);
```

### URN helper methods

Every `#[derive(Urn)]` type gets the following methods (namespace is auto-derived or set via `#[urn(namespace = "...")]`):

| Method | Description |
| --- | --- |
| `XxxUrn::new(id)` | Build from any `Display` value. Accepts a plain ID (`"acct-1"`) or a full URN string (`"urn:account:acct-1"`). Validates the namespace, returns `Err` if it doesn't match. Automatically unwraps nested same-namespace URNs. |
| `XxxUrn::new_random()` | Build with a random UUID v4 NSS. Infallible. |
| `XxxUrn::parse(s)` | Like `new` but returns `Err(String)` with a descriptive message instead of `urn::Error`. |
| `XxxUrn::namespace()` | Returns the NID as a `&'static str`. |
| `.nid()` | NID of this URN instance. |
| `.nss()` | NSS (the ID part) of this instance. |
| `.to_urn()` | Borrow the inner `&urn::Urn`. |

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
        source: replay::Error,
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
  - `YourTypeUrn::new(id)` - Creates a URN with the configured namespace. If `id` already starts with `"urn:"` it is parsed and namespace-validated instead of being used as a raw identifier; nested same-namespace URNs (e.g. `urn:customer:urn:customer:123`) are automatically unwrapped to the innermost id
  - `YourTypeUrn::parse(input)` - Parses a full URN string and validates the namespace
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

// You can also extend an existing service trait:
define_aggregate! {
    Order {
        state: {
            order_id: String,
            items: Vec<String>,
        },
        commands: {
            CreateOrder { items: Vec<String> }
        },
        events: {
            OrderCreated { order_id: String, items: Vec<String> }
        },
        service: FileService {
            async fn validate_items(items: &[String]) -> bool;
        }
    }
}

// This generates:
// pub trait OrderServices: FileService + Send + Sync {
//     async fn validate_items(&self, items: &[String]) -> bool;
// }
//
// Your implementation must now implement both FileService and OrderServices

// You can also extend multiple service traits:
define_aggregate! {
    AuditLog {
        state: {
            entries: Vec<String>,
        },
        commands: {
            AddEntry { message: String }
        },
        events: {
            EntryAdded { message: String }
        },
        service: FileService + LogService {
            fn validate_entry(entry: &str) -> bool;
        }
    }
}

// This generates:
// pub trait AuditLogServices: FileService + LogService + Send + Sync {
//     fn validate_entry(&self, entry: &str) -> bool;
// }

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

#### Service Function Lifetimes

Service functions can use lifetime parameters when working with borrowed data:

```rust
define_aggregate! {
    Document {
        state: {
            content: String,
            validated: bool,
        },
        commands: {
            UpdateContent { content: String }
        },
        events: {
            ContentUpdated { content: String }
        },
        service: {
            // Lifetime parameters for borrowed data
            fn validate_content<'a>(content: &'a str) -> Result<&'a str, String>;
            
            // Multiple lifetime parameters work too
            fn compare_contents<'a, 'b>(old: &'a str, new: &'b str) -> bool;
        }
    }
}

// The generated trait includes the lifetime parameters
// pub trait DocumentServices: Send + Sync {
//     fn validate_content<'a>(&self, content: &'a str) -> Result<&'a str, String>;
//     fn compare_contents<'a, 'b>(&self, old: &'a str, new: &'b str) -> bool;
// }

impl DocumentServices for MyDocumentServices {
    fn validate_content<'a>(&self, content: &'a str) -> Result<&'a str, String> {
        if content.is_empty() {
            Err("Content cannot be empty".to_string())
        } else {
            Ok(content)
        }
    }

    fn compare_contents<'a, 'b>(&self, old: &'a str, new: &'b str) -> bool {
        old != new
    }
}
```

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

// new() is smart about full URN strings — passing a URN is the same as parse()
let same = CustomerUrn::new("urn:customer:peter@example.com").unwrap();
assert_eq!(same.to_string(), "urn:customer:peter@example.com");

// Accidentally nested URNs are automatically unwrapped
let nested = CustomerUrn::new("urn:customer:urn:customer:urn:customer:peter@example.com").unwrap();
assert_eq!(nested.to_string(), "urn:customer:peter@example.com");

// Wrong namespace is rejected
assert!(CustomerUrn::new("urn:other-namespace:peter@example.com").is_err());

// Get the namespace identifier
assert_eq!(CustomerUrn::namespace(), "customer");

// Access URN components
assert_eq!(customer_urn.nid(), "customer");
assert_eq!(customer_urn.nss(), "peter@example.com");

// Get reference to inner URN
let inner_urn: &Urn = customer_urn.to_urn();
```

### Generic Type Parameters

Aggregates can use generic type parameters to make them reusable with different data types. The macro automatically adds required trait bounds (`Clone`, `Default`, `Debug`, `Serialize`, `DeserializeOwned`, `Send`, `Sync`) to all type parameters.

```rust
use replay_macros::define_aggregate;
use serde::{Deserialize, Serialize};

// Define an aggregate with a generic type parameter
define_aggregate! {
    FileManager<T: PartialEq> {
        state: {
            processed: T,
            count: usize,
        },
        commands: {
            ProcessFile { data: T }
        },
        events: {
            FileProcessed { data: T }
        }
    }
}

// The macro generates:
// - FileManager<T> struct with automatic bounds
// - FileManagerCommand<T> enum
// - FileManagerEvent<T> enum (only includes T since T is used in events)
// - FileManagerUrn type

impl EventStream for FileManager<String> {
    type Event = FileManagerEvent<String>;

    fn stream_type() -> String {
        "FileManager".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            FileManagerEvent::FileProcessed { data } => {
                self.processed = data;
                self.count += 1;
            }
        }
    }
}

// Usage with concrete type
let id = FileManagerUrn::new("manager-1").unwrap();
let mut manager: FileManager<String> = FileManager::with_id(id);
let event = FileManagerEvent::FileProcessed { 
    data: "file.txt".to_string() 
};
manager.apply(event);
```

#### Smart Event Enum Generics

The macro intelligently analyzes which type parameters are actually used in event variants. **If a type parameter is not used in any event**, it won't be included in the Event enum's generic parameters.

This means you don't need to add `PartialEq` bounds unless the type is actually used in events:

```rust
use serde::{Deserialize, Serialize};

// Type without PartialEq
#[derive(Clone, Default, Debug, Serialize, Deserialize)]
struct NoCompare {
    data: String,
}

// T is used in state and commands, but NOT in events
define_aggregate! {
    Container<T> {
        state: {
            item: T,
            count: usize,
        },
        commands: {
            Store { value: T }
        },
        events: {
            Stored { count: usize }  // T is NOT used here!
        }
    }
}

// The macro generates:
// - Container<T> with all required bounds (Clone, Default, etc.)
// - ContainerCommand<T> (includes T since used in commands)
// - ContainerEvent (NO type parameter! T not used in events)

impl EventStream for Container<NoCompare> {
    type Event = ContainerEvent;  // Note: No <NoCompare> needed!

    fn stream_type() -> String {
        "Container".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            ContainerEvent::Stored { count } => {
                self.count = count;
            }
        }
    }
}

// This works even though NoCompare doesn't implement PartialEq,
// because ContainerEvent doesn't need it
let id = ContainerUrn::new("container-1").unwrap();
let container: Container<NoCompare> = Container::with_id(id);
let event = ContainerEvent::Stored { count: 5 };
```

**Key points about generic type parameters:**

- **Automatic bounds**: The macro adds `Clone + Default + Debug + Serialize + DeserializeOwned + Send + Sync` to all type parameters
- **PartialEq is conditional**: Only add `PartialEq` bound (e.g., `T: PartialEq`) if T is used in events (required by the `Event` trait)
- **Smart Event enum**: Event enum only includes type parameters that are actually used in event variants
- **Aggregate comparison**: Aggregates always compare by ID only (using `WithId`), regardless of their generic type parameters
- **Flexibility**: Allows using types that don't implement `PartialEq` as long as they're not in events

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

### Composing Scoped URNs

When two streams are related (e.g., a bank account belonging to a branch), you can embed that
relationship directly in the URN using `ScopedUrn::at` and recover it with `ScopedUrn::extract_scope`.

The resulting URN uses the format `urn:<nid>:<nss>@<scope_nid>:<scope_nss>`.

`ScopedUrn` is a blanket trait automatically available on **every URN type** that implements
`Into<Urn> + TryFrom<Urn> + Clone + AsRef<Urn>` — which includes all `#[derive(Urn)]` types (and therefore
all URN types generated by `define_aggregate!` too). You call it directly on the URN value,
with no stream wrapper needed.

```rust
use replay::prelude::*;

let account: BankAccountUrn = BankAccountUrn::new("acct-1")?;
let branch:  BranchUrn      = BranchUrn::new("london")?;

// urn:bank-account:acct-1  +  urn:branch:london  →  urn:bank-account:acct-1@branch:london
// Pass by reference — neither account nor branch is consumed.
let scoped: BankAccountUrn = account.at(&branch)?;
println!("{}", scoped); // urn:bank-account:acct-1@branch:london
println!("{}", branch); // branch is still usable here

// Extract the scope back — specify the expected output type as a type parameter.
// The output type's TryFrom<Urn> implementation validates the NID.
let extracted: BranchUrn = scoped.extract_scope::<BranchUrn>()?;
```

`at` takes `&self` and accepts any type that implements `AsRef<urn::Urn>`, which includes every
URN type generated by `#[derive(Urn)]` or `define_aggregate!`. Neither the receiver nor the
scope argument is consumed, so both remain usable after the call.

`extract_scope` is **generic over the output type**. You declare what URN type you expect and
the conversion is handled by that type's `TryFrom<Urn>` implementation, which validates the NID.
Requesting the wrong type is a compile-time-safe, runtime-checked error:

```rust
// ✅ correct — scope NID is "branch", BranchUrn accepts it
let branch: BranchUrn = scoped.extract_scope::<BranchUrn>()?;

// ❌ wrong type — scope NID is "branch", but BankAccountUrn expects "bank-account"
let wrong: BankAccountUrn = scoped.extract_scope::<BankAccountUrn>()?; // Err: NID mismatch
```

**Validation rules enforced by `at`:**

| Condition | Error |
| --- | --- |
| Current URN's NSS already contains `@` | "URN is already scoped" |
| `other`'s NSS contains `@` | "scope URN is already scoped" |

**Validation rules enforced by `extract_scope`:**

| Input NSS | Error |
| --- | --- |
| `acct-1` — no `@` | "not scoped (no '@' in NSS)" |
| `@branch:london` — empty own NSS | "empty NSS before '@'" |
| `acct-1@branch` — no `:` after `@` | "missing ':' (expected '`<nid>:<nss>`')" |
| `acct@:nss` — empty scope NID | "Scope NID is empty" |
| `acct@nid:` — empty scope NSS | "Scope NSS is empty" |
| `a@b:c@d:e` — multiple `@` | "multiple '@' in NSS (ambiguous scope)" |
| wrong output type | "NID mismatch" (from `TryFrom<Urn>` on the output type) |

The output type can be any type that implements `TryFrom<Urn>` — it does not have to be the
same as the original URN's type. This allows a `BankAccountUrn` to extract a `BranchUrn`,
a `TenantUrn`, or any other domain type, as long as the NID embedded in the scope part matches.

### Prelude

There are two prelude options depending on your dependencies.

**Core prelude** (`replay`) — traits only, no persistence or macros:

```rust
use replay::prelude::*;
```

| Export | Purpose |
| --- | --- |
| `ScopedUrn` | `at` and `extract_scope` on URN types |
| `WithId` | `with_id`, `get_id`, `with_string_id` on aggregate structs |
| `EventStream` | `apply`, `stream_type` |
| `Aggregate` | `handle` |
| `Compactable` | `compacted_events` |
| `Event` | `event_type` |

**Full prelude** (`replay_persistence`) — everything in one import, including macros and persistence:

```rust
use replay_persistence::prelude::*;
```

| Export | Purpose |
| --- | --- |
| `ScopedUrn`, `WithId`, `EventStream`, `Aggregate`, `Compactable`, `Event` | Core traits (same as above) |
| `Error`, `Result` | Core error / result types |
| `Urn` | `#[derive(Urn)]` derive macro |
| `EventDerive` | `#[derive(Event)]` derive macro (re-exported as `EventDerive`) |
| `define_aggregate!` | Aggregate scaffolding macro |
| `query_events!` | Multi-aggregate event wrapper macro |
| `Cqrs` | Command/query execution engine |
| `EventStore` | Trait for pluggable event store backends |
| `InMemoryEventStore` | In-memory backend (testing) |
| `PostgresEventStore` | PostgreSQL backend |
| `InlineProjection` | Trait for inline read-model projections |
| `PostgresInlineProjection` | Postgres-specific inline projection marker trait |
| `PersistedEvent` | Wrapper holding an event with its metadata |
| `Query` | Trait for read-model projections |
| `StreamFilter` | Filter builder for event queries |
| `AggregateVersion` | Current / archived snapshot version discriminant |

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

## Filtering Events with `StreamFilter`

`StreamFilter` controls which persisted events are returned by `stream_events` and which events a
`Query` processes. Filters compose freely using `.and()`, `.or()` and the `!` operator.

### Available filters

| Constructor | Matches events where… |
| --- | --- |
| `StreamFilter::all()` | everything (no restriction) |
| `StreamFilter::with_stream_id::<S>(&id)` | `stream_id` equals the given URN |
| `StreamFilter::for_stream_type::<S>()` | stream type equals `S::stream_type()` |
| `StreamFilter::with_metadata(value)` | metadata equals the serialised value |
| `StreamFilter::after_version(n)` | sequence version **>** `n` (exclusive) |
| `StreamFilter::up_to_version(n)` | sequence version **≤** `n` (inclusive) |
| `StreamFilter::created_after(ts)` | creation timestamp **>** `ts` (exclusive) |
| `StreamFilter::created_before(ts)` | creation timestamp **≤** `ts` (inclusive) |
| `StreamFilter::with_aggregate_version(v)` | `aggregate_version` equals `v` (`None` = current events, `Some(n)` = archived snapshot `n`) |

### Combining filters

All filters implement a fluent builder API:

```rust
use replay_persistence::StreamFilter;

// AND: both conditions must hold
let filter = StreamFilter::with_stream_id::<OrderStream>(&order_id)
    .and(StreamFilter::after_version(10));

// OR: either condition is sufficient
let filter = StreamFilter::for_stream_type::<OrderStream>()
    .or(StreamFilter::for_stream_type::<PaymentStream>());

// NOT: negate any filter (also available via the `!` operator)
let filter = StreamFilter::after_version(5).not();
// equivalently:
let filter = !StreamFilter::after_version(5);

// Chaining helpers — each returns a new StreamFilter with the extra condition ANDed in
let filter = StreamFilter::with_stream_id::<BankAccountStream>(&account_id)
    .and_with_metadata(metadata_value)
    .and_at_stream_version(50)          // ≤ version 50
    .and_at_timestamp(cutoff);          // created ≤ cutoff
```

### Per-filter examples

#### Select a single stream

```rust
let filter = StreamFilter::with_stream_id::<BankAccountStream>(&account_id);
```

#### Select all streams of a given type

```rust
let filter = StreamFilter::for_stream_type::<BankAccountStream>();
```

#### Events after a known checkpoint (e.g. catch-up subscriptions)

```rust
let filter = StreamFilter::for_stream_type::<OrderStream>()
    .and(StreamFilter::after_version(last_processed_version));
```

#### Time-travel read — replay a stream as it looked at a past instant

```rust
let cutoff = "2026-01-01T00:00:00Z".parse::<chrono::DateTime<chrono::Utc>>().unwrap();
let filter = StreamFilter::with_stream_id::<BankAccountStream>(&account_id)
    .and_at_timestamp(cutoff);
```

#### Version-range slice — events between two sequence numbers

```rust
let filter = StreamFilter::with_stream_id::<BankAccountStream>(&account_id)
    .and(StreamFilter::after_version(10))   // > 10
    .and(StreamFilter::up_to_version(20));  // ≤ 20
```

#### Read a specific compaction snapshot (`aggregate_version = Some(n)`) or the live stream (`None`)

```rust
// Live (current) events
let filter = StreamFilter::with_stream_id::<BankAccountStream>(&account_id)
    .and_aggregate_version(None);

// Archived snapshot created during the 2nd compaction
let filter = StreamFilter::with_stream_id::<BankAccountStream>(&account_id)
    .and_aggregate_version(Some(2));
```

#### Filter by metadata (e.g. events tagged with a specific correlation ID)

```rust
#[derive(Serialize)]
struct OrderMeta { correlation_id: String }

let filter = StreamFilter::with_metadata(OrderMeta {
    correlation_id: "req-abc".into(),
});
```

#### Optional bounds — `_optional` variants are no-ops when the value is `None`, useful when

the bound comes from an API query parameter:

```rust
// at_version and at_timestamp both come from optional query params
let filter = StreamFilter::with_stream_id::<BankAccountStream>(&account_id)
    .and_at_stream_version_optional(at_version)   // Some(n) → UpToVersion(n), None → no-op
    .and_at_timestamp_optional(at_timestamp);      // Some(ts) → CreatedBefore(ts), None → no-op
```

### Using `StreamFilter` in a `Query`

Override `stream_filter` to restrict which events your query receives:

```rust
use replay_persistence::{Query, StreamFilter};

struct AccountSummaryQuery {
    account_id: BankAccountUrn,
    total_deposited: f64,
}

impl Query for AccountSummaryQuery {
    type Event = BankAccountEvent;

    fn stream_filter(&self) -> StreamFilter {
        StreamFilter::with_stream_id::<BankAccountStream>(&self.account_id)
    }

    fn update(&mut self, event: PersistedEvent<Self::Event>) {
        if let BankAccountEvent::Deposited { amount } = event.data {
            self.total_deposited += amount;
        }
    }
}

// Run it
let mut query = AccountSummaryQuery { account_id: id, total_deposited: 0.0 };
cqrs.run_query(&mut query).await?;
println!("total deposited: {}", query.total_deposited);
```

### Using `StreamFilter` directly with the store

```rust
use replay_persistence::StreamFilter;

let filter = StreamFilter::for_stream_type::<BankAccountStream>()
    .and(StreamFilter::after_version(last_seen));

let events = store.stream_events::<BankAccountEvent>(filter);
```

## Inline Projections (Postgres)

`Query` gives you a **live** read model: it folds events in memory when you ask for it.

An **inline projection** is different: it persists a read model inside the same Postgres
transaction that appends the events. That means the event append and the projection write
commit together or not at all.

### When to use it

Use an inline projection when:

- the read model lives in the same Postgres datastore as the event store
- the projection table/indexes are managed by normal SQL migrations
- on each event batch you just want to execute SQL against that table

### Migrations

There are two migration concerns:

1. **Your projection schema**: create the projection table/indexes in your own SQL migrations.
2. **Replay projection metadata**: ensure the `projections` registry table exists.

The registry table tracks projection `name()` and `version()`:

```sql
CREATE TABLE IF NOT EXISTS projections (
    name        TEXT                        NOT NULL PRIMARY KEY,
    version     INTEGER                     NOT NULL,
    updated_at  TIMESTAMP WITH TIME ZONE    NOT NULL DEFAULT (now())
);
```

`append_event` should also return the persisted event metadata used by inline projections
(`id`, `version`, `created`) so the store can build `PersistedEvent`s without a second
read-back query.

### Lowest-ceremony path: register a Postgres event handler

If your schema is already created by migrations, the simplest API is
`register_postgres_event_handler(...)`. You provide:

- a stable projection name
- a projection version
- a closure that receives `&mut sqlx::PgConnection` and the matching persisted events

```rust
use futures::future::BoxFuture;
use replay_persistence::{Cqrs, PostgresEventStore, PersistedEvent};

// Example event type from your aggregate
use crate::BankAccountEvent;

let store = PostgresEventStore::builder(pg_pool.clone())
    .register_postgres_event_handler::<BankAccountEvent, _>(
        "account_balance_view",
        1,
        |conn, events| {
            Box::pin(async move {
                for event in events {
                    let delta = match &event.data {
                        BankAccountEvent::Deposited { amount, .. } => *amount,
                        BankAccountEvent::Withdrawn { amount, .. } => -*amount,
                        BankAccountEvent::MonthlyClosed { .. } => continue,
                    };

                    sqlx::query(
                        "INSERT INTO account_balances (stream_id, balance)
                         VALUES ($1, $2)
                         ON CONFLICT (stream_id)
                         DO UPDATE SET balance = account_balances.balance + EXCLUDED.balance",
                    )
                    .bind(event.stream_id.to_string())
                    .bind(delta)
                    .execute(&mut *conn)
                    .await?;
                }

                Ok(())
            })
        },
    )
    .build()
    .await?;

let cqrs = Cqrs::new(store);
```

This helper assumes:

- the projection table already exists
- `init` is a no-op
- the only runtime work is "run SQL for this batch of events"

### Full control: implement `InlineProjection`

If you need more control, implement `InlineProjection` directly. This is useful when you want a
named type, custom `init`, or more involved logic than a single handler closure.

`PostgresInlineProjection` is also re-exported as a Postgres-specific marker for this case.

### Runtime behavior

- `PostgresEventStore::builder(...).build().await?` runs first-time projection setup and records
  the current version in the `projections` table.
- On each successful append, the store constructs `PersistedEvent`s from the metadata returned by
  `append_event(...)` and passes them to every registered projection.
- Projection handlers run inside the **same Postgres transaction** as the event append.
- If a projection handler returns an error, the whole append rolls back.

Inline projections are Postgres-only. The in-memory store remains useful for tests, but the
transactional guarantee belongs to the Postgres backend.

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

## Stream Compaction

As an aggregate accumulates events over a long lifetime the full history grows large, making every
replay slower. **Compaction** lets a `Compactable` aggregate replace its live event stream with the
minimum set of events needed to reconstruct its current state, while archiving the original history
under a versioned snapshot.

### The `Compactable` trait

`compacted_events` receives the **current live event stream** from the store and returns the
shortest subsequence that, when replayed from scratch, reproduces the same state. The aggregate
does **not** need to store events in its own fields:

```rust
use futures::TryStream;       // trait bound used in the signature
use futures::TryStreamExt;    // .try_fold() extension method
use replay::Compactable;

impl Compactable for BankAccountAggregate {
    async fn compacted_events(
        &self,
        events: impl TryStream<Ok = Self::Event, Error = replay::Error> + Send,
    ) -> replay::Result<Vec<Self::Event>> {
        // Sliding-window via try_fold: only events from the last MonthlyClosed
        // onward are kept in the accumulator. Prior months are never buffered.
        events
            .try_fold(Vec::new(), |mut tail, event| async move {
                if matches!(event, BankAccountEvent::MonthlyClosed { .. }) {
                    tail.clear();
                }
                tail.push(event);
                Ok(tail)
            })
            .await
    }
}
```

### Bank-account example with `MonthlyClosed`

`MonthlyClosed { month, closing_balance }` encodes an entire month's activity. Applying it sets
the running balance directly, so the aggregate needs no extra state fields for compaction:

```rust
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Event)]
enum BankAccountEvent {
    Deposited { operation_date: NaiveDate, amount: f64 },
    Withdrawn { operation_date: NaiveDate, amount: f64 },
    MonthlyClosed { month: NaiveDate, closing_balance: f64 },
}

struct BankAccountAggregate {
    pub id: BankAccountUrn,
    pub balance: f64,   // only what business logic needs — no compaction bookkeeping
}

impl EventStream for BankAccountAggregate {
    type Event = BankAccountEvent;

    fn stream_type() -> String { "BankAccount".to_string() }

    fn apply(&mut self, event: Self::Event) {
        match event {
            BankAccountEvent::Deposited { amount, .. }             => self.balance += amount,
            BankAccountEvent::Withdrawn { amount, .. }             => self.balance -= amount,
            BankAccountEvent::MonthlyClosed { closing_balance, .. } => self.balance = closing_balance,
        }
    }
}
```

**Before compaction** — 6 events in the live stream:

```text
Deposited     { operation_date: 2026-01-01, amount: 1000.00 }
Withdrawn     { operation_date: 2026-01-15, amount:  200.00 }
Deposited     { operation_date: 2026-01-31, amount:  500.00 }
MonthlyClosed { month: 2026-01,  closing_balance: 1300.00  }
Deposited     { operation_date: 2026-02-15, amount:  400.00 }
Withdrawn     { operation_date: 2026-02-28, amount:  100.00 }
```

**After compaction** — 3 events in the live stream, 6 archived as `Version(1)`:

```text
MonthlyClosed { month: 2026-01, closing_balance: 1300.00 }   <- all of January
Deposited     { operation_date: 2026-02-15, amount: 400.00 } <- preserved
Withdrawn     { operation_date: 2026-02-28, amount: 100.00 } <- preserved
```

Both streams yield `balance == 1600.00`. The full history is still accessible via
`AggregateVersion::Version(1)`.

### Running compaction via `Cqrs`

```rust
// Execute commands as usual.
cqrs.execute::<BankAccountAggregate>(
    &stream_id, meta.clone(),
    BankAccountCommand::CloseMonth { month: jan_1 },
    &services, None,
).await?;

// Fetch the aggregate to pass to compact.
// fetch_aggregate is a shorthand for fetch_aggregate_at with the latest version.
let aggregate = cqrs
    .fetch_aggregate::<BankAccountAggregate>(&stream_id)
    .await?;

// Compact: archives the full history and writes the minimal live stream.
let archive_version = cqrs.compact(&aggregate, meta).await?;
// archive_version == 1 on the first compaction, 2 on the second, etc.

// Future fetches replay only the 3 compacted events.
let compacted = cqrs
    .fetch_aggregate::<BankAccountAggregate>(&stream_id)
    .await?;

// Original history is still accessible.
let archived = cqrs
    .fetch_aggregate_at::<BankAccountAggregate>(
        &stream_id, AggregateVersion::Version(1), None, None,
    )
    .await?;
```

## Policies

A `Policy` is a checkpointed background subscriber that **reacts to events by
issuing commands**. It is the event-sourcing equivalent of a process manager or
saga: given an event it returns zero or more [`Dispatch`]es — commands the runner
executes against aggregates. No read model is derived; side effects happen through
the aggregate write path so causation, idempotency, and optimistic locking are all
inherited for free.

The `Policy` trait lives in `es-replay-persistence`. The implementor stays pure —
`react` takes an event and returns commands with no I/O — while the
[`PolicyRunner`] handles reading the feed, stamping causation metadata, persisting
cursors, and executing the [`Dispatch`]es.

### Implementing `Policy`

The minimal implementation requires only `name` and `react`. All other methods
have sensible defaults.

```rust,ignore
use replay_persistence::{Dispatch, PersistedEvent, Policy, StartAt, StreamFilter};

struct FeePolicy {
    ledger_id: FeeLedgerUrn,
}

impl Policy for FeePolicy {
    type Event = BankAccountEvent;

    fn name(&self) -> &str {
        "deposit_fee"
    }

    fn start_at(&self) -> StartAt {
        StartAt::Beginning // process all history on first run
    }

    fn stream_filter(&self) -> StreamFilter {
        StreamFilter::for_stream_type::<BankAccount>()
    }

    fn react(&self, event: &PersistedEvent<BankAccountEvent>) -> Vec<Dispatch> {
        match &event.data {
            BankAccountEvent::Deposited { amount } => vec![
                Dispatch::to::<FeeLedger>(
                    self.ledger_id.clone(),
                    FeeLedgerCommand::ChargeFee { amount: amount * FEE_RATE },
                )
            ],
            _ => vec![],
        }
    }
}
```

`react` is pure — it returns [`Dispatch`]es with no I/O. The runner automatically
stamps causation metadata onto every dispatched command before executing it:

```json
{
  "causation": {
    "policy":           "deposit_fee",
    "event_id":         "<uuid of the triggering Deposited event>",
    "stream_id":        "urn:account:alice-checking",
    "global_position":  42,
    "depth":            1
  }
}
```

This metadata travels with the resulting events, enabling:

- **Idempotency** — target aggregates can key duplicate detection on `causation.event_id` rather than command-value equality.
- **Loop prevention** — the `depth` counter is incremented at each hop; the runner skips reactions once it reaches the configured limit (see [Loop prevention](#loop-prevention)).
- **Observability** — every policy-driven event is traceable back to the original triggering event by `causation.event_id`.

You can attach additional metadata to a specific dispatch with [`Dispatch::with_metadata`]; the runner merges it with the causation block (colliding top-level keys are rejected):

```rust,ignore
Dispatch::to::<FeeLedger>(ledger_id.clone(), ChargeFee { amount })
    .with_metadata(Metadata::from([("correlation_id", request_id)]))
```

### Closure shortcut

For simple, single-aggregate reactions you can skip the struct and `impl Policy`
entirely with `register_policy_fn`. The closure runs through the exact same runner
machinery — causation stamping, failure handling, batching, advisory lock — as a
full `Policy` impl.

```rust,ignore
let runner = PolicyRunnerBuilder::new(cqrs, pool)
    .register_services::<FeeLedger>(fee_services)
    .register_policy_fn::<BankAccountEvent, _>(
        "deposit_fee",
        StartAt::Beginning,
        |event| match &event.data {
            BankAccountEvent::Deposited { amount } => vec![
                Dispatch::to::<FeeLedger>(ledger_id.clone(), ChargeFee { amount: amount * 0.01 })
            ],
            _ => vec![],
        },
    )
    .build();
```

### Building and starting the runner

`PolicyRunnerBuilder` collects services and policies, then `build()` produces a
`PolicyRunner`. Call `start_polling` to spawn a background daemon task per policy:

```rust,ignore
use std::time::Duration;
use replay_persistence::{PolicyRunnerBuilder, StartAt};

let runner = PolicyRunnerBuilder::new(cqrs, pool)
    .register_services::<BankAccount>(())          // enable Dispatch::to::<BankAccount>
    .register_services::<FeeLedger>(fee_services)
    .register_policy(FeePolicy { ledger_id })
    .build();

let daemon = runner.start_polling(Duration::from_secs(30));

// … application runs …

daemon.shutdown().await; // stops all tasks cleanly
```

### Cursor checkpointing and bootstrap

Each policy has a **stable name** that acts as its cursor key in the
`policy_cursors` table. On first registration the cursor is bootstrapped according
to `start_at()`:

| `StartAt` | Behaviour |
|-----------|-----------|
| `StartAt::Now` (default) | Cursor begins at the current global head; only newly appended events are processed. Safe when you don't want to fire commands retroactively across existing history. |
| `StartAt::Beginning` | Cursor begins at position 0; the full event history is drained once, then the policy follows live appends. Use this for backfill or projections derived from audit events. |

The cursor is written to Postgres **at least every `checkpoint_batch_size` events**
and unconditionally at the end of every drain pass. A crash after a command is
executed but before the cursor is saved will re-deliver the triggering event.
Correctness therefore depends on **idempotent command handling** keyed by
causation identity in the target aggregate.

### Advisory-lock leader election

Each policy task acquires a **Postgres session-scoped advisory lock** (keyed on
`hashtext(policy_name)`) before entering the leadership loop. At most one instance
across the cluster holds the lock at a time, so only one instance drives reactions
for a given policy name regardless of how many application nodes are running.

A node that fails to acquire the lock stands by, retrying every `interval`. When
the leader shuts down it explicitly calls `pg_advisory_unlock` so the standby can
take over without waiting for a TCP session timeout.

### LISTEN/NOTIFY latency optimisation

After acquiring the advisory lock each task opens a `PgListener` and subscribes to
the `replay_events` channel (the value of `REPLAY_NOTIFY_CHANNEL`). Whenever
`store_events` commits it fires `pg_notify('replay_events', stream_type)`, waking
the task immediately instead of waiting out the poll interval.

If `PgListener` setup fails (e.g. in environments without `LISTEN` support) the
task falls back silently to pure polling. You can also opt-out explicitly:

```rust,ignore
let runner = PolicyRunnerBuilder::new(cqrs, pool)
    .register_policy(my_policy)
    .without_notifications() // pure polling; no PgListener connection opened
    .build();
```

The constant `REPLAY_NOTIFY_CHANNEL` is re-exported from `es-replay-persistence`
so custom listeners can subscribe to the same channel:

```rust,ignore
use replay_persistence::REPLAY_NOTIFY_CHANNEL;

let mut listener = PgListener::connect_with(&pool).await?;
listener.listen(REPLAY_NOTIFY_CHANNEL).await?;
```

### Loop prevention

The runner prevents runaway event→command→event cascades via a **causation-depth
circuit breaker**. Each appended event carries a `causation.depth` counter
(incremented by the runner on every hop). If an event's depth reaches the
configured limit the runner skips reactions for it and logs a warning — the cursor
still advances so the policy is never permanently wedged.

Resolution order (most-specific wins):

| Source | How to set |
|--------|------------|
| Per-policy override | `fn max_causation_depth(&self) -> Option<u32> { Some(5) }` |
| Environment variable | `REPLAY_MAX_CAUSATION_DEPTH=5` |
| Built-in default | `10` |

### Batching

Two batch sizes control throughput vs checkpoint frequency:

| Setting | `Policy` override | Env var | Default |
|---------|-------------------|---------|---------|
| Events read per drain | `read_batch_size() -> Option<u32>` | `REPLAY_READ_BATCH_SIZE` | `100` |
| Events between cursor saves | `checkpoint_batch_size() -> Option<u32>` | `REPLAY_CHECKPOINT_BATCH_SIZE` | `100` |

The runner enforces `read_batch_size ≥ checkpoint_batch_size`.

### Failure handling

When a dispatch fails the runner classifies the error and responds accordingly:

| Error category | Condition | Action |
|----------------|-----------|--------|
| **Business-rule violation** | `ErrorKind::BusinessRuleViolation` | Advance cursor immediately — the event is correct, the domain logic rejected the command. No retry, no dead-letter. |
| **Retryable** | `Unavailable`, `RateLimited`, `Conflict` | Exponential back-off, up to `MAX_DISPATCH_RETRIES` (3) attempts. |
| **Permanent** | All other errors, or retries exhausted | Write to `policy_dead_letters`, advance cursor. The policy keeps running. |

#### `policy_dead_letters` table

```sql
CREATE TABLE IF NOT EXISTS policy_dead_letters (
    id               BIGSERIAL   PRIMARY KEY,
    policy_name      TEXT        NOT NULL,   -- stable policy name / cursor key
    global_position  BIGINT      NOT NULL,   -- position of the triggering event
    event_id         UUID        NOT NULL,   -- UUID of the triggering event
    error_kind       TEXT        NOT NULL,   -- e.g. "Permanent", "Conflict"
    error_message    TEXT        NOT NULL,   -- human-readable detail for triage
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dead_letters_policy
    ON policy_dead_letters (policy_name, created_at DESC);
```

**Triage queries:**

```sql
-- Recent failures for a specific policy
SELECT * FROM policy_dead_letters
WHERE  policy_name = 'deposit_fee'
ORDER  BY created_at DESC
LIMIT  20;

-- Look up the original event for manual replay
SELECT * FROM events WHERE id = '<event_id from dead letter>';
```

#### Retrying and discarding dead letters

Three operator controls on `PolicyRunner` resolve parked dead letters out of
band. None take an advisory lock or move the policy cursor:

| Method | Reaction | Outcome |
|--------|----------|---------|
| `retry_dead_letter(id)` | Re-runs the policy's reaction against **current** aggregate state through the same `Cqrs` path the live drain uses. | `Resolved` (succeeded, or now declined with a `BusinessRuleViolation`), `StillFailing` (re-parked in place with the fresh error), or `NotFound`. |
| `discard_dead_letter(id)` | None — pure bookkeeping: no `react`, no command, no new event. | `Discarded` or `NotFound`. |
| `retry_policy_dead_letters(name)` | Bulk: applies `retry_dead_letter` to every parked row for the policy, **oldest-first**. | `DeadLetterRetrySummary { resolved, still_failing }`. |

```rust,ignore
use replay_persistence::{DeadLetterRetry, DeadLetterDiscard};

// Give a parked failure another chance against today's state.
match runner.retry_dead_letter(id).await? {
    DeadLetterRetry::Resolved => { /* recovered: no longer Degraded */ }
    DeadLetterRetry::StillFailing => { /* updated in place, stays Degraded */ }
    DeadLetterRetry::NotFound => { /* nothing matched the id */ }
}

// Or give up on it permanently.
match runner.discard_dead_letter(id).await? {
    DeadLetterDiscard::Discarded => { /* archived */ }
    DeadLetterDiscard::NotFound => { /* nothing matched the id */ }
}

// Replay a whole backlog after a downstream outage, oldest-first.
let summary = runner.retry_policy_dead_letters("deposit_fee").await?;
println!("resolved {}, still failing {}", summary.resolved, summary.still_failing);
```

Neither method destroys data. When a dead letter leaves the active set —
resolved by a retry or discarded — the runner **moves** it into the
`discarded_dead_letters` archive in a single statement, so `policy_dead_letters`
keeps exactly the parked failures the status read model reports while the full
history is preserved for audit:

```sql
CREATE TABLE IF NOT EXISTS discarded_dead_letters (
    id               BIGSERIAL   PRIMARY KEY,
    dead_letter_id   BIGINT      NOT NULL,   -- id it had in policy_dead_letters
    policy_name      TEXT        NOT NULL,
    global_position  BIGINT      NOT NULL,
    event_id         UUID        NOT NULL,
    error_kind       TEXT        NOT NULL,
    error_message    TEXT        NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL,   -- when the dead letter was written
    reason           TEXT        NOT NULL,   -- 'retried' | 'discarded'
    discarded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_discarded_dead_letters_policy
    ON discarded_dead_letters (policy_name, discarded_at DESC);
```

#### `A::Error: Into<replay::Error>` migration note

`register_services::<A>` requires `A::Error: Into<replay::Error>`. If your
aggregate error type is a custom enum you must provide the conversion:

```rust,ignore
impl From<MyAggregateError> for replay::Error {
    fn from(e: MyAggregateError) -> Self {
        match e {
            MyAggregateError::InsufficientFunds => {
                replay::Error::business_rule_violation("Insufficient funds")
            }
            MyAggregateError::Persistence(inner) => inner,
        }
    }
}
```

The simplest path is `type Error = replay::Error` (used throughout the examples
here), which satisfies the bound with the identity conversion.

### Monitoring policy status

A running policy is otherwise opaque: its cursor and dead letters live in
internal tables. `PolicyStatusStore` turns them into a read-only health signal you
can poll from a dashboard or health check. It is **not** a projection — it reads
the operational tables (`policy_cursors`, the `events` head, and
`policy_dead_letters`) in a **single** query and never scans the event log. See
[ADR-0006](docs/adr/0006-policy-status-read-only-operational-snapshot.md) for the
rationale.

```rust,ignore
use replay_persistence::{PolicyStatusStore, PolicyCondition};

let statuses = PolicyStatusStore::new(pool.clone()).list().await?;

for s in &statuses {
    println!(
        "{:<20} {:<8} lag={} dead_letters={}",
        s.name, s.condition, s.lag, s.dead_letter_count
    );
}

// React to anything needing attention.
let degraded: Vec<_> = statuses
    .iter()
    .filter(|s| s.condition == PolicyCondition::Degraded)
    .collect();
```

Each `PolicyStatus` carries the raw numbers plus a derived condition:

| Field | Meaning |
|-------|---------|
| `name` | Stable policy name (the cursor key). |
| `position` | Last processed `global_position`. |
| `head` | Current global head (`MAX(global_position)`). |
| `lag` | Events still to process (`head - position`). |
| `last_checkpoint_at` | When the cursor last advanced (staleness signal). |
| `dead_letter_count` | Number of `policy_dead_letters` rows for this policy. |
| `last_dead_letter_at` | Timestamp of the most recent dead letter, if any. |
| `condition` | At-a-glance health label (see below). |

`condition` is derived with a strict precedence — **dead letters outrank lag** —
so a parked failure is never hidden behind a "still catching up" label:

| Condition | When | Meaning |
|-----------|------|---------|
| `Degraded` | `dead_letter_count > 0` | At least one event was skipped; needs operator attention. |
| `Working` | no dead letters, `lag > 0` | Healthy and catching up. |
| `CaughtUp` | no dead letters, `lag == 0` | Fully drained and up to date. |

`condition` has a stable `as_str()` / `Display` form (`"CaughtUp"`, `"Working"`,
`"Degraded"`) for JSON/UI consumers.

Only policies that have actually run appear: a registered-but-never-started policy
has no `policy_cursors` row and is therefore absent from `list()`. The store only
*observes* — retrying or discarding a dead letter is a separate, deliberate action
(see the triage queries above).
