# Replay

Event Sourcing and CQRS library

In event sourcing events are the source of truth, they are organized in streams that have an id and a version.
Everytime an event is applied into a stream its state changes and its version is incremented.

You can replay all events of a stream to reproduce previous states (hence the name of this library).

Then from DDD we have aggregates that are implemented like a stream that accepts commands.
You can chose you implement just `Stream` (state will be built from events) or `Aggregate` (stream that accepts commands).

> Important to note `Streams` never fails as the are built from events that happened in the past (so there are no side effects, error handling, etc.). All of these concerns are managed in the aggregate.

## Requirements

`es-replay-persistence` requires **PostgreSQL 15 or later**.

That floor is a feature floor, not just the oldest release the suite is willing to
claim. The features that hold it up:

- **13** — every event is stamped with the transaction that wrote it, in the `xid8` type
  PostgreSQL 13 added
  ([0018](persistence/tests/migrations/0018_event_commit_txid.sql));
- **15** — `NULLS NOT DISTINCT`, which keeps a parked dead letter unique per command
  per reaction over an identity that is nullable for a reaction with no dispatch to
  name ([0030](persistence/tests/migrations/0030_dead_letter_unique_command.sql));
  before 15 a unique index treats those rows as all different
  (funkode-io/replay#220).

The promise moved ahead of that change rather than with it, so a deployment learned
which server it needed before the migration that needs it. 13 and 14 are both out of
upstream support either way.

The integration suite runs against 15 itself — the floor is what is promised, so the
floor is what is verified — and the pinned image tag lives in
`persistence/src/infrastructure/postgres_tag.rs`, included by the two test modules that
pin a version: `tests/common/postgres_image.rs` and `cursor_tests` in
`src/policy_runner.rs`. The one test that deliberately takes whatever server it is given
says so where it starts the container.

The core `es-replay` crate has no database requirement at all, and is the half that runs
on WASM.

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
relationship directly in the URN using `ScopedUrn::at`, recover it with
`ScopedUrn::extract_scope`, and drop it again with `ScopedUrn::unscoped` or `ScopedUrn::to_slug`.

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
| Current URN's NSS already contains `@` | "URN is already scoped (contains '@')" |

The scope argument may already be scoped — see [Nested scopes](#nested-scopes).

**Validation rules enforced by `extract_scope`:**

| Input NSS | Error |
| --- | --- |
| `acct-1` — no `@` | "URN is not scoped (no '@' in NSS)" |
| `@branch:london` — empty own NSS | "URN has empty NSS before '@'" |
| `acct-1@branch` — no `:` after `@` | "Scope part after '@' is missing ':' (expected '`<nid>:<nss>`')" |
| `acct@:nss` — empty scope NID | "Scope NID is empty" |
| `acct@nid:` — empty scope NSS | "Scope NSS is empty" |
| wrong output type | "NID mismatch" (from `TryFrom<Urn>` on the output type) |

The output type can be any type that implements `TryFrom<Urn>` — it does not have to be the
same as the original URN's type. This allows a `BankAccountUrn` to extract a `BranchUrn`,
a `TenantUrn`, or any other domain type, as long as the NID embedded in the scope part matches.

#### Nested scopes

A scope may itself be scoped:

```rust
let region: RegionUrn = RegionUrn::new("uk")?;
let branch: BranchUrn = BranchUrn::new("london")?.at(&region)?;   // urn:branch:london@region:uk
let account: BankAccountUrn = BankAccountUrn::new("acct-1")?.at(&branch)?;
// urn:bank-account:acct-1@branch:london@region:uk

// extract_scope peels one level — the result is still scoped
let branch: BranchUrn = account.extract_scope::<BranchUrn>()?;    // urn:branch:london@region:uk
let region: RegionUrn = branch.extract_scope::<RegionUrn>()?;     // urn:region:uk

// unscoped drops the whole scope
let base: BankAccountUrn = account.unscoped()?;                   // urn:bank-account:acct-1

// to_slug drops it too, but hands back the NSS borrowed instead of a rebuilt URN
let slug: &str = account.to_slug();                               // "acct-1"
```

Re-scoping an already-scoped URN is refused — call `unscoped()` first. That guard is what
makes the left-most `@` the outermost scope
([ADR-0010](docs/adr/0010-nested-scoped-urns-parse-at-the-first-at-sign.md)).

#### `unscoped` or `to_slug`

Both drop the scope at the same boundary — the first `@` — and differ in what they hand back:

| | returns | scoped input | unscoped input | allocates |
| --- | --- | --- | --- | --- |
| `unscoped()` | `Result<Self>` — the base as a typed URN | the base | `Err` (nothing to drop) | yes: clones, rebuilds, revalidates |
| `to_slug()` | `&str` — the base's NSS | the base's NSS | its own NSS | **no** — a slice of the NSS already there |

Take `unscoped` when you need the base as a URN to pass on or re-scope. Take `to_slug` when you
need the identity as text — a database key, a display label, a lookup in a map — which is the
common case and the one where rebuilding a URN is pure overhead. `to_slug` is infallible on
purpose: a caller that may hold either a scoped or a bare URN gets one answer from both, with no
`unwrap_or` at the call site.

```rust
let bare: AttributeUrn = AttributeUrn::new("color")?;
let scoped: AttributeUrn = bare.clone().at(&catalog)?;

assert_eq!(bare.to_slug(), "color");     // no scope to drop
assert_eq!(scoped.to_slug(), "color");   // scope dropped
assert!(bare.unscoped().is_err());       // whereas unscoped insists on one
```

### Prelude

There are two prelude options depending on your dependencies.

**Core prelude** (`replay`) — traits only, no persistence or macros:

```rust
use replay::prelude::*;
```

| Export | Purpose |
| --- | --- |
| `ScopedUrn` | `at`, `extract_scope`, `unscoped` and `to_slug` on URN types |
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
| `ObservedEvent` | What a `Policy` reads: payload, stream, metadata, timestamp |
| `PersistedEvent` | An `ObservedEvent` plus the identity and position the store gave it |
| `PolicySettings` | How the runner drives one `Policy`, given at registration |
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

### Batching: a single append may arrive as several calls

A streamed append pulls its producer one event at a time, so a bulk import never has to fit
in memory. Registered projections would break that, since the store has to keep each appended
event to hand it over. It therefore flushes in bounded chunks: at most `flush_size` events are
held, applied to every projection **on the same transaction**, then dropped.

So an append of `B` events reaches `handle` as `⌈B / flush_size⌉` calls, in order, all
committing or rolling back together — a failure in the last chunk rolls back the writes of the
first. **Do not assume one call carries a whole append**: keep anything that must span an
append in the projection's own fields or its view, not in a local of one `handle` call. Which
events arrive, and their order, are unchanged.

| Setting | Store override | Env var | Default |
|---------|----------------|---------|---------|
| Events held before a projection flush | `builder(pool).projection_flush_size(n)` | `REPLAY_PROJECTION_FLUSH_SIZE` | `500` |

Stores with no projections registered never buffer at all. The same size bounds the history
replayed on first registration or a version-drift rebuild: `build()` pages through it with a
keyset cursor, inside a single `REPEATABLE READ` transaction, so startup memory scales with
the chunk while every page still reads one snapshot of the log. See
[ADR-0011](docs/adr/0011-inline-projections-flushed-in-bounded-chunks.md).

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

`compacted_events` receives the **current live event stream** from the store and returns a
`Compaction`: either `Rewrite(events)` with the shortest subsequence that, when replayed from
scratch, reproduces the same state — or `AlreadyCompacted` to signal the live stream is already
minimal so the store writes nothing. The aggregate does **not** need to store events in its own
fields:

```rust
use futures::TryStream;       // trait bound used in the signature
use futures::TryStreamExt;    // .try_fold() extension method
use replay::{Compactable, Compaction};

impl Compactable for BankAccountAggregate {
    async fn compacted_events(
        &self,
        events: impl TryStream<Ok = Self::Event, Error = replay::Error> + Send,
    ) -> replay::Result<Compaction<Self::Event>> {
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
            .map(Into::into) // Vec -> Compaction::Rewrite
    }
}
```

`compacted_events` must be a **fixpoint**: applied to a stream it already produced it must
reproduce it (or return `AlreadyCompacted`). Returning `AlreadyCompacted` on an already-minimal
stream lets `compact` skip the archive-and-rewrite entirely — it writes nothing but still advances
the compaction watermark, so a stream that was born minimal never pays even a one-time rewrite.
Note `Rewrite(vec![])` is *not* the same as `AlreadyCompacted`: the empty rewrite archives
everything and leaves the stream empty (compact to nothing), the opposite write from skipping.

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
// Returns a `CompactionOutcome`: `Compacted { archive_version }` (1 on the first
// compaction, 2 on the second, …) or `Skipped` when the aggregate reported
// `AlreadyCompacted`.
let outcome = cqrs.compact(&aggregate, meta).await?;

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

### What compaction does to an event's numbers

Compaction restarts a stream's `version` at 1, so hydrating a compacted aggregate always
replays `1..N`. That makes `version` a *replay* number: after a compaction it names a
place in the current live stream, not a place in the stream's history, and the same
`(stream_id, version)` pair recurs over a long-lived stream.

Each event therefore also carries `stream_seq`, its place in its own stream, which
compaction continues rather than restarts — the snapshot rows take the numbers after the
ones they archived. One function writes every event, so the two numbers are decided
together. Nothing reads it yet
([ADR-0023](docs/adr/0023-a-stream-is-numbered-twice.md),
[funkode-io/replay#195](https://github.com/funkode-io/replay/issues/195)).

Migration [0027](persistence/tests/migrations/0027_event_stream_seq.sql) backfills the
column, and it is not an online migration. It runs as one transaction that takes ACCESS
EXCLUSIVE on `streams` in its first statement and on `events` in its second, and holds
both until the last one commits, so for its whole duration — the row-by-row backfill, the
`NOT NULL` scan and the unique index build — every reader and every writer of either table
waits, not just appends. Budget WAL and dead-tuple space of about one table copy, and run
it in a maintenance window: on a large log a Policy poll blocks along with everything
else, so a fleet will look stalled rather than slow.

**Order the rollout: quiesce writers, migrate, then deploy.** Two things fail if they
overlap the migration, both loudly and neither corrupting anything:

- **An append already inside `append_event` when the migration commits.** It keeps the
  function body it entered with, which writes no place, and its insert is refused. A
  command *issued* during the migration is fine — it waits and then calls the new
  function — so this is specifically the write that was already in flight.
- **Compaction, from either side of a mixed-version fleet.** An old process writes its
  snapshot rows with an `INSERT` naming no place, which this migration makes impossible;
  a new process calls a function an un-migrated database does not have. It is best-effort
  maintenance, so the next run after the rollout succeeds.

Appends from an old process are otherwise safe once the migration has landed:
`append_event` keeps its signature and is numbered by the new function underneath it, so
the fleet can be rolled at leisure.

A place, once given, is permanent. Nothing in the database enforces that: the event log
is written by this library and by nothing else — `write_event` and the migrations — and
its invariants are maintained by that one writer, as `global_position`'s uniqueness and a
stream's `version` contiguity always have been. Editing `events` with hand-written SQL
breaks them. The row that *is* meant to be edited by hand is `policy_cursors`
([ADR-0012](docs/adr/0012-policy-cursor-is-an-operator-writable-control-surface.md)).

### Skipping unchanged streams (`needs_compaction`)

A maintenance job that compacts many streams on a schedule should not re-archive a
stream that has not changed since its last compaction — doing so rewrites an identical
snapshot and, because archived originals are retained for policies, grows the store for
no reason. `needs_compaction` is a cheap pre-check that lets the job skip `compact`
entirely for such streams:

```rust,ignore
// Blanket daily job over every stream in a catalog.
for stream_id in stream_ids {
    if cqrs.needs_compaction::<BankAccountAggregate>(&stream_id).await? {
        let aggregate = cqrs.fetch_aggregate::<BankAccountAggregate>(&stream_id).await?;
        cqrs.compact(&aggregate, meta.clone()).await?;
    }
    // else: nothing appended since the last compaction — no transaction, no fold.
}
```

Each compaction records a per-stream watermark (the post-compaction stream version);
`needs_compaction` returns `true` only when at least one event has been appended past
it — including a stream that has never been compacted but has events. The check takes
**no lock**: the race against a concurrent append is benign, because compaction is
best-effort maintenance, so an append that lands just after a `false` read is simply
picked up on the next run. Both the Postgres and in-memory stores implement it.

## Policies

A `Policy` is a checkpointed background subscriber that **reacts to events by
issuing commands**. It is the event-sourcing equivalent of a process manager or
saga: given an event it returns zero or more [`Dispatch`]es — commands the runner
executes against aggregates. No read model is derived; side effects happen through
the aggregate write path so causation, idempotency, and optimistic locking are all
inherited for free.

The `Policy` trait lives in **`es-replay`**, the core crate: a rule is domain
vocabulary, so declaring one needs neither `Cqrs` nor a store
([ADR-0027](docs/adr/0027-the-policy-rule-is-core-vocabulary.md)). It is re-exported
from `es-replay-persistence` along with [`Dispatch`] and `ObservedEvent`, so the
runner's own imports stay in one place. The implementor stays pure — `react` takes an
[`ObservedEvent`] and returns commands with no I/O — while the [`PolicyRunner`] handles
reading each stream, stamping causation metadata, persisting cursors, and executing the
[`Dispatch`]es.

### Implementing `Policy`

The trait is the rule and nothing else: `name` and `react`. How the runner drives it —
where it starts, what feed it reads, how it batches, how long a dispatch may run — is a
[`PolicySettings`] value given at registration.

```rust,ignore
use replay::{Dispatch, ObservedEvent, Policy};

struct FeePolicy {
    ledger_id: FeeLedgerUrn,
}

impl Policy for FeePolicy {
    type Event = BankAccountEvent;

    fn name(&self) -> &str {
        "deposit_fee"
    }

    fn react(&self, event: &ObservedEvent<BankAccountEvent>) -> Vec<Dispatch> {
        match &event.data {
            // `reference` is the deposit's own identifier. The ledger absorbs a
            // repeated `charge_key` as a no-op, which is what makes a redelivery
            // harmless — see below.
            BankAccountEvent::Deposited { amount, reference } => vec![
                Dispatch::to::<FeeLedger>(
                    self.ledger_id.clone(),
                    FeeLedgerCommand::ChargeFee {
                        amount: amount * FEE_RATE,
                        charge_key: format!("{}#{reference}", event.stream_id),
                    },
                )
            ],
            _ => vec![],
        }
    }
}
```

An `ObservedEvent` carries the four fields a rule may read — `data`, `stream_id`,
`metadata` and `created`. The store's identity and position (`id`, `type`, `version`,
`aggregate_version`) stay on `PersistedEvent`, which embeds the observed half and derefs
to it, so a read path's `event.data` is unchanged.

A rule is therefore not given the triggering event's id, and cannot mint a causation key
into the command it emits: duplicate deliveries are absorbed by **idempotent command
shape**, keyed on an identifier the triggering event carries — a payment reference, an
order number, whatever names the operation upstream. Nothing in the envelope stands in
for it: `created` comes from Postgres's transaction-stable `now()`, so every event of one
append shares it.

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

- **Diagnosis** — `causation.event_id` names the triggering event for an operator reading the resulting events, so a duplicate delivery can be recognised after the fact. It makes nothing safe: `Aggregate::handle` receives no metadata, so absorbing the duplicate is the command's job.
- **Loop prevention** — the `depth` counter is incremented at each hop; the runner skips reactions once it reaches the configured limit (see [Loop prevention](#loop-prevention)).
- **Observability** — every policy-driven event is traceable back to the original triggering event by `causation.event_id`.

You can attach additional metadata to a specific dispatch with [`Dispatch::with_metadata`]; the runner merges it with the causation block (colliding top-level keys are rejected):

```rust,ignore
Dispatch::to::<FeeLedger>(ledger_id.clone(), ChargeFee { amount, charge_key })
    .with_metadata(Metadata::from([("correlation_id", request_id)]))
```

### Closure shortcut

For simple, single-aggregate reactions you can skip the struct and `impl Policy`
entirely with `register_policy_fn`. The closure runs through the exact same runner
machinery — causation stamping, failure handling, batching, advisory lock — as a
full `Policy` impl, and takes the same [`PolicySettings`].

```rust,ignore
let runner = PolicyRunner::builder(cqrs)
    .register_services::<FeeLedger>(fee_services)
    .register_policy_fn::<BankAccountEvent, _>(
        "deposit_fee",
        PolicySettings::new()
            .starting_at(StartAt::Beginning)
            .with_stream_filter(StreamFilter::for_stream_type::<BankAccount>()),
        |event| match &event.data {
            BankAccountEvent::Deposited { amount, reference } => vec![
                Dispatch::to::<FeeLedger>(
                    ledger_id.clone(),
                    ChargeFee {
                        amount: amount * 0.01,
                        charge_key: format!("{}#{reference}", event.stream_id),
                    },
                )
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
use replay_persistence::{PolicyRunner, PolicySettings, StartAt, StreamFilter};

let runner = PolicyRunner::builder(cqrs)
    .register_services::<BankAccount>(())          // enable Dispatch::to::<BankAccount>
    .register_services::<FeeLedger>(fee_services)
    .register_policy(
        FeePolicy { ledger_id },
        PolicySettings::new()
            .starting_at(StartAt::Beginning)       // process all history on first run
            .with_stream_filter(StreamFilter::for_stream_type::<BankAccount>()),
    )
    .build();

let daemon = runner.start_polling(Duration::from_secs(30));

// … application runs …

daemon.shutdown().await; // stops all tasks cleanly
```

### How a policy tracks where it is

A policy's position is **one row per stream** in `policy_stream_cursors`, holding the
place it has reached in that stream. Nothing orders one stream against another: a policy
reads each stream in that stream's own order, and a write that is slow, stuck or rolled
back delays the stream it is writing to and no other
([ADR-0026](docs/adr/0026-a-policy-tracks-its-position-per-stream.md)).

Which streams to look at is found two ways. Every poll sweeps the log past
`policy_cursors.discovered_through` for streams with new events — indexed, and bounded by
`read_batch_size`. That sweep is fast because it never waits: it passes positions it
cannot see, including one held by a write still in flight. A reconciliation then compares
every stream's head with the policy's place, catching exactly what the sweep passed. It
costs a scan of one row per stream, so it runs on a cadence rather than per poll:

| Setting | Default | What it bounds |
|---------|---------|----------------|
| `REPLAY_POLICY_RECONCILE_SECS` | 5 | How often the reconciliation runs, and so the unit the delivery bound below is counted in. Never *whether*. |

Lower it if that tail latency matters more than the scan; raise it if you have millions of
streams and no long-running writes.

The reconciliation reads up to `read_batch_size` streams per cadence, **resumes where it
left off** and wraps. A policy behind on no more streams than that is inside one cadence;
beyond it, the pass takes `ceil(streams behind / read_batch_size)` cadences while the
policy is keeping up, and longer when it is not — the reconciliation leads the poll it
runs on, so it always reads at least one stream, and its rotation never steps over a
stream it did not read. [ADR-0026](docs/adr/0026-a-policy-tracks-its-position-per-stream.md)
owns the bound.

`read_batch_size` is one budget for a whole poll, spent across the streams that poll looks
at — not a batch per stream. A policy owed work in a hundred streams reads the same number
of events per poll as one owed work in a single stream; the streams a poll does not reach
go to the front of the next poll's queue, and a stream it could not finish goes to the
back, so a stream written to faster than it can be read never holds up the rest.

Each policy has a **stable name** that keys both tables. On first registration it is
bootstrapped according to the registration's `starting_at`:

| `StartAt` | Behaviour |
|-----------|-----------|
| `StartAt::Now` (default) | Every existing stream is recorded at its current head, so only newly appended events are processed. Safe when you don't want to fire commands retroactively across existing history. This writes one row per existing stream, once. |
| `StartAt::Beginning` | No places are recorded, and a stream with no place starts at its first event; the full history is drained once, then the policy follows live appends. Use this for backfill or projections derived from audit events. |

Places are written to Postgres **at least every `checkpoint_batch_size` events** and
unconditionally at the end of every drain pass. A crash after a command is executed but
before the place is saved will re-deliver the triggering event. Correctness therefore
depends on **idempotent command handling** in the target aggregate, keyed on an
identifier the triggering event carries (see [Policies](#policies)).

### Moving a policy on a running system

`policy_stream_cursors` is an **operator-writable control surface**, not private runner
state: you can reposition a policy against a live deployment with plain SQL, without
restarting a process or dropping leadership.

```sql
-- re-deliver a stream from the place after this one
UPDATE policy_stream_cursors SET stream_seq = 41, updated_at = now()
WHERE policy = 'price_fanout' AND stream_id = 'urn:instrument:xyz';

-- re-deliver a stream from its first event
DELETE FROM policy_stream_cursors
WHERE policy = 'price_fanout' AND stream_id = 'urn:instrument:xyz';
```

One stream at a time, which is the point: a redelivery no longer rewinds the policy over
every other stream to reach the one that needs it.

**When the move takes effect** depends on how the stream comes to the leader's attention,
because a place is only read for a stream that poll is looking at:

| The stream you moved | When it is picked up |
|---|---|
| is still being written to | the next poll, on the sweep |
| is quiet and the sweep has passed it | the next reconciliation that reaches it — one `REPLAY_POLICY_RECONCILE_SECS` for a policy behind on no more streams than its read batch, and `ceil(streams behind / read_batch_size)` cadences beyond that ([ADR-0026](docs/adr/0026-a-policy-tracks-its-position-per-stream.md)) |

Places themselves are never held in memory between polls, so no running process carries a
stale copy of one forward.

Place writes are a compare-and-set against the row your poll read — compared by row
version, so a checkpoint can never reinstate a place that predates your update, even when
you rewind to exactly the place the running poll started from. Deleting a row cannot be
undone by a poll recreating it either. A runner that loses the race abandons **that
stream** for the poll — the others in its batch carry on — and picks your place up on the
next one. Moving forward skips the events in between (they are never delivered); moving
backward re-delivers them, which is safe under the same idempotency contract that
covers crash re-delivery. See
[ADR-0012](docs/adr/0012-policy-cursor-is-an-operator-writable-control-surface.md).

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
let runner = PolicyRunner::builder(cqrs)
    .register_policy(my_policy, PolicySettings::new())
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
| Per-policy setting | `PolicySettings::new().with_max_causation_depth(5)` |
| Environment variable | `REPLAY_MAX_CAUSATION_DEPTH=5` |
| Built-in default | `10` |

### Batching

Two batch sizes control throughput vs checkpoint frequency:

| Setting | `PolicySettings` | Env var | Default |
|---------|------------------|---------|---------|
| Events read per drain | `with_read_batch_size(u32)` | `REPLAY_READ_BATCH_SIZE` | `100` |
| Events between cursor saves | `with_checkpoint_batch_size(u32)` | `REPLAY_CHECKPOINT_BATCH_SIZE` | `100` |

The runner enforces `read_batch_size ≥ checkpoint_batch_size`.

`read_batch_size` counts positions in the log, not matches: a `stream_filter` decides
what a policy reacts to, the cursor still walks past everything else
([ADR-0013](docs/adr/0013-policy-feed-contiguity-on-unfiltered-positions.md)). A
selective policy over a busy log may need several drains to reach its next event;
raise `read_batch_size` if that latency matters.

### Dispatch timeout

Every dispatch is awaited for a bounded time, so a command that never returns
cannot hold a worker for the life of the process:

| Setting | `PolicySettings` | Env var | Default |
|---------|------------------|---------|---------|
| Time one dispatch may run | `with_dispatch_timeout(Duration)` | `REPLAY_DISPATCH_TIMEOUT_MS` | `30s` |

Exceeding it is a **retryable** failure: the dispatch is abandoned, retried under
the same back-off as an `Unavailable` error, and parked with
`error_kind = 'Timeout'` once the retries are exhausted
([ADR-0018](docs/adr/0018-a-hung-dispatch-is-cut-loose-by-a-timeout.md)). It
bounds the future the runner awaits, and cannot interrupt work the reaction moved
onto another task or a command that never yields — see `CONTEXT.md`'s
non-guarantees.

It also does not cancel work already running in Postgres. A dispatch abandoned
inside an append that is blocked on another transaction's stream lock holds its
pool connection until the server ends that statement — which is what the
[stream-lock wait](#stream-lock-wait) below is for. A reaction that hangs in its
own code holds no connection: the command handler runs before the append opens a
transaction.

### Stream lock wait

An append takes the stream's row with `SELECT ... FOR UPDATE` and holds it for the
rest of its transaction; `compact` takes the same row and holds it across the
whole fold. Whoever is waiting is bounded on the server, so an append nobody is
still waiting for ends and hands its connection back:

| Setting | Store override | Env var | Default |
|---------|----------------|---------|---------|
| Time a transaction waits for a stream row | `builder(pool).stream_lock_wait(d)` | `REPLAY_STREAM_LOCK_WAIT_MS` | `30s` |

```rust,ignore
let store = PostgresEventStore::builder(pool)
    .stream_lock_wait(Duration::from_secs(30))  // Duration::ZERO waits forever
    .build()
    .await?;
```

Exceeding it is a **retryable** `Unavailable` error naming the stream and the
limit, so the policy runner retries it under the same back-off as any transient
failure and parks a dead letter once the retries are spent. It is set with
`SET LOCAL`, so a connection carries no `lock_timeout` back to the pool
([ADR-0022](docs/adr/0022-a-stream-lock-wait-is-bounded-on-the-server.md)).

It bounds the whole append transaction, inline projections included. A projection
handler that maps its errors with `db_error` reports a contended write as the same
retryable `Unavailable`; one that maps sqlx errors its own way decides that for
itself.

**`Duration::ZERO` — or `REPLAY_STREAM_LOCK_WAIT_MS=0` — disables the bound** and
waits forever, which is the behaviour before this existed. It is written out as
`lock_timeout = '0'` rather than left unset, so it overrides a `lock_timeout` your
own pool or role may carry: asking for no limit gets no limit. That is the opposite
of `REPLAY_DISPATCH_TIMEOUT_MS=0`, which reads as *unset*: this value is passed to
Postgres, where zero already means "no limit". Negative and unparseable values
fall back to the default rather than silently removing the bound, and a wait
longer than `lock_timeout` can express (about 24.8 days) is clamped to that
ceiling rather than failing every append.

Diagnosis: **a spike of these means a long holder, not a broken append.** Look for
what is holding the stream the error names — `pg_locks` joined to
`pg_stat_activity` — before looking at the service that failed. The floor on any
value you choose is the longest honest hold, and in this library that is `compact`:
it is O(stream length), so a stream long enough to take more than the wait will
fail the appends queued behind it.

### Failure handling

When a dispatch fails the runner classifies the error and responds accordingly:

| Error category | Condition | Action |
|----------------|-----------|--------|
| **Business-rule violation** | `ErrorKind::BusinessRuleViolation` | Advance cursor immediately — the event is correct, the domain logic rejected the command. No retry, no dead-letter. |
| **Retryable** | `Unavailable`, `RateLimited`, `Conflict`, or a dispatch that exceeded its timeout | Exponential back-off, `MAX_DISPATCH_RETRIES` (3) retries after the first attempt — four in all. |
| **Permanent** | All other errors, or retries exhausted | Write to `policy_dead_letters`, advance cursor. The policy keeps running. |
| **Timeout** | The dispatch was still running when its `dispatch_timeout` expired | Abandon it, log at `warn` with the elapsed time, retry; on exhaustion write `error_kind = 'Timeout'` and advance cursor. |
| **Panic** | The reaction (or a command it dispatched) panicked | Write to `policy_dead_letters` with `error_kind = 'Panic'` and the panic's message, log at `error`, advance cursor. Never retried — a reaction that panicked panics again ([ADR-0016](docs/adr/0016-panicking-reaction-parked-as-a-permanent-failure.md)). |

The panic boundary is the delivery of **one event**, so the worker survives and
the Policy reacts to every later event. Two panics are outside it: one inside a
task the reaction **spawns itself** (it unwinds in its own task, and nothing
parks a dead letter for it), and any panic in a binary built with
`panic = "abort"`, where the process ends before a catch can run.

#### Restarting a worker that dies

That table covers what the delivery of one event can contain. A worker can also
die outright — a panic in the drain loop, in place I/O, in the stream read. The
runner restarts it on a budget; the restart resumes from the last durable
checkpoint, so it costs at most a checkpoint's worth of re-delivery
([ADR-0017](docs/adr/0017-dead-policy-worker-restarted-on-a-budget.md)).

```rust,ignore
use std::time::Duration;
use replay_persistence::{PolicyRunner, WorkerSupervision};

let runner = PolicyRunner::builder(cqrs)
    .register_policy(my_policy, PolicySettings::new())
    .with_worker_supervision(
        WorkerSupervision::default()   // 5 restarts a minute, 100 ms → 30 s
            .max_restarts(10)          // 0 disables restarting entirely
            .restart_window(Duration::from_secs(300))
            .initial_backoff(Duration::from_millis(250))
            .max_backoff(Duration::from_secs(60)),
    )
    .build();
```

Each restart waits `initial_backoff` doubled per restart already spent in the
window, capped at `max_backoff`, and logs at `warn` with the policy, the cause
and the window's restart count. A restart re-reads that policy's cursor only;
leadership is untouched, because the advisory locks live on the process's shared
lock-manager session rather than on the worker task.

A worker that spends its budget is **stopped**, logged at `error`, and
**escalated**: the runner calls the hook the consumer supplied on the builder,
naming the Policy and why it is down. The default hook **exits the process**
([ADR-0019](docs/adr/0019-escalation-is-a-consumer-hook-that-exits-by-default.md)).

```rust,ignore
use replay_persistence::{EscalationReason, PolicyRunner};

let runner = PolicyRunner::builder(cqrs)
    .register_policy(my_policy, PolicySettings::new())
    .on_escalation(|escalation| {
        // `escalation.policy` is down; `escalation.reason` says whether it spent
        // its restart budget or lost the lock manager that elects it.
        metrics.policy_down(&escalation.policy);
        liveness_probe.fail();  // something must end this process — see below
    })
    .build();
```

Why the default exits: leadership is held per Policy by *this process's*
lock-manager session, not by the worker task, so a stopped worker's replica keeps
the advisory lock and no standby takes over. Ending the process drops the session,
which releases the lock, which is what lets a standby take the Policy over. A
service that configures nothing therefore recovers by being restarted instead of
lingering half-dead. The exit code is `ESCALATION_EXIT_CODE` (70, `EX_SOFTWARE`),
so a crash-looping pod's exit code tells a supervision escalation apart from an
ordinary error exit.

What you take on by overriding it: **a hook that returns leaves the Policy stopped
fleet-wide.** The advisory lock stays held, no replica reacts for that Policy, and
nothing changes until the process ends — so your hook must arrange that ending
(fail a liveness probe, drain and exit, page someone). The library keeps its own
half of the bargain either way: the worker is recorded as stopped before the hook
runs, so it is never silently absent, and a hook that panics is caught and logged
rather than taking the report with it.

Every escalated worker is also named by `daemon.stopped_workers()`, and reads as
`Liveness::Stopped` — a poll for a consumer whose hook returns:

```rust,ignore
for stopped in daemon.stopped_workers() {
    // Nothing is reacting for `stopped.policy`, and nothing in this process
    // will start it again.
    tracing::error!(policy = %stopped.policy, restarts = stopped.restarts, "policy is down");
}
```

The lock manager and the NOTIFY listener are supervised on the same budget. They
own no policy, so they are reported through their consequences: a lock manager
that gives up takes every policy's leadership with it and each of those workers
escalates as `EscalationReason::Abandoned`, so escalation names policies rather
than plumbing. A listener that gives up costs latency only — workers fall back to
the poll interval. A clean `daemon.shutdown()` escalates nothing.

Two deaths the runner does **not** contain: a panic inside a task the reaction
spawns itself (it unwinds in its own task, outside both boundaries) and an OOM
kill.

#### Reading a process that died without saying so

An OOM kill leaves no log line of its own, so every election logs at `info` where
the worker picks up its search:

```text
INFO policy worker is leading; resuming its search after its last sweep
     policy=price_fanout swept_through=264785
```

Once per election, not per event. `swept_through` is where discovery resumes, not
what has been processed — in a crash loop it reappears unchanged on every restart,
which says the worker is dying before it finishes a poll rather than which event is
killing it. For that, read the places, which do record progress:

```sql
-- what the policy is behind on, worst first
SELECT s.id, s.stream_seq - COALESCE(c.stream_seq, 0) AS owed
FROM streams s
LEFT JOIN policy_stream_cursors c ON c.policy = 'price_fanout' AND c.stream_id = s.id
WHERE s.stream_seq > COALESCE(c.stream_seq, 0)
ORDER BY owed DESC LIMIT 10;

-- the next event the worst-off stream owes, which is the one to look at
SELECT * FROM events WHERE stream_id = 'urn:instrument:xyz' AND stream_seq = 42;
```

A place that does not move across restarts names the stream; the event after it is
the one being died on.

A position that advances between restarts means the opposite: the process is
making progress and still dying, i.e. leaking rather than choking on one event.

#### `policy_dead_letters` table

```sql
CREATE TABLE IF NOT EXISTS policy_dead_letters (
    id               BIGSERIAL   PRIMARY KEY,
    policy_name      TEXT        NOT NULL,   -- stable policy name / cursor key
    global_position  BIGINT      NOT NULL,   -- position of the triggering event
    event_id         UUID        NOT NULL,   -- UUID of the triggering event
    error_kind       TEXT        NOT NULL,   -- ErrorKind text, or "Panic" / "Timeout"
    error_message    TEXT        NOT NULL,   -- human-readable detail for triage
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
    aggregate_name   TEXT,                   -- Rust type name of the target aggregate
    target_stream_id TEXT,                   -- URN of the instance the command was sent to
    command_name     TEXT,                   -- Rust type name of the command
    dispatch_ordinal INTEGER,                -- the dispatch's place in the reaction
    deliveries       INTEGER     NOT NULL DEFAULT 1,  -- deliveries that parked this command
    last_parked_at   TIMESTAMPTZ NOT NULL DEFAULT now(), -- when the last of them did
    retry_count      INTEGER     NOT NULL DEFAULT 0,  -- settlements a retry has made on this row
    last_retried_at  TIMESTAMPTZ             -- when the last of them was made
);

CREATE INDEX CONCURRENTLY idx_dead_letters_policy_created_parked
    ON policy_dead_letters (policy_name, created_at DESC) INCLUDE (last_parked_at);

CREATE INDEX CONCURRENTLY idx_dead_letters_policy_reaction
    ON policy_dead_letters (policy_name, global_position, event_id, id);

CREATE UNIQUE INDEX CONCURRENTLY idx_dead_letters_parked_command
    ON policy_dead_letters (policy_name, event_id, aggregate_name, target_stream_id,
                            command_name, dispatch_ordinal)
    NULLS NOT DISTINCT;
```

The three identity columns are captured on the `Dispatch` itself, so a policy
needs no change to get them. They are nullable for the two cases with no
dispatch to name: a row parked before the identity migration
([0024](persistence/tests/migrations/0024_dead_letter_identity.sql)), and a panic
in `react` itself, which fails before it has built a dispatch. The command's
*variant* and payload are not recorded — `Aggregate::Command` carries no `Debug`
or `Serialize` bound.

`retry_count` and `last_retried_at`
([0025](persistence/tests/migrations/0025_dead_letter_retry_bookkeeping.sql)) are
written by every settlement a retry makes on a row that already existed —
archiving a row that resolved as much as re-parking one that did not — and never
by a discard. A row parked for the first time is untried whichever path parked
it: the columns count retries made on a row, not executions of a command.

`idx_dead_letters_policy_reaction`
([0026](persistence/tests/migrations/0026_dead_letter_reaction_index.sql)) is the
access path a retry uses: `(policy_name, created_at DESC)` answers "what failed
recently", not "which rows belong to this reaction". Built `CONCURRENTLY`, like
every index this schema adds to a populated table, so parking keeps working while
it builds.

`idx_dead_letters_parked_command`
([0030](persistence/tests/migrations/0030_dead_letter_unique_command.sql)) is what
makes a parked command **one row**
([ADR-0024](docs/adr/0024-a-parked-command-is-one-row.md)). The park is written before the batched cursor
checkpoint, so a crash in between — or an operator rewinding the cursor — delivers
the event again; the park is an `ON CONFLICT DO UPDATE` against this key, which
refreshes the error, counts the delivery in `deliveries` and stamps
`last_parked_at`, leaving `created_at` and the retry bookkeeping alone. A
redelivery is not a retry. `dispatch_ordinal` is in the key because a reaction may
emit the same command type to the same instance twice: those are two parked
commands and keep two rows. `NULLS NOT DISTINCT` (the reason the floor is
PostgreSQL 15) extends the key to rows with no dispatch to name, which collapse
per `(policy_name, event_id)`. A row parked before the ordinal existed names its
command but not its place — and one parked before the identity columns existed
names nothing at all — so the migration cannot tell a redelivery's duplicate from
two different commands: those rows are numbered apart with a **negative** ordinal
rather than collapsed. Only a panicking reaction's row is collapsed by
[0029](persistence/tests/migrations/0029_dead_letter_dedupe.sql), because a panic
settles the delivery by unwinding and so parks exactly one row per delivery: the
newest parking is kept and the rest archived with reason `superseded`.

Apply these migrations with the release that parks through `ON CONFLICT`, before
it runs: a replica still on the previous version parks with a plain INSERT and
takes a `23505` if it re-parks a command it has already parked. That fails the
poll, not the record — the row it could not write is the one already there.

`PolicyStatus::last_dead_letter_at` reads `MAX(last_parked_at)`, not
`MAX(created_at)`: a reaction failing on every delivery must not read like one
that failed once and stopped. `idx_dead_letters_policy_created_parked`
([0031](persistence/tests/migrations/0031_dead_letter_status_index.sql), which
replaces `idx_dead_letters_policy`) carries that column as an index payload, so
the status poll stays index-only.

**Triage queries:**

```sql
-- Recent failures for a specific policy
SELECT * FROM policy_dead_letters
WHERE  policy_name = 'deposit_fee'
ORDER  BY created_at DESC
LIMIT  20;

-- Everything parked against one aggregate instance: "which customer is stuck"
SELECT * FROM policy_dead_letters
WHERE  target_stream_id = 'urn:bank-account:42';

-- Look up the original event for manual replay
SELECT * FROM events WHERE id = '<event_id from dead letter>';

-- Reactions that panicked: defects in the reaction, not refused commands
SELECT * FROM policy_dead_letters WHERE error_kind = 'Panic';

-- Reactions that never came back: look at what the command was waiting for
SELECT * FROM policy_dead_letters WHERE error_kind = 'Timeout';

-- What has already been tried, and when: a row nobody has retried says 0 / NULL
SELECT policy_name, target_stream_id, retry_count, last_retried_at, error_message
FROM   policy_dead_letters
ORDER  BY retry_count DESC;

-- Commands failing on every delivery: parked once, but parked again and again
SELECT policy_name, target_stream_id, command_name, deliveries, last_parked_at
FROM   policy_dead_letters
WHERE  deliveries > 1
ORDER  BY deliveries DESC;
```

#### Retrying and discarding dead letters

Three operator controls on `PolicyRunner` resolve parked dead letters out of
band. None take an advisory lock or move the policy cursor:

| Method | Reaction | Outcome |
|--------|----------|---------|
| `retry_dead_letter(id)` | Re-runs the reaction the row belongs to against **current** aggregate state through the same `Cqrs` path the live drain uses, and settles **every** row that reaction parked. | For the row `id` names: `Resolved` (its command succeeded, was declined with a `BusinessRuleViolation`, or is no longer emitted), `StillFailing` (re-parked in place with its **own** fresh error), `Superseded` (another writer parked that command while the replay ran, and its row was left as it stands), or `NotFound`. |
| `discard_dead_letter(id)` | None — pure bookkeeping: no `react`, no command, no new event. | `Discarded` or `NotFound`. |
| `retry_policy_dead_letters(name)` | Bulk: groups the policy's parked rows by the reaction they came from and replays each **once**, oldest-first. | `DeadLetterRetrySummary { reactions_resolved, reactions_still_failing }`. |

The unit of a retry is the **reaction** — one Policy's reaction to one event —
not the row ([ADR-0021](docs/adr/0021-retry-settles-a-reaction-not-a-row.md)). A
reaction that dispatched three commands and parked all three costs **one**
replay, the replay carries on past a failure the way the drain does, and each
row is settled by its own command: the ones that now succeed are archived, the
ones that still fail keep their own error and stay retryable. Every settlement
bumps the row's `retry_count` and stamps `last_retried_at`, the archived copy
included.

A settlement only settles the group the replay **read**. A parked command is one
row, so a delivery of the event arriving while the replay runs refreshes a row the
replay is about to settle; settling it anyway would archive a failure nobody
retried, or overwrite it with the staler error the replay produced. So a retry
carries a digest of the reaction's rows — how many there are, their summed
`deliveries` and `retry_count`, their latest `last_parked_at` — and re-reads it
with the rows locked before settling any of them: a group that moved settles
nothing, and the command the retry parks *without* a row is guarded by the key
itself ([ADR-0025](docs/adr/0025-a-retry-settles-the-group-it-locked.md)). Either
way the retry reports `Superseded` and leaves the rows alone — retry again to act
on what is parked now. The bulk summary counts such a reaction as still failing,
which it is.

The group is read a page at a time, so what a retry holds is a page of rows
rather than everything one reaction has parked — which, after an upgrade, is the
old code's commands times the deliveries they saw. Every row of the reaction is
still settled, from one replay, in one transaction.

The summary counts reactions; `PolicyStatus::dead_letter_count` keeps counting
**rows** (parked commands), so one broken two-command reaction reads as
`dead_letter_count = 2`, `reactions_still_failing = 1`.

```rust,ignore
use replay_persistence::{DeadLetterRetry, DeadLetterDiscard};

// Give a parked failure another chance against today's state.
match runner.retry_dead_letter(id).await? {
    DeadLetterRetry::Resolved => { /* this row's command resolved: archived */ }
    DeadLetterRetry::StillFailing => { /* updated in place, still retryable */ }
    DeadLetterRetry::Superseded => { /* another writer parked it mid-replay */ }
    DeadLetterRetry::NotFound => { /* nothing matched the id */ }
}

// Or give up on it permanently — the only way a row leaves for good.
match runner.discard_dead_letter(id).await? {
    DeadLetterDiscard::Discarded => { /* archived */ }
    DeadLetterDiscard::NotFound => { /* nothing matched the id */ }
}

// Replay a whole backlog after a downstream outage, oldest-first.
let summary = runner.retry_policy_dead_letters("deposit_fee").await?;
println!(
    "resolved {} reaction(s), {} still failing",
    summary.reactions_resolved, summary.reactions_still_failing
);
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
    reason           TEXT        NOT NULL,   -- 'retried' | 'discarded' | 'superseded'
    discarded_at     TIMESTAMPTZ NOT NULL DEFAULT now(),
    aggregate_name   TEXT,                   -- identity the row carried, kept as-is
    target_stream_id TEXT,
    command_name     TEXT,
    dispatch_ordinal INTEGER,
    deliveries       INTEGER     NOT NULL DEFAULT 1,  -- deliveries that parked it
    last_parked_at   TIMESTAMPTZ DEFAULT now(), -- when the last of them did; NULL if archived before 0028
    retry_count      INTEGER     NOT NULL DEFAULT 0,  -- retries made, the settling one included
    last_retried_at  TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_discarded_dead_letters_policy
    ON discarded_dead_letters (policy_name, discarded_at DESC);
```

`superseded` is the third way out, and the only one no operator asked for: the
duplicate generations the dedupe migration retired when a parked command became
unique.

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

### Reading each worker's liveness

`daemon.liveness()` answers "is this worker running". `PolicyStatusStore` (below)
answers "is this Policy moving". **Neither implies the other**: a standby replica runs
and advances nothing, and a leader whose every reaction is failing into the dead-letter
table runs and advances plenty. Liveness is known only to the process running the
workers, so it is published from memory and never derived from the tables
([ADR-0020](docs/adr/0020-liveness-is-published-from-memory-and-beaten-on-a-cadence.md)).

```rust,ignore
use replay_persistence::Liveness;

for worker in daemon.liveness() {
    let last_poll = worker.last_polled_at.map(|at| at.elapsed());
    match worker.liveness {
        // Nothing is reacting for this policy, and nothing here will start it.
        Liveness::Stopped => probe.fail(&worker.policy),
        _ => tracing::info!(
            policy = %worker.policy, liveness = %worker.liveness, ?last_poll, "policy worker"
        ),
    }
}
```

| `Liveness` | Meaning |
|------------|---------|
| `Leading` | Elected for this policy and draining its feed. |
| `StandingBy` | Running, holding no advisory lock for it — another replica leads, or none does yet. Healthy and deliberately idle. |
| `Restarting` | Dead, inside the backoff before its next restart. |
| `Stopped` | Down for good: budget spent, or the lock manager that elects it stopped. Already escalated. |
| `Unknown` | Spawned and not yet at its first election. Never a guess at "stopped". |

`last_polled_at` is a monotonic `Instant`, recorded when a poll **comes back**, so
a worker held inside one long reaction reads as `Leading` with an ageing stamp —
which is what tells it from an idle one. A `StandingBy` worker drives nothing and
normally has none. `Liveness` has a stable `as_str()` / `Display` form for JSON/UI
consumers.

#### Reading liveness from outside the process

The accessor above only sees this process's workers. For a UI, a dashboard or a
sidecar with a connection string rather than a handle, each replica **beats** for
the policies it leads — on a fixed cadence, independent of the poll interval and
of what any worker is doing:

```sql
ALTER TABLE policy_cursors
    ADD COLUMN IF NOT EXISTS last_beat_at   TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS liveness       TEXT,
    ADD COLUMN IF NOT EXISTS last_polled_at TIMESTAMPTZ,
    ADD COLUMN IF NOT EXISTS led_by         TEXT;
```

```sql
SELECT name, liveness, led_by,
       now() - last_beat_at   AS since_beat,
       last_beat_at - last_polled_at AS poll_age
FROM   policy_cursors
ORDER  BY name;
```

| Reading | Means |
|---------|-------|
| `since_beat` small, `liveness = 'Leading'` | healthy leader |
| `since_beat` small, `liveness = 'Stopped'` | the policy is down and its replica is fine — nothing reacts, and no standby takes over until that process exits |
| `since_beat` small, `poll_age` large | alive but not finishing polls: a long batch, or a reaction that hangs |
| `since_beat` small, `last_polled_at` null | the current leader has not completed a poll yet: it has just taken over or just started — or, if it stays null, it wedged inside its first poll |
| `since_beat` large, or the row never beat | **no successful beat** — usually no live leader (process gone, or no replica holds the lock); check this replica's heartbeat `warn` and whether something holds the row before concluding the leader is dead |

Fixed cadence is the whole point of the beat: a stamp written by the worker as it
polls would go quiet during a restart backoff, while a reaction hangs, and while
standing by, so staleness would mean "busy or dead" and answer nothing. The beat
is written by a task of its own, supervised alongside the lock manager, so it
keeps time while a worker is held inside a reaction.

```rust,ignore
let runner = PolicyRunner::builder(cqrs)
    .register_policy(my_policy, PolicySettings::new())
    .with_heartbeat(Duration::from_secs(5))  // default: HEARTBEAT_CADENCE
    .replica_id(std::env::var("POD_NAME")?)  // default: HOSTNAME; `led_by` in the row
    .build();
```

The cadence is a schedule, not a sleep between beats: time spent writing is
charged to the tick it happened in, so a slow write moves one beat rather than
every beat after it. Cadences below `HEARTBEAT_MIN_CADENCE` (100 ms) are floored —
that end of the range is a write loop, not a faster signal, and
`.without_heartbeat()` is how you turn it off.

Alerting:

- **Page** on `last_beat_at` older than 3 beats (15 s at the default) and on
  `liveness = 'Stopped'`. A stale beat means "no successful beat": a leader whose
  writes keep failing looks the same from here, and says so once at `warn` in its
  own logs.
- **Warn, do not page**, on `last_polled_at` older than `5 × dispatch_timeout`
  (≈2.5 min at defaults), and on a `Leading` row whose `last_polled_at` stays null
  for that long. One hung dispatch legitimately costs `dispatch_timeout`
  × four attempts, and the runner already handles that by parking a dead letter.

`last_polled_at` describes the worker that is leading *now*, so a replica taking a
policy over reports no poll until it completes one — it never inherits the previous
leader's. A null is therefore normal for one poll interval after a failover or a
start; it is only a signal once it persists, which is the case of a leader that
wedged inside its very first poll.

All of a replica's led policies are beaten in one statement, taken with
`FOR UPDATE SKIP LOCKED`: a row somebody else is holding — an operator part-way
through a cursor move in an open transaction — costs that one policy a beat, not
every policy on the replica.

One caveat on `led_by` and failover: the beat reports leadership, it does not
fence it. A replica whose lock session has just dropped can write one last beat
before its lock manager notices and revokes, so for up to a beat the row can still
name the previous leader — the same split-brain window
[ADR-0008](docs/adr/0008-policy-runner-shared-connection-leadership.md) bounds for
the workers, self-healed by the new leader's next beat. The advisory lock decides
who may act; this row only says who did.

The columns are yours, not the crate's: they are written when present, and their
absence turns the durable heartbeat off for the daemon (attempted once, reported
once) — so the migration can be applied before or after the crate version that
writes it. Add all four or none. Only the replica holding a policy's advisory lock
writes its row, so a standby never overwrites a leader's beat, and the crate never
reads any of it back. `.without_heartbeat()` turns it off entirely; liveness stays
readable in-process.

### What a policy logs

A policy's output is proportional to how often it changes state, not to how many
events it processes
([ADR-0021](docs/adr/0021-a-policy-narrates-its-transitions.md)). A burst of work
is bracketed by two `info` records, and a policy with nothing to do writes
nothing at any level, however often it polls — so silence means "nothing
happened", not "nothing is known".

| Record | When | Carries |
|--------|------|---------|
| `policy has work to do` | a poll finds a stream owing it something, before any of it is dispatched | the policy |
| `policy is working through its backlog` | the first cursor advance at least 30 s after the previous record | events so far, elapsed |
| `policy is caught up` | the first poll that finds nothing owed | events in the burst, elapsed |
| `policy dispatch committed` | every dispatch that commits, at `debug` | event, aggregate, elapsed |

The counts are places the policy advanced over, not reactions executed: a policy whose
`stream_filter` excludes a whole stream worked through it, and is not caught up until
nothing it is owed is left. The elapsed time runs from the read that
found the work to the last position the burst advanced over, so the idle interval
before the empty poll that notices is not charged to it — which also means the
catch-up record arrives up to one poll interval late.

Records are written as the policy moves, not when a poll returns, so a batch whose
dispatches take minutes still reports progress while it runs — and the opening record
precedes the first reaction, so everything that reaction logs falls inside the bracket.
A worker held inside a single reaction narrates nothing at all — that is the liveness
axis's question, and the heartbeat answers it from a task of its own.

Turn `debug` on for `replay_persistence::policy_runner` to see each dispatch that
commits while you are looking at one policy; it is six figures of records for a
large import, which is why it is off by default. A dispatch that is declined,
retried or parked reports at its own level, and restarts and escalations are logged by
the machinery that owns them (`warn` and `error`), not by this path.

### Monitoring policy status

A running policy is otherwise opaque: its cursor and dead letters live in
internal tables. `PolicyStatusStore` turns them into a read-only health signal you
can poll from a dashboard or health check. It is **not** a projection — it reads the
operational tables (`policy_cursors`, each stream's head against this policy's places in
`policy_stream_cursors`, and `policy_dead_letters`) in a **single** query and never scans
the event log. It reads one row per stream, which is affordable for a health check
scraped every few seconds and is the reason the runner does not find its work this way.
See
[ADR-0006](docs/adr/0006-policy-status-read-only-operational-snapshot.md) for the
rationale.

```rust,ignore
use replay_persistence::{PolicyStatusStore, PolicyCondition};

let statuses = PolicyStatusStore::new(pool.clone()).list().await?;

for s in &statuses {
    println!(
        "{:<20} {:<8} lag={} over {} streams dead_letters={}",
        s.name, s.condition, s.lag, s.streams_behind, s.dead_letter_count
    );
}

// React to anything needing attention.
let needs_attention: Vec<_> = statuses
    .iter()
    .filter(|s| matches!(s.condition, PolicyCondition::Degraded))
    .collect();
```

Each `PolicyStatus` carries the raw numbers plus a derived condition:

| Field | Meaning |
|-------|---------|
| `name` | Stable policy name (the cursor key). |
| `lag` | Events written and not yet passed, summed over every stream this policy is behind on. Exact: it counts events, including the ones its filter will skip, and nothing else. |
| `streams_behind` | How many streams that lag is spread across. One stream a million events behind and a million streams one event behind are the same `lag` and very different problems. |
| `discovered_through` | How far this policy's search of the log has swept. Not progress — progress is per stream — and not an operator control: a running worker reads it once per leadership term and keeps it in memory, so resetting it moves nothing until that worker restarts. To redeliver, move a place. |
| `last_checkpoint_at` | When the policy last advanced in any stream (staleness signal). |
| `dead_letter_count` | Number of `policy_dead_letters` rows for this policy. |
| `last_dead_letter_at` | Timestamp of the most recent dead letter, if any. |
| `condition` | At-a-glance health label (see below). |

`lag` is a subtraction per stream, so a position burned by a failed write does not
inflate it and another policy's traffic does not appear in it. It is computed by scanning
one row per stream, which is affordable for a status endpoint scraped every few seconds and
would not be on every poll — which is why the runner does not find its work this way
([ADR-0026](docs/adr/0026-a-policy-tracks-its-position-per-stream.md)).

When you need a **stable cut** of the log — the largest position `H` such that every
position in `1..=H` is present, e.g. to freeze a version at publish time — use
`PostgresEventStore::contiguous_high_water_mark()`; replaying events with
`global_position <= H` then observes the same set of events on every later read.

`condition` is derived with a strict precedence — **dead letters outrank lag**:

| Condition | When | Meaning |
|-----------|------|---------|
| `Degraded` | `dead_letter_count > 0` | At least one event was skipped; needs operator attention. |
| `Working` | no dead letters, `lag > 0` | Healthy and catching up. |
| `CaughtUp` | no dead letters, `lag == 0` | Fully drained and up to date. |

`condition` has a stable `as_str()` / `Display` form (`"CaughtUp"`, `"Working"`,
`"Degraded"`) for JSON/UI consumers.

> **Breaking, since the per-stream cursor.** `PolicyStatus` lost `position`, `head`,
> `next_position` and `missing_position`, and `PolicyCondition` lost `Blocked`. Their
> replacements are `lag` (now a count of events, not of positions), `streams_behind` and
> `discovered_through`. `Blocked` has no replacement because it has no cause: a policy
> reads each stream over a sequence with no holes in it, so there is no number it can be
> parked in front of, and a write still in flight makes a policy *not behind at all* —
> its events are invisible to every reader, including the one computing lag. A policy
> that is not moving is lagging, `Degraded`, or not alive, and the third is
> `daemon.liveness()`, not this.

Only policies that have actually run appear: a registered-but-never-started policy
has no `policy_cursors` row and is therefore absent from `list()`. The store only
*observes* — retrying or discarding a dead letter is a separate, deliberate action
(see the triage queries above).

`PolicyStatus` carries **no liveness field**, deliberately: every field here is
derived from the operational tables, and no table can see whether a worker task
exists. A `CaughtUp` policy whose worker died looks exactly like one that is idle
— `daemon.liveness()` is what tells them apart.

### Upgrading a running Policy to per-stream cursors

Migration [0034](persistence/tests/migrations/0034_policy_stream_cursors.sql) carries every
running policy over at exactly what it has processed: for each stream, the place it had
reached by the position its cursor stopped at. Nothing is redelivered and nothing is
skipped.

**Stop the policy daemons, migrate, then deploy.** This migration renames
`policy_cursors.position` and drops `policy_cursors.commit_txid`, so a process running the
old code against the new schema fails on every poll — loudly, which is the point: the old
code reads a global order that no longer means what it did, and failing is better than
delivering from it. Appends and command handling are untouched; only policy workers need
to be down.

The backfill joins the whole log once per policy, so budget it like a scan of `events`.
It is the reason the window includes the migration rather than just the deployment.

### A write that is slow, stuck or rolled back

None of the three stops a policy, and none of them needs an operator:

| What happened | What the policy does |
|---------------|----------------------|
| A write failed after taking a `global_position` | Nothing. The number is burned — `nextval` is not transactional — and a policy that reads no global order never looks at it. The place the write took in its stream *is* handed back, because that counter is a row and rolls back with the transaction. |
| A write is still running | Its stream waits for it, and only its stream. Every other stream is delivered meanwhile. |
| A write commits below a policy's sweep | It is delivered by the reconciliation, within `ceil(streams behind / read_batch_size)` cadences of `REPLAY_POLICY_RECONCILE_SECS` — one for a policy behind on no more streams than its read batch ([ADR-0026](docs/adr/0026-a-policy-tracks-its-position-per-stream.md)). |

This is the structural fix for
[#164](https://github.com/funkode-io/replay/issues/164), where a burned position
wedged a policy until an operator moved it by hand, and for
[#214](https://github.com/funkode-io/replay/issues/214), where one long write delayed
every policy in the deployment. The machinery that used to tell one kind of hole from
another — a `pg_locks` probe, a rate-gated `blocked` warning, a `Blocked` status — is
gone, along with the `REPLAY_BLOCKED_WARN_AFTER_SECS` knob that paced it.

A caught-up idle policy logs nothing at all.

