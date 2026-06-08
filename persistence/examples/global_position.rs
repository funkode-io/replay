//! End-to-end tour of the `replay` event-sourcing toolkit using a small banking
//! domain. It is the runnable companion to the example at the top of the README,
//! so the two are kept in lock-step.
//!
//! The domain has two aggregate roots:
//!
//! * [`User`] — a person who can register and own accounts.
//! * [`BankAccount`] — an account opened *for* a user that money flows in and out of.
//!
//! From those events we build the same read model — a user's **global position**
//! (their name plus the summed balance of every account they own) — in two
//! different ways so the trade-offs are visible side by side:
//!
//! * a [`Query`] folded **live** in memory on every read, and
//! * an [`InlineProjection`] **materialised** into Postgres inside the append
//!   transaction.
//!
//! Run the live half (no database required) with:
//!
//! ```bash
//! cargo run -p es-replay-persistence --example global_position
//! ```
//!
//! The inline-projection half needs Postgres, so it is exercised by the
//! Docker-gated integration test `global_position_*_postgres_test`; here it is
//! defined so it type-checks under `cargo build --examples`.

use std::collections::HashSet;

// NOTE: we import the traits and macros explicitly rather than glob-importing
// `replay_persistence::prelude::*`. That prelude re-exports `Result`/`Error`,
// which would shadow the `std`/`serde` names the derive macros expand to.
// `replay::prelude` is safe to glob: it brings in the core traits
// (`Aggregate`, `EventStream`, `Compactable`, …) without those names.
use replay::prelude::*;
use replay_macros::{define_aggregate, query_events};
use replay_persistence::{db_error, InlineProjection, PersistedEvent, Query, StreamFilter};

// ─────────────────────────────────────────────────────────────────────────────
// User aggregate
// ─────────────────────────────────────────────────────────────────────────────

define_aggregate! {
    User {
        // No `namespace`: it auto-derives from the type name (`User` -> "user"),
        // so `UserUrn::new("alice")` yields `urn:user:alice`.
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

// ─────────────────────────────────────────────────────────────────────────────
// BankAccount aggregate
// ─────────────────────────────────────────────────────────────────────────────

define_aggregate! {
    BankAccount {
        // Override the namespace: the type name would auto-derive to
        // "bank-account", but we pin the shorter "account" so URNs read
        // `urn:account:alice-checking`.
        namespace: "account",
        state: {
            owner: Option<UserUrn>,
            balance: f64,
        },
        commands: {
            // An account is opened *for* a user: the command only references the
            // owning root by its URN, it never reaches into the User aggregate.
            OpenAccount { owner: UserUrn },
            Deposit { amount: f64 },
            Withdraw { amount: f64 },
            CloseMonth { month: chrono::NaiveDate },
        },
        events: {
            // Only `AccountOpened` carries the owner; movements stay lean and the
            // read models resolve account -> owner from this event.
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
            BankAccountEvent::MonthlyClosed {
                closing_balance, ..
            } => self.balance = closing_balance,
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

impl Compactable for BankAccount {
    async fn compacted_events(
        &self,
        events: impl futures::TryStream<Ok = BankAccountEvent, Error = replay::Error> + Send,
    ) -> replay::Result<Vec<BankAccountEvent>> {
        use futures::TryStreamExt;
        // Keep only the tail after the most recent monthly checkpoint: each
        // `MonthlyClosed` snapshots the balance, so everything before it is
        // redundant once we replace the balance on replay.
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

// ── Policy idempotency recipe (causation guard) ─────────────────────────────

// This aggregate is a copy-pasteable recipe for policy targets under
// at-least-once delivery:
// - command carries `causation_event_id` from the triggering event
// - state tracks applied causation ids
// - duplicate causation id => no-op (returns no events)
define_aggregate! {
    PolicyFeeLedger {
        namespace: "policy-fee-ledger",
        state: {
            balance: f64,
            applied_causation_ids: HashSet<uuid::Uuid>,
        },
        commands: {
            Credit { amount: f64 },
            ChargeFee {
                amount: f64,
                causation_event_id: uuid::Uuid,
            },
        },
        events: {
            Credited { amount: f64 },
            FeeCharged {
                amount: f64,
                causation_event_id: uuid::Uuid,
            },
        }
    }
}

impl EventStream for PolicyFeeLedger {
    type Event = PolicyFeeLedgerEvent;

    fn stream_type() -> String {
        "PolicyFeeLedger".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            PolicyFeeLedgerEvent::Credited { amount } => self.balance += amount,
            PolicyFeeLedgerEvent::FeeCharged {
                amount,
                causation_event_id,
            } => {
                self.applied_causation_ids.insert(causation_event_id);
                self.balance -= amount;
            }
        }
    }
}

impl Aggregate for PolicyFeeLedger {
    type Command = PolicyFeeLedgerCommand;
    type Error = replay::Error;
    type Services = ();

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> replay::Result<Vec<Self::Event>> {
        match command {
            PolicyFeeLedgerCommand::Credit { amount } => {
                Ok(vec![PolicyFeeLedgerEvent::Credited { amount }])
            }
            PolicyFeeLedgerCommand::ChargeFee {
                amount,
                causation_event_id,
            } => {
                if self.applied_causation_ids.contains(&causation_event_id) {
                    // Duplicate delivery for the same causation identity:
                    // absorb as no-op.
                    return Ok(Vec::new());
                }

                Ok(vec![PolicyFeeLedgerEvent::FeeCharged {
                    amount,
                    causation_event_id,
                }])
            }
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Deposit-fee policy (closure shortcut example)
//
// This section shows the low-ceremony `register_policy_fn` path.  The same
// domain logic is exercised by the Docker-gated integration test
// `global_position_closure_policy_charges_fee_postgres_test`.
// ─────────────────────────────────────────────────────────────────────────────

/// Stable cursor key for the deposit-fee policy.
pub const DEPOSIT_FEE_POLICY_NAME: &str = "deposit_fee";

/// Fee fraction charged on every deposit (1 %).
pub const DEPOSIT_FEE_RATE: f64 = 0.01;

/// URN of the shared fee ledger aggregate instance.
pub const DEPOSIT_FEE_LEDGER_ID: &str = "global-fees";

/// Pure reaction for the deposit-fee policy.
///
/// Reacts to every [`BankAccountEvent::Deposited`] by charging a 1 % fee to
/// the shared [`PolicyFeeLedger`].  The `causation_event_id` field in the
/// command carries the triggering event's identity so the ledger can absorb
/// duplicate deliveries as no-ops.
///
/// Exported so the integration test can pass it directly to
/// [`PolicyRunnerBuilder::register_policy_fn`] and keep the logic in one place.
pub fn deposit_fee_react(
    event: &replay_persistence::PersistedEvent<BankAccountEvent>,
) -> Vec<replay_persistence::Dispatch> {
    match &event.data {
        BankAccountEvent::Deposited { amount } => {
            let ledger = PolicyFeeLedgerUrn::new(DEPOSIT_FEE_LEDGER_ID).unwrap();
            vec![replay_persistence::Dispatch::to::<PolicyFeeLedger>(
                ledger,
                PolicyFeeLedgerCommand::ChargeFee {
                    amount: amount * DEPOSIT_FEE_RATE,
                    causation_event_id: event.id,
                },
            )]
        }
        _ => Vec::new(),
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Cross-aggregate read model: a user's global position
// ─────────────────────────────────────────────────────────────────────────────

// One merged event type lets a single reader consume both streams. Its
// `Deserialize` impl tries each underlying type in turn (deserialize-or-skip),
// which is exactly how unrelated events are filtered out during a fold or while
// routing to an inline projection.
query_events!(GlobalPositionEvent => [UserEvent, BankAccountEvent]);

/// A user's name together with the summed balance of every account they own.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct GlobalPosition {
    pub name: String,
    pub total_balance: f64,
}

// ── Strategy 1: a live in-memory query ───────────────────────────────────────

/// Computes [`GlobalPosition`] for a single user by folding history on demand.
///
/// Because deposits and withdrawals do not carry the owner, the query first learns
/// which accounts belong to the user (from `AccountOpened`) and then applies only
/// the movements on those accounts.
///
/// Trade-off: nothing is stored, so every read folds the log from the start.
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
        // Read this user's own stream plus every bank-account stream, then resolve
        // ownership from the events themselves. The filter is a hint the store
        // pushes down where it can; the live strategy still rescans the matched
        // history on every read.
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

// ── Strategy 2: a materialised inline projection ─────────────────────────────

/// Maintains the global position for every user in two Postgres tables, written
/// inside the same transaction that appends the events.
///
/// * `gp_account_owner` maps an account URN to its owning user URN.
/// * `gp_user_position` holds each user's name and running total balance.
///
/// The events live in a single stream table; the normalised read model brings the
/// classic owner/account/position join back. Trade-off: reads are a single indexed
/// `SELECT`, paid for with schema, versioning, and write-time cost.
///
/// The view tables are owned by a SQL migration
/// (`tests/migrations/0006_global_position_projection.sql`), not created here.
/// Prefer migrations over DDL in [`init`](InlineProjection::init): they keep
/// schema creation and evolution in your auditable migration history instead of
/// coupling it to projection registration.
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
        // No-op: the `gp_account_owner` / `gp_user_position` tables are created by
        // a migration, not here. Running schema DDL from `init` is discouraged
        // because it bypasses your migration history; manage the view schema with
        // your migration tool and keep `init` empty (or for one-off setup that
        // genuinely can't live in a migration).
        Ok(())
    }

    async fn handle(
        &mut self,
        conn: &mut Self::Exec,
        events: &[PersistedEvent<Self::Event>],
    ) -> replay::Result<()> {
        for event in events {
            match &event.data {
                GlobalPositionEvent::UserEvent(UserEvent::Registered { name }) => {
                    sqlx::query(
                        "INSERT INTO gp_user_position (user_urn, name)
                         VALUES ($1, $2)
                         ON CONFLICT (user_urn) DO UPDATE SET name = EXCLUDED.name",
                    )
                    .bind(event.stream_id.to_string())
                    .bind(name)
                    .execute(&mut *conn)
                    .await
                    .map_err(db_error)?;
                }
                GlobalPositionEvent::BankAccountEvent(BankAccountEvent::AccountOpened {
                    owner,
                }) => {
                    sqlx::query(
                        "INSERT INTO gp_account_owner (account_urn, user_urn)
                         VALUES ($1, $2)
                         ON CONFLICT (account_urn) DO UPDATE SET user_urn = EXCLUDED.user_urn",
                    )
                    .bind(event.stream_id.to_string())
                    .bind(owner.to_string())
                    .execute(&mut *conn)
                    .await
                    .map_err(db_error)?;

                    // Make sure the owner has a position row even before any movement.
                    sqlx::query(
                        "INSERT INTO gp_user_position (user_urn)
                         VALUES ($1)
                         ON CONFLICT (user_urn) DO NOTHING",
                    )
                    .bind(owner.to_string())
                    .execute(&mut *conn)
                    .await
                    .map_err(db_error)?;
                }
                GlobalPositionEvent::BankAccountEvent(BankAccountEvent::Deposited { amount }) => {
                    apply_balance_delta(conn, &event.stream_id, *amount).await?;
                }
                GlobalPositionEvent::BankAccountEvent(BankAccountEvent::Withdrawn { amount }) => {
                    apply_balance_delta(conn, &event.stream_id, -*amount).await?;
                }
                GlobalPositionEvent::BankAccountEvent(BankAccountEvent::MonthlyClosed {
                    ..
                }) => {}
            }
        }

        Ok(())
    }
}

/// Add `delta` to the running balance of whichever user owns `account_urn`.
async fn apply_balance_delta(
    conn: &mut sqlx::PgConnection,
    account_urn: &urn::Urn,
    delta: f64,
) -> replay::Result<()> {
    sqlx::query(
        "UPDATE gp_user_position
            SET total_balance = total_balance + $1
          WHERE user_urn = (
              SELECT user_urn FROM gp_account_owner WHERE account_urn = $2
          )",
    )
    .bind(delta)
    .bind(account_urn.to_string())
    .execute(conn)
    .await
    .map_err(db_error)?;

    Ok(())
}

// ─────────────────────────────────────────────────────────────────────────────
// Runnable walkthrough (live query, no database required)
// ─────────────────────────────────────────────────────────────────────────────

// Gated out of the `cfg(test)` build so this file can be `#[path]`-included as a
// module by the Docker-gated integration test, which drives the same types
// against Postgres and provides its own test harness.
#[cfg(not(test))]
#[tokio::main]
async fn main() -> replay::Result<()> {
    use replay_persistence::{Cqrs, InMemoryEventStore};

    let cqrs = Cqrs::new(InMemoryEventStore::new());

    // Register a user.
    let alice = UserUrn::new("alice").unwrap();
    cqrs.execute::<User>(
        &alice,
        Default::default(),
        UserCommand::Register {
            name: "Alice".to_string(),
        },
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
            BankAccountCommand::OpenAccount {
                owner: alice.clone(),
            },
            &(),
            None,
        )
        .await?;
    }

    cqrs.execute::<BankAccount>(
        &checking,
        Default::default(),
        BankAccountCommand::Deposit { amount: 1_000.0 },
        &(),
        None,
    )
    .await?;
    cqrs.execute::<BankAccount>(
        &checking,
        Default::default(),
        BankAccountCommand::Withdraw { amount: 250.0 },
        &(),
        None,
    )
    .await?;
    cqrs.execute::<BankAccount>(
        &savings,
        Default::default(),
        BankAccountCommand::Deposit { amount: 500.0 },
        &(),
        None,
    )
    .await?;

    // A single account always knows its own balance straight from the aggregate.
    let checking_account = cqrs.fetch_aggregate::<BankAccount>(&checking).await?;
    println!("checking balance: {}", checking_account.balance);

    // The global position spans every account Alice owns. Here it is folded live.
    let mut position = GlobalPositionQuery::for_user(alice.clone());
    cqrs.run_query::<_, GlobalPositionEvent>(&mut position)
        .await?;

    println!(
        "global position for {}: {} across all accounts",
        position.position().name,
        position.position().total_balance,
    );

    assert_eq!(position.position().name, "Alice");
    assert_eq!(position.position().total_balance, 1_250.0);

    Ok(())
}
