use std::future::Future;

use futures::TryStream;

use crate::{Error, EventStream};

/// An aggregate is a domain-driven design pattern that allows you to model a domain entity as a sequence of events.
///
/// It extends the `EventStream` trait and adds a `Command` type that represents the commands that can be applied to the aggregate.
///
/// In the example of a bank account the aggregate can validate a withdraw command and return an error if the account has insufficient balance.
///
/// # Thread Safety
/// This version of the trait (for non-WASM targets) includes `Send` bounds on the aggregate, commands, services, and futures.
/// This enables multi-threaded async runtimes (like Tokio) to safely move aggregates across threads, which is essential for:
/// - Web server request handling across thread pools
/// - Concurrent command processing with `tokio::spawn`
/// - Message passing through async channels
///
/// For WASM targets (single-threaded), a separate trait definition without `Send` bounds is provided.
///
/// # Constructor Pattern
/// Aggregates should be created using `with_id(id)` to ensure they always have an identifier.
///  `with_id` is the required constructor pattern; using `Default` is not supported for constructing aggregates.
///
/// # Methods
/// - `handle`: Validates and processes a command, returning the resulting events or an error.
/// - `handle_and_apply`: Processes a command and, if successful, applies the resulting events to the aggregate instance. This is a convenience method for typical aggregate workflows where you want to both validate and mutate state in one step.
/// - `with_id`: Creates a new aggregate instance with the given id (recommended constructor).
/// - `id`: Returns the aggregate's identifier (URN).
#[cfg(not(target_arch = "wasm32"))]
pub trait Aggregate: Sync + Send + EventStream {
    type Command: Send;

    type Error: std::error::Error + From<Error> + Sync + Send;
    type Services: Sync + Send;

    fn handle(
        &self,
        command: Self::Command,
        services: &Self::Services,
    ) -> impl Future<Output = crate::Result<Vec<Self::Event>>> + Send;

    fn handle_and_apply<'a>(
        &'a mut self,
        command: Self::Command,
        services: &'a Self::Services,
    ) -> impl Future<Output = crate::Result<Vec<Self::Event>>> + Send + 'a
    where
        Self: Sized,
    {
        async move {
            let events = self.handle(command, services).await?;
            self.apply_all(events.clone());
            Ok(events)
        }
    }
}

/// An aggregate is a domain-driven design pattern that allows you to model a domain entity as a sequence of events.
///
/// It extends the `EventStream` trait and adds a `Command` type that represents the commands that can be applied to the aggregate.
///
/// In the example of a bank account the aggregate can validate a withdraw command and return an error if the account has insufficient balance.
///
/// # WASM Compatibility
/// This version of the trait (for WASM targets) omits `Send` bounds because WASM runs in a single-threaded environment.
/// This allows services to use `async_trait(?Send)` for async methods, which is necessary for WASM compatibility.
///
/// For non-WASM targets (servers), a separate trait definition with `Send` bounds is provided to enable multi-threaded execution.
///
/// # Constructor Pattern
/// Aggregates should be created using `with_id(id)` to ensure they always have an identifier.
///  `with_id` is the required constructor pattern; using `Default` is not supported for constructing aggregates.
///
/// # Methods
/// - `handle`: Validates and processes a command, returning the resulting events or an error.
/// - `handle_and_apply`: Processes a command and, if successful, applies the resulting events to the aggregate instance. This is a convenience method for typical aggregate workflows where you want to both validate and mutate state in one step.
/// - `with_id`: Creates a new aggregate instance with the given id (recommended constructor).
/// - `id`: Returns the aggregate's identifier (URN).
#[cfg(target_arch = "wasm32")]
pub trait Aggregate: Sync + EventStream {
    type Command;

    type Error: std::error::Error + From<Error> + Sync;
    type Services: Sync;

    fn handle(
        &self,
        command: Self::Command,
        services: &Self::Services,
    ) -> impl Future<Output = crate::Result<Vec<Self::Event>>>;

    fn handle_and_apply<'a>(
        &'a mut self,
        command: Self::Command,
        services: &'a Self::Services,
    ) -> impl Future<Output = crate::Result<Vec<Self::Event>>> + 'a
    where
        Self: Sized,
    {
        async move {
            let events = self.handle(command, services).await?;
            self.apply_all(events.clone());
            Ok(events)
        }
    }
}

/// A trait for aggregates that can produce the minimum set of events needed to reconstruct
/// their current state, discarding redundant or superseded events from the full history.
///
/// In event sourcing, an aggregate's full event history may contain events that cancel each other
/// out (e.g. `Add("a")` followed by `Delete("a")`). Storing and replaying all of them is
/// correct but inefficient. `Compactable` lets an aggregate express the *minimal* event sequence
/// that, when replayed from a clean state, would produce the same current state.
///
/// The original history is **not** discarded by this trait — it is the caller's responsibility
/// to archive or retain old events before replacing them with the compacted set.
/// The store's `compact` operation handles this automatically: it archives the full event log under a
/// numbered version before writing the compacted set as the new live stream.
///
/// # Implementation contract
///
/// `compacted_events` receives the **current live event stream** supplied by the store and must
/// return the shortest subsequence that, when replayed from a freshly constructed aggregate
/// (via `with_id`), reproduces the identical state observable before compaction.
///
/// Implementations should process the stream lazily — buffering only the events that are truly
/// needed in the output — rather than collecting the full history into memory first.
///
/// # Monthly bank-account compaction example
///
/// ## The problem
///
/// A busy bank account accumulates thousands of `Deposited` and `Withdrawn` events over many
/// months. Replaying the full stream on every command grows increasingly slow. However, individual
/// transactions within the *current* period must remain queryable (e.g. for a monthly statement),
/// so they cannot simply be dropped.
///
/// ## The strategy
///
/// Introduce a single summary event:
///
/// - **`MonthlyClosed`** — recorded when a month ends; carries the month and its closing balance.
///   Applying it sets the running balance directly, replacing the entire transaction history of
///   all prior months.
///
/// When `compact` is called:
///
/// 1. All history is archived under a new version number (handled by the store).
/// 2. The store passes the current live event stream to `compacted_events`.
/// 3. The live stream is replaced with the minimal sequence returned by `compacted_events`:
///    - The last `MonthlyClosed` event (encodes the entire prior history).
///    - Any individual `Deposited` / `Withdrawn` events recorded **after** it.
///
/// Replaying these few events is enough to reconstruct the exact same aggregate state.
/// The full history is still accessible via `AggregateVersion::Version(n)`.
///
/// ## Code
///
/// ```rust,ignore
/// use chrono::NaiveDate;
/// use serde::{Deserialize, Serialize};
/// use replay::{Compactable, EventStream, WithId};
/// use replay_macros::Event;
///
/// // ── Events ──────────────────────────────────────────────────────────────
///
/// #[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Event)]
/// enum BankAccountEvent {
///     /// A positive amount was credited to the account.
///     Deposited { operation_date: NaiveDate, amount: f64 },
///     /// A positive amount was debited from the account.
///     Withdrawn { operation_date: NaiveDate, amount: f64 },
///     /// Marks the end of a calendar month and records the closing balance.
///     /// Applying this event restores the balance without replaying prior transactions.
///     MonthlyClosed { month: NaiveDate, closing_balance: f64 },
/// }
///
/// // ── Aggregate ────────────────────────────────────────────────────────────
/// // No extra state fields needed — the aggregate only tracks what it needs for
/// // business logic.  Compaction is driven by the event stream itself.
///
/// struct BankAccountAggregate {
///     pub id: BankAccountUrn,
///     pub balance: f64,
/// }
///
/// // ── EventStream ──────────────────────────────────────────────────────────
///
/// impl EventStream for BankAccountAggregate {
///     type Event = BankAccountEvent;
///
///     fn stream_type() -> String { "BankAccount".to_string() }
///
///     fn apply(&mut self, event: Self::Event) {
///         match event {
///             BankAccountEvent::Deposited { amount, .. }  => self.balance += amount,
///             BankAccountEvent::Withdrawn { amount, .. }  => self.balance -= amount,
///             BankAccountEvent::MonthlyClosed { closing_balance, .. } => {
///                 self.balance = closing_balance;
///             }
///         }
///     }
/// }
///
/// // ── Compactable ──────────────────────────────────────────────────────────
///
/// impl Compactable for BankAccountAggregate {
///     async fn compacted_events(
///         &self,
///         events: impl TryStream<Ok = Self::Event, Error = replay::Error> + Send,
///     ) -> replay::Result<Vec<Self::Event>> {
///         use futures::TryStreamExt;
///         // Sliding-window via try_fold: only keep events from the last
///         // MonthlyClosed onward.  Prior months are never buffered in memory.
///         events
///             .try_fold(Vec::new(), |mut tail, event| async move {
///                 if matches!(event, BankAccountEvent::MonthlyClosed { .. }) {
///                     tail.clear(); // discard all history before this checkpoint
///                 }
///                 tail.push(event);
///                 Ok(tail)
///             })
///             .await
///     }
/// }
/// ```
///
/// ## Before and after compaction
///
/// **Full history (stored as `Version(1)` after the first compact call):**
/// ```text
/// Deposited    { operation_date: 2026-01-01, amount: 1000.00 }
/// Withdrawn    { operation_date: 2026-01-15, amount:  200.00 }
/// Deposited    { operation_date: 2026-01-31, amount:  500.00 }
/// MonthlyClosed { month: 2026-01, closing_balance: 1300.00 }
/// Deposited    { operation_date: 2026-02-15, amount:  400.00 }  ← current period
/// Withdrawn    { operation_date: 2026-02-28, amount:  100.00 }  ← current period
/// ```
///
/// **Compacted live stream (3 events instead of 6):**
/// ```text
/// MonthlyClosed { month: 2026-01, closing_balance: 1300.00 }  ← all of January
/// Deposited    { operation_date: 2026-02-15, amount: 400.00 } ← preserved
/// Withdrawn    { operation_date: 2026-02-28, amount: 100.00 } ← preserved
/// ```
///
/// Replaying the compacted stream yields `balance == 1600.00`, identical to replaying
/// the full history. The full history remains accessible via `AggregateVersion::Version(1)`.
pub trait Compactable: EventStream {
    /// Consumes the current live event stream for this aggregate and returns the minimal
    /// subsequence that, when replayed from a fresh instance, reproduces the current state.
    ///
    /// The store passes the live stream to this method so implementations can process events
    /// one at a time without loading the entire history into memory.  Only events that must
    /// be preserved (e.g. the most recent summary + the current-period transactions) need to
    /// be buffered.
    fn compacted_events(
        &self,
        events: impl TryStream<Ok = Self::Event, Error = Error> + Send,
    ) -> impl Future<Output = crate::Result<Vec<Self::Event>>> + Send;
}

// tests
#[cfg(all(test, not(target_arch = "wasm32")))]
mod tests {
    use std::vec;

    use super::*;
    use replay_macros::Event;
    use serde::{Deserialize, Serialize};
    use tracing_test::traced_test;
    use urn::Urn;

    // Initialize tracing subscriber for tests

    #[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
    enum BankAccountCommand {
        OpenAccount { account_number: String },
        Deposit { amount: f64 },
        Withdraw { amount: f64 },
    }

    // hack to use macros inside this crate
    use crate::{self as replay, WithId};

    #[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Event)]
    enum BankAccountEvent {
        AccountOpened { account_number: String },
        Deposited { amount: f64 },
        Withdrawn { amount: f64 },
    }

    #[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
    struct BankAccountServices;

    #[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
    struct BankAccountAggregate {
        pub id: BankAccountUrn,
        pub account_number: String,
        pub balance: f64,
    }

    #[derive(Clone, PartialEq, Debug, Serialize, Deserialize)]
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
                Err(format!(
                    "Invalid namespace: expected 'bank-account', got '{}'",
                    urn.nid()
                ))
            }
        }
    }

    impl WithId for BankAccountAggregate {
        type StreamId = BankAccountUrn;

        fn with_id(id: Self::StreamId) -> Self {
            BankAccountAggregate {
                id,
                account_number: String::new(),
                balance: 0.0,
            }
        }

        fn get_id(&self) -> &Self::StreamId {
            &self.id
        }
    }

    impl EventStream for BankAccountAggregate {
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
                BankAccountEvent::Withdrawn { amount } => {
                    self.balance -= amount;
                }
            }
        }
    }

    impl Aggregate for BankAccountAggregate {
        type Command = BankAccountCommand;
        type Error = crate::Error;
        type Services = BankAccountServices;

        async fn handle(
            &self,
            command: Self::Command,
            _services: &Self::Services,
        ) -> Result<Vec<Self::Event>, Self::Error> {
            match command {
                BankAccountCommand::OpenAccount { account_number } => {
                    Ok(vec![BankAccountEvent::AccountOpened { account_number }])
                }
                BankAccountCommand::Deposit { amount } => {
                    if self.account_number.is_empty() {
                        return Err(crate::Error::business_rule_violation("Account not opened")
                            .with_operation("Deposit"));
                    }

                    Ok(vec![BankAccountEvent::Deposited { amount }])
                }
                BankAccountCommand::Withdraw { amount } => {
                    // validate that the account has enough balance
                    if self.balance < amount {
                        Err(
                            crate::Error::business_rule_violation("Insufficient balance")
                                .with_operation("Withdraw")
                                .with_context("account_number", &self.account_number)
                                .with_context("amount_withdrawn", amount),
                        )
                    } else {
                        Ok(vec![BankAccountEvent::Withdrawn { amount }])
                    }
                }
            }
        }
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    #[traced_test]
    async fn test_bank_account_aggregate() {
        use urn::UrnBuilder;
        let bank_id = BankAccountUrn(UrnBuilder::new("bank-account", "123").build().unwrap());
        let mut aggregate = BankAccountAggregate::with_id(bank_id);
        let services = BankAccountServices;

        let open_account = BankAccountCommand::OpenAccount {
            account_number: "123456".to_string(),
        };
        let deposit = BankAccountCommand::Deposit { amount: 100.0 };
        let withdraw = BankAccountCommand::Withdraw { amount: 60.0 };

        // for each command we handle the command and apply the events to the aggregate
        for command in [open_account, deposit, withdraw] {
            let events = aggregate.handle(command, &services).await.unwrap();
            aggregate.apply_all(events);
        }

        assert_eq!(aggregate.balance, 40.0);
    }

    // test that the aggregate returns an error when the account has insufficient balance
    #[cfg(not(target_arch = "wasm32"))]
    #[traced_test]
    #[tokio::test]
    async fn test_aggregate_with_insufficient_balance() {
        use urn::UrnBuilder;
        let bank_id = BankAccountUrn(UrnBuilder::new("bank-account", "456").build().unwrap());
        let mut aggregate = BankAccountAggregate::with_id(bank_id);
        let services = BankAccountServices;

        let open_account = BankAccountCommand::OpenAccount {
            account_number: "123456".to_string(),
        };
        let deposit = BankAccountCommand::Deposit { amount: 100.0 };
        let withdraw = BankAccountCommand::Withdraw { amount: 200.0 };

        // open account
        aggregate
            .handle_and_apply(open_account, &services)
            .await
            .unwrap();

        // deposit 100
        aggregate
            .handle_and_apply(deposit, &services)
            .await
            .unwrap();

        // withdraw 200
        let result = aggregate.handle(withdraw, &services).await;

        assert!(result.is_err());
        let error = result.err().unwrap();
        tracing::error!("Expected error: {}", error);
        assert!(error.to_string().contains("Insufficient balance"));
        assert!(error.is_permanent());
    }

    // test aggregate id management
    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn test_aggregate_id() {
        use urn::UrnBuilder;

        // Test with_id
        let urn = BankAccountUrn(UrnBuilder::new("bank-account", "123456").build().unwrap());
        let aggregate = BankAccountAggregate::with_id(urn.clone());

        // Verify id is set
        assert_eq!(aggregate.get_id(), &urn);
    }

    // test aggregate with_string_id constructor
    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn test_aggregate_with_string_id() {
        // Test with valid URN string
        let aggregate = BankAccountAggregate::with_string_id("urn:bank-account:789").unwrap();
        assert_eq!(aggregate.get_id().0.nss(), "789");
        assert_eq!(aggregate.get_id().0.nid(), "bank-account");

        // Test with invalid URN format should fail
        let result = BankAccountAggregate::with_string_id("not-a-urn");
        assert!(result.is_err());

        // Test with wrong namespace should fail
        let result = BankAccountAggregate::with_string_id("urn:wrong-namespace:123");
        assert!(result.is_err());
    }
}
