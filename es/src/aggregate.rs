use std::future::Future;

use crate::{Error, EventStream};

/// An aggregate is a domain-driven design pattern that allows you to model a domain entity as a sequence of events.
///
/// It extends the `EventStream` trait and adds a `Command` type that represents the commands that can be applied to the aggregate.
///
/// In the example of a bank account the aggregate can validate a withdraw command and return an error if the account has insufficient balance.
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
pub trait Aggregate: Sync + Send + EventStream {
    type Command: Send + Sync;

    type Error: std::error::Error + From<Error> + Send + Sync;
    type Services: Send + Sync;

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
