use std::future::Future;

use serde::de::DeserializeOwned;
use serde::Serialize;

use crate::{persistence::EventStoreError, Stream};

/// An aggregate is a domain-driven design pattern that allows you to model a domain entity as a sequence of events.
///
/// It extends the `Stream` trait and adds a `Command` type that represents the commands that can be applied to the aggregate.
///
/// In the example of a bank account the aggregate can validate a withdraw command and return an error if the account has insufficient balance.

pub trait Aggregate: Default + Serialize + DeserializeOwned + Sync + Send + Stream {
    type Command: Send + Sync;

    type Error: std::error::Error + From<EventStoreError> + Send + Sync;
    type Services: Send + Sync;

    fn handle(
        &self,
        command: Self::Command,
        services: &Self::Services,
    ) -> impl Future<Output = Result<Vec<Self::Event>, Self::Error>> + Send;
}

// tests
#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;
    use crate::persistence::EventStoreError;
    use crate::Event;
    use replay_macros::Event;
    use serde::{Deserialize, Serialize};
    use thiserror::Error;
    use urn::Urn;

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

    #[derive(Debug, Error)]
    enum BankAccountError {
        #[error("Insufficient balance")]
        InsufficientBalance,
        #[error("Aggregate store error: {0}")]
        PersistenceError(EventStoreError),
    }

    impl From<EventStoreError> for BankAccountError {
        fn from(error: EventStoreError) -> Self {
            BankAccountError::PersistenceError(error)
        }
    }

    #[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
    struct BankAccountServices;

    #[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Default)]
    struct BankAccountAggregate {
        pub balance: f64,
    }

    #[derive(Clone, PartialEq, Debug)]
    struct BankAccountUrn(Urn);

    impl From<BankAccountUrn> for Urn {
        fn from(urn: BankAccountUrn) -> Self {
            urn.0
        }
    }

    impl Stream for BankAccountAggregate {
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

    impl Aggregate for BankAccountAggregate {
        type Command = BankAccountCommand;
        type Error = BankAccountError;
        type Services = BankAccountServices;

        async fn handle(
            &self,
            command: Self::Command,
            _services: &Self::Services,
        ) -> Result<Vec<Self::Event>, Self::Error> {
            match command {
                BankAccountCommand::Deposit { amount } => {
                    Ok(vec![BankAccountEvent::Deposited { amount }])
                }
                BankAccountCommand::Withdraw { amount } => {
                    // validate that the account has enough balance
                    if self.balance < amount {
                        Err(BankAccountError::InsufficientBalance)
                    } else {
                        Ok(vec![BankAccountEvent::Withdrawn { amount }])
                    }
                }
            }
        }
    }

    #[tokio::test]
    async fn test_bank_account_aggregate() {
        let mut aggregate = BankAccountAggregate::default();
        let services = BankAccountServices;

        let deposit = BankAccountCommand::Deposit { amount: 100.0 };
        let withdraw = BankAccountCommand::Withdraw { amount: 60.0 };

        // for each command we handle the command and apply the events to the aggregate
        for command in [deposit, withdraw] {
            let events = aggregate.handle(command, &services).await.unwrap();
            aggregate.apply_all(events);
        }

        assert_eq!(aggregate.balance, 40.0);
    }

    // test that the aggregate returns an error when the account has insufficient balance
    #[tokio::test]
    async fn test_aggregate_with_insufficient_balance() {
        let mut aggregate = BankAccountAggregate::default();
        let services = BankAccountServices;

        let deposit = BankAccountCommand::Deposit { amount: 100.0 };
        let withdraw = BankAccountCommand::Withdraw { amount: 200.0 };

        // deposit 100
        let events = aggregate.handle(deposit, &services).await.unwrap();
        aggregate.apply_all(events);

        // withdraw 200
        let result = aggregate.handle(withdraw, &services).await;

        assert!(matches!(result, Err(BankAccountError::InsufficientBalance)));
    }
}
