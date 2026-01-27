#![cfg(target_arch = "wasm32")]

use async_trait::async_trait;
// Only compile for wasm target
use wasm_bindgen_test::*;

// Configure tests to run in a browser or Node.js
wasm_bindgen_test_configure!(run_in_browser); // or run_in_nodejs

use replay::{Aggregate, EventStream, WithId};
use replay_macros::define_aggregate;

define_aggregate! {
    BankAccount {
        state: {
            account_number: String,
            balance: f64
        },
        commands: {
            OpenAccount { account_number: String },
            Deposit { amount: f64 },
            Withdraw { amount: f64 }
        },
        events: {
            AccountOpened { account_number: String },
            Deposited { amount: f64 },
            Withdrawn { amount: f64 }
        },
        service: {
            async fn validate_account_number(account_number: &str) -> bool;
        }
    }
}

// Implement the service trait
#[derive(Clone)]
pub struct MockBankAccountServices;

#[async_trait(?Send)]
impl BankAccountServices for MockBankAccountServices {
    async fn validate_account_number(&self, account_number: &str) -> bool {
        // Simple validation: account number must be at least 5 characters
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
            BankAccountEvent::Withdrawn { amount } => {
                self.balance -= amount;
            }
        }
    }
}

impl Aggregate for BankAccount {
    type Command = BankAccountCommand;
    type Error = replay::Error;
    type Services = std::sync::Arc<dyn BankAccountServices>;

    async fn handle(
        &self,
        command: Self::Command,
        services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            BankAccountCommand::OpenAccount { account_number } => {
                if !services.validate_account_number(&account_number).await {
                    return Err(replay::Error::business_rule_violation(
                        "Invalid account number: must be at least 5 characters",
                    )
                    .with_operation("OpenAccount")
                    .with_context("account_number", account_number.clone()));
                }
                Ok(vec![BankAccountEvent::AccountOpened { account_number }])
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

                let event = BankAccountEvent::Withdrawn { amount };
                Ok(vec![event])
            }
        }
    }
}

#[wasm_bindgen_test]
async fn test_bank_account_aggregate_in_wasm() {
    let id = BankAccountUrn::new_random();
    let mut aggregate = BankAccount::with_id(id);
    let services: std::sync::Arc<dyn BankAccountServices> =
        std::sync::Arc::new(MockBankAccountServices);

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

#[wasm_bindgen_test]
async fn test_service_validation_in_wasm() {
    let id = BankAccountUrn::new_random();
    let aggregate = BankAccount::with_id(id);
    let services: std::sync::Arc<dyn BankAccountServices> =
        std::sync::Arc::new(MockBankAccountServices);

    // Test valid account number
    let valid_command = BankAccountCommand::OpenAccount {
        account_number: "ACC12345".to_string(),
    };
    let result = aggregate.handle(valid_command, &services).await;
    assert!(result.is_ok());

    // Test invalid account number (too short)
    let invalid_command = BankAccountCommand::OpenAccount {
        account_number: "A12".to_string(),
    };
    let result = aggregate.handle(invalid_command, &services).await;
    assert!(result.is_err());

    let error = result.unwrap_err();
    assert_eq!(error.operation(), "OpenAccount");
}
