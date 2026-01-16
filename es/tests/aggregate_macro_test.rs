use replay_macros::define_aggregate;

// This is needed to make the macro work inside tests
use replay::{Aggregate, EventStream};

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
        }
    }
}

impl EventStream for BankAccount {
    type Event = BankAccountEvent;
    type StreamId = BankAccountUrn;

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
                Ok(vec![BankAccountEvent::Deposited { amount }])
            }
            BankAccountCommand::Withdraw { amount } => {
                if self.balance < amount {
                    Err(
                        replay::Error::business_rule_violation("Insufficient balance")
                            .with_operation("Withdraw")
                            .with_context("tried_amount", amount),
                    )
                } else {
                    Ok(vec![BankAccountEvent::Withdrawn { amount }])
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_aggregate_generation() {
        // Test that the aggregate struct was created
        let account = BankAccount::default();
        assert_eq!(account.balance, 0.0);
        assert_eq!(account.account_number, "");

        // Test that command enum was created
        let cmd = BankAccountCommand::OpenAccount {
            account_number: "ACC001".to_string(),
        };
        assert!(matches!(cmd, BankAccountCommand::OpenAccount { .. }));

        // Test that event enum was created
        let evt = BankAccountEvent::Deposited { amount: 100.0 };
        assert!(matches!(evt, BankAccountEvent::Deposited { .. }));

        // Test that namespace is auto-generated (BankAccount -> bank-account)
        assert_eq!(BankAccountUrn::namespace(), "bank-account");

        // Test URN creation and validation
        let urn = BankAccountUrn::new("acc-123").unwrap();
        assert_eq!(urn.to_string(), "urn:bank-account:acc-123");

        // Test parse with correct namespace
        let parsed = BankAccountUrn::parse("urn:bank-account:acc-456").unwrap();
        assert_eq!(parsed.to_string(), "urn:bank-account:acc-456");

        // Test parse with wrong namespace fails
        let wrong = BankAccountUrn::parse("urn:customer:acc-789");
        assert!(wrong.is_err());
    }

    #[test]
    fn test_event_type() {
        use replay::Event;

        let evt = BankAccountEvent::AccountOpened {
            account_number: "ACC001".to_string(),
        };
        assert_eq!(evt.event_type(), "AccountOpened");

        let evt = BankAccountEvent::Deposited { amount: 100.0 };
        assert_eq!(evt.event_type(), "Deposited");
    }

    #[test]
    fn test_stream_type() {
        use replay::EventStream;

        assert_eq!(BankAccount::stream_type(), "BankAccount");
    }

    #[test]
    fn test_lifecycle_of_aggregate() {
        let mut account = BankAccount::default();

        // Apply AccountOpened event
        let open_event = BankAccountEvent::AccountOpened {
            account_number: "ACC001".to_string(),
        };
        account.apply(open_event);
        assert_eq!(account.account_number, "ACC001");

        // Apply Deposited event
        let deposit_event = BankAccountEvent::Deposited { amount: 150.0 };
        account.apply(deposit_event);
        assert_eq!(account.balance, 150.0);

        // Apply Withdrawn event
        let withdraw_event = BankAccountEvent::Withdrawn { amount: 50.0 };
        account.apply(withdraw_event);
        assert_eq!(account.balance, 100.0);
    }

    // test an aggregate with custom urn namespace using macros
    #[test]
    fn test_aggregate_with_urn_namespace() {
        define_aggregate! {
            Customer {
                namespace: "my-customer",
                state: {
                    name: String,
                },
                commands: {
                    CreateCustomer { name: String },
                },
                events: {
                    CustomerCreated { name: String },
                }
            }
        };

        let customer_urn = CustomerUrn::new("peter@gmail.com").unwrap();
        assert_eq!(customer_urn.to_string(), "urn:my-customer:peter@gmail.com");

        // Test serialization
        let json = serde_json::to_string(&customer_urn).unwrap();
        assert_eq!(json, "\"urn:my-customer:peter@gmail.com\"");

        // Test deserialization with correct namespace
        let deserialized: CustomerUrn = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized, customer_urn);

        // Test deserialization with wrong namespace should fail
        let wrong_namespace = "\"urn:wrong-namespace:peter@gmail.com\"";
        let result: Result<CustomerUrn, _> = serde_json::from_str(wrong_namespace);
        assert!(result.is_err());
        let err_msg = result.unwrap_err().to_string();
        assert!(err_msg.contains("Invalid URN namespace"));

        // Test creating URN from string directly
        use std::str::FromStr;
        let urn_from_str = urn::Urn::from_str("urn:my-customer:john@example.com").unwrap();
        let customer_urn2 = CustomerUrn(urn_from_str);
        assert_eq!(
            customer_urn2.to_string(),
            "urn:my-customer:john@example.com"
        );

        // Test parse method with valid namespace
        let parsed_urn = CustomerUrn::parse("urn:my-customer:alice@example.com").unwrap();
        assert_eq!(parsed_urn.to_string(), "urn:my-customer:alice@example.com");

        // Test parse method with wrong namespace should fail
        let wrong_parse = CustomerUrn::parse("urn:wrong-namespace:bob@example.com");
        assert!(wrong_parse.is_err());

        // Verify namespace method
        assert_eq!(CustomerUrn::namespace(), "my-customer");
    }
}
