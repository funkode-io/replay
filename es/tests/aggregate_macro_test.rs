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
    ) -> replay::Result<Vec<Self::Event>> {
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

    use replay::WithId;

    #[test]
    fn test_aggregate_generation() {
        // Test that the aggregate struct was created
        let id = BankAccountUrn::new("test-account").expect("Failed to create URN");
        let account = BankAccount::with_id(id);
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
        let mut account = BankAccount::with_string_id("urn:bank-account:acc-001")
            .expect("Failed to create aggregate from string id");

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

    // Test proper camelCase to kebab-case conversion including acronym handling
    #[test]
    fn test_aggregate_namespace_conversion() {
        // Test simple camelCase
        define_aggregate! {
            BankAccount {
                state: { value: i32 },
                commands: { DoSomething },
                events: { SomethingDone }
            }
        }
        assert_eq!(BankAccountUrn::namespace(), "bank-account");

        // Test leading acronym
        define_aggregate! {
            HTTPConnection {
                state: { value: i32 },
                commands: { DoSomething },
                events: { SomethingDone }
            }
        }
        assert_eq!(HTTPConnectionUrn::namespace(), "http-connection");

        // Test trailing acronym
        define_aggregate! {
            ConnectionHTTP {
                state: { value: i32 },
                commands: { DoSomething },
                events: { SomethingDone }
            }
        }
        assert_eq!(ConnectionHTTPUrn::namespace(), "connection-http");

        // Test middle acronym
        define_aggregate! {
            MyHTTPServer {
                state: { value: i32 },
                commands: { DoSomething },
                events: { SomethingDone }
            }
        }
        assert_eq!(MyHTTPServerUrn::namespace(), "my-http-server");

        // Test single word
        define_aggregate! {
            Account {
                state: { value: i32 },
                commands: { DoSomething },
                events: { SomethingDone }
            }
        }
        assert_eq!(AccountUrn::namespace(), "account");

        // Test all caps word
        define_aggregate! {
            API {
                state: { value: i32 },
                commands: { DoSomething },
                events: { SomethingDone }
            }
        }
        assert_eq!(APIUrn::namespace(), "api");

        // Test multiple acronyms
        define_aggregate! {
            HTTPSAPIGateway {
                state: { value: i32 },
                commands: { DoSomething },
                events: { SomethingDone }
            }
        }
        assert_eq!(HTTPSAPIGatewayUrn::namespace(), "httpsapi-gateway");

        // Test consecutive caps followed by lowercase
        define_aggregate! {
            XMLHttpRequest {
                state: { value: i32 },
                commands: { DoSomething },
                events: { SomethingDone }
            }
        }
        assert_eq!(XMLHttpRequestUrn::namespace(), "xml-http-request");
    }

    #[test]
    fn test_urn_hash_in_hashmap() {
        use std::collections::HashMap;

        define_aggregate! {
            Product {
                state: { name: String, price: f64 },
                commands: { CreateProduct { name: String, price: f64 } },
                events: { ProductCreated { name: String, price: f64 } }
            }
        }

        // Create URNs
        let urn1 = ProductUrn::new("product-1").unwrap();
        let urn2 = ProductUrn::new("product-2").unwrap();
        let urn3 = ProductUrn::new("product-1").unwrap(); // Same as urn1

        // Use URNs as HashMap keys
        let mut products = HashMap::new();
        products.insert(urn1.clone(), "Laptop".to_string());
        products.insert(urn2.clone(), "Mouse".to_string());

        // Verify retrieval
        assert_eq!(products.get(&urn1), Some(&"Laptop".to_string()));
        assert_eq!(products.get(&urn2), Some(&"Mouse".to_string()));

        // Verify that urn3 (equal to urn1) retrieves the same value
        assert_eq!(products.get(&urn3), Some(&"Laptop".to_string()));

        // Update using equivalent URN
        products.insert(urn3, "Gaming Laptop".to_string());
        assert_eq!(products.get(&urn1), Some(&"Gaming Laptop".to_string()));
        assert_eq!(products.len(), 2); // Still only 2 entries
    }

    #[test]
    fn test_urn_hash_in_hashset() {
        use std::collections::HashSet;

        define_aggregate! {
            User {
                state: { email: String },
                commands: { RegisterUser { email: String } },
                events: { UserRegistered { email: String } }
            }
        }

        // Create URNs
        let urn1 = UserUrn::new("user-1").unwrap();
        let urn2 = UserUrn::new("user-2").unwrap();
        let urn3 = UserUrn::new("user-1").unwrap(); // Same as urn1

        // Use URNs in HashSet
        let mut users = HashSet::new();
        users.insert(urn1.clone());
        users.insert(urn2.clone());
        users.insert(urn3.clone()); // Should not increase size

        // Verify set operations
        assert_eq!(users.len(), 2); // Only 2 unique URNs
        assert!(users.contains(&urn1));
        assert!(users.contains(&urn2));
        assert!(users.contains(&urn3)); // urn3 == urn1

        // Test removal
        users.remove(&urn3); // Remove using equivalent URN
        assert_eq!(users.len(), 1);
        assert!(!users.contains(&urn1)); // urn1 should also be gone
    }
}
