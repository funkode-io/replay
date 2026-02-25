#![cfg(not(target_arch = "wasm32"))] // Skip for wasm target

use async_trait::async_trait;
use replay_macros::define_aggregate;

// This is needed to make the macro work inside tests
use replay::{Aggregate, EventStream};

define_aggregate! {
    BankAccount {
        state: {
            account_number: String,
            balance: f64,
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

#[async_trait]
impl BankAccountServices for MockBankAccountServices {
    async fn validate_account_number(&self, account_number: &str) -> bool {
        let account_number = account_number.to_string();

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
    ) -> replay::Result<Vec<Self::Event>> {
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
    use serde::{Deserialize, Serialize};

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

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn test_service_validation_success() {
        use replay::WithId;

        let id = BankAccountUrn::new("test-account").expect("Failed to create URN");
        let account = BankAccount::with_id(id);
        let services: std::sync::Arc<dyn BankAccountServices> =
            std::sync::Arc::new(MockBankAccountServices);

        // Valid account number (>= 5 characters)
        let command = BankAccountCommand::OpenAccount {
            account_number: "ACC12345".to_string(),
        };

        let result = account.handle(command, &services).await;
        assert!(result.is_ok());
        let events = result.unwrap();
        assert_eq!(events.len(), 1);
        assert!(matches!(events[0], BankAccountEvent::AccountOpened { .. }));
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[tokio::test]
    async fn test_service_validation_failure() {
        use replay::WithId;

        let id = BankAccountUrn::new("test-account").expect("Failed to create URN");
        let account = BankAccount::with_id(id);
        let services: std::sync::Arc<dyn BankAccountServices> =
            std::sync::Arc::new(MockBankAccountServices);

        // Invalid account number (< 5 characters)
        let command = BankAccountCommand::OpenAccount {
            account_number: "A123".to_string(),
        };

        let result = account.handle(command, &services).await;
        assert!(result.is_err());

        let error = result.unwrap_err();
        assert_eq!(error.operation(), "OpenAccount");
        assert!(error.to_string().contains("Invalid account number"));
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

        // new() with a full URN string should be the same as parse()
        let from_full_urn = CustomerUrn::new("urn:my-customer:peter@gmail.com").unwrap();
        assert_eq!(from_full_urn.to_string(), "urn:my-customer:peter@gmail.com");

        // new() with a nested same-namespace URN should unwrap to innermost id
        let nested =
            CustomerUrn::new("urn:my-customer:urn:my-customer:urn:my-customer:peter@gmail.com")
                .unwrap();
        assert_eq!(nested.to_string(), "urn:my-customer:peter@gmail.com");

        // new() with a wrong namespace in the URN prefix should fail
        let wrong_ns = CustomerUrn::new("urn:wrong-namespace:peter@gmail.com");
        assert!(wrong_ns.is_err());
        assert_eq!(wrong_ns.unwrap_err(), urn::Error::InvalidNid);

        // new() with an outer-correct but inner-different namespace stops at the boundary
        // e.g. urn:my-customer:urn:other:123 → urn:my-customer:urn:other:123 (no further unwrap)
        let mixed = CustomerUrn::new("urn:my-customer:urn:other:123").unwrap();
        assert_eq!(mixed.to_string(), "urn:my-customer:urn:other:123");
    }

    #[test]
    fn test_streamid_serde_as_urn_string() {
        // Test that StreamId (URN) serializes and deserializes as URN string
        define_aggregate! {
            Order {
                namespace: "order",
                state: { total: f64 },
                commands: { CreateOrder { total: f64 } },
                events: { OrderCreated { total: f64 } }
            }
        }

        // Create a URN
        let order_id = OrderUrn::new("order-123").unwrap();

        // Verify it displays as URN string
        assert_eq!(order_id.to_string(), "urn:order:order-123");

        // Test JSON serialization - should be a simple string
        let json = serde_json::to_string(&order_id).unwrap();
        assert_eq!(json, "\"urn:order:order-123\"");

        // Test JSON deserialization from URN string
        let deserialized: OrderUrn = serde_json::from_str("\"urn:order:order-456\"").unwrap();
        assert_eq!(deserialized.to_string(), "urn:order:order-456");

        // Test round-trip
        let round_trip: OrderUrn = serde_json::from_str(&json).unwrap();
        assert_eq!(round_trip, order_id);

        // Test that invalid URN format fails
        let invalid: Result<OrderUrn, _> = serde_json::from_str("\"not-a-urn\"");
        assert!(invalid.is_err());

        // Test that wrong namespace fails
        let wrong_ns: Result<OrderUrn, _> = serde_json::from_str("\"urn:wrong:id-123\"");
        assert!(wrong_ns.is_err());
        assert!(wrong_ns
            .unwrap_err()
            .to_string()
            .contains("Invalid URN namespace"));

        // Test in a struct to ensure it works in composite types
        #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq)]
        struct OrderResponse {
            order_id: OrderUrn,
            status: String,
        }

        let response = OrderResponse {
            order_id: OrderUrn::new("order-789").unwrap(),
            status: "confirmed".to_string(),
        };

        // Serialize struct containing URN
        let response_json = serde_json::to_string(&response).unwrap();
        assert!(response_json.contains("\"urn:order:order-789\""));

        // Deserialize struct containing URN
        let parsed: OrderResponse = serde_json::from_str(&response_json).unwrap();
        assert_eq!(parsed, response);
        assert_eq!(parsed.order_id.to_string(), "urn:order:order-789");
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

    #[tokio::test]
    async fn test_service_with_lifetime() {
        // Test that service functions can use lifetime parameters
        define_aggregate! {
            Document {
                state: {
                    title: String,
                    content: String,
                },
                commands: {
                    CreateDocument { title: String, content: String },
                    UpdateContent { content: String }
                },
                events: {
                    DocumentCreated { title: String, content: String },
                    ContentUpdated { content: String }
                },
                service: {
                    async fn validate_content<'a>(content: &'a str) -> Result<&'a str, String>;
                    fn check_title<'a>(title: &'a str) -> Option<&'a str>;
                }
            }
        }

        // Implement the service trait with lifetime parameters
        #[derive(Clone)]
        struct MockDocumentServices;

        #[async_trait]
        impl DocumentServices for MockDocumentServices {
            async fn validate_content<'a>(&self, content: &'a str) -> Result<&'a str, String> {
                if content.len() < 10 {
                    Err("Content too short".to_string())
                } else {
                    Ok(content)
                }
            }

            fn check_title<'a>(&self, title: &'a str) -> Option<&'a str> {
                if title.is_empty() {
                    None
                } else {
                    Some(title)
                }
            }
        }

        // Test the services
        let services = MockDocumentServices;

        // Test validate_content
        let valid_content = "This is a long enough content string";
        let result = services.validate_content(valid_content).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), valid_content);

        let short_content = "Short";
        let result = services.validate_content(short_content).await;
        assert!(result.is_err());

        // Test check_title
        let title = "My Document";
        let result = services.check_title(title);
        assert_eq!(result, Some(title));

        let empty_title = "";
        let result = services.check_title(empty_title);
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_service_with_base_trait() {
        // Define a base service trait
        #[async_trait]
        pub trait FileService: Send + Sync {
            async fn read_file(&self, path: &str) -> Result<String, String>;
        }

        // Test that we can extend an existing service trait
        define_aggregate! {
            Report {
                state: {
                    title: String,
                    content: String,
                },
                commands: {
                    GenerateReport { title: String }
                },
                events: {
                    ReportGenerated { title: String, content: String }
                },
                service: FileService {
                    async fn validate_title(title: &str) -> bool;
                }
            }
        }

        // Implement the generated service trait
        #[derive(Clone)]
        struct MockReportServices;

        #[async_trait]
        impl FileService for MockReportServices {
            async fn read_file(&self, path: &str) -> Result<String, String> {
                if path.is_empty() {
                    Err("Path cannot be empty".to_string())
                } else {
                    Ok(format!("Contents of {}", path))
                }
            }
        }

        #[async_trait]
        impl ReportServices for MockReportServices {
            async fn validate_title(&self, title: &str) -> bool {
                !title.is_empty() && title.len() <= 100
            }
        }

        // Test the services
        let services = MockReportServices;

        // Test the base trait method
        let result = services.read_file("test.txt").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Contents of test.txt");

        let result = services.read_file("").await;
        assert!(result.is_err());

        // Test the aggregate-specific method
        assert!(services.validate_title("Valid Title").await);
        assert!(!services.validate_title("").await);
        assert!(!services.validate_title(&"x".repeat(101)).await);
    }

    #[tokio::test]
    async fn test_service_with_multiple_base_traits() {
        // Define multiple base service traits
        #[async_trait]
        pub trait FileService: Send + Sync {
            async fn read_file(&self, path: &str) -> Result<String, String>;
        }

        pub trait LogService: Send + Sync {
            fn log(&self, message: &str);
        }

        // Test that we can extend multiple existing service traits
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

        // Implement the generated service trait
        #[derive(Clone)]
        struct MockAuditLogServices;

        #[async_trait]
        impl FileService for MockAuditLogServices {
            async fn read_file(&self, path: &str) -> Result<String, String> {
                Ok(format!("Log from {}", path))
            }
        }

        impl LogService for MockAuditLogServices {
            fn log(&self, message: &str) {
                println!("LOG: {}", message);
            }
        }

        impl AuditLogServices for MockAuditLogServices {
            fn validate_entry(&self, entry: &str) -> bool {
                !entry.is_empty()
            }
        }

        // Test the services
        let services = MockAuditLogServices;

        // Test the first base trait method
        let result = services.read_file("audit.log").await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), "Log from audit.log");

        // Test the second base trait method
        services.log("Test message");

        // Test the aggregate-specific method
        assert!(services.validate_entry("Valid entry"));
        assert!(!services.validate_entry(""));
    }

    #[test]
    fn test_aggregate_with_generic_type_parameter() {
        // Test aggregate with generic type parameter - bounds are automatically added by macro
        // Note: PartialEq is required here because T is used in events
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

        // Test with String type
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

        // Create an instance
        let id = FileManagerUrn::new("manager-1").unwrap();
        let mut manager: FileManager<String> = FileManager::with_id(id);
        assert_eq!(manager.count, 0);
        assert_eq!(manager.processed, String::default());

        // Apply an event
        let event = FileManagerEvent::FileProcessed {
            data: "file1.txt".to_string(),
        };
        manager.apply(event);
        assert_eq!(manager.processed, "file1.txt");
        assert_eq!(manager.count, 1);

        // Test with i32 type
        impl EventStream for FileManager<i32> {
            type Event = FileManagerEvent<i32>;

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

        let id2 = FileManagerUrn::new("manager-2").unwrap();
        let mut manager2: FileManager<i32> = FileManager::with_id(id2);
        let event2 = FileManagerEvent::FileProcessed { data: 42 };
        manager2.apply(event2);
        assert_eq!(manager2.processed, 42);
        assert_eq!(manager2.count, 1);
    }

    #[test]
    fn test_aggregate_generic_without_partialeq_in_events() {
        // Test that T doesn't need PartialEq when it's not used in events
        // This type deliberately doesn't implement PartialEq
        #[derive(Clone, Default, Debug, Serialize, Deserialize)]
        struct NoCompare {
            data: String,
        }

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
                    Stored { count: usize }  // T is not in events, so no PartialEq needed!
                }
            }
        }

        impl EventStream for Container<NoCompare> {
            type Event = ContainerEvent; // No <NoCompare> since T not used in events

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

        let id = ContainerUrn::new("container-1").unwrap();
        let mut container: Container<NoCompare> = Container::with_id(id);
        let event = ContainerEvent::Stored { count: 5 };
        container.apply(event);
        assert_eq!(container.count, 5);
    }

    #[test]
    fn test_type_parameter_detection_avoids_false_positives() {
        // Test that type parameter detection uses proper AST traversal
        // and doesn't falsely match on types that contain the parameter name as a substring

        // Type that contains 'T' in its name but is not the type parameter T
        #[derive(Clone, Default, Debug, Serialize, Deserialize, PartialEq)]
        struct Thing {
            value: String,
        }

        // T is used in state but NOT in commands or events
        // Commands/events use "Thing" which contains 'T' but is not the type parameter T
        define_aggregate! {
            Manager<T> {
                state: {
                    data: T,
                    count: usize,
                },
                commands: {
                    Update { thing: Thing }  // "Thing" contains 'T' but is NOT the type parameter T
                },
                events: {
                    Updated { thing: Thing }  // "Thing" contains 'T' but is NOT the type parameter T
                }
            }
        }

        // If the macro incorrectly used string matching with .contains(),
        // it would think T is used in commands/events because "Thing" contains 'T'
        // But with proper AST traversal:
        // - ManagerCommand should have NO type parameter
        // - ManagerEvent should have NO type parameter

        impl EventStream for Manager<String> {
            type Event = ManagerEvent; // No <String> - T not actually used in events!

            fn stream_type() -> String {
                "Manager".to_string()
            }

            fn apply(&mut self, event: Self::Event) {
                match event {
                    ManagerEvent::Updated { thing: _ } => {
                        self.count += 1;
                    }
                }
            }
        }

        impl Aggregate for Manager<String> {
            type Command = ManagerCommand; // No <String> - T not actually used in commands!
            type Error = replay::Error;
            type Services = ();

            async fn handle(
                &self,
                command: Self::Command,
                _services: &Self::Services,
            ) -> replay::Result<Vec<Self::Event>> {
                match command {
                    ManagerCommand::Update { thing } => Ok(vec![ManagerEvent::Updated { thing }]),
                }
            }
        }

        // Test the full workflow
        let id = ManagerUrn::new("mgr-1").unwrap();
        let mut manager: Manager<String> = Manager::with_id(id);

        // Create a command - no type parameter needed!
        let command = ManagerCommand::Update {
            thing: Thing {
                value: "test".to_string(),
            },
        };

        // Handle the command
        let services = ();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let events = runtime
            .block_on(manager.handle(command, &services))
            .unwrap();

        // Apply the events
        manager.apply_all(events);
        assert_eq!(manager.count, 1);

        // This all compiles, proving that neither ManagerCommand nor ManagerEvent
        // have type parameters, even though the aggregate Manager<T> does!
    }

    #[test]
    fn test_associated_type_parameter_detection() {
        // Test that associated types like T::Item are correctly detected as using type parameter T
        use serde::de::DeserializeOwned;
        use std::fmt;

        // A trait with an associated type
        trait Container:
            Clone + Default + fmt::Debug + Serialize + DeserializeOwned + Sync + Send
        {
            type Item: Clone + Default + fmt::Debug + Serialize + DeserializeOwned + Sync + Send;
        }

        #[derive(Clone, Default, Debug, Serialize, Deserialize)]
        struct StringContainer;
        impl Container for StringContainer {
            type Item = String;
        }

        // Aggregate where commands use T::Item but events don't
        define_aggregate! {
            Processor<T: Container> {
                state: {
                    item: T::Item,
                    count: usize,
                },
                commands: {
                    Process { item: T::Item }  // Uses T::Item - should include T in command generics
                },
                events: {
                    Processed { count: usize }  // Doesn't use T - should NOT include T in event generics
                }
            }
        }

        impl EventStream for Processor<StringContainer> {
            type Event = ProcessorEvent; // No <StringContainer> since T not used in events

            fn stream_type() -> String {
                "Processor".to_string()
            }

            fn apply(&mut self, event: Self::Event) {
                match event {
                    ProcessorEvent::Processed { count } => {
                        self.count = count;
                    }
                }
            }
        }

        impl Aggregate for Processor<StringContainer> {
            type Command = ProcessorCommand<StringContainer>; // Should have <StringContainer> since T::Item used!
            type Error = replay::Error;
            type Services = ();

            async fn handle(
                &self,
                command: Self::Command,
                _services: &Self::Services,
            ) -> replay::Result<Vec<Self::Event>> {
                match command {
                    ProcessorCommand::Process { item } => {
                        // item is T::Item which is String
                        let _: String = item;
                        Ok(vec![ProcessorEvent::Processed {
                            count: self.count + 1,
                        }])
                    }
                }
            }
        }

        // Test it works
        let id = ProcessorUrn::new("proc-1").unwrap();
        let mut processor: Processor<StringContainer> = Processor::with_id(id);

        let command = ProcessorCommand::Process {
            item: "test".to_string(),
        };

        let services = ();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let events = runtime
            .block_on(processor.handle(command, &services))
            .unwrap();

        processor.apply_all(events);
        assert_eq!(processor.count, 1);

        // This proves that:
        // - ProcessorCommand<T> has type parameter T (because T::Item is used)
        // - ProcessorEvent has NO type parameter (because T is not used in events)
    }

    #[test]
    fn test_command_with_byte_stream() {
        // Test that commands can handle non-serializable types like byte streams
        // since commands are effectful and don't need serde
        use std::io::Read;

        // A reader that wraps bytes - not serializable but has Debug
        #[derive(Debug)]
        struct ByteReader {
            data: std::io::Cursor<Vec<u8>>,
        }

        impl ByteReader {
            fn new(data: Vec<u8>) -> Self {
                Self {
                    data: std::io::Cursor::new(data),
                }
            }

            fn read_to_string(&mut self) -> String {
                let mut result = String::new();
                self.data.read_to_string(&mut result).unwrap();
                result
            }
        }

        define_aggregate! {
            FileProcessor {
                state: {
                    processed_content: String,
                    file_count: usize,
                },
                commands: {
                    ProcessBytes { reader: ByteReader }  // Non-serializable command!
                },
                events: {
                    BytesProcessed { content: String }  // Event is serializable
                }
            }
        }

        impl EventStream for FileProcessor {
            type Event = FileProcessorEvent;

            fn stream_type() -> String {
                "FileProcessor".to_string()
            }

            fn apply(&mut self, event: Self::Event) {
                match event {
                    FileProcessorEvent::BytesProcessed { content } => {
                        self.processed_content = content;
                        self.file_count += 1;
                    }
                }
            }
        }

        impl Aggregate for FileProcessor {
            type Command = FileProcessorCommand;
            type Error = replay::Error;
            type Services = ();

            async fn handle(
                &self,
                command: Self::Command,
                _services: &Self::Services,
            ) -> replay::Result<Vec<Self::Event>> {
                match command {
                    FileProcessorCommand::ProcessBytes { mut reader } => {
                        // Process the byte stream
                        let content = reader.read_to_string();
                        Ok(vec![FileProcessorEvent::BytesProcessed { content }])
                    }
                }
            }
        }

        // Usage
        let id = FileProcessorUrn::new("processor-1").unwrap();
        let mut processor = FileProcessor::with_id(id);

        // Create a command with a byte stream
        let bytes = b"Hello from bytes!".to_vec();
        let reader = ByteReader::new(bytes);
        let command = FileProcessorCommand::ProcessBytes { reader };

        // Handle the command using tokio runtime
        let services = ();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let events = runtime
            .block_on(processor.handle(command, &services))
            .unwrap();

        // Apply the events
        processor.apply_all(events);

        // Verify the result
        assert_eq!(processor.processed_content, "Hello from bytes!");
        assert_eq!(processor.file_count, 1);
    }

    #[test]
    fn test_command_with_non_sync_type() {
        // Test that commands can handle Send but not Sync types (like channels)
        // This verifies that commands only require Send, not Sync
        use std::sync::mpsc;

        // mpsc::Receiver is Send but NOT Sync
        define_aggregate! {
            StreamProcessor {
                state: {
                    messages_received: usize,
                },
                commands: {
                    // Receiver<String> is Send but NOT Sync - this should compile
                    ProcessStream { receiver: mpsc::Receiver<String> }
                },
                events: {
                    StreamProcessed { count: usize }
                }
            }
        }

        impl EventStream for StreamProcessor {
            type Event = StreamProcessorEvent;

            fn stream_type() -> String {
                "StreamProcessor".to_string()
            }

            fn apply(&mut self, event: Self::Event) {
                match event {
                    StreamProcessorEvent::StreamProcessed { count } => {
                        self.messages_received = count;
                    }
                }
            }
        }

        impl Aggregate for StreamProcessor {
            type Command = StreamProcessorCommand;
            type Error = replay::Error;
            type Services = ();

            async fn handle(
                &self,
                command: Self::Command,
                _services: &Self::Services,
            ) -> replay::Result<Vec<Self::Event>> {
                match command {
                    StreamProcessorCommand::ProcessStream { receiver } => {
                        let mut count = 0;
                        // Consume all messages from the receiver
                        while let Ok(_msg) = receiver.try_recv() {
                            count += 1;
                        }
                        Ok(vec![StreamProcessorEvent::StreamProcessed {
                            count: self.messages_received + count,
                        }])
                    }
                }
            }
        }

        // Usage - verify it compiles and works
        let id = StreamProcessorUrn::new("processor-1").unwrap();
        let mut processor = StreamProcessor::with_id(id);

        // Create a channel (Receiver is Send but not Sync)
        let (sender, receiver) = mpsc::channel();
        sender.send("Message 1".to_string()).unwrap();
        sender.send("Message 2".to_string()).unwrap();
        sender.send("Message 3".to_string()).unwrap();
        drop(sender); // Close the channel

        // Create command with the receiver (which is NOT Sync)
        let command = StreamProcessorCommand::ProcessStream { receiver };

        // This should compile because commands only need Send, not Sync
        let services = ();
        let runtime = tokio::runtime::Runtime::new().unwrap();
        let events = runtime
            .block_on(processor.handle(command, &services))
            .unwrap();

        processor.apply_all(events);
        assert_eq!(processor.messages_received, 3);
    }
}
