#![cfg(not(target_arch = "wasm32"))]

use replay::Event;
use replay_macros::query_events;
use serde::{Deserialize, Serialize};

// Define two separate event types representing different aggregates

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum UserEvent {
    UserCreated { user_id: String, name: String },
    UserUpdated { user_id: String, name: String },
    UserDeleted { user_id: String },
}

impl Event for UserEvent {
    fn event_type(&self) -> String {
        match self {
            UserEvent::UserCreated { .. } => "UserCreated".to_string(),
            UserEvent::UserUpdated { .. } => "UserUpdated".to_string(),
            UserEvent::UserDeleted { .. } => "UserDeleted".to_string(),
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CatalogEvent {
    ProductAdded { product_id: String, name: String },
    ProductUpdated { product_id: String, name: String },
    ProductRemoved { product_id: String },
}

impl Event for CatalogEvent {
    fn event_type(&self) -> String {
        match self {
            CatalogEvent::ProductAdded { .. } => "ProductAdded".to_string(),
            CatalogEvent::ProductUpdated { .. } => "ProductUpdated".to_string(),
            CatalogEvent::ProductRemoved { .. } => "ProductRemoved".to_string(),
        }
    }
}

// Use the query_events! macro to create a merged event type
query_events!(UserHistoryEvent => [UserEvent, CatalogEvent]);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_query_events_enum_generation() {
        // Test that variants were created correctly
        let user_evt = UserHistoryEvent::UserEvent(UserEvent::UserCreated {
            user_id: "user-1".to_string(),
            name: "Alice".to_string(),
        });
        assert!(matches!(user_evt, UserHistoryEvent::UserEvent(_)));

        let catalog_evt = UserHistoryEvent::CatalogEvent(CatalogEvent::ProductAdded {
            product_id: "prod-1".to_string(),
            name: "Laptop".to_string(),
        });
        assert!(matches!(catalog_evt, UserHistoryEvent::CatalogEvent(_)));
    }

    #[test]
    fn test_from_trait_implementations() {
        // Test From<UserEvent> for UserHistoryEvent
        let user_event = UserEvent::UserCreated {
            user_id: "user-2".to_string(),
            name: "Bob".to_string(),
        };
        let merged: UserHistoryEvent = user_event.clone().into();
        assert!(matches!(merged, UserHistoryEvent::UserEvent(_)));

        // Test From<CatalogEvent> for UserHistoryEvent
        let catalog_event = CatalogEvent::ProductAdded {
            product_id: "prod-2".to_string(),
            name: "Phone".to_string(),
        };
        let merged: UserHistoryEvent = catalog_event.clone().into();
        assert!(matches!(merged, UserHistoryEvent::CatalogEvent(_)));
    }

    #[test]
    fn test_event_trait_delegation() {
        let user_evt = UserHistoryEvent::UserEvent(UserEvent::UserCreated {
            user_id: "user-3".to_string(),
            name: "Charlie".to_string(),
        });
        assert_eq!(user_evt.event_type(), "UserCreated");

        let catalog_evt = UserHistoryEvent::CatalogEvent(CatalogEvent::ProductUpdated {
            product_id: "prod-3".to_string(),
            name: "Tablet".to_string(),
        });
        assert_eq!(catalog_evt.event_type(), "ProductUpdated");
    }

    #[test]
    fn test_partial_eq_implementation() {
        let evt1 = UserHistoryEvent::UserEvent(UserEvent::UserCreated {
            user_id: "user-4".to_string(),
            name: "Diana".to_string(),
        });

        let evt2 = UserHistoryEvent::UserEvent(UserEvent::UserCreated {
            user_id: "user-4".to_string(),
            name: "Diana".to_string(),
        });

        let evt3 = UserHistoryEvent::UserEvent(UserEvent::UserCreated {
            user_id: "user-5".to_string(),
            name: "Eve".to_string(),
        });

        let evt4 = UserHistoryEvent::CatalogEvent(CatalogEvent::ProductAdded {
            product_id: "prod-4".to_string(),
            name: "Monitor".to_string(),
        });

        // Same variant, same data
        assert_eq!(evt1, evt2);

        // Same variant, different data
        assert_ne!(evt1, evt3);

        // Different variants
        assert_ne!(evt1, evt4);
    }

    #[test]
    fn test_display_implementation() {
        let user_evt = UserHistoryEvent::UserEvent(UserEvent::UserDeleted {
            user_id: "user-6".to_string(),
        });
        let display_str = format!("{}", user_evt);
        assert!(display_str.contains("UserDeleted"));

        let catalog_evt = UserHistoryEvent::CatalogEvent(CatalogEvent::ProductRemoved {
            product_id: "prod-5".to_string(),
        });
        let display_str = format!("{}", catalog_evt);
        assert!(display_str.contains("ProductRemoved"));
    }

    #[test]
    fn test_serialization() {
        let user_evt = UserHistoryEvent::UserEvent(UserEvent::UserCreated {
            user_id: "user-7".to_string(),
            name: "Frank".to_string(),
        });

        // Serialize to JSON
        let json = serde_json::to_string(&user_evt).expect("Failed to serialize");
        assert!(json.contains("UserCreated"));
        assert!(json.contains("user-7"));
        assert!(json.contains("Frank"));

        let catalog_evt = UserHistoryEvent::CatalogEvent(CatalogEvent::ProductAdded {
            product_id: "prod-6".to_string(),
            name: "Keyboard".to_string(),
        });

        let json = serde_json::to_string(&catalog_evt).expect("Failed to serialize");
        assert!(json.contains("ProductAdded"));
        assert!(json.contains("prod-6"));
        assert!(json.contains("Keyboard"));
    }

    #[test]
    fn test_deserialization() {
        // Test deserializing UserEvent
        let user_json = r#"{"UserCreated":{"user_id":"user-8","name":"Grace"}}"#;
        let deserialized: UserHistoryEvent =
            serde_json::from_str(user_json).expect("Failed to deserialize UserEvent");

        assert!(matches!(deserialized, UserHistoryEvent::UserEvent(_)));
        assert_eq!(deserialized.event_type(), "UserCreated");

        // Test deserializing CatalogEvent
        let catalog_json = r#"{"ProductAdded":{"product_id":"prod-7","name":"Mouse"}}"#;
        let deserialized: UserHistoryEvent =
            serde_json::from_str(catalog_json).expect("Failed to deserialize CatalogEvent");

        assert!(matches!(deserialized, UserHistoryEvent::CatalogEvent(_)));
        assert_eq!(deserialized.event_type(), "ProductAdded");
    }

    #[test]
    fn test_round_trip_serialization() {
        let original = UserHistoryEvent::UserEvent(UserEvent::UserUpdated {
            user_id: "user-9".to_string(),
            name: "Hannah".to_string(),
        });

        // Serialize
        let json = serde_json::to_string(&original).expect("Failed to serialize");

        // Deserialize
        let deserialized: UserHistoryEvent =
            serde_json::from_str(&json).expect("Failed to deserialize");

        // Compare
        assert_eq!(original, deserialized);
    }

    #[test]
    fn test_merge_events_in_collection() {
        // Create a collection with mixed event types
        let events: Vec<UserHistoryEvent> = vec![
            UserEvent::UserCreated {
                user_id: "user-10".to_string(),
                name: "Ivan".to_string(),
            }
            .into(),
            CatalogEvent::ProductAdded {
                product_id: "prod-8".to_string(),
                name: "Headphones".to_string(),
            }
            .into(),
            UserEvent::UserUpdated {
                user_id: "user-10".to_string(),
                name: "Ivan Smith".to_string(),
            }
            .into(),
            CatalogEvent::ProductUpdated {
                product_id: "prod-8".to_string(),
                name: "Wireless Headphones".to_string(),
            }
            .into(),
        ];

        // Verify we can iterate and access all events
        assert_eq!(events.len(), 4);

        // Filter user events
        let user_events: Vec<_> = events
            .iter()
            .filter(|e| matches!(e, UserHistoryEvent::UserEvent(_)))
            .collect();
        assert_eq!(user_events.len(), 2);

        // Filter catalog events
        let catalog_events: Vec<_> = events
            .iter()
            .filter(|e| matches!(e, UserHistoryEvent::CatalogEvent(_)))
            .collect();
        assert_eq!(catalog_events.len(), 2);

        // Verify event types
        assert_eq!(events[0].event_type(), "UserCreated");
        assert_eq!(events[1].event_type(), "ProductAdded");
        assert_eq!(events[2].event_type(), "UserUpdated");
        assert_eq!(events[3].event_type(), "ProductUpdated");
    }

    #[test]
    fn test_clone_and_debug() {
        let evt = UserHistoryEvent::UserEvent(UserEvent::UserCreated {
            user_id: "user-11".to_string(),
            name: "Julia".to_string(),
        });

        // Test Clone
        let cloned = evt.clone();
        assert_eq!(evt, cloned);

        // Test Debug
        let debug_str = format!("{:?}", evt);
        assert!(debug_str.contains("UserEvent"));
        assert!(debug_str.contains("UserCreated"));
    }
}
