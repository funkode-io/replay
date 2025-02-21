mod error;
mod filters;
mod infrastructure;
mod persisted_event;
mod store;

pub use error::EventStoreError;
pub use filters::StreamFilter;
pub use persisted_event::PersistedEvent;
pub use store::{EventStore, LocalEventStore};

pub use infrastructure::{InMemoryEventStore, PostgresEventStore};
