mod aggregate_version;
mod cqrs;
mod error;
mod filters;
mod infrastructure;
mod inline_projection;
mod persisted_event;
mod query;
mod store;

pub use aggregate_version::AggregateVersion;
pub use cqrs::Cqrs;
pub use error::{concurrency_error, db_error, deser_error, ser_error};
pub use filters::StreamFilter;
pub use infrastructure::{InMemoryEventStore, PostgresEventStore};
pub use inline_projection::InlineProjection;
pub use persisted_event::PersistedEvent;
pub use query::Query;
pub use store::EventStore;

/// Convenience re-exports of the most commonly used types and traits across
/// `replay`, `replay_macros`, and `replay_persistence`.
///
/// A single glob import brings everything into scope:
///
/// ```rust,ignore
/// use replay_persistence::prelude::*;
/// ```
pub mod prelude {
    // Core traits from es-replay
    pub use replay::{
        Aggregate, Compactable, Error, Event, EventStream, Result, ScopedUrn, WithId,
    };

    // Macros from es-replay-macros
    pub use replay_macros::{define_aggregate, query_events, Event as EventDerive, Urn};

    // Persistence types from this crate
    pub use super::{
        AggregateVersion, Cqrs, EventStore, InMemoryEventStore, InlineProjection, PersistedEvent,
        PostgresEventStore, Query, StreamFilter,
    };
}
