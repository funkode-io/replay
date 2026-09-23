use chrono::{DateTime, Utc};
use urn::Urn;

use crate::{Event, Metadata};

/// An event as a rule sees it: the payload, the stream it happened on, the metadata it
/// was appended with, and when.
///
/// This is the input to [`Policy::react`](crate::Policy::react) and the whole of what a
/// rule may read. Storage identity and position — an event's id, type, version and
/// aggregate version — are the store's, and stay on the persisted envelope that embeds
/// this one.
///
/// Dropping the event id from a rule's view is deliberate: a policy cannot mint a
/// causation key into the command it emits, so at-least-once delivery is absorbed by
/// idempotent command shape rather than identity dedup (ADR-0027).
#[derive(Debug, Clone, PartialEq)]
pub struct ObservedEvent<E> {
    pub data: E,
    pub stream_id: Urn,
    pub metadata: Metadata,
    pub created: DateTime<Utc>,
}

impl<E: Event> ObservedEvent<E> {
    /// An event that was never read from a store: a fixture, assembled from the two
    /// fields a rule is usually about.
    ///
    /// `created` is the epoch and `metadata` empty, so neither carries meaning; a test
    /// that is about either states it with [`with_created`](Self::with_created) or
    /// [`with_metadata`](Self::with_metadata). The epoch rather than the current time
    /// keeps this crate off the clock, which is what lets it build for `wasm32`.
    pub fn of(stream_id: impl Into<Urn>, data: E) -> Self {
        ObservedEvent {
            data,
            stream_id: stream_id.into(),
            metadata: Metadata::default(),
            created: DateTime::UNIX_EPOCH,
        }
    }
}

impl<E> ObservedEvent<E> {
    pub fn with_created(self, created: DateTime<Utc>) -> Self {
        ObservedEvent { created, ..self }
    }

    pub fn with_metadata(self, metadata: impl Into<Metadata>) -> Self {
        ObservedEvent {
            metadata: metadata.into(),
            ..self
        }
    }
}
