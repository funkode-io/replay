use std::future::Future;

use futures::TryStream;

use replay::{Compactable, Event};

use super::{AggregateVersion, PersistedEvent};

pub trait EventStore: Send + Sync {
    fn store_events<S: replay::EventStream>(
        &self,
        stream_id: &S::StreamId,
        stream_type: String,
        metadata: replay::Metadata,
        domain_events: &[S::Event],
        expected_version: Option<i64>,
    ) -> impl Future<Output = Result<(), replay::Error>> + Send;

    fn stream_events<E: Event>(
        &self,
        filter: crate::StreamFilter,
    ) -> impl TryStream<Ok = PersistedEvent<E>, Error = replay::Error> + Send;

    /// Stream the events for a specific aggregate stream, optionally scoped to a particular
    /// compaction version.
    ///
    /// - `aggregate_version`: `Latest` loads the current (non-archived) events; `Version(n)`
    ///   loads the events that were archived during the nth compaction run.
    /// - `at_stream_version`: upper bound on the event sequence number (for time-travel queries).
    /// - `at_timestamp`: upper bound on the event creation timestamp.
    fn stream_events_by_stream_id<S: replay::EventStream>(
        &self,
        stream_id: &S::StreamId,
        aggregate_version: AggregateVersion,
        at_stream_version: Option<i64>,
        at_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> impl TryStream<Ok = PersistedEvent<S::Event>, Error = replay::Error> + Send {
        let filter = crate::StreamFilter::WithStreamId(stream_id.clone().into())
            .and_aggregate_version(aggregate_version.as_option())
            .and_at_stream_version_optional(at_stream_version)
            .and_at_timestamp_optional(at_timestamp);

        self.stream_events::<S::Event>(filter)
    }

    /// Compact the event stream for an aggregate.
    ///
    /// The method performs the following steps atomically (where the store supports it):
    ///
    /// 1. Determines the next archive version number (max existing archive version + 1, starting
    ///    at 1 for the first compaction).
    /// 2. Copies all current events (those with no aggregate version tag) for the given aggregate
    ///    into the new archive version.
    /// 3. Removes all current (un-versioned) events for the aggregate from the live stream.
    /// 4. Calls [`Compactable::compacted_events`] on the aggregate, passing the current live
    ///    events, to obtain the minimal event set.
    /// 5. Persists the compacted events as the new current event stream (no version tag).
    ///
    /// Returns the archive version number that was created.
    fn compact<A>(
        &self,
        aggregate: &A,
        metadata: replay::Metadata,
    ) -> impl Future<Output = Result<i32, replay::Error>> + Send
    where
        A: replay::Aggregate + Compactable + Sync;
}
