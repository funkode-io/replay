use std::future::Future;

use futures::stream;
use futures::TryStream;

use replay::{Compactable, Event};
use urn::Urn;

use super::{AggregateVersion, PersistedEvent};

pub trait EventSink<E: Event>: Send {
    fn on_event(&mut self, event: &PersistedEvent<E>);
}

impl<E, F> EventSink<E> for F
where
    E: Event,
    F: FnMut(&PersistedEvent<E>) + Send,
{
    fn on_event(&mut self, event: &PersistedEvent<E>) {
        self(event);
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct NoSink;

impl<E: Event> EventSink<E> for NoSink {
    fn on_event(&mut self, _event: &PersistedEvent<E>) {}
}

pub trait EventStore: Send + Sync {
    fn store_events_stream<S, ES, Sink>(
        &self,
        stream_id: &S::StreamId,
        stream_type: String,
        metadata: replay::Metadata,
        domain_events: ES,
        expected_version: Option<i64>,
        sink: Sink,
    ) -> impl Future<Output = Result<(), replay::Error>> + Send
    where
        S: replay::EventStream,
        ES: TryStream<Ok = S::Event, Error = replay::Error> + Send,
        Sink: EventSink<S::Event> + Send;

    fn store_events<S: replay::EventStream>(
        &self,
        stream_id: &S::StreamId,
        stream_type: String,
        metadata: replay::Metadata,
        domain_events: &[S::Event],
        expected_version: Option<i64>,
    ) -> impl Future<Output = Result<(), replay::Error>> + Send {
        let domain_events = stream::iter(domain_events.iter().cloned().map(Ok::<_, replay::Error>));
        self.store_events_stream::<S, _, _>(
            stream_id,
            stream_type,
            metadata,
            domain_events,
            expected_version,
            NoSink,
        )
    }

    fn stream_events<E: Event>(
        &self,
        filter: crate::StreamFilter,
    ) -> impl TryStream<Ok = PersistedEvent<E>, Error = replay::Error> + Send;

    /// Stream the events for a specific aggregate stream, optionally scoped to a particular
    /// compaction version.
    ///
    /// - `aggregate_version`: `Latest` loads the current (non-archived) events; `Version(n)`
    ///   loads the events that were archived during the nth compaction run.
    /// - `at_stream_version`: inclusive upper bound on the event sequence number
    ///   (events with version ≤ n are included; use for time-travel reads).
    /// - `at_timestamp`: inclusive upper bound on the event creation timestamp
    ///   (events created at or before the given instant are included).
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
    ///
    /// Whether the given stream has changed since it was last compacted.
    ///
    /// Returns `true` when at least one event has been appended past the stream's
    /// compaction watermark (`streams.last_compacted_version`) — including a stream
    /// that has never been compacted but has events. A blanket maintenance job uses
    /// this as a cheap pre-check to skip [`compact`](EventStore::compact) entirely
    /// for unchanged streams, avoiding the transaction, lock, and fold:
    ///
    /// ```rust,ignore
    /// if store.needs_compaction(&stream_id).await? {
    ///     store.compact(&aggregate, metadata).await?;
    /// }
    /// ```
    ///
    /// The check takes **no lock**: the race against a concurrent append is benign
    /// because compaction is best-effort idempotent maintenance — an append landing
    /// just after a `false` read is simply picked up on the next cycle. A stream that
    /// does not exist reports `false` (nothing to compact).
    fn needs_compaction(
        &self,
        stream_id: &Urn,
    ) -> impl Future<Output = Result<bool, replay::Error>> + Send;
}
