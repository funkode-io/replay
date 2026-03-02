use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};

use replay::{Aggregate, Event};

use super::{AggregateVersion, EventStore};

#[derive(Clone)]
pub struct Cqrs<ES: EventStore> {
    store: Arc<ES>,
}

impl<ES: EventStore> Cqrs<ES> {
    pub fn new(event_store: ES) -> Self {
        Self {
            store: Arc::new(event_store),
        }
    }

    /// Reconstruct an aggregate from its persisted event stream.
    ///
    /// - `aggregate_version`: choose which snapshot of the stream to load.  Use
    ///   [`AggregateVersion::Latest`] (the default) to replay the current event stream; use
    ///   [`AggregateVersion::Version(n)`] to inspect a specific archived compaction version.
    /// - `at_stream_version`: optional upper bound on the event sequence number.
    /// - `at_timestamp`: optional upper bound on the event creation timestamp.
    pub async fn fetch_aggregate<A: Aggregate + Sync>(
        &self,
        id: &A::StreamId,
        aggregate_version: AggregateVersion,
        at_stream_version: Option<i64>,
        at_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<A, A::Error> {
        let events = self
            .store
            .stream_events_by_stream_id::<A>(id, aggregate_version, at_stream_version, at_timestamp)
            .map_err(A::Error::from);

        let mut stream = A::with_id(id.clone());

        futures::pin_mut!(events);

        while let Some(event) = events.try_next().await? {
            stream.apply(event.data);
        }

        Ok(stream)
    }

    pub async fn execute<A: Aggregate>(
        &self,
        id: &A::StreamId,
        metadata: replay::Metadata,
        command: A::Command,
        services: &A::Services,
        expected_version: Option<i64>,
    ) -> Result<A, A::Error> {
        // Always load the latest (current) event stream for command handling.
        let mut aggregate = self
            .fetch_aggregate::<A>(id, AggregateVersion::Latest, expected_version, None)
            .await?;

        let stream_type = A::stream_type();

        let events = aggregate.handle(command, services).await?;

        self.store
            .store_events::<A>(id, stream_type, metadata, &events, expected_version)
            .await
            .map_err(A::Error::from)?;

        aggregate.apply_all(events);

        Ok(aggregate)
    }

    /// Compact the event stream for an aggregate.
    ///
    /// Archives the current full history under a new version number, then replaces
    /// the live stream with the minimal set of events returned by
    /// [`Compactable::compacted_events`].  Returns the archive version number that
    /// was created (starting at `1` for the first compaction).
    ///
    /// See [`EventStore::compact`] for the full description of the algorithm.
    pub async fn compact<A>(
        &self,
        aggregate: &A,
        metadata: replay::Metadata,
    ) -> Result<u32, replay::Error>
    where
        A: replay::Aggregate + replay::Compactable + Sync,
    {
        self.store.compact(aggregate, metadata).await
    }

    pub async fn run_query<'a, Q, E>(&'a self, query: &'a mut Q) -> Result<(), replay::Error>
    where
        E: Event + 'a,
        Q: crate::Query<Event = E>,
    {
        let events = self
            .store
            .stream_events::<E>(query.stream_filter())
            .into_stream();

        futures::pin_mut!(events);

        while let Some(Ok(event)) = events.next().await {
            query.update(event);
        }

        Ok(())
    }
}
