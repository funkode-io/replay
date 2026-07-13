use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};

use replay::{Aggregate, Event};
use urn::Urn;

use super::{AggregateVersion, EventStore, PersistedEvent};

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

    /// Shared handle to the underlying event store.
    pub(crate) fn store(&self) -> &Arc<ES> {
        &self.store
    }

    /// Reconstruct an aggregate from its persisted event stream.
    ///
    /// - `aggregate_version`: choose which snapshot of the stream to load.  Use
    ///   [`AggregateVersion::Latest`] (the default) to replay the current event stream; use
    ///   [`AggregateVersion::Version(n)`] to inspect a specific archived compaction version.
    /// - `at_stream_version`: optional inclusive upper bound on the event sequence number
    ///   (events with version ≤ n are included).  Use this for time-travel reads.
    /// - `at_timestamp`: optional inclusive upper bound on the event creation timestamp
    ///   (events created at or before the given instant are included).
    pub async fn fetch_aggregate_at<A: Aggregate + Sync>(
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

    /// Reconstruct an aggregate at its latest state.
    ///
    /// This is a convenience wrapper around [`Self::fetch_aggregate_at`] that
    /// always replays the current (latest) event stream without any stream-version
    /// or timestamp upper bound.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let account = cqrs.fetch_aggregate::<BankAccountAggregate>(&account_id).await?;
    /// println!("balance: {}", account.balance);
    /// ```
    pub async fn fetch_aggregate<A: Aggregate + Sync>(
        &self,
        id: &A::StreamId,
    ) -> Result<A, A::Error> {
        self.fetch_aggregate_at(id, AggregateVersion::Latest, None, None)
            .await
    }

    pub async fn execute<A: Aggregate>(
        &self,
        id: &A::StreamId,
        metadata: replay::Metadata,
        command: A::Command,
        services: &A::Services,
        expected_version: Option<i64>,
    ) -> Result<A, A::Error>
    where
        A::Event: 'static,
        A::Error: 'static,
    {
        // Always load the latest (current) event stream for command handling.
        let mut aggregate = self
            .fetch_aggregate_at::<A>(id, AggregateVersion::Latest, expected_version, None)
            .await?;

        let stream_type = A::stream_type();

        // Stream-first: the producer yields events lazily and owns its data — it does not
        // borrow the aggregate — so once it is built the borrow on `&aggregate` is released
        // and we can fold each persisted event back into the same aggregate as it streams
        // into the store (apply-as-you-stream). Events are never buffered in a `Vec`.
        //
        // A producer error is fatal and rolls back the whole append; it is surfaced to the
        // store as a `replay::Error` so the streaming contract (`Error = replay::Error`) holds.
        let event_stream = aggregate
            .handle_stream(command, services)
            .await?
            .map_err(|e| replay::Error::internal("aggregate event producer failed").with_source(e));

        self.store
            .store_events_stream::<A, _, _>(
                id,
                stream_type,
                metadata,
                event_stream,
                expected_version,
                |event: &PersistedEvent<A::Event>| aggregate.apply(event.data.clone()),
            )
            .await
            .map_err(A::Error::from)?;

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
    ) -> Result<i32, replay::Error>
    where
        A: replay::Aggregate + replay::Compactable + Sync,
    {
        self.store.compact(aggregate, metadata).await
    }

    /// Whether `id`'s stream has changed since it was last compacted.
    ///
    /// Cheap pre-check for a blanket compaction job: skip [`compact`](Self::compact)
    /// when this returns `false`, avoiding the transaction, lock, and fold for an
    /// unchanged stream. See [`EventStore::needs_compaction`](crate::EventStore::needs_compaction).
    pub async fn needs_compaction<A>(&self, id: &A::StreamId) -> Result<bool, replay::Error>
    where
        A: replay::Aggregate,
    {
        let stream_id: Urn = id.clone().into();
        self.store.needs_compaction(&stream_id).await
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
