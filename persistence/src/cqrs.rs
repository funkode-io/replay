use std::sync::Arc;

use futures::{StreamExt, TryStream, TryStreamExt};

use replay::{Aggregate, Event};
use urn::Urn;

use super::{AggregateVersion, CompactionOutcome, EventStore, PersistedEvent};

/// Built-in default for [`Cqrs::compaction_chunk_size`].
const DEFAULT_COMPACTION_CHUNK_SIZE: u32 = 100;

#[derive(Clone)]
pub struct Cqrs<ES: EventStore> {
    store: Arc<ES>,
    /// How many stream ids [`Cqrs::streams_needing_compaction`] reads per store query.
    compaction_chunk_size: u32,
}

impl<ES: EventStore> Cqrs<ES> {
    pub fn new(event_store: ES) -> Self {
        Self {
            store: Arc::new(event_store),
            compaction_chunk_size: DEFAULT_COMPACTION_CHUNK_SIZE,
        }
    }

    /// Cap how many stream ids [`streams_needing_compaction`](Self::streams_needing_compaction)
    /// reads per store query (the `LIMIT` of each
    /// [`EventStore::streams_needing_compaction_page`](crate::EventStore::streams_needing_compaction_page)).
    ///
    /// It bounds the ids held at once and how long one query runs; the consumer still
    /// sees one stream and decides how much of it to read. Defaults to 100. Values below
    /// 1 are clamped to 1, so every query makes progress.
    pub fn compaction_chunk_size(mut self, ids: u32) -> Self {
        self.compaction_chunk_size = ids.max(1);
        self
    }

    /// Shared handle to the underlying event store.
    pub(crate) fn store(&self) -> &Arc<ES> {
        &self.store
    }

    /// Reconstruct an aggregate from its persisted event stream.
    ///
    /// - `aggregate_version`: choose which snapshot of the stream to load.  Use
    ///   [`AggregateVersion::Latest`] (the default) to replay the current event stream; use
    ///   [`AggregateVersion::Version`]`(n)` to inspect a specific archived compaction version.
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
            stream.apply(event.into_data());
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
    /// [`Compactable::compacted_events`](replay::Compactable::compacted_events).  Returns a [`CompactionOutcome`]:
    /// `Compacted { archive_version }` (starting at `1` for the first compaction), or
    /// `Skipped` when the aggregate reports the stream is already minimal
    /// ([`replay::Compaction::AlreadyCompacted`]) — in which case nothing is written but the
    /// compaction watermark is still advanced.
    ///
    /// See [`EventStore::compact`] for the full description of the algorithm.
    pub async fn compact<A>(
        &self,
        aggregate: &A,
        metadata: replay::Metadata,
    ) -> Result<CompactionOutcome, replay::Error>
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

    /// Every stream of aggregate `A` within `scope` that
    /// [`needs_compaction`](Self::needs_compaction) would answer `true` for, in stream-id
    /// order, starting after `after` (`None` for the beginning).
    ///
    /// The batch form of the pre-check, for a compaction pass over a whole scope — for
    /// example every stream of one tenant, `StreamScope::within(&tenant)`:
    ///
    /// ```rust,ignore
    /// let due = cqrs.streams_needing_compaction::<Account>(StreamScope::within(&tenant), None);
    /// futures::pin_mut!(due);
    /// while let Some(id) = due.try_next().await? {
    ///     let account = cqrs.fetch_aggregate::<Account>(&id).await?;
    ///     cqrs.compact(&account, metadata.clone()).await?;
    /// }
    /// ```
    ///
    /// Same watermark as `needs_compaction`, so a stream whose compaction was `Skipped`
    /// drops out of the next pass until a new event is appended. The stream reads in
    /// short keyset chunks of [`compaction_chunk_size`](Self::compaction_chunk_size) ids
    /// rather than through one long query, so a pass that holds it for minutes holds no connection or snapshot between chunks
    /// (`docs/adr/0029-the-public-api-is-stream-first.md`); a caller that cannot hold it
    /// across requests resumes with `after` set to the last id it saw, and takes a page
    /// with `.take(n)`. `scope` selects by stream id only; see [`crate::StreamScope`].
    pub fn streams_needing_compaction<A>(
        &self,
        scope: crate::StreamScope,
        after: Option<A::StreamId>,
    ) -> impl TryStream<Ok = A::StreamId, Error = replay::Error> + Send
    where
        A: replay::Aggregate,
    {
        struct Pass<ES> {
            store: Arc<ES>,
            stream_type: String,
            scope: crate::StreamScope,
            cursor: Option<Urn>,
            // Bounded by `chunk_size`: one store page at a time, never the whole scope.
            chunk: std::vec::IntoIter<Urn>,
            last_chunk: bool,
            chunk_size: u32,
        }

        let pass = Pass {
            store: self.store.clone(),
            stream_type: A::stream_type(),
            scope,
            cursor: after.map(Into::into),
            chunk: Vec::new().into_iter(),
            last_chunk: false,
            chunk_size: self.compaction_chunk_size,
        };

        futures::stream::try_unfold(pass, |mut pass| async move {
            loop {
                if let Some(urn) = pass.chunk.next() {
                    let id = A::StreamId::try_from(urn.clone()).map_err(|e| {
                        replay::Error::internal(
                            "stored stream id does not parse as the aggregate's StreamId",
                        )
                        .with_operation("streams_needing_compaction")
                        .with_context("stream_id", urn.to_string())
                        .with_context("error", format!("{:?}", e))
                    })?;
                    return Ok(Some((id, pass)));
                }
                if pass.last_chunk {
                    return Ok(None);
                }
                let chunk = pass
                    .store
                    .streams_needing_compaction_page(
                        &pass.stream_type,
                        &pass.scope,
                        pass.cursor.as_ref(),
                        pass.chunk_size,
                    )
                    .await?;
                pass.last_chunk = chunk.len() < pass.chunk_size as usize;
                if let Some(last) = chunk.last() {
                    pass.cursor = Some(last.clone());
                }
                pass.chunk = chunk.into_iter();
            }
        })
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
