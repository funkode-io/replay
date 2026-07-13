use std::{collections::HashMap, sync::RwLock};

use chrono::Utc;
use futures::{TryStream, TryStreamExt};
use serde_json::Value;
use tokio::sync::Mutex;
use urn::Urn;
use uuid::Uuid;

use crate::inline_projection::ErasedInlineProjection;
use crate::{
    CompactionOutcome, EventSink, EventStore, InlineProjection, PersistedEvent, StreamFilter,
};
use replay::{Compactable, Event};

/// In-memory event store implementation, only for testing purpose.
///
/// Events are stored per-stream-URN in insertion order. The `aggregate_version` field on each
/// event distinguishes current events (`None`) from archived compaction snapshots (`Some(n)`).
///
/// Stream filters are normally *pushed down* to the database; the in-memory store has no
/// schema to push down to, so it evaluates every filter as a per-event predicate instead. To
/// support [`StreamFilter::ForStreamTypes`] it records each stream's type on append (the type
/// is stable per stream URN) and matches against it while scanning.
///
/// The store can also run **best-effort inline projections** (test-only): after each
/// successful append it routes the newly-appended events to every registered projection's
/// `handle` by deserialize-or-skip, exactly like the Postgres store. Unlike Postgres, this
/// path makes NO atomicity guarantee — the events are already stored when `handle` runs, and
/// a failing `handle` does not roll them back. It exists purely to exercise projection
/// routing and batch-handling logic in fast unit tests without a database.
pub struct InMemoryEventStore {
    events: RwLock<HashMap<Urn, Vec<PersistedEvent<Value>>>>,
    /// Stream type per stream URN, recorded on append so [`StreamFilter::ForStreamTypes`] can
    /// be evaluated per-event without a database to push the filter down to.
    stream_types: RwLock<HashMap<Urn, String>>,
    /// Best-effort inline projections, applied after events are appended. Each projection is
    /// locked individually while it handles a batch. The in-memory store passes a unit (`()`)
    /// write handle, so projections must keep their view in their own state (e.g. shared via
    /// `Arc`).
    projections: Vec<Mutex<Box<dyn ErasedInlineProjection<Exec = ()>>>>,
    /// Compaction watermark per stream: the live head `version` recorded at the last
    /// compaction. `needs_compaction` compares the current live head against it so an
    /// unchanged stream is skipped. Absent means "never compacted" (eligible if it has
    /// events). Mirrors `streams.last_compacted_version` in the Postgres store.
    last_compacted_version: RwLock<HashMap<Urn, i64>>,
}

impl InMemoryEventStore {
    pub fn new() -> Self {
        Self {
            events: RwLock::new(HashMap::new()),
            stream_types: RwLock::new(HashMap::new()),
            projections: Vec::new(),
            last_compacted_version: RwLock::new(HashMap::new()),
        }
    }

    /// Register a best-effort inline projection (test-only).
    ///
    /// After each successful append, the store routes the newly-appended events to this
    /// projection's [`handle`](InlineProjection::handle) by deserialize-or-skip — events that
    /// don't deserialize into the projection's event type are skipped, and `handle` is called
    /// at most once per append with the matching batch (never with an empty batch).
    ///
    /// This is NOT atomic: the events are already stored before `handle` runs, and a failing
    /// `handle` does not roll them back (atomicity is a Postgres-only property). The store
    /// passes a unit (`()`) write handle, so a projection keeps its view in its own state —
    /// typically shared with the test via `Arc` for assertions. `init`/`reset`/`version` are
    /// not invoked by the in-memory store.
    pub fn register_projection<P>(mut self, projection: P) -> Self
    where
        P: InlineProjection<Exec = ()> + 'static,
    {
        self.projections.push(Mutex::new(Box::new(projection)));
        self
    }

    /// Drive every registered projection over the just-appended events, best-effort.
    ///
    /// Each projection routes the batch by deserialize-or-skip and runs at most once with the
    /// events that belong to it. Errors propagate to the caller, but the events have already
    /// been stored — the in-memory store does not roll them back.
    async fn apply_projections(
        &self,
        events: &[PersistedEvent<Value>],
    ) -> Result<(), replay::Error> {
        if events.is_empty() {
            return Ok(());
        }

        let mut exec = ();
        for projection in self.projections.iter() {
            let mut projection = projection.lock().await;
            projection.handle(&mut exec, events).await?;
        }

        Ok(())
    }

    /// Walk a filter tree and return the first `WithStreamId` URN found (used for fast lookup).
    fn extract_stream_id(filter: &StreamFilter) -> Option<Urn> {
        match filter {
            StreamFilter::WithStreamId(id) => Some(id.clone()),
            StreamFilter::And(left, right) => {
                Self::extract_stream_id(left).or_else(|| Self::extract_stream_id(right))
            }
            _ => None,
        }
    }

    /// Apply a filter to a raw (un-typed) persisted event.
    ///
    /// Every [`StreamFilter`] variant is evaluated as a per-event predicate. `stream_type` is
    /// the type of the stream the event belongs to (resolved from the store's side map); it is
    /// only needed by [`StreamFilter::ForStreamTypes`].
    fn evaluate<E>(
        filter: &StreamFilter,
        event: &PersistedEvent<E>,
        stream_type: Option<&str>,
    ) -> bool {
        match filter {
            StreamFilter::All => true,
            StreamFilter::WithStreamId(stream_id) => event.stream_id == *stream_id,
            StreamFilter::ForStreamTypes(stream_types) => {
                stream_type.is_some_and(|st| stream_types.iter().any(|t| t == st))
            }
            StreamFilter::WithMetadata(metadata) => event.metadata == *metadata,
            StreamFilter::AfterVersion(version) => event.version > *version,
            StreamFilter::UpToVersion(version) => event.version <= *version,
            StreamFilter::CreatedAfter(timestamp) => event.created > *timestamp,
            StreamFilter::CreatedBefore(timestamp) => event.created <= *timestamp,
            StreamFilter::WithAggregateVersion(v) => event.aggregate_version == *v,
            StreamFilter::And(left, right) => {
                Self::evaluate(left, event, stream_type)
                    && Self::evaluate(right, event, stream_type)
            }
            StreamFilter::Or(left, right) => {
                Self::evaluate(left, event, stream_type)
                    || Self::evaluate(right, event, stream_type)
            }
            StreamFilter::Not(inner) => !Self::evaluate(inner, event, stream_type),
        }
    }
}

impl Default for InMemoryEventStore {
    fn default() -> Self {
        Self::new()
    }
}

impl EventStore for InMemoryEventStore {
    async fn store_events_stream<S, ES, Sink>(
        &self,
        stream_id: &S::StreamId,
        stream_type: String,
        metadata: replay::Metadata,
        domain_events: ES,
        expected_version: Option<i64>,
        mut sink: Sink,
    ) -> Result<(), replay::Error>
    where
        S: replay::EventStream,
        ES: TryStream<Ok = S::Event, Error = replay::Error> + Send,
        Sink: EventSink<S::Event> + Send,
    {
        let stream_id: Urn = stream_id.clone().into();

        // Record the stream's type so `ForStreamTypes` filters can be evaluated per-event.
        self.stream_types
            .write()
            .unwrap()
            .insert(stream_id.clone(), stream_type);

        // Determine the starting version and check optimistic concurrency once, up front
        // (mirrors the Postgres "check expected_version at the head of the transaction").
        let mut last_version = {
            let store = self.events.read().unwrap();
            store
                .get(&stream_id)
                .map(|events| {
                    events
                        .iter()
                        .rfind(|e| e.aggregate_version.is_none())
                        .map(|e| e.version)
                        .unwrap_or(0)
                })
                .unwrap_or(0)
        };

        if let Some(expected_version) = expected_version {
            if last_version != expected_version {
                return Err(crate::concurrency_error(
                    stream_id.clone(),
                    expected_version,
                    last_version,
                ));
            }
        }

        // Pull events from the producer one at a time and notify the sink as each is appended,
        // exactly like the Postgres backend. The `std` `RwLock` guard cannot be held across the
        // producer `await`, so events are staged into a local buffer and only published to the
        // shared store once the whole producer has drained. A producer error therefore discards
        // the batch (all-or-nothing, matching the Postgres transaction rollback).
        let mut domain_events = std::pin::pin!(domain_events.into_stream());
        let mut staged: Vec<PersistedEvent<Value>> = Vec::new();

        while let Some(event) = domain_events.try_next().await? {
            let id = Uuid::new_v4();
            let created = Utc::now();
            let r#type = event.event_type();
            let version = last_version + 1;
            last_version = version;

            let data = serde_json::to_value(&event).map_err(crate::ser_error)?;

            // Notify the sink with the typed event as it is appended, instead of accumulating a
            // parallel `Vec<PersistedEvent<S::Event>>` to replay afterwards. The JSON-encoded
            // copy staged below is what the store retains for querying and inline projections.
            sink.on_event(&PersistedEvent {
                id,
                data: event,
                stream_id: stream_id.clone(),
                r#type: r#type.clone(),
                version,
                created,
                metadata: metadata.clone(),
                aggregate_version: None,
            });

            staged.push(PersistedEvent {
                id,
                data,
                stream_id: stream_id.clone(),
                r#type,
                version,
                created,
                metadata: metadata.clone(),
                aggregate_version: None,
            });
        }

        // Publish the staged events atomically under the write lock, then release it before
        // driving any async projections (the `RwLockWriteGuard` is not held across an await).
        {
            let mut store = self.events.write().unwrap();
            let stream = store.entry(stream_id.clone()).or_default();
            stream.extend(staged.iter().cloned());
        }

        // Best-effort: drive registered projections after the events are stored and the write
        // lock is released. No atomicity — a failing projection does not roll back the append.
        if !self.projections.is_empty() {
            self.apply_projections(&staged).await?;
        }

        Ok(())
    }

    fn stream_events<E: Event>(
        &self,
        filter: StreamFilter,
    ) -> impl TryStream<Ok = PersistedEvent<E>, Error = replay::Error> + Send {
        // Optimise: if the filter references a specific stream URN, only scan that stream.
        let (candidate_events, stream_types): (Vec<PersistedEvent<Value>>, HashMap<Urn, String>) = {
            let store = self.events.read().unwrap();
            let stream_types = self.stream_types.read().unwrap().clone();
            let events = if let Some(stream_id) = Self::extract_stream_id(&filter) {
                store.get(&stream_id).cloned().unwrap_or_default()
            } else {
                store.values().flatten().cloned().collect()
            };
            (events, stream_types)
        };

        async_stream::stream! {
            for event in candidate_events {
                let stream_type = stream_types.get(&event.stream_id).map(String::as_str);
                if !Self::evaluate(&filter, &event, stream_type) {
                    continue;
                }
                let data: E = serde_json::from_value(event.data).map_err(crate::deser_error)?;
                yield Ok(PersistedEvent {
                    id: event.id,
                    data,
                    stream_id: event.stream_id,
                    r#type: event.r#type,
                    version: event.version,
                    created: event.created,
                    metadata: event.metadata,
                    aggregate_version: event.aggregate_version,
                });
            }
        }
    }

    async fn compact<A>(
        &self,
        aggregate: &A,
        metadata: replay::Metadata,
    ) -> Result<CompactionOutcome, replay::Error>
    where
        A: replay::Aggregate + Compactable + Sync,
    {
        let stream_id: Urn = aggregate.get_id().clone().into();

        // 1. Collect current live events while holding the read lock (brief, sync).
        //    Wrap them in a TryStream so that compacted_events can process them
        //    without assuming an in-memory slice is available.
        let current_events: Vec<A::Event> = {
            let store = self.events.read().unwrap();
            match store.get(&stream_id) {
                None => Vec::new(),
                Some(stream) => stream
                    .iter()
                    .filter(|e| e.aggregate_version.is_none())
                    .map(|e| {
                        serde_json::from_value::<A::Event>(e.data.clone())
                            .map_err(crate::deser_error)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            }
        };

        let event_stream =
            futures::stream::iter(current_events.into_iter().map(Ok::<_, replay::Error>));
        let compacted = match aggregate.compacted_events(event_stream).await? {
            replay::Compaction::Rewrite(events) => events,
            replay::Compaction::AlreadyCompacted => {
                // The author reports the live stream is already minimal: write nothing, but
                // settle the watermark at the current head so `needs_compaction` skips this
                // stream until a new event is appended.
                let head = {
                    let store = self.events.read().unwrap();
                    let stream = store.get(&stream_id).ok_or_else(|| {
                        replay::Error::not_found("Stream not found")
                            .with_operation("compact")
                            .with_context("stream_id", stream_id.to_string())
                    })?;
                    stream
                        .iter()
                        .filter(|e| e.aggregate_version.is_none())
                        .map(|e| e.version)
                        .max()
                        .unwrap_or(0)
                };
                self.last_compacted_version
                    .write()
                    .unwrap()
                    .insert(stream_id.clone(), head);
                return Ok(CompactionOutcome::Skipped);
            }
        };

        // 2. Determine the next archive version number and archive all current events.
        {
            let mut store = self.events.write().unwrap();

            if !store.contains_key(&stream_id) {
                return Err(replay::Error::not_found("Stream not found")
                    .with_operation("compact")
                    .with_context("stream_id", stream_id.to_string()));
            }

            let stream = store.entry(stream_id.clone()).or_default();

            let next_version: i32 = stream
                .iter()
                .filter_map(|e| e.aggregate_version)
                .max()
                .unwrap_or(0)
                + 1;

            // Archive: mark every current (aggregate_version = None) event with the new version.
            for event in stream.iter_mut() {
                if event.aggregate_version.is_none() {
                    event.aggregate_version = Some(next_version);
                }
            }

            // Insert compacted events as the new current stream (aggregate_version = None).
            // Sequence versions restart from 1.
            for (seq, event) in (0_i64..).zip(compacted.iter()) {
                let seq = seq + 1;
                let data = serde_json::to_value(event).map_err(crate::ser_error)?;
                stream.push(PersistedEvent {
                    id: Uuid::new_v4(),
                    data,
                    stream_id: stream_id.clone(),
                    r#type: event.event_type(),
                    version: seq,
                    created: Utc::now(),
                    metadata: metadata.clone(),
                    aggregate_version: None,
                });
            }

            // Advance the compaction watermark to the new live head version so
            // `needs_compaction` skips this stream until a new event is appended.
            self.last_compacted_version
                .write()
                .unwrap()
                .insert(stream_id.clone(), compacted.len() as i64);

            Ok(CompactionOutcome::Compacted {
                archive_version: next_version,
            })
        }
    }

    async fn needs_compaction(&self, stream_id: &Urn) -> Result<bool, replay::Error> {
        // Current live head version (max version among un-archived events), 0 if none.
        let head = {
            let store = self.events.read().unwrap();
            match store.get(stream_id) {
                None => return Ok(false),
                Some(stream) => stream
                    .iter()
                    .filter(|e| e.aggregate_version.is_none())
                    .map(|e| e.version)
                    .max()
                    .unwrap_or(0),
            }
        };

        let watermark = self
            .last_compacted_version
            .read()
            .unwrap()
            .get(stream_id)
            .copied()
            .unwrap_or(0);

        Ok(head > watermark)
    }
}

// tests
#[cfg(test)]
mod tests {

    use super::*;
    use replay::{EventStream, WithId};

    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex as StdMutex};

    use futures::TryStreamExt;

    use replay_macros::Event;
    use serde::{Deserialize, Serialize};
    use urn::{Urn, UrnBuilder};

    //  bank account stream (id of stream is not part of the model)
    struct BankAccountStream {
        pub id: BankAccountUrn,
        pub balance: f64,
    }

    // create bank account events enum: Deposited and Withdrawn
    #[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Event)]
    enum BankAccountEvent {
        Deposited { amount: f64 },
        Withdrawn { amount: f64 },
    }

    // bank account urn
    #[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
    struct BankAccountUrn(Urn);

    impl From<BankAccountUrn> for Urn {
        fn from(urn: BankAccountUrn) -> Self {
            urn.0
        }
    }

    impl TryFrom<Urn> for BankAccountUrn {
        type Error = String;

        fn try_from(urn: Urn) -> Result<Self, Self::Error> {
            Ok(BankAccountUrn(urn))
        }
    }

    impl WithId for BankAccountStream {
        type StreamId = BankAccountUrn;

        fn with_id(id: Self::StreamId) -> Self {
            BankAccountStream {
                id: id.clone(),
                balance: 0.0,
            }
        }

        fn get_id(&self) -> &Self::StreamId {
            &self.id
        }
    }

    // bank account stream
    impl replay::EventStream for BankAccountStream {
        type Event = BankAccountEvent;

        fn stream_type() -> String {
            "BankAccount".to_string()
        }

        fn apply(&mut self, event: Self::Event) {
            match event {
                BankAccountEvent::Deposited { amount } => {
                    self.balance += amount;
                }
                BankAccountEvent::Withdrawn { amount } => {
                    self.balance -= amount;
                }
            }
        }
    }

    // ── Compactable support ──────────────────────────────────────────────────

    /// A compaction strategy that collapses the whole history into a single
    /// `Deposited` event whose amount equals the current balance.
    impl Compactable for BankAccountStream {
        async fn compacted_events(
            &self,
            events: impl futures::TryStream<Ok = Self::Event, Error = replay::Error> + Send,
        ) -> replay::Result<replay::Compaction<Self::Event>> {
            use futures::TryStreamExt;
            let all: Vec<BankAccountEvent> = events.try_collect().await?;
            let balance = all.iter().fold(0.0_f64, |acc, ev| match ev {
                BankAccountEvent::Deposited { amount } => acc + amount,
                BankAccountEvent::Withdrawn { amount } => acc - amount,
            });
            Ok(vec![BankAccountEvent::Deposited { amount: balance }].into())
        }
    }

    impl replay::Aggregate for BankAccountStream {
        type Command = ();
        type Error = replay::Error;
        type Services = ();

        async fn handle(
            &self,
            _command: Self::Command,
            _services: &Self::Services,
        ) -> Result<Vec<Self::Event>, Self::Error> {
            Ok(vec![])
        }
    }

    // ── SnapshotStream: an aggregate that exercises the Guard 2 outcomes ──────
    // `compacted_events` returns `AlreadyCompacted` for an already-minimal stream (a
    // lone `Snapshot`), an empty `Rewrite` when a `Reset` is present (compact to
    // nothing), and otherwise folds to a single `Snapshot`.
    #[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Event)]
    enum SnapshotEvent {
        Bumped,
        Reset,
        Snapshot { total: i64 },
    }

    struct SnapshotStream {
        id: BankAccountUrn,
        total: i64,
    }

    impl WithId for SnapshotStream {
        type StreamId = BankAccountUrn;

        fn with_id(id: Self::StreamId) -> Self {
            SnapshotStream { id, total: 0 }
        }

        fn get_id(&self) -> &Self::StreamId {
            &self.id
        }
    }

    impl replay::EventStream for SnapshotStream {
        type Event = SnapshotEvent;

        fn stream_type() -> String {
            "Snapshot".to_string()
        }

        fn apply(&mut self, event: Self::Event) {
            match event {
                SnapshotEvent::Bumped => self.total += 1,
                SnapshotEvent::Reset => self.total = 0,
                SnapshotEvent::Snapshot { total } => self.total = total,
            }
        }
    }

    impl replay::Aggregate for SnapshotStream {
        type Command = ();
        type Error = replay::Error;
        type Services = ();

        async fn handle(
            &self,
            _command: Self::Command,
            _services: &Self::Services,
        ) -> Result<Vec<Self::Event>, Self::Error> {
            Ok(vec![])
        }
    }

    impl Compactable for SnapshotStream {
        async fn compacted_events(
            &self,
            events: impl futures::TryStream<Ok = Self::Event, Error = replay::Error> + Send,
        ) -> replay::Result<replay::Compaction<Self::Event>> {
            let all: Vec<SnapshotEvent> = events.try_collect().await?;
            // Already minimal: a lone Snapshot and nothing else.
            if matches!(all.as_slice(), [SnapshotEvent::Snapshot { .. }]) {
                return Ok(replay::Compaction::AlreadyCompacted);
            }
            // A Reset collapses the stream to nothing (compact to empty).
            if all.iter().any(|e| matches!(e, SnapshotEvent::Reset)) {
                return Ok(replay::Compaction::Rewrite(vec![]));
            }
            let total = all.iter().fold(0_i64, |acc, e| match e {
                SnapshotEvent::Bumped => acc + 1,
                SnapshotEvent::Reset => 0,
                SnapshotEvent::Snapshot { total } => *total,
            });
            Ok(replay::Compaction::Rewrite(vec![SnapshotEvent::Snapshot {
                total,
            }]))
        }
    }

    async fn add_snapshot_events(
        store: &InMemoryEventStore,
        id: &BankAccountUrn,
        events: &[SnapshotEvent],
    ) {
        store
            .store_events::<SnapshotStream>(
                id,
                "Snapshot".to_string(),
                replay::Metadata::default(),
                events,
                None,
            )
            .await
            .unwrap();
    }

    async fn live_snapshot_events(
        store: &InMemoryEventStore,
        id: &BankAccountUrn,
    ) -> Vec<SnapshotEvent> {
        store
            .stream_events::<SnapshotEvent>(StreamFilter::And(
                Box::new(StreamFilter::with_stream_id::<SnapshotStream>(id)),
                Box::new(StreamFilter::WithAggregateVersion(None)),
            ))
            .map_ok(|e| e.data)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    fn make_stream_id(n: &str) -> BankAccountUrn {
        BankAccountUrn(UrnBuilder::new("bank-account", n).build().unwrap())
    }

    async fn add_events(
        store: &InMemoryEventStore,
        id: &BankAccountUrn,
        events: &[BankAccountEvent],
    ) {
        store
            .store_events::<BankAccountStream>(
                id,
                "BankAccount".to_string(),
                replay::Metadata::default(),
                events,
                None,
            )
            .await
            .unwrap();
    }

    async fn live_events(store: &InMemoryEventStore, id: &BankAccountUrn) -> Vec<BankAccountEvent> {
        use futures::TryStreamExt;
        store
            .stream_events::<BankAccountEvent>(StreamFilter::And(
                Box::new(StreamFilter::with_stream_id::<BankAccountStream>(id)),
                Box::new(StreamFilter::WithAggregateVersion(None)),
            ))
            .map_ok(|e| e.data)
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    }

    async fn archived_events(
        store: &InMemoryEventStore,
        id: &BankAccountUrn,
        version: i32,
    ) -> Vec<PersistedEvent<BankAccountEvent>> {
        use futures::TryStreamExt;
        store
            .stream_events::<BankAccountEvent>(StreamFilter::And(
                Box::new(StreamFilter::with_stream_id::<BankAccountStream>(id)),
                Box::new(StreamFilter::WithAggregateVersion(Some(version))),
            ))
            .try_collect::<Vec<_>>()
            .await
            .unwrap()
    }

    // ── compact tests ────────────────────────────────────────────────────────

    #[tokio::test]
    async fn test_compact_reduces_live_events() {
        let store = InMemoryEventStore::new();
        let id = make_stream_id("compact-1");

        add_events(
            &store,
            &id,
            &[
                BankAccountEvent::Deposited { amount: 100.0 },
                BankAccountEvent::Withdrawn { amount: 40.0 },
                BankAccountEvent::Deposited { amount: 50.0 },
            ],
        )
        .await;

        // Build the aggregate state so `compacted_events` has the right `self`.
        let mut account = BankAccountStream::with_id(id.clone());
        for ev in live_events(&store, &id).await {
            account.apply(ev);
        }
        assert_eq!(account.balance, 110.0);

        let archive_version = store
            .compact(&account, replay::Metadata::default())
            .await
            .unwrap();

        assert_eq!(
            archive_version,
            CompactionOutcome::Compacted { archive_version: 1 }
        );

        // Live stream is now a single synthetic Deposited event.
        let live = live_events(&store, &id).await;
        assert_eq!(live, vec![BankAccountEvent::Deposited { amount: 110.0 }]);

        // Original 3 events are archived under version 1.
        let archived = archived_events(&store, &id, 1).await;
        assert_eq!(archived.len(), 3);
        assert!(archived.iter().all(|e| e.aggregate_version == Some(1)));
    }

    #[tokio::test]
    async fn test_compact_increments_archive_version() {
        let store = InMemoryEventStore::new();
        let id = make_stream_id("compact-2");

        add_events(
            &store,
            &id,
            &[
                BankAccountEvent::Deposited { amount: 200.0 },
                BankAccountEvent::Withdrawn { amount: 50.0 },
            ],
        )
        .await;

        let mut account = BankAccountStream::with_id(id.clone());
        for ev in live_events(&store, &id).await {
            account.apply(ev);
        }

        // First compaction → archive version 1, live = [Deposited(150)].
        store
            .compact(&account, replay::Metadata::default())
            .await
            .unwrap();

        // Add more events after the first compaction.
        store
            .store_events::<BankAccountStream>(
                &id,
                "BankAccount".to_string(),
                replay::Metadata::default(),
                &[BankAccountEvent::Withdrawn { amount: 30.0 }],
                None,
            )
            .await
            .unwrap();

        // Rebuild aggregate for second compaction.
        let mut account2 = BankAccountStream::with_id(id.clone());
        for ev in live_events(&store, &id).await {
            account2.apply(ev);
        }
        assert_eq!(account2.balance, 120.0);

        let archive_version2 = store
            .compact(&account2, replay::Metadata::default())
            .await
            .unwrap();

        assert_eq!(
            archive_version2,
            CompactionOutcome::Compacted { archive_version: 2 }
        );

        let live = live_events(&store, &id).await;
        assert_eq!(live, vec![BankAccountEvent::Deposited { amount: 120.0 }]);

        // Version 1 archive still intact (2 original events).
        let arch1 = archived_events(&store, &id, 1).await;
        assert_eq!(arch1.len(), 2);

        // Version 2 archive contains the compacted event + the post-compaction withdrawal.
        let arch2 = archived_events(&store, &id, 2).await;
        assert_eq!(arch2.len(), 2);
    }

    #[tokio::test]
    async fn test_compact_empty_stream_is_noop() {
        let store = InMemoryEventStore::new();
        let id = make_stream_id("compact-empty");

        let account = BankAccountStream::with_id(id.clone());

        // Compacting a stream that has never had events should return NotFound,
        // matching the behaviour of PostgresEventStore::compact.
        let result = store.compact(&account, replay::Metadata::default()).await;

        assert!(result.is_err());
        let err = result.unwrap_err();
        assert_eq!(err.kind(), replay::ErrorKind::NotFound);
    }

    #[tokio::test]
    async fn needs_compaction_tracks_the_watermark() {
        let store = InMemoryEventStore::new();
        let id = make_stream_id("needs-compaction");
        let urn: Urn = id.clone().into();

        // A stream that does not exist has nothing to compact.
        assert!(!store.needs_compaction(&urn).await.unwrap());

        // A never-compacted stream with events is eligible.
        add_events(
            &store,
            &id,
            &[
                BankAccountEvent::Deposited { amount: 100.0 },
                BankAccountEvent::Withdrawn { amount: 40.0 },
            ],
        )
        .await;
        assert!(store.needs_compaction(&urn).await.unwrap());

        // Compaction advances the watermark to the live head, so an unchanged stream
        // is skipped — and stays skipped on repeated reads with no intervening append.
        let mut account = BankAccountStream::with_id(id.clone());
        for ev in live_events(&store, &id).await {
            account.apply(ev);
        }
        store
            .compact(&account, replay::Metadata::default())
            .await
            .unwrap();
        assert!(!store.needs_compaction(&urn).await.unwrap());
        assert!(!store.needs_compaction(&urn).await.unwrap());

        // A new append past the watermark makes it eligible again.
        add_events(&store, &id, &[BankAccountEvent::Deposited { amount: 10.0 }]).await;
        assert!(store.needs_compaction(&urn).await.unwrap());

        // Compacting again settles it once more.
        let mut account2 = BankAccountStream::with_id(id.clone());
        for ev in live_events(&store, &id).await {
            account2.apply(ev);
        }
        store
            .compact(&account2, replay::Metadata::default())
            .await
            .unwrap();
        assert!(!store.needs_compaction(&urn).await.unwrap());
    }

    #[tokio::test]
    async fn needs_compaction_survives_version_coincidence() {
        // A stream can read the same live head version before a compaction and after
        // later appends, yet still need compacting: the watermark tracks the
        // post-compaction count (1), not the pre-compaction version (4).
        let store = InMemoryEventStore::new();
        let id = make_stream_id("version-coincidence");
        let urn: Urn = id.clone().into();

        add_events(
            &store,
            &id,
            &[
                BankAccountEvent::Deposited { amount: 100.0 },
                BankAccountEvent::Deposited { amount: 50.0 },
                BankAccountEvent::Withdrawn { amount: 30.0 },
                BankAccountEvent::Deposited { amount: 10.0 },
            ],
        )
        .await;
        assert_eq!(
            live_events(&store, &id).await.len(),
            4,
            "four live events — version 4"
        );
        assert!(store.needs_compaction(&urn).await.unwrap());

        // Compact 4 → 1: the watermark becomes 1.
        let mut account = BankAccountStream::with_id(id.clone());
        for ev in live_events(&store, &id).await {
            account.apply(ev);
        }
        let v1 = store
            .compact(&account, replay::Metadata::default())
            .await
            .unwrap();
        assert_eq!(v1, CompactionOutcome::Compacted { archive_version: 1 });
        assert_eq!(
            live_events(&store, &id).await.len(),
            1,
            "compacted to a single event"
        );
        assert!(!store.needs_compaction(&urn).await.unwrap());

        // Three more events climb the live head version back to 4.
        add_events(
            &store,
            &id,
            &[
                BankAccountEvent::Deposited { amount: 5.0 },
                BankAccountEvent::Deposited { amount: 5.0 },
                BankAccountEvent::Deposited { amount: 5.0 },
            ],
        )
        .await;
        assert_eq!(
            live_events(&store, &id).await.len(),
            4,
            "live head version is 4 again — the same value as before the first compaction"
        );

        // The key assertion: version 4 equals the pre-compaction version, but the
        // watermark is 1, so the three new events are detected.
        assert!(
            store.needs_compaction(&urn).await.unwrap(),
            "version 4 is compared against watermark 1, not the stale pre-compaction 4"
        );

        // A real second compaction runs (archive version 2) and settles it again.
        let mut account2 = BankAccountStream::with_id(id.clone());
        for ev in live_events(&store, &id).await {
            account2.apply(ev);
        }
        let v2 = store
            .compact(&account2, replay::Metadata::default())
            .await
            .unwrap();
        assert_eq!(v2, CompactionOutcome::Compacted { archive_version: 2 });
        assert!(!store.needs_compaction(&urn).await.unwrap());
    }

    #[tokio::test]
    async fn already_compacted_skips_and_writes_nothing() {
        let store = InMemoryEventStore::new();
        let id = make_stream_id("already-compacted");
        let urn: Urn = id.clone().into();

        // Born minimal: a single Snapshot event, never compacted.
        add_snapshot_events(&store, &id, &[SnapshotEvent::Snapshot { total: 5 }]).await;
        assert!(store.needs_compaction(&urn).await.unwrap());

        // compact → the author reports AlreadyCompacted → nothing written.
        let account = SnapshotStream::with_id(id.clone());
        let outcome = store
            .compact(&account, replay::Metadata::default())
            .await
            .unwrap();
        assert_eq!(outcome, CompactionOutcome::Skipped);

        // The live stream is untouched and no archive row was written.
        assert_eq!(
            live_snapshot_events(&store, &id).await,
            vec![SnapshotEvent::Snapshot { total: 5 }]
        );
        let all = store
            .stream_events::<SnapshotEvent>(StreamFilter::with_stream_id::<SnapshotStream>(&id))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(
            all.len(),
            1,
            "AlreadyCompacted must neither archive nor insert any row"
        );

        // The watermark advanced, so a re-check skips it, and a direct re-compaction
        // is still Skipped (fixpoint).
        assert!(!store.needs_compaction(&urn).await.unwrap());
        let outcome2 = store
            .compact(&account, replay::Metadata::default())
            .await
            .unwrap();
        assert_eq!(outcome2, CompactionOutcome::Skipped);
    }

    #[tokio::test]
    async fn compact_folds_then_reports_already_compacted() {
        let store = InMemoryEventStore::new();
        let id = make_stream_id("fold-then-fixpoint");
        let urn: Urn = id.clone().into();

        add_snapshot_events(&store, &id, &[SnapshotEvent::Bumped, SnapshotEvent::Bumped]).await;

        // Real work: folds two Bumped into a single Snapshot.
        let account = SnapshotStream::with_id(id.clone());
        let outcome = store
            .compact(&account, replay::Metadata::default())
            .await
            .unwrap();
        assert_eq!(outcome, CompactionOutcome::Compacted { archive_version: 1 });
        assert_eq!(
            live_snapshot_events(&store, &id).await,
            vec![SnapshotEvent::Snapshot { total: 2 }]
        );
        assert!(!store.needs_compaction(&urn).await.unwrap());

        // The live stream is now a lone Snapshot, so a direct re-compaction is a
        // no-op skip — the guards do not over-fold a settled stream.
        let outcome2 = store
            .compact(&account, replay::Metadata::default())
            .await
            .unwrap();
        assert_eq!(outcome2, CompactionOutcome::Skipped);
    }

    #[tokio::test]
    async fn empty_rewrite_archives_to_empty_distinct_from_skip() {
        let store = InMemoryEventStore::new();
        let id = make_stream_id("empty-rewrite");

        add_snapshot_events(&store, &id, &[SnapshotEvent::Bumped, SnapshotEvent::Reset]).await;

        // Rewrite(vec![]) is a real compaction: it archives everything and empties the
        // live stream — the opposite write from Skipped, which writes nothing.
        let account = SnapshotStream::with_id(id.clone());
        let outcome = store
            .compact(&account, replay::Metadata::default())
            .await
            .unwrap();
        assert_eq!(outcome, CompactionOutcome::Compacted { archive_version: 1 });
        assert!(live_snapshot_events(&store, &id).await.is_empty());

        // The two originals were archived under version 1.
        let archived = store
            .stream_events::<SnapshotEvent>(StreamFilter::And(
                Box::new(StreamFilter::with_stream_id::<SnapshotStream>(&id)),
                Box::new(StreamFilter::WithAggregateVersion(Some(1))),
            ))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(archived.len(), 2);
    }

    #[tokio::test]
    async fn test_store_events() {
        let store = InMemoryEventStore::new();

        let stream_id = BankAccountUrn(UrnBuilder::new("bank-account", "1").build().unwrap());

        let events = vec![
            BankAccountEvent::Deposited { amount: 100.0 },
            BankAccountEvent::Withdrawn { amount: 40.0 },
        ];

        store
            .store_events::<BankAccountStream>(
                &stream_id,
                "BankAccount".to_string(),
                replay::Metadata::default(),
                &events,
                None,
            )
            .await
            .unwrap();

        let stream_events = store
            .stream_events::<BankAccountEvent>(StreamFilter::with_stream_id::<BankAccountStream>(
                &stream_id,
            ))
            .map_ok(|persisted_event| persisted_event.data)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(stream_events.len(), 2);

        let mut stream = BankAccountStream {
            id: stream_id.clone(),
            balance: 0.0,
        };
        stream.apply_all(stream_events);

        assert_eq!(stream.balance, 60.0);
    }

    #[tokio::test]
    async fn store_events_stream_notifies_sink_in_order() {
        let store = InMemoryEventStore::new();
        let stream_id = make_stream_id("stream-sink");
        let events = vec![
            BankAccountEvent::Deposited { amount: 100.0 },
            BankAccountEvent::Withdrawn { amount: 40.0 },
        ];

        let mut observed = Vec::new();
        store
            .store_events_stream::<BankAccountStream, _, _>(
                &stream_id,
                "BankAccount".to_string(),
                replay::Metadata::default(),
                futures::stream::iter(events.clone().into_iter().map(Ok::<_, replay::Error>)),
                None,
                |event: &PersistedEvent<BankAccountEvent>| {
                    observed.push((event.version, event.data.clone()));
                },
            )
            .await
            .unwrap();

        assert_eq!(
            observed,
            vec![
                (1, BankAccountEvent::Deposited { amount: 100.0 }),
                (2, BankAccountEvent::Withdrawn { amount: 40.0 })
            ]
        );
    }

    #[tokio::test]
    async fn store_events_stream_multi_event_with_expected_version_succeeds() {
        // Parity with the Postgres regression guard: a multi-event append with a correct
        // expected version is validated once at the head and then appends every event.
        let store = InMemoryEventStore::new();
        let stream_id = make_stream_id("stream-multi-expected");
        let events = vec![
            BankAccountEvent::Deposited { amount: 100.0 },
            BankAccountEvent::Withdrawn { amount: 40.0 },
            BankAccountEvent::Deposited { amount: 10.0 },
        ];

        let mut observed_versions = Vec::new();
        store
            .store_events_stream::<BankAccountStream, _, _>(
                &stream_id,
                "BankAccount".to_string(),
                replay::Metadata::default(),
                futures::stream::iter(events.into_iter().map(Ok::<_, replay::Error>)),
                Some(0),
                |event: &PersistedEvent<BankAccountEvent>| observed_versions.push(event.version),
            )
            .await
            .expect("multi-event append with correct expected version must succeed");

        assert_eq!(observed_versions, vec![1, 2, 3]);
    }

    #[tokio::test]
    async fn store_events_stream_stale_expected_version_conflicts() {
        let store = InMemoryEventStore::new();
        let stream_id = make_stream_id("stream-stale-expected");

        // Advance the stream to version 1.
        store
            .store_events_stream::<BankAccountStream, _, _>(
                &stream_id,
                "BankAccount".to_string(),
                replay::Metadata::default(),
                futures::stream::iter(
                    vec![Ok::<_, replay::Error>(BankAccountEvent::Deposited {
                        amount: 100.0,
                    })]
                    .into_iter(),
                ),
                Some(0),
                crate::NoSink,
            )
            .await
            .expect("first append must succeed");

        // A second append still expecting version 0 must conflict and persist nothing new.
        let result = store
            .store_events_stream::<BankAccountStream, _, _>(
                &stream_id,
                "BankAccount".to_string(),
                replay::Metadata::default(),
                futures::stream::iter(
                    vec![Ok::<_, replay::Error>(BankAccountEvent::Deposited {
                        amount: 5.0,
                    })]
                    .into_iter(),
                ),
                Some(0),
                crate::NoSink,
            )
            .await;

        let err = result.expect_err("stale expected_version must conflict");
        assert_eq!(
            err.kind(),
            replay::ErrorKind::Conflict,
            "stale append must surface an optimistic-concurrency conflict"
        );

        let stream_events = store
            .stream_events::<BankAccountEvent>(StreamFilter::with_stream_id::<BankAccountStream>(
                &stream_id,
            ))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();
        assert_eq!(stream_events.len(), 1, "stale append must not persist");
    }

    #[tokio::test]
    async fn store_events_stream_rolls_back_on_producer_error() {
        let store = InMemoryEventStore::new();
        let stream_id = make_stream_id("stream-producer-error");

        let result = store
            .store_events_stream::<BankAccountStream, _, _>(
                &stream_id,
                "BankAccount".to_string(),
                replay::Metadata::default(),
                futures::stream::iter(vec![
                    Ok(BankAccountEvent::Deposited { amount: 100.0 }),
                    Err(replay::Error::internal("producer failed")),
                ]),
                None,
                crate::NoSink,
            )
            .await;

        assert!(result.is_err());

        let stream_events = store
            .stream_events::<BankAccountEvent>(StreamFilter::with_stream_id::<BankAccountStream>(
                &stream_id,
            ))
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert!(
            stream_events.is_empty(),
            "producer error must discard the whole batch"
        );
    }

    #[tokio::test]
    async fn stream_events_filters_by_stream_type() {
        let store = InMemoryEventStore::new();

        let checking = make_stream_id("checking");
        let savings = make_stream_id("savings");

        // Two streams recorded under two different stream types. The in-memory store
        // remembers each stream's type so it can evaluate `ForStreamTypes` per-event
        // (there is no database to push the filter down to).
        store
            .store_events::<BankAccountStream>(
                &checking,
                "Checking".to_string(),
                replay::Metadata::default(),
                &[BankAccountEvent::Deposited { amount: 100.0 }],
                None,
            )
            .await
            .unwrap();
        store
            .store_events::<BankAccountStream>(
                &savings,
                "Savings".to_string(),
                replay::Metadata::default(),
                &[BankAccountEvent::Deposited { amount: 50.0 }],
                None,
            )
            .await
            .unwrap();

        // A single stream type matches only its own events instead of erroring.
        let checking_only: Vec<BankAccountEvent> = store
            .stream_events::<BankAccountEvent>(StreamFilter::ForStreamTypes(vec![
                "Checking".to_string()
            ]))
            .map_ok(|e| e.data)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(
            checking_only,
            vec![BankAccountEvent::Deposited { amount: 100.0 }]
        );

        // Several stream types match the union of their events.
        let both: Vec<BankAccountEvent> = store
            .stream_events::<BankAccountEvent>(StreamFilter::ForStreamTypes(vec![
                "Checking".to_string(),
                "Savings".to_string(),
            ]))
            .map_ok(|e| e.data)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(both.len(), 2);

        // A stream type nobody was stored under matches nothing.
        let none: Vec<BankAccountEvent> = store
            .stream_events::<BankAccountEvent>(StreamFilter::ForStreamTypes(vec![
                "Unknown".to_string()
            ]))
            .map_ok(|e| e.data)
            .try_collect()
            .await
            .unwrap();
        assert!(none.is_empty());
    }

    // ── Best-effort inline projections (issue #61) ───────────────────────────

    /// A projection event type that only knows `Deposited`. A `Withdrawn` event fails to
    /// deserialize into it, so the deserialize-or-skip router drops it before `handle`.
    #[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Event)]
    enum DepositOnlyEvent {
        Deposited { amount: f64 },
    }

    /// An unrelated projection event type. No `BankAccountEvent` deserializes into it, so the
    /// router never calls this projection's `handle`.
    #[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Event)]
    enum NotificationEvent {
        Notified { message: String },
    }

    /// Records the deposit amounts it is handed and counts how many times `handle` runs, via
    /// `Arc`-shared state so the test can assert after the projection has been moved into the store.
    #[derive(Default)]
    struct DepositRecorder {
        deposits: Arc<StdMutex<Vec<f64>>>,
        handle_calls: Arc<AtomicUsize>,
    }

    impl InlineProjection for DepositRecorder {
        type Exec = ();
        type Event = DepositOnlyEvent;

        fn name(&self) -> &str {
            "deposit_recorder"
        }

        fn version(&self) -> i32 {
            1
        }

        async fn init(&mut self, _conn: &mut Self::Exec) -> Result<(), replay::Error> {
            Ok(())
        }

        async fn handle(
            &mut self,
            _conn: &mut Self::Exec,
            events: &[PersistedEvent<Self::Event>],
        ) -> Result<(), replay::Error> {
            self.handle_calls.fetch_add(1, Ordering::SeqCst);
            let mut deposits = self.deposits.lock().unwrap();
            for event in events {
                let DepositOnlyEvent::Deposited { amount } = &event.data;
                deposits.push(*amount);
            }
            Ok(())
        }
    }

    /// Counts `handle` invocations only; used to prove a non-matching projection is skipped.
    #[derive(Default)]
    struct NotificationRecorder {
        handle_calls: Arc<AtomicUsize>,
    }

    impl InlineProjection for NotificationRecorder {
        type Exec = ();
        type Event = NotificationEvent;

        fn name(&self) -> &str {
            "notification_recorder"
        }

        fn version(&self) -> i32 {
            1
        }

        async fn init(&mut self, _conn: &mut Self::Exec) -> Result<(), replay::Error> {
            Ok(())
        }

        async fn handle(
            &mut self,
            _conn: &mut Self::Exec,
            _events: &[PersistedEvent<Self::Event>],
        ) -> Result<(), replay::Error> {
            self.handle_calls.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    /// A registered projection receives the appended events as a single batch, and the
    /// deserialize-or-skip router drops events that don't belong to its event type.
    #[tokio::test]
    async fn inline_projection_routes_and_batches() {
        let recorder = DepositRecorder::default();
        let deposits = recorder.deposits.clone();
        let handle_calls = recorder.handle_calls.clone();

        let store = InMemoryEventStore::new().register_projection(recorder);

        let stream_id = BankAccountUrn(UrnBuilder::new("bank-account", "routing").build().unwrap());

        // A single append carrying both Deposited and Withdrawn events.
        store
            .store_events::<BankAccountStream>(
                &stream_id,
                "bank-account".to_string(),
                replay::Metadata::default(),
                &[
                    BankAccountEvent::Deposited { amount: 100.0 },
                    BankAccountEvent::Withdrawn { amount: 40.0 },
                    BankAccountEvent::Deposited { amount: 25.0 },
                ],
                None,
            )
            .await
            .unwrap();

        // Only the Deposited events reached the projection (Withdrawn failed to deserialize
        // into DepositOnlyEvent and was skipped), delivered as ONE batched handle call.
        assert_eq!(*deposits.lock().unwrap(), vec![100.0, 25.0]);
        assert_eq!(handle_calls.load(Ordering::SeqCst), 1);
    }

    /// A projection whose event type matches none of the appended events is never called.
    #[tokio::test]
    async fn inline_projection_skips_non_matching() {
        let recorder = NotificationRecorder::default();
        let handle_calls = recorder.handle_calls.clone();

        let store = InMemoryEventStore::new().register_projection(recorder);

        let stream_id = BankAccountUrn(UrnBuilder::new("bank-account", "skip").build().unwrap());

        store
            .store_events::<BankAccountStream>(
                &stream_id,
                "bank-account".to_string(),
                replay::Metadata::default(),
                &[BankAccountEvent::Deposited { amount: 100.0 }],
                None,
            )
            .await
            .unwrap();

        // No appended event deserializes into NotificationEvent, so handle is never called.
        assert_eq!(handle_calls.load(Ordering::SeqCst), 0);
    }

    /// Appends across multiple commands each drive `handle` once with that command's batch.
    #[tokio::test]
    async fn inline_projection_runs_once_per_append() {
        let recorder = DepositRecorder::default();
        let deposits = recorder.deposits.clone();
        let handle_calls = recorder.handle_calls.clone();

        let store = InMemoryEventStore::new().register_projection(recorder);

        let stream_id = BankAccountUrn(
            UrnBuilder::new("bank-account", "per-append")
                .build()
                .unwrap(),
        );

        store
            .store_events::<BankAccountStream>(
                &stream_id,
                "bank-account".to_string(),
                replay::Metadata::default(),
                &[BankAccountEvent::Deposited { amount: 10.0 }],
                None,
            )
            .await
            .unwrap();

        store
            .store_events::<BankAccountStream>(
                &stream_id,
                "bank-account".to_string(),
                replay::Metadata::default(),
                &[BankAccountEvent::Deposited { amount: 5.0 }],
                Some(1),
            )
            .await
            .unwrap();

        assert_eq!(*deposits.lock().unwrap(), vec![10.0, 5.0]);
        assert_eq!(handle_calls.load(Ordering::SeqCst), 2);
    }
}
