use std::future::Future;

use futures::future::BoxFuture;
use serde::{de::DeserializeOwned, Deserialize};
use serde_json::Value;

use crate::{PersistedEvent, StreamFilter};
use replay::Event;

/// An inline projection: a persisted read model whose write is applied inside the
/// same transaction that appends the events, against the same store instance.
///
/// The write handle type is supplied by the store as the associated `Exec` type.
/// The store drives the projection while it holds that transaction/connection,
/// so the read-model write commits atomically with the events.
///
/// Implementors declare the events they consume via the associated [`Event`](Self::Event)
/// type. The merged enum produced by the `query_events!` macro is the intended choice:
/// its `Deserialize` impl tries each underlying event type in turn, which lets the store
/// route appended events by deserialize-or-skip.
pub trait InlineProjection: Send + Sync {
    /// Concrete write-handle type supplied by the store.
    type Exec: Send;

    /// The event type this projection consumes. Typically a `query_events!`-merged enum.
    type Event: Event + DeserializeOwned;

    /// Stable identity of this projection in the `projections` registry table.
    ///
    /// This is the registry key and MUST be stable across refactors — do not derive it
    /// from the Rust type name.
    fn name(&self) -> &str;

    /// Author-managed code version of this projection's logic/schema.
    fn version(&self) -> i32;

    /// Events to replay when this projection is first registered or rebuilt after a
    /// version bump.
    ///
    /// Returned filter narrows which streams/events the store scans during either
    /// replay, so unrelated history is not read. Defaults to [`StreamFilter::All`].
    ///
    /// The live append does not apply it, and neither does the catch-up that stands in
    /// for the live append of a process without this projection (ADR-0032); to narrow
    /// those, declare [`event_types`](Self::event_types).
    ///
    /// Whatever the filter, a replay reads the true log (ADR-0004): archived
    /// originals and the real events appended after a compaction, never compaction
    /// snapshots. A rebuild therefore reproduces what the projection saw live, since
    /// compaction never runs inline projections.
    fn stream_filter(&self) -> StreamFilter {
        StreamFilter::All
    }

    /// The event `type`s this projection handles, as [`Event::event_type`] wrote them.
    ///
    /// `None`, the default, routes every event by deserialize-or-skip (ADR-0002).
    /// `Some(types)` narrows every path that feeds [`handle`](Self::handle) — the live
    /// append, the rebuild replay and the catch-up — to events of those types before
    /// anything is deserialized, so the three agree on the types they deliver and a replay
    /// or catch-up scan reads only those types
    /// ([ADR-0032](https://github.com/funkode-io/replay/blob/main/docs/adr/0032-an-inline-projection-catches-up-on-transactions-that-did-not-apply-it.md)).
    ///
    /// List every type the projection's [`Event`](Self::Event) can hold: an event of a
    /// type left out never reaches `handle`. For a derived event enum that is each
    /// variant's name.
    fn event_types(&self) -> Option<Vec<String>> {
        None
    }

    /// Create the projection's view table and any supporting schema.
    ///
    /// Called once when the projection is first registered. Runs against the store's
    /// connection so the setup participates in the registration transaction.
    fn init(
        &mut self,
        conn: &mut Self::Exec,
    ) -> impl Future<Output = Result<(), replay::Error>> + Send;

    /// Clear the projection's view so it can be rebuilt from history.
    ///
    /// Called during a version-drift rebuild, before events are replayed through
    /// [`handle`](Self::handle), inside the rebuild transaction. The default is a no-op;
    /// projections that bump [`version`](Self::version) to trigger a rebuild should
    /// override this to truncate their view, otherwise replay accumulates on top of the
    /// existing data.
    fn reset(
        &mut self,
        _conn: &mut Self::Exec,
    ) -> impl Future<Output = Result<(), replay::Error>> + Send {
        async { Ok(()) }
    }

    /// Apply a batch of newly-appended events to the read model.
    ///
    /// The batch contains only the events that belong to this projection (events that
    /// failed to deserialize into [`Event`](Self::Event) are filtered out before this is
    /// called) and is never empty. Writes go through `conn`, which is the store's active
    /// transaction, so they commit atomically with the appended events.
    ///
    /// **A single append may arrive as several calls.** The store bounds how many events
    /// it holds before flushing them here, so an append larger than that bound is
    /// delivered in order across ⌈size / flush⌉ calls — all on the same transaction,
    /// committing or rolling back together. The history replayed on first registration or
    /// a version-drift rebuild is delivered the same way, in chunks of the same size, on
    /// the rebuild transaction. Do not assume a call carries an append in its entirety:
    /// state that must span an append belongs in the projection's own fields or its view,
    /// not in a local of one `handle` call. The events delivered, and their order, are
    /// otherwise unchanged. See `PostgresEventStoreBuilder::projection_flush_size`
    /// and ADR-0011.
    fn handle(
        &mut self,
        conn: &mut Self::Exec,
        events: &[PersistedEvent<Self::Event>],
    ) -> impl Future<Output = Result<(), replay::Error>> + Send;
}

/// Object-safe, type-erased view of an [`InlineProjection`] used by the store's registry.
///
/// The store holds projections as `Box<dyn ErasedInlineProjection<Exec = ...>>` so that projections
/// over different event types can live in a single registry. The erased `handle` receives
/// raw JSON-backed events and bridges them to the projection's typed event via
/// deserialize-or-skip.
pub(crate) trait ErasedInlineProjection: Send + Sync {
    type Exec;

    fn name(&self) -> &str;

    fn version(&self) -> i32;

    fn stream_filter(&self) -> StreamFilter;

    fn event_types(&self) -> Option<Vec<String>>;

    fn init<'a>(&'a mut self, conn: &'a mut Self::Exec)
        -> BoxFuture<'a, Result<(), replay::Error>>;

    /// Clear the underlying projection's view ahead of a rebuild.
    fn reset<'a>(
        &'a mut self,
        conn: &'a mut Self::Exec,
    ) -> BoxFuture<'a, Result<(), replay::Error>>;

    /// Route raw appended events to the underlying typed projection.
    ///
    /// Events of a type outside [`event_types`](InlineProjection::event_types), when the
    /// projection declares them, are skipped first. Each remaining event's JSON `data` is
    /// deserialized *by borrow* into the projection's event type; events that do not
    /// match are skipped. If no events match, the underlying `handle` is not called.
    fn handle<'a>(
        &'a mut self,
        conn: &'a mut Self::Exec,
        events: &'a [PersistedEvent<Value>],
    ) -> BoxFuture<'a, Result<(), replay::Error>>;
}

impl<T> ErasedInlineProjection for T
where
    T: InlineProjection,
{
    type Exec = T::Exec;

    fn name(&self) -> &str {
        InlineProjection::name(self)
    }

    fn version(&self) -> i32 {
        InlineProjection::version(self)
    }

    fn stream_filter(&self) -> StreamFilter {
        InlineProjection::stream_filter(self)
    }

    fn event_types(&self) -> Option<Vec<String>> {
        InlineProjection::event_types(self)
    }

    fn init<'a>(
        &'a mut self,
        conn: &'a mut Self::Exec,
    ) -> BoxFuture<'a, Result<(), replay::Error>> {
        Box::pin(InlineProjection::init(self, conn))
    }

    fn reset<'a>(
        &'a mut self,
        conn: &'a mut Self::Exec,
    ) -> BoxFuture<'a, Result<(), replay::Error>> {
        Box::pin(InlineProjection::reset(self, conn))
    }

    fn handle<'a>(
        &'a mut self,
        conn: &'a mut Self::Exec,
        events: &'a [PersistedEvent<Value>],
    ) -> BoxFuture<'a, Result<(), replay::Error>> {
        Box::pin(async move {
            // Declared types first: an event of another type is never deserialized. The
            // replay and catch-up scans already filter on them in SQL; the live append is
            // what this check is for.
            let types = InlineProjection::event_types(self);

            // Deserialize-or-skip: keep only events that belong to this projection.
            //
            // `&serde_json::Value` is itself a `Deserializer`, so the payload is read by
            // borrow: a matching payload yields `Ok`, a non-matching one a recoverable
            // `Err`, exactly as `from_value` did — but without deep-copying the JSON.
            // The envelope is re-made from the borrowed event for the same reason, so
            // the batch is not duplicated once per registered projection.
            let typed: Vec<PersistedEvent<T::Event>> = events
                .iter()
                .filter(|event| {
                    types
                        .as_ref()
                        .is_none_or(|types| types.contains(&event.r#type))
                })
                .filter_map(|event| {
                    let data = T::Event::deserialize(&event.data).ok()?;
                    Some(event.with_data_from(data))
                })
                .collect();

            if typed.is_empty() {
                return Ok(());
            }

            InlineProjection::handle(self, conn, &typed).await
        })
    }
}
