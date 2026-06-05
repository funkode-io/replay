use std::future::Future;

use futures::future::BoxFuture;
use serde::de::DeserializeOwned;
use serde_json::Value;

use crate::PersistedEvent;
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

    /// Create the projection's view table and any supporting schema.
    ///
    /// Called once when the projection is first registered. Runs against the store's
    /// connection so the setup participates in the registration transaction.
    fn init(
        &mut self,
        conn: &mut Self::Exec,
    ) -> impl Future<Output = Result<(), replay::Error>> + Send;

    /// Apply a batch of newly-appended events to the read model.
    ///
    /// The batch contains only the events that belong to this projection (events that
    /// failed to deserialize into [`Event`](Self::Event) are filtered out before this is
    /// called) and is never empty. Writes go through `conn`, which is the store's active
    /// transaction, so they commit atomically with the appended events.
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

    fn init<'a>(&'a mut self, conn: &'a mut Self::Exec)
        -> BoxFuture<'a, Result<(), replay::Error>>;

    /// Route raw appended events to the underlying typed projection.
    ///
    /// Each event's JSON `data` is deserialized into the projection's event type; events
    /// that do not match are skipped. If no events match, the underlying `handle` is not
    /// called.
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

    fn init<'a>(
        &'a mut self,
        conn: &'a mut Self::Exec,
    ) -> BoxFuture<'a, Result<(), replay::Error>> {
        Box::pin(InlineProjection::init(self, conn))
    }

    fn handle<'a>(
        &'a mut self,
        conn: &'a mut Self::Exec,
        events: &'a [PersistedEvent<Value>],
    ) -> BoxFuture<'a, Result<(), replay::Error>> {
        Box::pin(async move {
            // Deserialize-or-skip: keep only events that belong to this projection.
            let typed: Vec<PersistedEvent<T::Event>> = events
                .iter()
                .filter_map(|event| {
                    serde_json::from_value::<T::Event>(event.data.clone())
                        .ok()
                        .map(|data| event.clone().with_data(data))
                })
                .collect();

            if typed.is_empty() {
                return Ok(());
            }

            InlineProjection::handle(self, conn, &typed).await
        })
    }
}
