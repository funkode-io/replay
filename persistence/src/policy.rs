//! Policies: checkpointed background subscribers that *react* to events by
//! issuing commands.
//!
//! A [`Policy`] is a sibling of [`crate::Query`] / inline projections, not a
//! kind of projection: it derives no read model. Given an event it returns a
//! list of [`Dispatch`]es — commands the runner should execute against
//! aggregates. This module is the **portable contract**: it carries no Postgres
//! or tokio types, so the same `Policy` / `Dispatch` shapes can drive a future
//! WASM runner. The server-side execution lives in the runner (native only).

use std::any::{Any, TypeId};

use replay::{Aggregate, Event, Metadata};

use crate::{PersistedEvent, StreamFilter};

/// Cursor initialization behavior used on first policy registration.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum StartAt {
    /// Start from the current global head, so only newly appended events are
    /// processed.
    #[default]
    Now,
    /// Start from position 0 and process full history once.
    Beginning,
}

/// A command a [`Policy`] wants the runner to execute against an aggregate.
///
/// `Dispatch` is an *erased descriptor*: it remembers which aggregate type the
/// command targets ([`TypeId`]) and carries the `(StreamId, Command)` pair as an
/// opaque `Box<dyn Any + Send>` payload. The runner — which registered the
/// concrete aggregate via `register_services::<A>` — downcasts the payload and
/// runs it through `Cqrs::execute`. Crucially this struct names no Postgres or
/// tokio types, so it stays WASM-ready.
pub struct Dispatch {
    pub(crate) target: TypeId,
    pub(crate) aggregate_name: &'static str,
    pub(crate) payload: Box<dyn Any + Send>,
    pub(crate) expected_version: Option<i64>,
    pub(crate) metadata: Option<Metadata>,
}

impl Dispatch {
    /// Builds a dispatch targeting aggregate `A`, identified by `id`, carrying
    /// `command`.
    ///
    /// ```rust,ignore
    /// Dispatch::to::<BankAccount>(account_id, BankAccountCommand::Freeze)
    /// ```
    pub fn to<A>(id: A::StreamId, command: A::Command) -> Self
    where
        A: Aggregate + 'static,
        A::StreamId: 'static,
        A::Command: 'static,
    {
        Dispatch {
            target: TypeId::of::<A>(),
            aggregate_name: std::any::type_name::<A>(),
            payload: Box::new((id, command)),
            expected_version: None,
            metadata: None,
        }
    }

    /// Attach user-defined metadata to this dispatch.
    ///
    /// The runner merges this metadata with causation metadata before executing
    /// the command. Colliding top-level keys are rejected at runtime.
    pub fn with_metadata(mut self, metadata: Metadata) -> Self {
        self.metadata = Some(metadata);
        self
    }

    /// The [`TypeId`] of the aggregate this dispatch targets.
    pub fn target(&self) -> TypeId {
        self.target
    }

    /// The Rust type name of the target aggregate (diagnostics only).
    pub fn aggregate_name(&self) -> &'static str {
        self.aggregate_name
    }
}

/// A checkpointed background subscriber that reacts to events with commands.
///
/// Implementors stay pure and store-agnostic: [`react`](Policy::react) takes an
/// event and returns the commands to issue, with no I/O. The runner handles
/// reading the feed, executing the returned [`Dispatch`]es, stamping causation
/// metadata, and advancing the cursor.
pub trait Policy: Send + Sync {
    /// The event type this policy understands. Use `query_events!` to merge
    /// events from several aggregates into one enum.
    type Event: Event;

    /// Stable identity used as the cursor key. Changing the Rust type must not
    /// change this string, or the policy would lose its checkpoint.
    fn name(&self) -> &str;

    /// Narrows the feed to the streams this policy cares about. Defaults to the
    /// whole log.
    fn stream_filter(&self) -> StreamFilter {
        StreamFilter::all()
    }

    /// Cursor bootstrap strategy used only when this policy name is first seen.
    ///
    /// Defaults to [`StartAt::Now`], the safe mode that avoids retroactively
    /// firing commands across existing history.
    fn start_at(&self) -> StartAt {
        StartAt::Now
    }

    /// Pure reaction: given an event, return the commands to dispatch.
    fn react(&self, event: &PersistedEvent<Self::Event>) -> Vec<Dispatch>;
}

/// Object-safe erasure of [`Policy`], mirroring `ErasedInlineProjection`.
///
/// The runner holds `Box<dyn ErasedPolicy>` and feeds it raw JSON events; the
/// blanket impl deserializes into the concrete `Policy::Event` and skips events
/// that don't belong to this policy (deserialize-or-skip routing).
pub(crate) trait ErasedPolicy: Send + Sync {
    fn name(&self) -> &str;

    fn stream_filter(&self) -> StreamFilter;

    fn start_at(&self) -> StartAt;

    fn react_erased(&self, raw: &PersistedEvent<serde_json::Value>) -> Vec<Dispatch>;
}

impl<P: Policy> ErasedPolicy for P {
    fn name(&self) -> &str {
        Policy::name(self)
    }

    fn stream_filter(&self) -> StreamFilter {
        Policy::stream_filter(self)
    }

    fn start_at(&self) -> StartAt {
        Policy::start_at(self)
    }

    fn react_erased(&self, raw: &PersistedEvent<serde_json::Value>) -> Vec<Dispatch> {
        // Deserialize-or-skip: events whose payload isn't this policy's Event
        // type simply produce no reaction.
        match serde_json::from_value::<P::Event>(raw.data.clone()) {
            Ok(event) => {
                let typed = raw.clone().with_data(event);
                self.react(&typed)
            }
            Err(_) => Vec::new(),
        }
    }
}
