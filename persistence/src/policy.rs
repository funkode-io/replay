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

use serde::Deserialize;

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
/// Delivery is **at-least-once**. A crash after command commit but before cursor
/// save can re-deliver the same triggering event. Correctness therefore depends
/// on idempotent command handling in the target aggregate, keyed by causation
/// identity (the triggering event id), not by command-value equality.
///
/// Implementors stay pure and store-agnostic: [`react`](Policy::react) takes
/// an event and returns the commands to issue, with no I/O. The runner handles
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

    /// Maximum causation depth this policy will react to.  The runner skips any
    /// event whose `causation.depth` is ≥ this value and logs loudly instead,
    /// acting as a circuit breaker for runaway event → command → event cascades.
    ///
    /// Resolution order (most-specific-first):
    ///   1. This per-policy override (when `Some`).
    ///   2. Environment variable `REPLAY_MAX_CAUSATION_DEPTH`.
    ///   3. Built-in default (10).
    ///
    /// Return `None` to defer to the global env var / built-in default.
    fn max_causation_depth(&self) -> Option<u32> {
        None
    }

    /// Maximum number of events fetched from the feed in a single drain call.
    ///
    /// Resolution order (most-specific-first):
    ///   1. This per-policy override (when `Some`).
    ///   2. Environment variable `REPLAY_READ_BATCH_SIZE`.
    ///   3. Built-in default (100).
    ///
    /// The runner enforces the invariant `read_batch_size ≥ checkpoint_batch_size`.
    fn read_batch_size(&self) -> Option<u32> {
        None
    }

    /// How many events are processed between cursor persistence calls.  The
    /// cursor is also written unconditionally at the end of every drain call.
    ///
    /// Resolution order (most-specific-first):
    ///   1. This per-policy override (when `Some`).
    ///   2. Environment variable `REPLAY_CHECKPOINT_BATCH_SIZE`.
    ///   3. Built-in default (100).
    fn checkpoint_batch_size(&self) -> Option<u32> {
        None
    }

    /// Pure reaction: given an event, return the commands to dispatch.
    ///
    /// Invariant: because delivery is at-least-once, target aggregate command
    /// handlers must absorb duplicate causation ids as no-ops.
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

    fn max_causation_depth_erased(&self) -> Option<u32>;

    fn read_batch_size_erased(&self) -> Option<u32>;

    fn checkpoint_batch_size_erased(&self) -> Option<u32>;

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

    fn max_causation_depth_erased(&self) -> Option<u32> {
        Policy::max_causation_depth(self)
    }

    fn read_batch_size_erased(&self) -> Option<u32> {
        Policy::read_batch_size(self)
    }

    fn checkpoint_batch_size_erased(&self) -> Option<u32> {
        Policy::checkpoint_batch_size(self)
    }

    fn react_erased(&self, raw: &PersistedEvent<serde_json::Value>) -> Vec<Dispatch> {
        // Deserialize-or-skip: events whose payload isn't this policy's Event
        // type simply produce no reaction.
        //
        // Read by borrow — `&serde_json::Value` is itself a `Deserializer`, so a
        // non-matching payload still yields a recoverable `Err` without the feed
        // paying a deep copy of every event it offers. The envelope is re-made
        // from the borrowed event rather than cloned, which would drag the JSON
        // payload along with it.
        match P::Event::deserialize(&raw.data) {
            Ok(event) => {
                let typed = raw.with_data_from(event);
                self.react(&typed)
            }
            Err(_) => Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use urn::{Urn, UrnBuilder};
    use uuid::Uuid;

    use replay::{Metadata, WithId};
    use replay_macros::Event;

    use super::*;

    #[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Event)]
    enum AccountEvent {
        Frozen { reason: String },
    }

    /// An unrelated event type: no `AccountEvent` payload deserializes into it.
    #[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Event)]
    enum ShippingEvent {
        Shipped { tracking: String },
    }

    #[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
    struct AccountUrn(Urn);

    impl From<AccountUrn> for Urn {
        fn from(urn: AccountUrn) -> Self {
            urn.0
        }
    }

    impl TryFrom<Urn> for AccountUrn {
        type Error = String;

        fn try_from(urn: Urn) -> Result<Self, Self::Error> {
            Ok(AccountUrn(urn))
        }
    }

    struct Account {
        id: AccountUrn,
    }

    impl WithId for Account {
        type StreamId = AccountUrn;

        fn with_id(id: Self::StreamId) -> Self {
            Account { id }
        }

        fn get_id(&self) -> &Self::StreamId {
            &self.id
        }
    }

    impl replay::EventStream for Account {
        type Event = AccountEvent;

        fn stream_type() -> String {
            "Account".to_string()
        }

        fn apply(&mut self, _event: Self::Event) {}
    }

    impl Aggregate for Account {
        type Command = String;
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

    /// Reacts to every event it is given, echoing the reason it received so the
    /// test can prove the envelope reached `react` intact.
    struct FreezeNotifier;

    impl Policy for FreezeNotifier {
        type Event = AccountEvent;

        fn name(&self) -> &str {
            "freeze_notifier"
        }

        fn react(&self, event: &PersistedEvent<Self::Event>) -> Vec<Dispatch> {
            let AccountEvent::Frozen { reason } = &event.data;
            vec![Dispatch::to::<Account>(
                AccountUrn(event.stream_id.clone()),
                reason.clone(),
            )]
        }
    }

    /// A policy over an unrelated event type; the router must never call `react`.
    struct ShippingNotifier;

    impl Policy for ShippingNotifier {
        type Event = ShippingEvent;

        fn name(&self) -> &str {
            "shipping_notifier"
        }

        fn react(&self, _event: &PersistedEvent<Self::Event>) -> Vec<Dispatch> {
            panic!("react must not be called for a non-matching payload");
        }
    }

    fn raw_frozen() -> PersistedEvent<serde_json::Value> {
        PersistedEvent {
            id: Uuid::new_v4(),
            data: json!({ "Frozen": { "reason": "fraud-review" } }),
            stream_id: UrnBuilder::new("account", "42").build().unwrap(),
            r#type: "Frozen".to_string(),
            version: 7,
            created: Utc::now(),
            metadata: Metadata::new(json!({ "correlation": "c-1" })),
            aggregate_version: None,
        }
    }

    /// A payload of the policy's own event type reaches `react`, with the
    /// envelope (identity, position, metadata) carried across the erasure intact.
    #[test]
    fn reacts_to_a_matching_payload_preserving_the_envelope() {
        let raw = raw_frozen();

        let dispatches = FreezeNotifier.react_erased(&raw);

        assert_eq!(
            dispatches.len(),
            1,
            "matching payload must produce one command"
        );
        assert_eq!(dispatches[0].target(), TypeId::of::<Account>());

        // The command was built from the deserialized payload and the borrowed
        // envelope's stream id, so both survived the erasure.
        let (id, command) = dispatches[0]
            .payload
            .downcast_ref::<(AccountUrn, String)>()
            .expect("dispatch must carry the aggregate's (id, command) pair");
        assert_eq!(id.0, raw.stream_id);
        assert_eq!(command, "fraud-review");
    }

    /// A payload that isn't this policy's event type is skipped: no reaction, and
    /// `react` is never called.
    #[test]
    fn skips_a_non_matching_payload() {
        let raw = raw_frozen();

        let dispatches = ShippingNotifier.react_erased(&raw);

        assert!(
            dispatches.is_empty(),
            "a non-matching payload must produce no reaction"
        );
    }
}
