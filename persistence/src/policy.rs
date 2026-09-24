//! What the runner needs around a [`Policy`]: how its feed is driven, and the erasure
//! that lets one runner hold policies over different event types.
//!
//! The rule itself — [`Policy`], [`Dispatch`], [`ObservedEvent`] — lives in `es-replay`
//! so a domain layer can declare one without this crate (ADR-0027). All three are
//! re-exported here so a consumer of the runner has one import.

use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;

use replay::{Aggregate, AggregatePolicy, Dispatch, Event, Metadata, ObservedEvent, Policy};

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

/// How the runner drives one [`Policy`]: everything about the reaction that is not the
/// rule.
///
/// Supplied at registration rather than declared on the trait, so a closure policy can
/// set the same knobs a `Policy` impl can. `None` leaves a tunable to the environment
/// variable and built-in default named on each setter.
#[derive(Debug, Clone, Default)]
pub struct PolicySettings {
    stream_filter: StreamFilter,
    start_at: StartAt,
    max_causation_depth: Option<u32>,
    read_batch_size: Option<u32>,
    checkpoint_batch_size: Option<u32>,
    dispatch_timeout: Option<Duration>,
}

impl PolicySettings {
    /// The defaults: the whole log, from the head, with every tunable left to its
    /// environment variable or built-in default.
    pub fn new() -> Self {
        Self::default()
    }

    /// Narrows the feed to the streams this policy cares about. Defaults to the
    /// whole log.
    ///
    /// Decides what the policy reacts to, not how far its cursor gets: excluded
    /// positions still advance it (ADR-0013), so a selective filter spends its read
    /// batch on positions rather than on matches.
    pub fn with_stream_filter(mut self, stream_filter: StreamFilter) -> Self {
        self.stream_filter = stream_filter;
        self
    }

    /// Cursor bootstrap strategy, used only when this policy name is first seen.
    ///
    /// Defaults to [`StartAt::Now`], the safe mode that avoids retroactively firing
    /// commands across existing history.
    pub fn starting_at(mut self, start_at: StartAt) -> Self {
        self.start_at = start_at;
        self
    }

    /// Maximum causation depth this policy will react to. The runner skips any
    /// event whose `causation.depth` is ≥ this value and logs loudly instead,
    /// acting as a circuit breaker for runaway event → command → event cascades.
    ///
    /// Unset: `REPLAY_MAX_CAUSATION_DEPTH`, then the built-in default (10).
    pub fn with_max_causation_depth(mut self, depth: u32) -> Self {
        self.max_causation_depth = Some(depth);
        self
    }

    /// Maximum number of events fetched from the feed in a single drain call.
    ///
    /// Unset: `REPLAY_READ_BATCH_SIZE`, then the built-in default (100). The runner
    /// enforces the invariant `read_batch_size ≥ checkpoint_batch_size`.
    pub fn with_read_batch_size(mut self, size: u32) -> Self {
        self.read_batch_size = Some(size);
        self
    }

    /// How many events are processed between cursor persistence calls. The cursor is
    /// also written unconditionally at the end of every drain call.
    ///
    /// Unset: `REPLAY_CHECKPOINT_BATCH_SIZE`, then the built-in default (100).
    pub fn with_checkpoint_batch_size(mut self, size: u32) -> Self {
        self.checkpoint_batch_size = Some(size);
        self
    }

    /// How long the runner awaits one [`Dispatch`] of this policy before it abandons
    /// it.
    ///
    /// Unset: `REPLAY_DISPATCH_TIMEOUT_MS`, then the built-in default (30s).
    ///
    /// Exceeding it is a **retryable** failure: the dispatch is retried under the
    /// back-off an `Unavailable` error gets, then parked as a dead letter of kind
    /// `Timeout`. Raise it for a reaction that is legitimately slow.
    ///
    /// It bounds the future the runner awaits: cancellation happens at a suspension
    /// point, so it cannot interrupt work the reaction moved onto another task, nor a
    /// command that never yields (see `CONTEXT.md`'s non-guarantees).
    pub fn with_dispatch_timeout(mut self, timeout: Duration) -> Self {
        self.dispatch_timeout = Some(timeout);
        self
    }

    pub fn stream_filter(&self) -> StreamFilter {
        self.stream_filter.clone()
    }

    pub fn start_at(&self) -> StartAt {
        self.start_at
    }

    pub fn max_causation_depth(&self) -> Option<u32> {
        self.max_causation_depth
    }

    pub fn read_batch_size(&self) -> Option<u32> {
        self.read_batch_size
    }

    pub fn checkpoint_batch_size(&self) -> Option<u32> {
        self.checkpoint_batch_size
    }

    pub fn dispatch_timeout(&self) -> Option<Duration> {
        self.dispatch_timeout
    }
}

/// One registration: the rule, and the settings the runner drives it with.
///
/// The runner works in these rather than in `dyn ErasedPolicy`, because every tunable it
/// resolves now comes from the registration rather than from the trait.
pub(crate) struct RegisteredPolicy {
    policy: Arc<dyn ErasedPolicy>,
    settings: PolicySettings,
}

impl RegisteredPolicy {
    pub(crate) fn new<P: Policy + 'static>(policy: P, settings: PolicySettings) -> Self {
        RegisteredPolicy {
            policy: Arc::new(policy),
            settings,
        }
    }

    pub(crate) fn name(&self) -> &str {
        self.policy.name()
    }

    pub(crate) fn settings(&self) -> &PolicySettings {
        &self.settings
    }

    pub(crate) fn stream_filter(&self) -> StreamFilter {
        self.settings.stream_filter()
    }

    pub(crate) fn start_at(&self) -> StartAt {
        self.settings.start_at()
    }

    pub(crate) fn react_erased(&self, raw: &PersistedEvent<serde_json::Value>) -> Vec<Dispatch> {
        self.policy.react_erased(raw)
    }
}

/// Object-safe erasure of [`Policy`], mirroring `ErasedInlineProjection`.
///
/// A [`RegisteredPolicy`] holds one of these and feeds it raw JSON events; the blanket
/// impl deserializes into the concrete `Policy::Event` and skips events that don't
/// belong to this policy (deserialize-or-skip routing).
pub(crate) trait ErasedPolicy: Send + Sync {
    fn name(&self) -> &str;

    fn react_erased(&self, raw: &PersistedEvent<serde_json::Value>) -> Vec<Dispatch>;
}

impl<P: Policy> ErasedPolicy for P {
    fn name(&self) -> &str {
        Policy::name(self)
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
                let observed: ObservedEvent<P::Event> = raw.observed_with_data(event);
                self.react(&observed)
            }
            Err(_) => Vec::new(),
        }
    }
}

/// A [`Policy`] backed by a plain closure, created via
/// [`PolicyRunnerBuilder::register_policy_fn`](crate::PolicyRunnerBuilder::register_policy_fn).
pub(crate) struct ClosurePolicy<E, F> {
    pub(crate) name: String,
    pub(crate) react: F,
    pub(crate) _phantom: std::marker::PhantomData<E>,
}

impl<E, F> Policy for ClosurePolicy<E, F>
where
    E: Event + 'static,
    F: Fn(&ObservedEvent<E>) -> Vec<Dispatch> + Send + Sync + 'static,
{
    type Event = E;

    fn name(&self) -> &str {
        &self.name
    }

    fn react(&self, event: &ObservedEvent<Self::Event>) -> Vec<Dispatch> {
        (self.react)(event)
    }
}

/// The per-reaction metadata a typed closure registration may carry, erased so one
/// struct serves a registration with the hook and one without.
type DispatchMetadataFn<E> = Box<dyn Fn(&ObservedEvent<E>) -> Option<Metadata> + Send + Sync>;

/// An [`AggregatePolicy`] backed by a closure, created via
/// [`PolicyRunnerBuilder::register_aggregate_policy_fn`](crate::PolicyRunnerBuilder::register_aggregate_policy_fn).
///
/// The typed twin of [`ClosurePolicy`]: the closure returns `(StreamId, Command)` pairs
/// for the declared target instead of building [`Dispatch`]es, so it is asserted by
/// equality. `Policy` arrives through the blanket impl in `es-replay`, which is what
/// lets [`register_policy`](crate::PolicyRunnerBuilder::register_policy) take one.
pub(crate) struct AggregateClosurePolicy<E, A, F> {
    pub(crate) name: String,
    pub(crate) react: F,
    pub(crate) dispatch_metadata: Option<DispatchMetadataFn<E>>,
    // `fn() -> (E, A)`, not `(E, A)`: the phantom must not ask the aggregate or its
    // event to be `Send`/`Sync` for the policy to be.
    pub(crate) _phantom: std::marker::PhantomData<fn() -> (E, A)>,
}

impl<E, A, F> AggregatePolicy for AggregateClosurePolicy<E, A, F>
where
    E: Event + 'static,
    A: Aggregate + 'static,
    A::StreamId: 'static,
    A::Command: 'static,
    F: Fn(&ObservedEvent<E>) -> Vec<(A::StreamId, A::Command)> + Send + Sync + 'static,
{
    type Event = E;
    type Target = A;

    fn name(&self) -> &str {
        &self.name
    }

    fn react(&self, event: &ObservedEvent<Self::Event>) -> Vec<(A::StreamId, A::Command)> {
        (self.react)(event)
    }

    fn dispatch_metadata(&self, event: &ObservedEvent<Self::Event>) -> Option<Metadata> {
        self.dispatch_metadata
            .as_ref()
            .and_then(|metadata| metadata(event))
    }
}

#[cfg(test)]
mod tests {
    use std::any::TypeId;

    use chrono::Utc;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use urn::{Urn, UrnBuilder};
    use uuid::Uuid;

    use replay::{Aggregate, Metadata, WithId};
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

        fn react(&self, event: &ObservedEvent<Self::Event>) -> Vec<Dispatch> {
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

        fn react(&self, _event: &ObservedEvent<Self::Event>) -> Vec<Dispatch> {
            panic!("react must not be called for a non-matching payload");
        }
    }

    fn raw_frozen() -> PersistedEvent<serde_json::Value> {
        PersistedEvent {
            id: Uuid::new_v4(),
            r#type: "Frozen".to_string(),
            version: 7,
            aggregate_version: None,
            observed: ObservedEvent {
                data: json!({ "Frozen": { "reason": "fraud-review" } }),
                stream_id: UrnBuilder::new("account", "42").build().unwrap(),
                metadata: Metadata::from_json(json!({ "correlation": "c-1" })),
                created: Utc::now(),
            },
        }
    }

    /// A payload of the policy's own event type reaches `react`, and the dispatch it
    /// returns carries the stream the event was read from. The store's identity and
    /// position do not cross the erasure at all — they are not on `ObservedEvent`.
    #[test]
    fn reacts_to_a_matching_payload_carrying_its_stream_into_the_dispatch() {
        let raw = raw_frozen();

        let dispatches = FreezeNotifier.react_erased(&raw);

        assert_eq!(
            dispatches.len(),
            1,
            "matching payload must produce one command"
        );
        assert_eq!(dispatches[0].target(), TypeId::of::<Account>());

        // The identity a parked dead letter is read by: the dispatch's own target
        // stream and command type, taken before either is erased.
        assert_eq!(dispatches[0].target_stream_id(), &raw.stream_id);
        assert_eq!(
            dispatches[0].command_name(),
            std::any::type_name::<<Account as Aggregate>::Command>()
        );

        // The command was built from the deserialized payload and the stream id the
        // rule read, so both survived the erasure.
        let (id, command) = dispatches[0]
            .parts::<Account>()
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

    /// The four fields a rule may read are the four it is handed; the store's identity
    /// and position stay behind on the persisted envelope.
    #[test]
    fn the_erasure_hands_over_the_observed_half_only() {
        let raw = raw_frozen();

        let observed = raw.observed_with_data(AccountEvent::Frozen {
            reason: "fraud-review".to_string(),
        });

        assert_eq!(observed.stream_id, raw.stream_id);
        assert_eq!(observed.metadata, raw.metadata);
        assert_eq!(observed.created, raw.created);
    }

    /// Defaults first: an unconfigured registration resolves every tunable elsewhere.
    #[test]
    fn settings_default_to_the_runner_wide_resolution() {
        let settings = PolicySettings::new();

        assert_eq!(settings.start_at(), StartAt::Now);
        assert_eq!(settings.max_causation_depth(), None);
        assert_eq!(settings.read_batch_size(), None);
        assert_eq!(settings.checkpoint_batch_size(), None);
        assert_eq!(settings.dispatch_timeout(), None);
        assert_eq!(settings.stream_filter(), StreamFilter::all());
    }

    /// Every knob the trait used to carry is reachable from a registration — which is
    /// what a closure policy could not do at all.
    #[test]
    fn settings_carry_every_knob_the_trait_used_to() {
        let settings = PolicySettings::new()
            .with_stream_filter(StreamFilter::for_stream_type::<Account>())
            .starting_at(StartAt::Beginning)
            .with_max_causation_depth(3)
            .with_read_batch_size(500)
            .with_checkpoint_batch_size(50)
            .with_dispatch_timeout(Duration::from_secs(90));

        assert_eq!(settings.start_at(), StartAt::Beginning);
        assert_eq!(settings.max_causation_depth(), Some(3));
        assert_eq!(settings.read_batch_size(), Some(500));
        assert_eq!(settings.checkpoint_batch_size(), Some(50));
        assert_eq!(settings.dispatch_timeout(), Some(Duration::from_secs(90)));
        assert_eq!(
            settings.stream_filter(),
            StreamFilter::for_stream_type::<Account>()
        );
    }

    /// The typed closure registration's rule: the closure returns pairs, the blanket
    /// impl addresses them to the declared target, and the metadata hook stamps the
    /// reaction — all of it reached through the runner's erasure, from raw JSON.
    #[test]
    fn a_typed_closure_policy_dispatches_to_its_declared_target() {
        let policy = AggregateClosurePolicy::<AccountEvent, Account, _> {
            name: "typed_freeze_notifier".to_string(),
            react: |event: &ObservedEvent<AccountEvent>| {
                let AccountEvent::Frozen { reason } = &event.data;
                vec![(AccountUrn(event.stream_id.clone()), reason.clone())]
            },
            dispatch_metadata: Some(Box::new(|event: &ObservedEvent<AccountEvent>| {
                Some(Metadata::from_json(
                    json!({ "frozen_stream": event.stream_id.to_string() }),
                ))
            })),
            _phantom: std::marker::PhantomData,
        };
        let raw = raw_frozen();

        let dispatches = policy.react_erased(&raw);

        assert_eq!(Policy::name(&policy), "typed_freeze_notifier");
        assert_eq!(dispatches.len(), 1);
        assert_eq!(dispatches[0].target(), TypeId::of::<Account>());
        assert_eq!(
            dispatches[0].parts::<Account>(),
            Some((
                &AccountUrn(raw.stream_id.clone()),
                &"fraud-review".to_string()
            ))
        );
        assert_eq!(
            dispatches[0].metadata(),
            Some(&Metadata::from_json(
                json!({ "frozen_stream": raw.stream_id.to_string() })
            ))
        );
    }

    /// Without the hook a typed closure dispatches exactly what an untyped one does.
    #[test]
    fn a_typed_closure_policy_without_the_hook_attaches_no_metadata() {
        let policy = AggregateClosurePolicy::<AccountEvent, Account, _> {
            name: "bare_freeze_notifier".to_string(),
            react: |event: &ObservedEvent<AccountEvent>| {
                let AccountEvent::Frozen { reason } = &event.data;
                vec![(AccountUrn(event.stream_id.clone()), reason.clone())]
            },
            dispatch_metadata: None,
            _phantom: std::marker::PhantomData,
        };

        let dispatches = policy.react_erased(&raw_frozen());

        assert!(dispatches[0].metadata().is_none());
    }

    /// Deserialize-or-skip routing is the blanket impl's, so a typed policy inherits it:
    /// a payload of another event type produces no reaction and never runs the closure.
    #[test]
    fn a_typed_closure_policy_skips_a_non_matching_payload() {
        let policy = AggregateClosurePolicy::<ShippingEvent, Account, _> {
            name: "typed_shipping_notifier".to_string(),
            react: |_: &ObservedEvent<ShippingEvent>| {
                panic!("react must not be called for a non-matching payload")
            },
            dispatch_metadata: None,
            _phantom: std::marker::PhantomData,
        };

        assert!(policy.react_erased(&raw_frozen()).is_empty());
    }
}
