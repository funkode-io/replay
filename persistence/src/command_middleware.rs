//! Command middleware: code a consumer registers once on [`Cqrs`](crate::Cqrs) to run
//! around every command it executes
//! ([ADR-0035](https://github.com/funkode-io/replay/blob/main/docs/adr/0035-a-command-passes-through-middleware-before-it-runs-and-before-it-commits.md)).

use std::future::ready;

use futures::future::BoxFuture;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use urn::Urn;

/// A middleware's answer: let the command go on, or stop it.
#[derive(Debug)]
pub enum Next {
    /// Go on: the next middleware runs, then the command.
    Continue,
    /// Not now; the same command may succeed later. Not a failure.
    ///
    /// The caller of [`Cqrs::execute`](crate::Cqrs::execute) sees a `replay::Error` of kind
    /// [`Busy`](replay::ErrorKind::Busy) carrying this reason as its message.
    Busy(String),
    /// The command is refused or the middleware failed. The caller sees this error as it
    /// is: a `Forbidden` refusal stays `Forbidden`.
    Error(replay::Error),
}

/// How a command finished, as [`CommandMiddleware::after`] sees it.
#[derive(Debug)]
pub enum Outcome<'a> {
    /// The append committed.
    Committed,
    /// A middleware answered [`Next::Busy`] with this reason.
    Busy(&'a str),
    /// The command failed, with this error.
    Failed(&'a (dyn std::error::Error + Send + Sync)),
}

/// Code that runs around every command a [`Cqrs`](crate::Cqrs) executes.
///
/// A middleware has three hooks, each defaulting to doing nothing, so one implements only
/// the hooks it needs:
///
/// - [`before`](CommandMiddleware::before) runs before the aggregate is loaded. It may stop
///   the command or add to its metadata.
/// - [`before_commit`](CommandMiddleware::before_commit) runs at the commit gate, just
///   before the append commits. It may roll the append back.
/// - [`after`](CommandMiddleware::after) runs once the command has finished. It only
///   observes.
///
/// The trait is object-safe, so middlewares of different types register on one `Cqrs`
/// with [`with_middleware`](crate::Cqrs::with_middleware). They run in registration
/// order. The first that does not answer [`Next::Continue`] stops the command, and later
/// ones do not run. A middleware runs on every command `Cqrs` executes, an application's
/// and a Policy's alike, and on nothing that is not a command: a direct append through
/// the store, compaction, stream deletion, a rebuild or a catch-up never reach it
/// ([ADR-0035](https://github.com/funkode-io/replay/blob/main/docs/adr/0035-a-command-passes-through-middleware-before-it-runs-and-before-it-commits.md)).
///
/// ```rust,ignore
/// use std::sync::atomic::{AtomicBool, Ordering};
/// use futures::future::BoxFuture;
/// use replay_persistence::{CommandContext, CommandMiddleware, Next};
///
/// /// Refuses every command while maintenance mode is on.
/// struct Maintenance(AtomicBool);
///
/// impl CommandMiddleware for Maintenance {
///     fn before<'a>(&'a self, _: &'a mut CommandContext) -> BoxFuture<'a, Next> {
///         Box::pin(async move {
///             if self.0.load(Ordering::SeqCst) {
///                 Next::Busy("maintenance in progress".to_string())
///             } else {
///                 Next::Continue
///             }
///         })
///     }
/// }
///
/// let cqrs = Cqrs::new(store).with_middleware(Maintenance(AtomicBool::new(false)));
/// ```
pub trait CommandMiddleware: Send + Sync {
    /// Runs before the aggregate is loaded.
    ///
    /// Anything but [`Next::Continue`] stops the command: nothing is loaded, handled or
    /// appended, and later middlewares do not run. The context is mutable so a middleware
    /// can add to the command's metadata; later middlewares see what it added, and the
    /// appended events carry it.
    fn before<'a>(&'a self, context: &'a mut CommandContext) -> BoxFuture<'a, Next> {
        let _ = context;
        Box::pin(ready(Next::Continue))
    }

    /// Runs at the commit gate: inside the append transaction, after the events are
    /// inserted and every inline projection has run, immediately before the commit.
    ///
    /// Anything but [`Next::Continue`] rolls the append back.
    ///
    /// Not yet called: `Cqrs` passes the commit gate to the store in a later release
    /// ([#313](https://github.com/funkode-io/replay/issues/313)).
    fn before_commit<'a>(&'a self, context: &'a CommandContext) -> BoxFuture<'a, Next> {
        let _ = context;
        Box::pin(ready(Next::Continue))
    }

    /// Runs once the command has finished, whatever its outcome. It cannot change it.
    ///
    /// Not yet called: `Cqrs` runs it in a later release
    /// ([#314](https://github.com/funkode-io/replay/issues/314)).
    fn after<'a>(
        &'a self,
        context: &'a CommandContext,
        outcome: &'a Outcome<'a>,
    ) -> BoxFuture<'a, ()> {
        let _ = (context, outcome);
        Box::pin(ready(()))
    }
}

/// What a middleware knows about the command it runs around.
///
/// A middleware is registered once for every aggregate, so it does not see the command's
/// payload: a rule that must decide on a command's content decides on its metadata, which
/// is where a consumer puts what such rules need.
#[derive(Debug, Clone)]
pub struct CommandContext {
    aggregate_type: &'static str,
    stream_type: String,
    stream_id: Urn,
    command_type: &'static str,
    metadata: replay::Metadata,
    causation: Option<Causation>,
}

impl CommandContext {
    /// The context of a command of aggregate `A` addressed to `stream_id`, carrying
    /// `metadata`.
    ///
    /// [`Cqrs`](crate::Cqrs) builds one per command; it is public so a consumer can test a
    /// middleware without a store. The [`causation`](Self::causation) is read from the
    /// metadata, where the Policy runner stamps it.
    pub fn new<A: replay::Aggregate>(stream_id: &A::StreamId, metadata: replay::Metadata) -> Self {
        Self {
            aggregate_type: std::any::type_name::<A>(),
            stream_type: A::stream_type(),
            stream_id: stream_id.clone().into(),
            command_type: std::any::type_name::<A::Command>(),
            causation: Causation::from_metadata(&metadata),
            metadata,
        }
    }

    /// The aggregate's Rust type name, as [`std::any::type_name`] spells it.
    pub fn aggregate_type(&self) -> &'static str {
        self.aggregate_type
    }

    /// The stream type the command's events are appended under.
    pub fn stream_type(&self) -> &str {
        &self.stream_type
    }

    /// The stream the command is addressed to.
    pub fn stream_id(&self) -> &Urn {
        &self.stream_id
    }

    /// The command's Rust type name, as [`std::any::type_name`] spells it.
    pub fn command_type(&self) -> &'static str {
        self.command_type
    }

    /// The metadata the appended events will carry, with what earlier middlewares added.
    pub fn metadata(&self) -> &replay::Metadata {
        &self.metadata
    }

    /// The [Causation] when a Policy dispatched the command; `None` for an application's
    /// command.
    ///
    /// Read once, from the metadata the command arrived with.
    ///
    /// [Causation]: https://github.com/funkode-io/replay/blob/main/CONTEXT.md#causation
    pub fn causation(&self) -> Option<&Causation> {
        self.causation.as_ref()
    }

    /// Whether this is still the command `arrived` describes: the same aggregate, stream
    /// and command, carrying the same `causation` key. Only the rest of the metadata may
    /// differ.
    pub(crate) fn is_the_command(&self, arrived: &CommandContext) -> bool {
        self.aggregate_type == arrived.aggregate_type
            && self.stream_type == arrived.stream_type
            && self.stream_id == arrived.stream_id
            && self.command_type == arrived.command_type
            && self.causation == arrived.causation
            && self.metadata.as_json().get(CAUSATION_KEY)
                == arrived.metadata.as_json().get(CAUSATION_KEY)
    }

    /// Replace the command's metadata.
    ///
    /// The `causation` key is the Policy runner's, and bounds how deep one event may
    /// cascade: the replacement must carry it exactly as the command arrived with it, or
    /// not at all when it arrived without one. Fails with
    /// [`InvalidInput`](replay::ErrorKind::InvalidInput), leaving the metadata as it was,
    /// when it does not.
    pub fn set_metadata(&mut self, metadata: replay::Metadata) -> Result<(), replay::Error> {
        if metadata.as_json().get(CAUSATION_KEY) != self.metadata.as_json().get(CAUSATION_KEY) {
            return Err(causation_is_reserved("set_metadata"));
        }
        self.metadata = metadata;
        Ok(())
    }

    /// Add `key` to the command's metadata, replacing any value it had.
    ///
    /// Metadata that is `null` (the default) becomes an object holding `key`. Fails with
    /// [`InvalidInput`](replay::ErrorKind::InvalidInput), leaving the metadata as it was,
    /// when `key` is `causation` (the Policy runner's), when the metadata is another
    /// non-object value, or when `value` does not serialize to JSON.
    pub fn insert_metadata(
        &mut self,
        key: impl Into<String>,
        value: impl Serialize,
    ) -> Result<(), replay::Error> {
        let key = key.into();
        if key == CAUSATION_KEY {
            return Err(causation_is_reserved("insert_metadata"));
        }
        let value = serde_json::to_value(value).map_err(|e| {
            replay::Error::invalid_input("command metadata value does not serialize to JSON")
                .with_operation("insert_metadata")
                .with_source(e)
        })?;
        let mut object = match Value::from(std::mem::take(&mut self.metadata)) {
            Value::Object(object) => object,
            Value::Null => Map::new(),
            other => {
                self.metadata = replay::Metadata::from_json(other);
                return Err(replay::Error::invalid_input(
                    "command metadata is not a JSON object, so a key cannot be added to it",
                )
                .with_operation("insert_metadata"));
            }
        };
        object.insert(key, value);
        self.metadata = replay::Metadata::from_json(Value::Object(object));
        Ok(())
    }
}

/// The link from the event that triggered a Policy to the command the Policy dispatched,
/// as the runner stamps it under the `causation` key of the command's metadata.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Causation {
    pub(crate) policy: String,
    pub(crate) event_id: String,
    pub(crate) stream_id: String,
    pub(crate) global_position: i64,
    /// How many policy hops deep this event is. Root events (from normal `append`) carry
    /// no causation block and are treated as depth 0.
    #[serde(default)]
    pub(crate) depth: u32,
}

/// The metadata key the runner stamps a [`Causation`] under.
const CAUSATION_KEY: &str = "causation";

/// Why a middleware may not change the `causation` key: the runner reads the depth it
/// carries to stop a cyclic Policy, so erasing it would reset that depth to zero.
fn causation_is_reserved(operation: &'static str) -> replay::Error {
    replay::Error::invalid_input(
        "command metadata key `causation` is the Policy runner's and a middleware cannot change it",
    )
    .with_operation(operation)
}

impl Causation {
    /// The causation stamped in `metadata`, if any.
    pub(crate) fn from_metadata(metadata: &replay::Metadata) -> Option<Self> {
        metadata
            .as_json()
            .get(CAUSATION_KEY)
            .and_then(|v| Causation::deserialize(v).ok())
    }

    /// The metadata a command dispatched for this causation carries.
    pub(crate) fn into_metadata(self) -> replay::Metadata {
        let mut object = Map::new();
        object.insert(
            CAUSATION_KEY.to_string(),
            serde_json::to_value(self).expect("a causation always serializes"),
        );
        replay::Metadata::from_json(Value::Object(object))
    }

    /// The name of the Policy that dispatched the command.
    pub fn policy(&self) -> &str {
        &self.policy
    }

    /// The id of the event the Policy reacted to.
    pub fn event_id(&self) -> &str {
        &self.event_id
    }

    /// The stream of the event the Policy reacted to.
    pub fn stream_id(&self) -> &str {
        &self.stream_id
    }

    /// The global position of the event the Policy reacted to.
    pub fn global_position(&self) -> i64 {
        self.global_position
    }

    /// How many Policy hops deep the command is: 1 for a reaction to an application's
    /// event.
    pub fn depth(&self) -> u32 {
        self.depth
    }
}

#[cfg(test)]
mod tests {
    //! `Cqrs::execute` on the in-memory store: what a caller gets back and what the store
    //! holds afterwards, never the order of internal calls.

    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};

    use futures::TryStreamExt;
    use replay::{Aggregate, ErrorKind, Metadata, WithId};
    use replay_macros::Event;
    use serde::{Deserialize, Serialize};
    use serde_json::json;
    use urn::{Urn, UrnBuilder};

    use super::*;
    use crate::{AggregateVersion, Cqrs, EventStore, InMemoryEventStore, PersistedEvent};

    #[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Event)]
    enum CounterEvent {
        Added { amount: i64 },
    }

    #[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
    struct CounterUrn(Urn);

    impl From<CounterUrn> for Urn {
        fn from(urn: CounterUrn) -> Self {
            urn.0
        }
    }

    impl TryFrom<Urn> for CounterUrn {
        type Error = String;

        fn try_from(urn: Urn) -> Result<Self, Self::Error> {
            Ok(CounterUrn(urn))
        }
    }

    struct Counter {
        id: CounterUrn,
        total: i64,
    }

    impl WithId for Counter {
        type StreamId = CounterUrn;

        fn with_id(id: Self::StreamId) -> Self {
            Counter { id, total: 0 }
        }

        fn get_id(&self) -> &Self::StreamId {
            &self.id
        }
    }

    impl replay::EventStream for Counter {
        type Event = CounterEvent;

        fn stream_type() -> String {
            "Counter".to_string()
        }

        fn apply(&mut self, event: Self::Event) {
            let CounterEvent::Added { amount } = event;
            self.total += amount;
        }
    }

    struct Add(i64);

    /// Whether the command handler ran: a stopped command must never reach it.
    #[derive(Default)]
    struct Handled(AtomicBool);

    impl Aggregate for Counter {
        type Command = Add;
        type Error = replay::Error;
        type Services = Handled;

        async fn handle(
            &self,
            command: Self::Command,
            services: &Self::Services,
        ) -> Result<Vec<Self::Event>, Self::Error> {
            services.0.store(true, Ordering::SeqCst);
            Ok(vec![CounterEvent::Added { amount: command.0 }])
        }
    }

    fn counter() -> CounterUrn {
        CounterUrn(UrnBuilder::new("counter", "1").build().unwrap())
    }

    /// What each middleware saw in `before`, in the order they ran.
    type Log = Arc<Mutex<Vec<(&'static str, CommandContext)>>>;

    /// Records the context it sees, then gives the answer it was built with.
    struct Answering {
        name: &'static str,
        log: Log,
        answer: fn() -> Next,
    }

    impl CommandMiddleware for Answering {
        fn before<'a>(&'a self, context: &'a mut CommandContext) -> BoxFuture<'a, Next> {
            Box::pin(async move {
                self.log.lock().unwrap().push((self.name, context.clone()));
                (self.answer)()
            })
        }
    }

    fn answering(name: &'static str, log: &Log, answer: fn() -> Next) -> Answering {
        Answering {
            name,
            log: log.clone(),
            answer,
        }
    }

    /// Adds the acting user to the command's metadata.
    struct AddsTheUser;

    impl CommandMiddleware for AddsTheUser {
        fn before<'a>(&'a self, context: &'a mut CommandContext) -> BoxFuture<'a, Next> {
            Box::pin(async move {
                match context.insert_metadata("user", "alice") {
                    Ok(()) => Next::Continue,
                    Err(error) => Next::Error(error),
                }
            })
        }
    }

    async fn appended(cqrs: &Cqrs<InMemoryEventStore>) -> Vec<PersistedEvent<CounterEvent>> {
        cqrs.store()
            .stream_events_by_stream_id::<Counter>(&counter(), AggregateVersion::Latest, None, None)
            .try_collect()
            .await
            .unwrap()
    }

    fn ran(log: &Log) -> Vec<&'static str> {
        log.lock().unwrap().iter().map(|(name, _)| *name).collect()
    }

    #[tokio::test]
    async fn a_busy_middleware_stops_the_command_before_it_runs() {
        let log = Log::default();
        let cqrs = Cqrs::new(InMemoryEventStore::new()).with_middleware(answering(
            "maintenance",
            &log,
            || Next::Busy("maintenance in progress".to_string()),
        ));
        let handled = Handled::default();

        let error = cqrs
            .execute::<Counter>(&counter(), Metadata::default(), Add(1), &handled, None)
            .await
            .err()
            .expect("a busy middleware stops the command");

        assert_eq!(error.kind(), ErrorKind::Busy);
        assert!(error.is_temporary());
        assert_eq!(error.to_string(), "maintenance in progress");
        assert!(!handled.0.load(Ordering::SeqCst), "the handler ran");
        assert!(appended(&cqrs).await.is_empty());
    }

    #[tokio::test]
    async fn a_middleware_error_reaches_the_caller_unchanged() {
        let log = Log::default();
        let cqrs =
            Cqrs::new(InMemoryEventStore::new()).with_middleware(answering("auth", &log, || {
                Next::Error(
                    replay::Error::permanent(ErrorKind::Forbidden, "alice may not add")
                        .with_operation("authorize")
                        .with_context("user", "alice"),
                )
            }));
        let handled = Handled::default();

        let error = cqrs
            .execute::<Counter>(&counter(), Metadata::default(), Add(1), &handled, None)
            .await
            .err()
            .expect("a refusing middleware stops the command");

        assert_eq!(error.kind(), ErrorKind::Forbidden);
        assert!(error.is_permanent());
        assert_eq!(error.to_string(), "alice may not add");
        assert_eq!(error.operation(), "authorize");
        assert_eq!(error.context(), &[("user", "alice".to_string())]);
        assert!(!handled.0.load(Ordering::SeqCst), "the handler ran");
        assert!(appended(&cqrs).await.is_empty());
    }

    #[tokio::test]
    async fn the_first_middleware_to_stop_the_command_is_the_last_to_run() {
        let log = Log::default();
        let cqrs = Cqrs::new(InMemoryEventStore::new())
            .with_middleware(answering("first", &log, || {
                Next::Busy("not now".to_string())
            }))
            .with_middleware(answering("second", &log, || Next::Continue));

        let result = cqrs
            .execute::<Counter>(
                &counter(),
                Metadata::default(),
                Add(1),
                &Handled::default(),
                None,
            )
            .await;

        assert_eq!(result.err().map(|e| e.kind()), Some(ErrorKind::Busy));
        assert_eq!(ran(&log), vec!["first"]);
    }

    #[tokio::test]
    async fn middlewares_that_continue_run_in_registration_order_and_the_command_commits() {
        let log = Log::default();
        let cqrs = Cqrs::new(InMemoryEventStore::new())
            .with_middleware(answering("first", &log, || Next::Continue))
            .with_middleware(answering("second", &log, || Next::Continue));

        let counted = cqrs
            .execute::<Counter>(
                &counter(),
                Metadata::default(),
                Add(3),
                &Handled::default(),
                None,
            )
            .await
            .unwrap();

        assert_eq!(counted.total, 3);
        assert_eq!(ran(&log), vec!["first", "second"]);
        assert_eq!(appended(&cqrs).await.len(), 1);
    }

    #[tokio::test]
    async fn metadata_a_middleware_adds_is_seen_by_the_next_and_carried_by_the_events() {
        let log = Log::default();
        let cqrs = Cqrs::new(InMemoryEventStore::new())
            .with_middleware(AddsTheUser)
            .with_middleware(answering("rule", &log, || Next::Continue));

        cqrs.execute::<Counter>(
            &counter(),
            Metadata::new(json!({ "tenant": "acme" })),
            Add(1),
            &Handled::default(),
            None,
        )
        .await
        .unwrap();

        let seen = log.lock().unwrap()[0].1.metadata().to_json();
        assert_eq!(seen, json!({ "tenant": "acme", "user": "alice" }));
        let events = appended(&cqrs).await;
        assert_eq!(events.len(), 1);
        assert_eq!(
            events[0].metadata.to_json(),
            json!({ "tenant": "acme", "user": "alice" })
        );
    }

    #[tokio::test]
    async fn the_context_names_the_aggregate_the_stream_and_the_command() {
        let log = Log::default();
        let cqrs =
            Cqrs::new(InMemoryEventStore::new())
                .with_middleware(answering("probe", &log, || Next::Continue));

        cqrs.execute::<Counter>(
            &counter(),
            Metadata::default(),
            Add(1),
            &Handled::default(),
            None,
        )
        .await
        .unwrap();

        let context = log.lock().unwrap()[0].1.clone();
        assert_eq!(context.aggregate_type(), std::any::type_name::<Counter>());
        assert_eq!(context.stream_type(), "Counter");
        assert_eq!(context.stream_id(), &Urn::from(counter()));
        assert_eq!(context.command_type(), std::any::type_name::<Add>());
    }

    #[tokio::test]
    async fn an_application_command_has_no_causation_and_a_policy_command_shows_its_own() {
        let log = Log::default();
        let cqrs =
            Cqrs::new(InMemoryEventStore::new())
                .with_middleware(answering("probe", &log, || Next::Continue));
        // The metadata the Policy runner stamps on every command it dispatches.
        let stamped = stamped();

        for metadata in [Metadata::default(), stamped.clone().into_metadata()] {
            cqrs.execute::<Counter>(&counter(), metadata, Add(1), &Handled::default(), None)
                .await
                .unwrap();
        }

        let log = log.lock().unwrap();
        assert_eq!(log[0].1.causation(), None);
        assert_eq!(log[1].1.causation(), Some(&stamped));
    }

    #[tokio::test]
    async fn middlewares_of_different_types_share_one_cqrs_and_its_clones() {
        let log = Log::default();
        let cqrs = Cqrs::new(InMemoryEventStore::new())
            .with_middleware(AddsTheUser)
            .with_middleware(answering("rule", &log, || Next::Continue));
        let clone = cqrs.clone();

        clone
            .execute::<Counter>(
                &counter(),
                Metadata::default(),
                Add(1),
                &Handled::default(),
                None,
            )
            .await
            .unwrap();

        assert_eq!(ran(&log), vec!["rule"]);
        assert_eq!(
            appended(&cqrs).await[0].metadata.to_json(),
            json!({ "user": "alice" })
        );
    }

    #[tokio::test]
    async fn a_cqrs_without_middleware_executes_as_before() {
        let cqrs = Cqrs::new(InMemoryEventStore::new());
        let handled = Handled::default();

        let counted = cqrs
            .execute::<Counter>(
                &counter(),
                Metadata::new(json!({ "tenant": "acme" })),
                Add(2),
                &handled,
                None,
            )
            .await
            .unwrap();

        assert_eq!(counted.total, 2);
        assert!(handled.0.load(Ordering::SeqCst));
        let events = appended(&cqrs).await;
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].metadata.to_json(), json!({ "tenant": "acme" }));
    }

    /// Tries to erase and to overwrite the Policy runner's causation stamp, ignores the
    /// refusals, and lets the command go on.
    struct TampersWithCausation;

    impl CommandMiddleware for TampersWithCausation {
        fn before<'a>(&'a self, context: &'a mut CommandContext) -> BoxFuture<'a, Next> {
            Box::pin(async move {
                let erased = context.set_metadata(Metadata::new(json!({ "user": "mallory" })));
                let overwritten = context.insert_metadata("causation", json!({ "depth": 0 }));
                assert_eq!(erased.unwrap_err().kind(), ErrorKind::InvalidInput);
                assert_eq!(overwritten.unwrap_err().kind(), ErrorKind::InvalidInput);
                Next::Continue
            })
        }
    }

    fn stamped() -> Causation {
        Causation {
            policy: "notifier".to_string(),
            event_id: "e-1".to_string(),
            stream_id: "urn:account:42".to_string(),
            global_position: 7,
            depth: 3,
        }
    }

    #[tokio::test]
    async fn a_middleware_cannot_change_the_causation_a_policy_command_carries() {
        let cqrs = Cqrs::new(InMemoryEventStore::new()).with_middleware(TampersWithCausation);

        cqrs.execute::<Counter>(
            &counter(),
            stamped().into_metadata(),
            Add(1),
            &Handled::default(),
            None,
        )
        .await
        .unwrap();

        let events = appended(&cqrs).await;
        assert_eq!(events[0].metadata, stamped().into_metadata());
    }

    /// Replaces the whole context with a fresh one: an application's command to another
    /// stream, without the causation the command arrived with.
    struct ReplacesTheContext;

    impl CommandMiddleware for ReplacesTheContext {
        fn before<'a>(&'a self, context: &'a mut CommandContext) -> BoxFuture<'a, Next> {
            Box::pin(async move {
                let elsewhere = CounterUrn(UrnBuilder::new("counter", "2").build().unwrap());
                *context = CommandContext::new::<Counter>(&elsewhere, Metadata::default());
                Next::Continue
            })
        }
    }

    #[tokio::test]
    async fn a_middleware_that_replaces_the_whole_context_fails_the_command() {
        let log = Log::default();
        let cqrs = Cqrs::new(InMemoryEventStore::new())
            .with_middleware(ReplacesTheContext)
            .with_middleware(answering("later", &log, || Next::Continue));
        let handled = Handled::default();

        let error = cqrs
            .execute::<Counter>(
                &counter(),
                stamped().into_metadata(),
                Add(1),
                &handled,
                None,
            )
            .await
            .err()
            .expect("a replaced context stops the command");

        assert_eq!(error.kind(), ErrorKind::Internal);
        assert!(
            ran(&log).is_empty(),
            "a later middleware saw the replaced context"
        );
        assert!(!handled.0.load(Ordering::SeqCst), "the handler ran");
        assert!(appended(&cqrs).await.is_empty());
    }

    #[test]
    fn metadata_may_be_replaced_when_the_causation_is_kept() {
        let mut context = CommandContext::new::<Counter>(&counter(), stamped().into_metadata());
        let mut kept = stamped().into_metadata().to_json();
        kept["user"] = json!("alice");

        context
            .set_metadata(Metadata::from_json(kept.clone()))
            .unwrap();

        assert_eq!(context.metadata().to_json(), kept);
    }

    #[test]
    fn an_application_command_cannot_be_given_a_causation() {
        let mut context = CommandContext::new::<Counter>(&counter(), Metadata::default());

        let set = context.set_metadata(stamped().into_metadata());
        let inserted = context.insert_metadata("causation", stamped());

        assert_eq!(set.unwrap_err().kind(), ErrorKind::InvalidInput);
        assert_eq!(inserted.unwrap_err().kind(), ErrorKind::InvalidInput);
        assert_eq!(context.metadata(), &Metadata::default());
        assert_eq!(context.causation(), None);
    }

    #[test]
    fn metadata_that_is_not_an_object_takes_no_key_and_is_left_as_it_was() {
        let mut context =
            CommandContext::new::<Counter>(&counter(), Metadata::from_json(json!("a string")));

        let error = context.insert_metadata("user", "alice").unwrap_err();

        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(context.metadata().to_json(), json!("a string"));
    }
}
