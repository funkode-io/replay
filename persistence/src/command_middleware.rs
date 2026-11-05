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

impl Next {
    /// `Ok` on [`Continue`](Next::Continue); otherwise the error a caller sees: a
    /// `replay::Error` of kind [`Busy`](replay::ErrorKind::Busy) carrying the reason, or
    /// the middleware's error as it is.
    ///
    /// What a store returns when its [`CommitGate`](crate::CommitGate) does not continue.
    pub fn into_result(self) -> Result<(), replay::Error> {
        match self {
            Next::Continue => Ok(()),
            Next::Busy(reason) => Err(replay::Error::busy(reason)),
            Next::Error(error) => Err(error),
        }
    }
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
/// use replay_persistence::{CommandContextMut, CommandMiddleware, Next};
///
/// /// Refuses every command while maintenance mode is on.
/// struct Maintenance(AtomicBool);
///
/// impl CommandMiddleware for Maintenance {
///     fn before<'a>(&'a self, _: CommandContextMut<'a>) -> BoxFuture<'a, Next> {
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
    /// appended, and later middlewares do not run. Through the [`CommandContextMut`] a
    /// middleware reads the context and adds to the command's metadata; later middlewares
    /// see what it added, and the appended events carry it. It can change nothing else.
    fn before<'a>(&'a self, context: CommandContextMut<'a>) -> BoxFuture<'a, Next> {
        let _ = context;
        Box::pin(ready(Next::Continue))
    }

    /// Runs at the commit gate: inside the append transaction, after the events are
    /// inserted and every inline projection has run, immediately before the commit.
    ///
    /// Anything but [`Next::Continue`] rolls the append back, inline projection writes
    /// included, and later middlewares do not run: the caller of
    /// [`Cqrs::execute`](crate::Cqrs::execute) sees it as `before` would make it see it.
    /// The context is the one `before` left, read-only. A middleware needs no transaction
    /// handle, so it works on every store.
    fn before_commit<'a>(&'a self, context: &'a CommandContext) -> BoxFuture<'a, Next> {
        let _ = context;
        Box::pin(ready(Next::Continue))
    }

    /// Runs once the command has finished, whatever its outcome. It cannot change it: it
    /// is the hook for auditing and metrics.
    ///
    /// It runs only when [`Cqrs::execute`](crate::Cqrs::execute) runs to its end. If the
    /// call is dropped before then (a caller's timeout, or the Policy runner's dispatch
    /// timeout), or the command panics, no `after` runs. Something `before` took that must be
    /// given back is given back from a guard's `Drop`, not only from here.
    ///
    /// It runs in reverse registration order, and only for the middlewares whose
    /// [`before`](CommandMiddleware::before) answered [`Next::Continue`], so the hooks
    /// nest as web middleware does: a middleware that stopped the command in `before`, and
    /// every one registered after it, runs no `after`. A refusal at the commit gate comes
    /// after every `before` continued, so every middleware runs its `after`, the one that
    /// refused included. The context carries the metadata the
    /// appended events carry, with what every `before` added.
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

/// The command context as [`CommandMiddleware::before`] holds it: all of it to read, and
/// only its metadata to change, through the checked
/// [`insert_metadata`](Self::insert_metadata) and [`set_metadata`](Self::set_metadata).
///
/// It gives no `&mut CommandContext`, so a middleware cannot replace the context whole:
/// the aggregate, stream, command and [Causation] a middleware sees are the ones the
/// command arrived with, and need no check afterwards.
///
/// ```rust,compile_fail
/// use replay_persistence::{CommandContext, CommandContextMut};
///
/// fn replace(mut context: CommandContextMut<'_>, other: CommandContext) {
///     *context = other; // no `DerefMut`: the context cannot be replaced
/// }
/// ```
///
/// [Causation]: https://github.com/funkode-io/replay/blob/main/CONTEXT.md#causation
#[derive(Debug)]
pub struct CommandContextMut<'a> {
    context: &'a mut CommandContext,
}

impl<'a> CommandContextMut<'a> {
    /// The handle `before` gets on `context`.
    ///
    /// [`Cqrs`](crate::Cqrs) makes one per middleware; it is public so a consumer can test
    /// a middleware's `before` without a store.
    pub fn new(context: &'a mut CommandContext) -> Self {
        Self { context }
    }

    /// Replace the command's metadata; see [`CommandContext::set_metadata`].
    pub fn set_metadata(&mut self, metadata: replay::Metadata) -> Result<(), replay::Error> {
        self.context.set_metadata(metadata)
    }

    /// Add `key` to the command's metadata; see [`CommandContext::insert_metadata`].
    pub fn insert_metadata(
        &mut self,
        key: impl Into<String>,
        value: impl Serialize,
    ) -> Result<(), replay::Error> {
        self.context.insert_metadata(key, value)
    }
}

impl std::ops::Deref for CommandContextMut<'_> {
    type Target = CommandContext;

    fn deref(&self) -> &CommandContext {
        self.context
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
    use replay::{Aggregate, ErrorKind, EventStream, Metadata, WithId};
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
            if command.0 < 0 {
                return Err(replay::Error::invalid_input("cannot add a negative amount"));
            }
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
        fn before<'a>(&'a self, context: CommandContextMut<'a>) -> BoxFuture<'a, Next> {
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
        fn before<'a>(&'a self, mut context: CommandContextMut<'a>) -> BoxFuture<'a, Next> {
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
        fn before<'a>(&'a self, mut context: CommandContextMut<'a>) -> BoxFuture<'a, Next> {
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

    /// What each middleware saw in `after`, in the order they ran: its name, the outcome
    /// as text, and the context.
    type Afters = Arc<Mutex<Vec<(&'static str, String, CommandContext)>>>;

    /// Answers `before` as it was built to, and records what its `after` sees.
    struct Observing {
        name: &'static str,
        answer: fn() -> Next,
        afters: Afters,
    }

    impl CommandMiddleware for Observing {
        fn before<'a>(&'a self, _: CommandContextMut<'a>) -> BoxFuture<'a, Next> {
            Box::pin(ready((self.answer)()))
        }

        fn after<'a>(
            &'a self,
            context: &'a CommandContext,
            outcome: &'a Outcome<'a>,
        ) -> BoxFuture<'a, ()> {
            Box::pin(async move {
                let outcome = match outcome {
                    Outcome::Committed => "committed".to_string(),
                    Outcome::Busy(reason) => format!("busy: {reason}"),
                    Outcome::Failed(error) => format!("failed: {error}"),
                };
                self.afters
                    .lock()
                    .unwrap()
                    .push((self.name, outcome, context.clone()));
            })
        }
    }

    fn observing(name: &'static str, afters: &Afters, answer: fn() -> Next) -> Observing {
        Observing {
            name,
            answer,
            afters: afters.clone(),
        }
    }

    /// Who ran `after`, in order, and the outcome each saw.
    fn observed(afters: &Afters) -> Vec<(&'static str, String)> {
        afters
            .lock()
            .unwrap()
            .iter()
            .map(|(name, outcome, _)| (*name, outcome.clone()))
            .collect()
    }

    #[tokio::test]
    async fn after_sees_a_committed_command_in_reverse_registration_order() {
        let afters = Afters::default();
        let cqrs = Cqrs::new(InMemoryEventStore::new())
            .with_middleware(AddsTheUser)
            .with_middleware(observing("first", &afters, || Next::Continue))
            .with_middleware(observing("second", &afters, || Next::Continue))
            .with_middleware(observing("third", &afters, || Next::Continue));

        cqrs.execute::<Counter>(
            &counter(),
            Metadata::default(),
            Add(1),
            &Handled::default(),
            None,
        )
        .await
        .unwrap();

        assert_eq!(
            observed(&afters),
            vec![
                ("third", "committed".to_string()),
                ("second", "committed".to_string()),
                ("first", "committed".to_string()),
            ]
        );
        let afters = afters.lock().unwrap();
        let context = &afters[0].2;
        assert_eq!(context.stream_id(), &Urn::from(counter()));
        assert_eq!(context.metadata().to_json(), json!({ "user": "alice" }));
    }

    #[tokio::test]
    async fn after_runs_only_for_the_middlewares_before_one_that_answered_busy() {
        let afters = Afters::default();
        let cqrs = Cqrs::new(InMemoryEventStore::new())
            .with_middleware(observing("first", &afters, || Next::Continue))
            .with_middleware(observing("second", &afters, || Next::Continue))
            .with_middleware(observing("maintenance", &afters, || {
                Next::Busy("maintenance in progress".to_string())
            }))
            .with_middleware(observing("later", &afters, || Next::Continue));

        let error = cqrs
            .execute::<Counter>(
                &counter(),
                Metadata::default(),
                Add(1),
                &Handled::default(),
                None,
            )
            .await
            .err()
            .expect("a busy middleware stops the command");

        assert_eq!(error.kind(), ErrorKind::Busy);
        assert_eq!(
            observed(&afters),
            vec![
                ("second", "busy: maintenance in progress".to_string()),
                ("first", "busy: maintenance in progress".to_string()),
            ]
        );
    }

    #[tokio::test]
    async fn after_runs_only_for_the_middlewares_before_one_that_answered_an_error() {
        let afters = Afters::default();
        let cqrs = Cqrs::new(InMemoryEventStore::new())
            .with_middleware(observing("first", &afters, || Next::Continue))
            .with_middleware(observing("auth", &afters, || {
                Next::Error(replay::Error::permanent(
                    ErrorKind::Forbidden,
                    "alice may not add",
                ))
            }))
            .with_middleware(observing("later", &afters, || Next::Continue));

        let error = cqrs
            .execute::<Counter>(
                &counter(),
                Metadata::default(),
                Add(1),
                &Handled::default(),
                None,
            )
            .await
            .err()
            .expect("a refusing middleware stops the command");

        assert_eq!(error.kind(), ErrorKind::Forbidden);
        assert_eq!(
            observed(&afters),
            vec![("first", format!("failed: {error}"))]
        );
    }

    #[tokio::test]
    async fn when_the_first_middleware_stops_the_command_no_after_runs() {
        let afters = Afters::default();
        let cqrs = Cqrs::new(InMemoryEventStore::new())
            .with_middleware(observing("maintenance", &afters, || {
                Next::Busy("not now".to_string())
            }))
            .with_middleware(observing("later", &afters, || Next::Continue));

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
        assert!(observed(&afters).is_empty());
    }

    #[tokio::test]
    async fn after_sees_a_command_that_failed_in_its_handler_as_failed_with_that_error() {
        let afters = Afters::default();
        let cqrs = Cqrs::new(InMemoryEventStore::new())
            .with_middleware(observing("first", &afters, || Next::Continue))
            .with_middleware(observing("second", &afters, || Next::Continue));

        let error = cqrs
            .execute::<Counter>(
                &counter(),
                Metadata::default(),
                Add(-1),
                &Handled::default(),
                None,
            )
            .await
            .err()
            .expect("the handler refuses a negative amount");

        assert_eq!(error.kind(), ErrorKind::InvalidInput);
        assert_eq!(error.to_string(), "cannot add a negative amount");
        let failed = format!("failed: {error}");
        assert_eq!(
            observed(&afters),
            vec![("second", failed.clone()), ("first", failed)]
        );
        assert!(appended(&cqrs).await.is_empty());
    }

    #[tokio::test]
    async fn after_sees_a_command_that_failed_in_the_store_as_failed_with_that_error() {
        let afters = Afters::default();
        let cqrs = Cqrs::new(InMemoryEventStore::new()).with_middleware(observing(
            "audit",
            &afters,
            || Next::Continue,
        ));

        // The stream is empty, so expecting it at version 5 conflicts in the store.
        let error = cqrs
            .execute::<Counter>(
                &counter(),
                Metadata::default(),
                Add(1),
                &Handled::default(),
                Some(5),
            )
            .await
            .err()
            .expect("a stale expected version conflicts");

        assert_eq!(error.kind(), ErrorKind::Conflict);
        assert_eq!(
            observed(&afters),
            vec![("audit", format!("failed: {error}"))]
        );
        assert!(appended(&cqrs).await.is_empty());
    }

    /// Its `after` does everything it can to the outcome it sees: it only gets to read it.
    struct BusyAfter(Arc<Mutex<u32>>);

    impl CommandMiddleware for BusyAfter {
        fn after<'a>(
            &'a self,
            _: &'a CommandContext,
            outcome: &'a Outcome<'a>,
        ) -> BoxFuture<'a, ()> {
            Box::pin(async move {
                tokio::task::yield_now().await;
                if !matches!(outcome, Outcome::Committed) {
                    *self.0.lock().unwrap() += 1;
                }
            })
        }
    }

    #[tokio::test]
    async fn nothing_after_does_changes_what_execute_returns() {
        let failures = Arc::new(Mutex::new(0));
        let plain = Cqrs::new(InMemoryEventStore::new());
        let observed =
            Cqrs::new(InMemoryEventStore::new()).with_middleware(BusyAfter(failures.clone()));

        for (amount, expected_version) in [(2, None), (-1, None), (1, Some(5))] {
            let without = plain
                .execute::<Counter>(
                    &counter(),
                    Metadata::default(),
                    Add(amount),
                    &Handled::default(),
                    expected_version,
                )
                .await;
            let with = observed
                .execute::<Counter>(
                    &counter(),
                    Metadata::default(),
                    Add(amount),
                    &Handled::default(),
                    expected_version,
                )
                .await;

            match (without, with) {
                (Ok(without), Ok(with)) => assert_eq!(without.total, with.total),
                (Err(without), Err(with)) => {
                    assert_eq!(without.kind(), with.kind());
                    assert_eq!(without.to_string(), with.to_string());
                    assert_eq!(without.operation(), with.operation());
                    assert_eq!(without.context(), with.context());
                }
                (without, with) => panic!(
                    "after changed the result: {:?} became {:?}",
                    without.map(|a| a.total),
                    with.map(|a| a.total)
                ),
            }
        }
        assert_eq!(*failures.lock().unwrap(), 2, "after saw both failures");
        assert_eq!(appended(&observed).await.len(), 1);
    }

    /// Answers `before` with `Continue` and `before_commit` with the answer it was built
    /// with, recording the context it saw at the gate.
    struct AtTheGate {
        name: &'static str,
        log: Log,
        answer: fn() -> Next,
    }

    impl CommandMiddleware for AtTheGate {
        fn before_commit<'a>(&'a self, context: &'a CommandContext) -> BoxFuture<'a, Next> {
            Box::pin(async move {
                self.log.lock().unwrap().push((self.name, context.clone()));
                (self.answer)()
            })
        }
    }

    fn at_the_gate(name: &'static str, log: &Log, answer: fn() -> Next) -> AtTheGate {
        AtTheGate {
            name,
            log: log.clone(),
            answer,
        }
    }

    /// An inline projection that keeps every amount it was handed.
    #[derive(Clone, Default)]
    struct Amounts(Arc<Mutex<Vec<i64>>>);

    impl crate::InlineProjection for Amounts {
        type Exec = ();
        type Event = CounterEvent;

        fn name(&self) -> &str {
            "amounts"
        }

        fn version(&self) -> i32 {
            1
        }

        async fn init(&mut self, _: &mut ()) -> Result<(), replay::Error> {
            Ok(())
        }

        async fn handle(
            &mut self,
            _: &mut (),
            events: &[PersistedEvent<CounterEvent>],
        ) -> Result<(), replay::Error> {
            for event in events {
                let CounterEvent::Added { amount } = event.data;
                self.0.lock().unwrap().push(amount);
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn a_busy_before_commit_rolls_the_append_back() {
        let log = Log::default();
        let amounts = Amounts::default();
        let store = InMemoryEventStore::new().register_projection(amounts.clone());
        let cqrs = Cqrs::new(store).with_middleware(at_the_gate("maintenance", &log, || {
            Next::Busy("maintenance in progress".to_string())
        }));
        let handled = Handled::default();

        let error = cqrs
            .execute::<Counter>(&counter(), Metadata::default(), Add(1), &handled, None)
            .await
            .err()
            .expect("a busy gate rolls the append back");

        assert_eq!(error.kind(), ErrorKind::Busy);
        assert!(error.is_temporary());
        assert_eq!(error.to_string(), "maintenance in progress");
        assert!(
            handled.0.load(Ordering::SeqCst),
            "the gate runs after the handler"
        );
        assert!(appended(&cqrs).await.is_empty());
        assert!(amounts.0.lock().unwrap().is_empty(), "the projection ran");
    }

    #[tokio::test]
    async fn a_before_commit_error_reaches_the_caller_unchanged() {
        let log = Log::default();
        let cqrs =
            Cqrs::new(InMemoryEventStore::new()).with_middleware(at_the_gate("auth", &log, || {
                Next::Error(
                    replay::Error::permanent(ErrorKind::Forbidden, "alice may not add")
                        .with_operation("authorize")
                        .with_context("user", "alice"),
                )
            }));

        let error = cqrs
            .execute::<Counter>(
                &counter(),
                Metadata::default(),
                Add(1),
                &Handled::default(),
                None,
            )
            .await
            .err()
            .expect("a refusing gate rolls the append back");

        assert_eq!(error.kind(), ErrorKind::Forbidden);
        assert_eq!(error.to_string(), "alice may not add");
        assert_eq!(error.operation(), "authorize");
        assert_eq!(error.context(), &[("user", "alice".to_string())]);
        assert!(appended(&cqrs).await.is_empty());
    }

    #[tokio::test]
    async fn the_first_middleware_to_refuse_the_commit_is_the_last_to_run() {
        let log = Log::default();
        let cqrs = Cqrs::new(InMemoryEventStore::new())
            .with_middleware(at_the_gate("first", &log, || Next::Continue))
            .with_middleware(at_the_gate("second", &log, || {
                Next::Busy("not now".to_string())
            }))
            .with_middleware(at_the_gate("third", &log, || Next::Continue));

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
        assert_eq!(ran(&log), vec!["first", "second"]);
        assert!(appended(&cqrs).await.is_empty());
    }

    #[tokio::test]
    async fn gates_that_continue_run_in_registration_order_and_see_what_before_added() {
        let log = Log::default();
        let amounts = Amounts::default();
        let store = InMemoryEventStore::new().register_projection(amounts.clone());
        let cqrs = Cqrs::new(store)
            .with_middleware(AddsTheUser)
            .with_middleware(at_the_gate("first", &log, || Next::Continue))
            .with_middleware(at_the_gate("second", &log, || Next::Continue));

        let counted = cqrs
            .execute::<Counter>(
                &counter(),
                Metadata::default(),
                Add(4),
                &Handled::default(),
                None,
            )
            .await
            .unwrap();

        assert_eq!(counted.total, 4);
        assert_eq!(ran(&log), vec!["first", "second"]);
        let seen = log.lock().unwrap()[0].1.metadata().to_json();
        assert_eq!(seen, json!({ "user": "alice" }));
        assert_eq!(appended(&cqrs).await.len(), 1);
        assert_eq!(*amounts.0.lock().unwrap(), vec![4]);
    }

    #[tokio::test]
    async fn after_sees_a_busy_commit_gate_as_busy_with_its_reason() {
        let afters = Afters::default();
        let log = Log::default();
        let cqrs = Cqrs::new(InMemoryEventStore::new())
            .with_middleware(observing("first", &afters, || Next::Continue))
            .with_middleware(at_the_gate("maintenance", &log, || {
                Next::Busy("maintenance in progress".to_string())
            }))
            .with_middleware(observing("last", &afters, || Next::Continue));

        let error = cqrs
            .execute::<Counter>(
                &counter(),
                Metadata::default(),
                Add(1),
                &Handled::default(),
                None,
            )
            .await
            .err()
            .expect("a busy gate rolls the append back");

        assert_eq!(error.kind(), ErrorKind::Busy);
        assert_eq!(
            observed(&afters),
            vec![
                ("last", "busy: maintenance in progress".to_string()),
                ("first", "busy: maintenance in progress".to_string()),
            ]
        );
        assert!(appended(&cqrs).await.is_empty());
    }

    #[tokio::test]
    async fn after_sees_a_commit_gate_error_as_failed_with_that_error() {
        let afters = Afters::default();
        let log = Log::default();
        let cqrs = Cqrs::new(InMemoryEventStore::new())
            .with_middleware(observing("first", &afters, || Next::Continue))
            .with_middleware(at_the_gate("auth", &log, || {
                Next::Error(replay::Error::permanent(
                    ErrorKind::Forbidden,
                    "alice may not add",
                ))
            }));

        let error = cqrs
            .execute::<Counter>(
                &counter(),
                Metadata::default(),
                Add(1),
                &Handled::default(),
                None,
            )
            .await
            .err()
            .expect("a refusing gate rolls the append back");

        assert_eq!(error.kind(), ErrorKind::Forbidden);
        assert_eq!(
            observed(&afters),
            vec![("first", "failed: alice may not add".to_string())]
        );
    }

    /// Refuses every command in both hooks.
    struct RefusesEverything(Log);

    impl CommandMiddleware for RefusesEverything {
        fn before<'a>(&'a self, context: CommandContextMut<'a>) -> BoxFuture<'a, Next> {
            Box::pin(async move {
                self.0.lock().unwrap().push(("before", context.clone()));
                Next::Busy("closed".to_string())
            })
        }

        fn before_commit<'a>(&'a self, context: &'a CommandContext) -> BoxFuture<'a, Next> {
            Box::pin(async move {
                self.0
                    .lock()
                    .unwrap()
                    .push(("before_commit", context.clone()));
                Next::Busy("closed".to_string())
            })
        }
    }

    #[tokio::test]
    async fn a_direct_append_through_the_store_runs_no_middleware() {
        let log = Log::default();
        let cqrs =
            Cqrs::new(InMemoryEventStore::new()).with_middleware(RefusesEverything(log.clone()));

        cqrs.store()
            .store_events::<Counter>(
                &counter(),
                Counter::stream_type(),
                Metadata::default(),
                &[CounterEvent::Added { amount: 5 }],
                None,
            )
            .await
            .expect("a direct append is not a command");

        assert!(ran(&log).is_empty(), "a middleware ran: {:?}", ran(&log));
        assert_eq!(appended(&cqrs).await.len(), 1);
    }

    #[tokio::test]
    async fn a_gate_that_does_not_continue_leaves_a_direct_store_append_unpublished() {
        let store = InMemoryEventStore::new();

        let error = store
            .store_events_stream::<Counter, _, _, _>(
                &counter(),
                Counter::stream_type(),
                Metadata::default(),
                futures::stream::iter([Ok(CounterEvent::Added { amount: 1 })]),
                None,
                crate::NoSink,
                || async { Next::Busy("not now".to_string()) },
            )
            .await
            .unwrap_err();

        assert_eq!(error.kind(), ErrorKind::Busy);
        let events: Vec<PersistedEvent<CounterEvent>> = store
            .stream_events_by_stream_id::<Counter>(&counter(), AggregateVersion::Latest, None, None)
            .try_collect()
            .await
            .unwrap();
        assert!(events.is_empty());
    }

    #[tokio::test]
    async fn an_append_held_at_the_gate_does_not_publish_a_version_another_append_took() {
        let store = Arc::new(InMemoryEventStore::new());
        let (reached, at_gate) = tokio::sync::oneshot::channel::<()>();
        let (release, released) = tokio::sync::oneshot::channel::<()>();

        let held = tokio::spawn({
            let store = Arc::clone(&store);
            async move {
                store
                    .store_events_stream::<Counter, _, _, _>(
                        &counter(),
                        "Stale".to_string(),
                        Metadata::default(),
                        futures::stream::iter([Ok(CounterEvent::Added { amount: 1 })]),
                        Some(0),
                        crate::NoSink,
                        || async move {
                            reached.send(()).unwrap();
                            released.await.unwrap();
                            Next::Continue
                        },
                    )
                    .await
            }
        });
        at_gate.await.unwrap();
        store
            .store_events::<Counter>(
                &counter(),
                Counter::stream_type(),
                Metadata::default(),
                &[CounterEvent::Added { amount: 2 }],
                Some(0),
            )
            .await
            .expect("the competing append commits first");
        release.send(()).unwrap();

        let error = held.await.unwrap().unwrap_err();

        assert_eq!(error.kind(), ErrorKind::Conflict);
        let events: Vec<PersistedEvent<CounterEvent>> = store
            .stream_events_by_stream_id::<Counter>(&counter(), AggregateVersion::Latest, None, None)
            .try_collect()
            .await
            .unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].version, 1);
        assert_eq!(events[0].data, CounterEvent::Added { amount: 2 });
        // The rejected append left the stream's type as the committed one recorded it.
        let typed: Vec<PersistedEvent<CounterEvent>> = store
            .stream_events(crate::StreamFilter::ForStreamTypes(vec![
                "Counter".to_string()
            ]))
            .try_collect()
            .await
            .unwrap();
        assert_eq!(typed.len(), 1);
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
