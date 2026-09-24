use crate::{Aggregate, Dispatch, Event, Metadata, ObservedEvent, WithId};

/// A rule of the form *when this event happens, issue these commands*.
///
/// A policy is business orchestration, so it is declared here, against the core
/// vocabulary alone: it names no store, no runner and no tunable. What drives it —
/// the feed it reads, where it starts, how it batches and how long a command may run —
/// belongs to whatever runs it, and is supplied there
/// (`replay_persistence::PolicySettings`).
///
/// Delivery is **at-least-once**: a crash after a command commits but before the
/// position is saved re-delivers the triggering event. Correctness therefore depends on
/// commands the target aggregate can absorb twice — the policy cannot help, since it
/// does not see the triggering event's identity (ADR-0027).
///
/// Implementors stay pure: [`react`](Policy::react) takes an event and returns the
/// commands to issue, with no I/O.
///
/// A rule whose commands all go to **one** aggregate type declares that type instead,
/// through [`AggregatePolicy`], and returns `(StreamId, Command)` pairs a test compares
/// with `==`. A type implements one trait or the other, never both: a blanket impl
/// makes every `AggregatePolicy` a `Policy`, and a second impl on the same type would
/// collide with it (`E0119`).
pub trait Policy: Send + Sync {
    /// The event type this policy understands. Use `query_events!` to merge
    /// events from several aggregates into one enum.
    type Event: Event;

    /// Stable identity used as the cursor key. Changing the Rust type must not
    /// change this string, or the policy would lose its checkpoint.
    fn name(&self) -> &str;

    /// Pure reaction: given an event, return the commands to dispatch.
    ///
    /// One event at a time, whatever the runner's batch size, and what is returned is a
    /// reaction to that one event.
    ///
    /// A panic here does not have to stop the policy: the runner in
    /// `es-replay-persistence` contains it at the event being delivered, parks a dead
    /// letter recording the panic, and carries on. A panic inside a task this reaction
    /// **spawns itself** is outside that boundary and is contained by nothing.
    fn react(&self, event: &ObservedEvent<Self::Event>) -> Vec<Dispatch>;
}

/// One command an [`AggregatePolicy`] issues: which instance of its target to command,
/// and what to tell it.
pub type TargetedCommand<A> = (<A as WithId>::StreamId, <A as Aggregate>::Command);

/// A [`Policy`] whose commands all go to **one** aggregate type, declared in the type
/// system.
///
/// `react` hands back `(StreamId, Command)` pairs for that target, so a unit test
/// compares the return value with `==`: no [`Dispatch`], no downcast, no store. A
/// blanket impl turns each pair into a `Dispatch` to `Target`, so the runner and every
/// registration keep working in `Policy` alone.
///
/// ```rust,ignore
/// impl AggregatePolicy for OpenCaseOnFreeze {
///     type Event = AccountEvent;
///     type Target = ComplianceCase;
///
///     fn name(&self) -> &str { "open_case_on_freeze" }
///
///     fn react(&self, event: &ObservedEvent<Self::Event>) -> Vec<(CaseUrn, CaseCommand)> {
///         vec![(CaseUrn(event.stream_id.clone()), CaseCommand::Open)]
///     }
/// }
///
/// assert_eq!(OpenCaseOnFreeze.react(&frozen), vec![(case_id, CaseCommand::Open)]);
/// ```
///
/// **A type implements this trait or [`Policy`], never both.** The blanket impl below
/// already supplies `Policy`; writing a second one for the same type is `E0119`. A
/// reaction that addresses several aggregate types — a business command plus a dead
/// letter back to the import stream, say — cannot name one `Target` and is a raw
/// `Policy` (ADR-0027, decision 6).
///
/// With **both** traits in scope — `replay::prelude` carries `Policy` — `policy.react(e)`
/// is ambiguous (`E0034`), since the blanket impl gives every implementor two `react`s.
/// Import `AggregatePolicy` alone, or name the trait: `AggregatePolicy::react(&p, &e)`.
pub trait AggregatePolicy: Send + Sync {
    /// The event type this rule understands, as on [`Policy::Event`].
    type Event: Event;

    /// The single aggregate every command of this rule is addressed to.
    type Target: Aggregate + 'static;

    /// Stable identity used as the cursor key, as on [`Policy::name`].
    fn name(&self) -> &str;

    /// Pure reaction: the instances to command, and the commands to send them.
    ///
    /// One reaction to one event, as [`Policy::react`], and the same `Vec`: its length is
    /// the commands this rule writes, not anything the store read. The blanket impl
    /// turns it into a `Vec<Dispatch>` of exactly that length, which is the shape
    /// `Policy::react` has to hand the runner.
    fn react(&self, event: &ObservedEvent<Self::Event>) -> Vec<TargetedCommand<Self::Target>>;

    /// Metadata to attach to every dispatch of one reaction, as
    /// [`Dispatch::with_metadata`] does for a raw [`Policy`].
    ///
    /// Called **once per reaction**, before the pairs are wrapped, so a correlation id
    /// minted here is the same on every command that reaction issues — and is not
    /// called at all when the reaction is empty. A runner merges it with causation
    /// metadata; colliding top-level keys are rejected there.
    fn dispatch_metadata(&self, _event: &ObservedEvent<Self::Event>) -> Option<Metadata> {
        None
    }
}

impl<P> Policy for P
where
    P: AggregatePolicy,
    <P::Target as WithId>::StreamId: 'static,
    <P::Target as Aggregate>::Command: 'static,
{
    type Event = P::Event;

    fn name(&self) -> &str {
        AggregatePolicy::name(self)
    }

    fn react(&self, event: &ObservedEvent<Self::Event>) -> Vec<Dispatch> {
        let pairs = AggregatePolicy::react(self, event);
        if pairs.is_empty() {
            return Vec::new();
        }

        let metadata = self.dispatch_metadata(event);
        pairs
            .into_iter()
            .map(|(id, command)| {
                let dispatch = Dispatch::to::<P::Target>(id, command);
                match &metadata {
                    Some(metadata) => dispatch.with_metadata(metadata.clone()),
                    None => dispatch,
                }
            })
            .collect()
    }
}
