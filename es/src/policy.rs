use crate::{Dispatch, Event, ObservedEvent};

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
