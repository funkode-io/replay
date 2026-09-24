mod aggregate;
mod dispatch;
mod error;
mod event;
mod metadata;
mod observed_event;
mod policy;
mod stream;

pub use aggregate::{Aggregate, Compactable, Compaction};
pub use dispatch::Dispatch;
pub use error::{Error, ErrorKind, ErrorStatus, Result};
pub use event::Event;
pub use metadata::Metadata;
pub use observed_event::ObservedEvent;
pub use policy::{AggregatePolicy, Policy, TargetedCommand};
pub use stream::{EventStream, ScopedUrn, WithId};

/// Convenience re-exports of the most commonly used traits.
///
/// A single glob import brings all core traits into scope so you don't
/// have to list them individually:
///
/// ```rust,ignore
/// use replay::prelude::*;
///
/// // ScopedUrn, WithId, EventStream, Aggregate, Compactable, Event
/// // are all available without further imports.
/// let scoped: BankAccountUrn = account_urn.at(branch_urn)?;
/// let branch: BranchUrn = scoped.extract_scope::<BranchUrn>()?;
/// ```
pub mod prelude {
    // `AggregatePolicy` stays out on purpose: the blanket impl gives its implementors a
    // `Policy::react` too, so with both traits in scope `policy.react(&event)` is
    // ambiguous (E0034). Importing it alone keeps the method call a single-target test
    // is written around.
    pub use super::{
        Aggregate, Compactable, Compaction, Dispatch, Event, EventStream, ObservedEvent, Policy,
        ScopedUrn, WithId,
    };
}
