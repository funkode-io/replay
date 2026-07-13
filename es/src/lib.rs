mod aggregate;
mod error;
mod event;
mod metadata;
mod stream;

pub use aggregate::{Aggregate, Compactable, Compaction};
pub use error::{Error, ErrorKind, ErrorStatus, Result};
pub use event::Event;
pub use metadata::Metadata;
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
    pub use super::{Aggregate, Compactable, Compaction, Event, EventStream, ScopedUrn, WithId};
}
