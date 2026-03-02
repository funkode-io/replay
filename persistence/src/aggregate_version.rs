/// Identifies which version of an aggregate's event stream to load.
///
/// In event sourcing, compaction archives the full event log under a numbered version
/// and replaces it with a minimal set of events that reproduce the same state.
/// `AggregateVersion` lets callers choose whether to replay the current (compacted)
/// events or to inspect a specific archived version.
///
/// # Variants
/// - `Latest` — load the current event stream (default, no version filter).
/// - `Version(u32)` — load the archived event stream created by a specific compaction run.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum AggregateVersion {
    /// Load the current event stream (post-compaction or uncompacted).
    #[default]
    Latest,
    /// Load the archived event stream produced by the nth compaction.
    Version(u32),
}

impl AggregateVersion {
    /// Returns `None` for `Latest` (stored as SQL NULL / no version tag)
    /// and `Some(n)` for `Version(n)`.
    pub fn as_option(&self) -> Option<u32> {
        match self {
            AggregateVersion::Latest => None,
            AggregateVersion::Version(v) => Some(*v),
        }
    }
}
