//! What the store builder's `build` would do to each registered inline projection, read
//! without doing it (funkode-io/replay#341).
//!
//! [`PlannedAction::decide`] is the one place the stored version is compared with the code
//! version: `build` branches on it and `plan` reports it, so the two cannot disagree.

/// The path the store builder's `build` takes for one registered inline projection,
/// decided by its stored registry version against its code
/// [`version`](crate::InlineProjection::version).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum PlannedAction {
    /// No stored version: first registration. `build` runs `init`, replays the matching
    /// history through `handle` and records the version.
    Init,
    /// The stored version is older than the code's: `build` runs `reset`, replays the
    /// matching history through `handle` and records the new version.
    Rebuild,
    /// The stored version is the code's: no reset and no replay, only a catch-up on the
    /// events of transactions that did not apply this projection.
    Current,
    /// The stored version is newer than the code's: `build` returns an error and the
    /// store does not start.
    Refuse,
}

impl PlannedAction {
    /// The action for a projection whose registry row holds `stored` (`None` when it has
    /// none) and whose code is at `code`.
    pub(crate) fn decide(stored: Option<i32>, code: i32) -> Self {
        match stored {
            None => Self::Init,
            Some(stored) if stored < code => Self::Rebuild,
            Some(stored) if stored > code => Self::Refuse,
            Some(_) => Self::Current,
        }
    }

    /// Whether `build` initialises or rebuilds the projection: replays history through it
    /// and writes its registry row.
    pub fn is_build(self) -> bool {
        matches!(self, Self::Init | Self::Rebuild)
    }
}

/// One registered inline projection in a [`BuildPlan`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlannedProjection {
    name: String,
    stored_version: Option<i32>,
    code_version: i32,
    action: PlannedAction,
}

impl PlannedProjection {
    pub(crate) fn new(name: String, stored_version: Option<i32>, code_version: i32) -> Self {
        Self {
            action: PlannedAction::decide(stored_version, code_version),
            name,
            stored_version,
            code_version,
        }
    }

    /// The projection's [`name`](crate::InlineProjection::name), its registry key.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The version the registry records for this projection, or `None` when it has never
    /// been built.
    pub fn stored_version(&self) -> Option<i32> {
        self.stored_version
    }

    /// The projection's code [`version`](crate::InlineProjection::version).
    pub fn code_version(&self) -> i32 {
        self.code_version
    }

    /// What `build` would do to this projection.
    pub fn action(&self) -> PlannedAction {
        self.action
    }
}

/// What the store builder's `build` would do to every registered inline projection, in
/// registration order, returned by its `plan`
/// ([`PostgresEventStore::builder`](crate::PostgresEventStore::builder)).
///
/// It holds one entry per registered projection, so it is bounded by the registry the
/// caller built, never by the data.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BuildPlan {
    projections: Vec<PlannedProjection>,
}

impl BuildPlan {
    pub(crate) fn new(projections: Vec<PlannedProjection>) -> Self {
        Self { projections }
    }

    /// One entry per registered projection, in registration order.
    pub fn projections(&self) -> &[PlannedProjection] {
        &self.projections
    }

    /// Whether `build` would initialise or rebuild any projection: `true` when any entry is
    /// [`Init`](PlannedAction::Init) or [`Rebuild`](PlannedAction::Rebuild).
    ///
    /// Read it together with [`refuses`](Self::refuses): a `build` that refuses rolls back
    /// and changes nothing, so the other entries say what it will do once the refusal is
    /// cleared, not what this `build` does.
    pub fn needs_build(&self) -> bool {
        self.projections.iter().any(|p| p.action.is_build())
    }

    /// Whether `build` would refuse to start: `true` when any entry is
    /// [`Refuse`](PlannedAction::Refuse). Such a `build` returns an error and rolls back
    /// whatever it had begun, so it initialises and rebuilds nothing.
    pub fn refuses(&self) -> bool {
        self.projections
            .iter()
            .any(|p| p.action == PlannedAction::Refuse)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn decide_matches_builds_four_branches() {
        assert_eq!(PlannedAction::decide(None, 1), PlannedAction::Init);
        assert_eq!(PlannedAction::decide(Some(1), 2), PlannedAction::Rebuild);
        assert_eq!(PlannedAction::decide(Some(2), 2), PlannedAction::Current);
        assert_eq!(PlannedAction::decide(Some(3), 2), PlannedAction::Refuse);
    }

    #[test]
    fn needs_build_only_for_init_or_rebuild() {
        let plan = |entries: &[(Option<i32>, i32)]| {
            BuildPlan::new(
                entries
                    .iter()
                    .map(|&(stored, code)| PlannedProjection::new("p".into(), stored, code))
                    .collect(),
            )
        };
        assert!(!plan(&[]).needs_build());
        assert!(!plan(&[(Some(1), 1), (Some(3), 2)]).needs_build());
        assert!(plan(&[(Some(1), 1), (None, 1)]).needs_build());
        assert!(plan(&[(Some(1), 2)]).needs_build());
        assert!(plan(&[(Some(3), 2)]).refuses());
        assert!(!plan(&[(None, 2)]).refuses());
    }
}
