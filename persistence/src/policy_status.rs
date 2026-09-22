//! Policy status read model.
//!
//! [`PolicyStatusStore`] reads operational tables that the runner already writes
//! (`policy_cursors`, `policy_stream_cursors`, `streams` and `policy_dead_letters`) and
//! returns one [`PolicyStatus`] per known policy — a lightweight health and lag signal
//! for monitoring.
//!
//! The lag it reports is a count of events, arrived at by subtracting each stream's place
//! from its head (funkode-io/replay#196). The number it replaces was a subtraction of
//! `global_position`s, which counted the positions of every stream the Policy does not
//! read, plus any a failed write had burned.
//!
//! This is **not** a Projection: it reads operational tables, not the event
//! log, and does not use the [`crate::Query`] / [`crate::InlineProjection`]
//! machinery.
//!
//! It is also the *progress* axis only — how far a Policy has got — and carries
//! no liveness field: no table can see whether a worker task exists, so a
//! `CaughtUp` Policy whose worker died is indistinguishable here from an idle
//! one. [`crate::PolicyRunnerDaemon::liveness`] is what tells them apart.

use std::fmt;

use chrono::{DateTime, Utc};
use sqlx::{Pool, Postgres};

// ── PolicyCondition ───────────────────────────────────────────────────────────

/// Headline health label for a [`PolicyStatus`].
///
/// Precedence (highest wins):
///
/// | Condition  | When                                                        |
/// |------------|-------------------------------------------------------------|
/// | `Degraded` | `dead_letter_count > 0`                                     |
/// | `Working`  | `dead_letter_count == 0`, `lag > 0`                         |
/// | `CaughtUp` | `dead_letter_count == 0`, `lag == 0`                        |
///
/// A policy that is *both* behind and has dead letters resolves to `Degraded`.
///
/// There is no `Blocked`: a Policy reads each stream over a sequence that has no holes,
/// so there is no position it can be parked in front of
/// ([ADR-0025](../../docs/adr/0025-a-policy-tracks-its-position-per-stream.md)).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyCondition {
    /// No dead letters and no lag: fully healthy and up to date.
    CaughtUp,
    /// No dead letters but lagging behind the global head (`lag > 0`).
    Working,
    /// At least one dead-letter row exists; needs operator attention.
    Degraded,
}

impl PolicyCondition {
    /// Stable string form of this condition, suitable for JSON/UI consumers.
    pub fn as_str(&self) -> &'static str {
        match self {
            PolicyCondition::CaughtUp => "CaughtUp",
            PolicyCondition::Working => "Working",
            PolicyCondition::Degraded => "Degraded",
        }
    }

    /// Derive the condition from the raw `lag` and `dead_letter_count` fields, highest
    /// precedence first: dead letters, then lag.
    pub fn from_fields(lag: i64, dead_letter_count: i64) -> Self {
        if dead_letter_count > 0 {
            PolicyCondition::Degraded
        } else if lag > 0 {
            PolicyCondition::Working
        } else {
            PolicyCondition::CaughtUp
        }
    }
}

impl fmt::Display for PolicyCondition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

// ── PolicyStatus ──────────────────────────────────────────────────────────────

/// Snapshot of one policy's operational health.
#[derive(Debug, Clone)]
pub struct PolicyStatus {
    /// Stable name of the policy (the cursor key).
    pub name: String,
    /// Events written and not yet passed, summed over every stream this Policy is behind
    /// on.
    ///
    /// An exact count, including the events its filter will skip and the snapshot rows
    /// compaction writes: those are passed rather than delivered, and until they are
    /// passed they are work.
    pub lag: i64,
    /// How many streams that lag is spread across. One stream a million events behind and
    /// a million streams one event behind are the same `lag` and very different problems.
    pub streams_behind: i64,
    /// How far this Policy's search of the log has swept. Not progress — progress is per
    /// stream — and not a control surface: a running worker reads this once when it takes
    /// leadership and keeps it in memory, writing it only forwards, so an operator's reset
    /// takes effect when that worker next starts. Places are the surface that is read
    /// fresh every poll ([ADR-0012](../../docs/adr/0012-policy-cursor-is-an-operator-writable-control-surface.md)).
    pub discovered_through: i64,
    /// When the policy last advanced in any stream (staleness signal).
    pub last_checkpoint_at: DateTime<Utc>,
    /// Number of parked commands recorded for this policy: one row per command
    /// per reaction, not per delivery of the triggering event.
    pub dead_letter_count: i64,
    /// The latest parking among the commands this policy still has parked, and
    /// `None` when it has none: the aggregate reads the active table, so a
    /// policy whose every row has been retried or discarded reports `None`
    /// however recently it parked.
    ///
    /// The last *parking*, not the oldest row's creation: a redelivery that
    /// re-parks a command already parked refreshes its row rather than adding
    /// one (funkode-io/replay#220), and a reaction failing on every delivery
    /// must not read like one that failed once and stopped.
    pub last_dead_letter_at: Option<DateTime<Utc>>,
    /// Derived condition: [`PolicyCondition::Degraded`] when `dead_letter_count > 0`,
    /// otherwise [`PolicyCondition::Working`] when `lag > 0`, otherwise
    /// [`PolicyCondition::CaughtUp`].
    pub condition: PolicyCondition,
}

// ── PolicyStatusStore ─────────────────────────────────────────────────────────

/// Read-only store that returns per-policy status from a single SQL query.
///
/// ```rust,ignore
/// let statuses = PolicyStatusStore::new(pool).list().await?;
/// ```
pub struct PolicyStatusStore {
    pool: Pool<Postgres>,
}

impl PolicyStatusStore {
    /// Construct a new store backed by `pool`.
    pub fn new(pool: Pool<Postgres>) -> Self {
        Self { pool }
    }

    /// Return one [`PolicyStatus`] for every policy that has a
    /// `policy_cursors` row.  A registered-but-never-run policy (no row)
    /// does not appear.
    ///
    /// The result is produced by a **single** SQL read: a per-policy `LATERAL` over
    /// `streams` against that policy's places, and a per-policy `LATERAL` aggregate over
    /// `policy_dead_letters`. The dead-letter lateral is filtered by `pc.name` and reads
    /// `last_parked_at`, which `idx_dead_letters_policy_created_parked` carries as a
    /// payload column, so it stays an index-only scan (funkode-io/replay#227).
    ///
    /// The event log is never scanned, but the frontier lateral reads one row per stream
    /// per policy, because no index can answer a comparison between two tables' columns
    /// (ADR-0025). That is affordable for a status endpoint scraped every few seconds and
    /// would not be on every poll, which is why the runner does not use it that way.
    pub async fn list(&self) -> Result<Vec<PolicyStatus>, replay::Error> {
        let rows = sqlx::query(
            r#"
            SELECT
                pc.name,
                pc.discovered_through,
                pc.updated_at,
                COALESCE(f.lag, 0) AS lag,
                COALESCE(f.streams_behind, 0) AS streams_behind,
                COALESCE(dl.dead_letter_count, 0) AS dead_letter_count,
                dl.last_dead_letter_at
            FROM policy_cursors pc
            LEFT JOIN LATERAL (
                SELECT
                    -- `SUM` of a bigint is numeric in PostgreSQL; the count fits.
                    SUM(s.stream_seq - COALESCE(c.stream_seq, 0))::bigint AS lag,
                    COUNT(*)                                      AS streams_behind
                FROM streams s
                LEFT JOIN policy_stream_cursors c
                       ON c.policy = pc.name AND c.stream_id = s.id
                WHERE s.stream_seq > COALESCE(c.stream_seq, 0)
            ) f ON TRUE
            LEFT JOIN LATERAL (
                SELECT
                    COUNT(*)             AS dead_letter_count,
                    MAX(last_parked_at)  AS last_dead_letter_at
                FROM policy_dead_letters
                WHERE policy_name = pc.name
            ) dl ON TRUE
            ORDER BY pc.name
            "#,
        )
        .fetch_all(&self.pool)
        .await
        .map_err(crate::db_error)?;

        let statuses = rows
            .into_iter()
            .map(|r: sqlx::postgres::PgRow| {
                use sqlx::Row as _;
                let name: String = r.get("name");
                let discovered_through: i64 = r.get("discovered_through");
                let updated_at: DateTime<Utc> = r.get("updated_at");
                let dead_letter_count: i64 = r.get("dead_letter_count");
                let last_dead_letter_at: Option<DateTime<Utc>> = r.get("last_dead_letter_at");
                // `SUM` over no rows is NULL, which the query has already collapsed: a
                // Policy behind on nothing is behind by nothing.
                let lag: i64 = r.get("lag");
                let streams_behind: i64 = r.get("streams_behind");
                let condition = PolicyCondition::from_fields(lag, dead_letter_count);
                PolicyStatus {
                    name,
                    lag,
                    streams_behind,
                    discovered_through,
                    last_checkpoint_at: updated_at,
                    dead_letter_count,
                    last_dead_letter_at,
                    condition,
                }
            })
            .collect();

        Ok(statuses)
    }
}

// ── Unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    /// Pure derivation test — no database required.
    ///
    /// Verifies the [`PolicyCondition::from_fields`] precedence as surfaced on a
    /// fully-built [`PolicyStatus`]: `dead_letter_count > 0` → `Degraded`, else
    /// `lag > 0` → `Working`, else `CaughtUp`.
    #[test]
    fn policy_status_condition_derived_from_fields() {
        let now = Utc::now();

        let caught_up = PolicyStatus {
            name: "test_policy".to_string(),
            lag: 0,
            streams_behind: 0,
            discovered_through: 10,
            last_checkpoint_at: now,
            dead_letter_count: 0,
            last_dead_letter_at: None,
            condition: PolicyCondition::from_fields(0, 0),
        };
        assert_eq!(caught_up.condition, PolicyCondition::CaughtUp);
        assert_eq!(caught_up.condition.as_str(), "CaughtUp");

        let working = PolicyStatus {
            name: "slow_policy".to_string(),
            lag: 5,
            streams_behind: 2,
            discovered_through: 10,
            last_checkpoint_at: now,
            dead_letter_count: 0,
            last_dead_letter_at: None,
            condition: PolicyCondition::from_fields(5, 0),
        };
        assert_eq!(working.condition, PolicyCondition::Working);
        assert_eq!(
            (working.lag, working.streams_behind),
            (5, 2),
            "five events owed, spread over two streams"
        );

        let degraded = PolicyStatus {
            name: "failing_policy".to_string(),
            lag: 3,
            streams_behind: 1,
            discovered_through: 10,
            last_checkpoint_at: now,
            dead_letter_count: 2,
            last_dead_letter_at: Some(now),
            condition: PolicyCondition::from_fields(3, 2),
        };
        assert_eq!(degraded.condition, PolicyCondition::Degraded);
        assert_eq!(degraded.dead_letter_count, 2);
    }

    /// Exhaustive precedence table for [`PolicyCondition::from_fields`].
    #[test]
    fn policy_condition_precedence() {
        assert_eq!(
            PolicyCondition::from_fields(0, 0),
            PolicyCondition::CaughtUp
        );
        assert_eq!(PolicyCondition::from_fields(5, 0), PolicyCondition::Working);
        assert_eq!(
            PolicyCondition::from_fields(0, 3),
            PolicyCondition::Degraded
        );
        // Dead letters win even when the policy is also behind.
        assert_eq!(
            PolicyCondition::from_fields(10, 2),
            PolicyCondition::Degraded
        );
    }

    /// Directly verify the `PolicyCondition` string representations.
    #[test]
    fn policy_condition_display_and_as_str() {
        assert_eq!(PolicyCondition::CaughtUp.as_str(), "CaughtUp");
        assert_eq!(PolicyCondition::Working.as_str(), "Working");
        assert_eq!(PolicyCondition::Degraded.as_str(), "Degraded");
        assert_eq!(format!("{}", PolicyCondition::CaughtUp), "CaughtUp");
        assert_eq!(format!("{}", PolicyCondition::Working), "Working");
        assert_eq!(format!("{}", PolicyCondition::Degraded), "Degraded");
    }
}
