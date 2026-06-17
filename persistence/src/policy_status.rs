//! Policy status read model.
//!
//! [`PolicyStatusStore`] reads operational tables that the runner already
//! writes (`policy_cursors`, `events`, and `policy_dead_letters`) and returns
//! one [`PolicyStatus`] per known policy — a lightweight health/lag signal for
//! monitoring.
//!
//! This is **not** a Projection: it reads operational tables, not the event
//! log, and does not use the [`crate::Query`] / [`crate::InlineProjection`]
//! machinery.

use std::fmt;

use chrono::{DateTime, Utc};
use sqlx::{Pool, Postgres};

// ── PolicyCondition ───────────────────────────────────────────────────────────

/// Headline health label for a [`PolicyStatus`].
///
/// Precedence (highest wins):
///
/// | Condition  | When                                |
/// |------------|-------------------------------------|
/// | `Degraded` | `dead_letter_count > 0`             |
/// | `Working`  | `dead_letter_count == 0`, `lag > 0` |
/// | `CaughtUp` | `dead_letter_count == 0`, `lag == 0`|
///
/// A policy that is *both* behind and has dead letters resolves to `Degraded`
/// so that parked failures are never hidden behind a progress label.
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

    /// Derive the condition from the raw `lag` and `dead_letter_count` fields.
    ///
    /// Dead letters take precedence over lag: a policy that is both behind and
    /// has parked failures resolves to [`PolicyCondition::Degraded`].
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
    /// Last processed `global_position`.
    pub position: i64,
    /// Current global head (`MAX(global_position)` on the events table).
    pub head: i64,
    /// Number of events the policy has yet to process (`head - position`).
    pub lag: i64,
    /// When the cursor was last advanced (staleness signal).
    pub last_checkpoint_at: DateTime<Utc>,
    /// Number of dead-letter rows recorded for this policy.
    pub dead_letter_count: i64,
    /// Timestamp of the most recent dead-letter row, if any.
    pub last_dead_letter_at: Option<DateTime<Utc>>,
    /// Derived condition: [`PolicyCondition::Degraded`] when
    /// `dead_letter_count > 0`, otherwise [`PolicyCondition::Working`] when
    /// `lag > 0`, otherwise [`PolicyCondition::CaughtUp`].
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
    /// The result is produced by a **single** SQL read that joins
    /// `policy_cursors`, `MAX(global_position)` on `events`, and a per-policy
    /// `LATERAL` aggregate over `policy_dead_letters`.  The lateral is filtered
    /// by `pc.name`, so it uses the `(policy_name, created_at)` index instead of
    /// aggregating the whole dead-letter table.  The event log is never scanned.
    pub async fn list(&self) -> Result<Vec<PolicyStatus>, replay::Error> {
        let rows = sqlx::query(
            r#"
            SELECT
                pc.name,
                pc.position,
                h.head,
                pc.updated_at,
                COALESCE(dl.dead_letter_count, 0) AS dead_letter_count,
                dl.last_dead_letter_at
            FROM policy_cursors pc
            CROSS JOIN (
                SELECT COALESCE(MAX(global_position), 0) AS head
                FROM events
            ) h
            LEFT JOIN LATERAL (
                SELECT
                    COUNT(*)        AS dead_letter_count,
                    MAX(created_at) AS last_dead_letter_at
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
                let position: i64 = r.get("position");
                let head: i64 = r.get("head");
                let updated_at: DateTime<Utc> = r.get("updated_at");
                let dead_letter_count: i64 = r.get("dead_letter_count");
                let last_dead_letter_at: Option<DateTime<Utc>> = r.get("last_dead_letter_at");
                let lag = head - position;
                let condition = PolicyCondition::from_fields(lag, dead_letter_count);
                PolicyStatus {
                    name,
                    position,
                    head,
                    lag,
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

        // lag == 0, no dead letters: caught up
        let lag0 = PolicyStatus {
            name: "test_policy".to_string(),
            position: 10,
            head: 10,
            lag: 0,
            last_checkpoint_at: now,
            dead_letter_count: 0,
            last_dead_letter_at: None,
            condition: PolicyCondition::from_fields(0, 0),
        };
        assert_eq!(lag0.condition, PolicyCondition::CaughtUp);
        assert_eq!(lag0.lag, 0);
        assert_eq!(lag0.condition.as_str(), "CaughtUp");
        assert_eq!(lag0.condition.to_string(), "CaughtUp");

        // lag > 0, no dead letters: working
        let lag5 = PolicyStatus {
            name: "slow_policy".to_string(),
            position: 5,
            head: 10,
            lag: 5,
            last_checkpoint_at: now,
            dead_letter_count: 0,
            last_dead_letter_at: None,
            condition: PolicyCondition::from_fields(5, 0),
        };
        assert_eq!(lag5.condition, PolicyCondition::Working);
        assert_eq!(lag5.lag, 5);
        assert_eq!(lag5.condition.as_str(), "Working");
        assert_eq!(lag5.condition.to_string(), "Working");

        // dead letters present: degraded, even when also behind
        let degraded = PolicyStatus {
            name: "failing_policy".to_string(),
            position: 7,
            head: 10,
            lag: 3,
            last_checkpoint_at: now,
            dead_letter_count: 2,
            last_dead_letter_at: Some(now),
            condition: PolicyCondition::from_fields(3, 2),
        };
        assert_eq!(degraded.condition, PolicyCondition::Degraded);
        assert_eq!(degraded.dead_letter_count, 2);
        assert_eq!(degraded.condition.as_str(), "Degraded");
        assert_eq!(degraded.condition.to_string(), "Degraded");
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
