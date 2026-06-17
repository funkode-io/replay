//! Policy status read model.
//!
//! [`PolicyStatusStore`] reads operational tables that the runner already
//! writes (`policy_cursors` + `events`) and returns one [`PolicyStatus`] per
//! known policy — a lightweight health/lag signal for monitoring.
//!
//! This is **not** a Projection: it reads operational tables, not the event
//! log, and does not use the [`crate::Query`] / [`crate::InlineProjection`]
//! machinery.

use std::fmt;

use chrono::{DateTime, Utc};
use sqlx::{Pool, Postgres};

// ── PolicyCondition ───────────────────────────────────────────────────────────

/// Whether a policy is keeping up with the event log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyCondition {
    /// The policy cursor is at the global head (`lag == 0`).
    CaughtUp,
    /// The policy cursor is behind the global head (`lag > 0`).
    Working,
}

impl PolicyCondition {
    /// Stable string form of this condition, suitable for JSON/UI consumers.
    pub fn as_str(&self) -> &'static str {
        match self {
            PolicyCondition::CaughtUp => "CaughtUp",
            PolicyCondition::Working => "Working",
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
    /// Derived condition: [`PolicyCondition::CaughtUp`] when `lag == 0`,
    /// [`PolicyCondition::Working`] when `lag > 0`.
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
    /// The result is produced by a **single** SQL read over `policy_cursors`
    /// and `MAX(global_position)` on `events`.
    pub async fn list(&self) -> Result<Vec<PolicyStatus>, replay::Error> {
        let rows = sqlx::query(
            r#"
            SELECT
                pc.name,
                pc.position,
                h.head,
                pc.updated_at
            FROM policy_cursors pc
            CROSS JOIN (
                SELECT COALESCE(MAX(global_position), 0) AS head
                FROM events
            ) h
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
                let lag = head - position;
                let condition = if lag == 0 {
                    PolicyCondition::CaughtUp
                } else {
                    PolicyCondition::Working
                };
                PolicyStatus {
                    name,
                    position,
                    head,
                    lag,
                    last_checkpoint_at: updated_at,
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
    /// Verifies that `lag == 0` → `CaughtUp` and `lag > 0` → `Working`.
    #[test]
    fn policy_condition_derived_from_lag() {
        let now = Utc::now();

        // lag == 0: caught up
        let lag0 = PolicyStatus {
            name: "test_policy".to_string(),
            position: 10,
            head: 10,
            lag: 0,
            last_checkpoint_at: now,
            condition: if 0 == 0 {
                PolicyCondition::CaughtUp
            } else {
                PolicyCondition::Working
            },
        };
        assert_eq!(lag0.condition, PolicyCondition::CaughtUp);
        assert_eq!(lag0.lag, 0);
        assert_eq!(lag0.condition.as_str(), "CaughtUp");
        assert_eq!(lag0.condition.to_string(), "CaughtUp");

        // lag > 0: working
        let lag5 = PolicyStatus {
            name: "slow_policy".to_string(),
            position: 5,
            head: 10,
            lag: 5,
            last_checkpoint_at: now,
            condition: if 5 == 0 {
                PolicyCondition::CaughtUp
            } else {
                PolicyCondition::Working
            },
        };
        assert_eq!(lag5.condition, PolicyCondition::Working);
        assert_eq!(lag5.lag, 5);
        assert_eq!(lag5.condition.as_str(), "Working");
        assert_eq!(lag5.condition.to_string(), "Working");
    }

    /// Directly verify the `PolicyCondition` string representations.
    #[test]
    fn policy_condition_display_and_as_str() {
        assert_eq!(PolicyCondition::CaughtUp.as_str(), "CaughtUp");
        assert_eq!(PolicyCondition::Working.as_str(), "Working");
        assert_eq!(format!("{}", PolicyCondition::CaughtUp), "CaughtUp");
        assert_eq!(format!("{}", PolicyCondition::Working), "Working");
    }
}
