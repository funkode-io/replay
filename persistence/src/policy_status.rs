//! Policy status read model.
//!
//! [`PolicyStatusStore`] reads operational tables that the runner already
//! writes (`policy_cursors`, `events`, and `policy_dead_letters`) and returns
//! one [`PolicyStatus`] per known policy — a lightweight health/lag signal for
//! monitoring, including whether a policy is parked in front of a
//! `global_position` that does not exist.
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
/// | `Blocked`  | the position after the cursor is absent, a later one exists |
/// | `Degraded` | `dead_letter_count > 0`                                     |
/// | `Working`  | `dead_letter_count == 0`, `lag > 0`                         |
/// | `CaughtUp` | `dead_letter_count == 0`, `lag == 0`                        |
///
/// A policy that is *both* behind and has dead letters resolves to `Degraded`.
/// An empty tail is not a hole: a drained policy is `CaughtUp`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PolicyCondition {
    /// No dead letters and no lag: fully healthy and up to date.
    CaughtUp,
    /// No dead letters but lagging behind the global head (`lag > 0`).
    Working,
    /// At least one dead-letter row exists; needs operator attention.
    Degraded,
    /// The cursor sits in front of a `global_position` that does not exist
    /// while a later one does, so the feed yields nothing. An append in flight
    /// looks the same and clears on a later poll; a burned position does not.
    Blocked,
}

impl PolicyCondition {
    /// Stable string form of this condition, suitable for JSON/UI consumers.
    pub fn as_str(&self) -> &'static str {
        match self {
            PolicyCondition::CaughtUp => "CaughtUp",
            PolicyCondition::Working => "Working",
            PolicyCondition::Degraded => "Degraded",
            PolicyCondition::Blocked => "Blocked",
        }
    }

    /// Derive the condition from the raw `lag`, `dead_letter_count` and
    /// `missing_position` fields, highest precedence first: a hole, then dead
    /// letters, then lag.
    pub fn from_fields(lag: i64, dead_letter_count: i64, missing_position: Option<i64>) -> Self {
        if missing_position.is_some() {
            PolicyCondition::Blocked
        } else if dead_letter_count > 0 {
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
    /// Positions the policy has yet to process (`head - position`).
    ///
    /// Counts positions, not events: a burned or deleted position inflates it.
    pub lag: i64,
    /// The lowest `global_position` greater than `position` that exists, or
    /// `None` when nothing past the cursor is in the log.
    pub next_position: Option<i64>,
    /// `Some(position + 1)` when that position does not exist while a later one
    /// does — the hole the feed stops at. A multi-position hole reports its
    /// first position.
    pub missing_position: Option<i64>,
    /// When the cursor was last advanced (staleness signal).
    pub last_checkpoint_at: DateTime<Utc>,
    /// Number of dead-letter rows recorded for this policy.
    pub dead_letter_count: i64,
    /// Timestamp of the most recent dead-letter row, if any.
    pub last_dead_letter_at: Option<DateTime<Utc>>,
    /// Derived condition: [`PolicyCondition::Blocked`] when
    /// `missing_position` is set, otherwise [`PolicyCondition::Degraded`] when
    /// `dead_letter_count > 0`, otherwise [`PolicyCondition::Working`] when
    /// `lag > 0`, otherwise [`PolicyCondition::CaughtUp`].
    pub condition: PolicyCondition,
}

/// The hole a cursor sits in front of, if any: set only when the position right
/// after `position` is absent *and* some later position exists.
fn missing_position(position: i64, next_position: Option<i64>) -> Option<i64> {
    match next_position {
        Some(next) if next > position + 1 => Some(position + 1),
        _ => None,
    }
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
    /// `policy_cursors`, `MAX(global_position)` on `events`, a per-policy
    /// `MIN(global_position) > cursor` probe, and a per-policy `LATERAL`
    /// aggregate over `policy_dead_letters`.  The dead-letter lateral is
    /// filtered by `pc.name`, so it uses the `(policy_name, created_at)` index;
    /// the `MIN`/`MAX` on `events` are probes on `idx_events_global_position_unique`.
    /// The event log is never scanned.
    pub async fn list(&self) -> Result<Vec<PolicyStatus>, replay::Error> {
        let rows = sqlx::query(
            r#"
            SELECT
                pc.name,
                pc.position,
                h.head,
                nx.next_position,
                pc.updated_at,
                COALESCE(dl.dead_letter_count, 0) AS dead_letter_count,
                dl.last_dead_letter_at
            FROM policy_cursors pc
            CROSS JOIN (
                SELECT COALESCE(MAX(global_position), 0) AS head
                FROM events
            ) h
            LEFT JOIN LATERAL (
                SELECT MIN(global_position) AS next_position
                FROM events
                WHERE global_position > pc.position
            ) nx ON TRUE
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
                let next_position: Option<i64> = r.get("next_position");
                let updated_at: DateTime<Utc> = r.get("updated_at");
                let dead_letter_count: i64 = r.get("dead_letter_count");
                let last_dead_letter_at: Option<DateTime<Utc>> = r.get("last_dead_letter_at");
                let lag = head - position;
                let missing_position = missing_position(position, next_position);
                let condition =
                    PolicyCondition::from_fields(lag, dead_letter_count, missing_position);
                PolicyStatus {
                    name,
                    position,
                    head,
                    lag,
                    next_position,
                    missing_position,
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
            next_position: None,
            missing_position: None,
            last_checkpoint_at: now,
            dead_letter_count: 0,
            last_dead_letter_at: None,
            condition: PolicyCondition::from_fields(0, 0, None),
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
            next_position: Some(6),
            missing_position: None,
            last_checkpoint_at: now,
            dead_letter_count: 0,
            last_dead_letter_at: None,
            condition: PolicyCondition::from_fields(5, 0, None),
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
            next_position: Some(8),
            missing_position: None,
            last_checkpoint_at: now,
            dead_letter_count: 2,
            last_dead_letter_at: Some(now),
            condition: PolicyCondition::from_fields(3, 2, None),
        };
        assert_eq!(degraded.condition, PolicyCondition::Degraded);
        assert_eq!(degraded.dead_letter_count, 2);
        assert_eq!(degraded.condition.as_str(), "Degraded");
        assert_eq!(degraded.condition.to_string(), "Degraded");

        // in front of a hole: blocked, ahead of both of the above
        let blocked = PolicyStatus {
            name: "blocked_policy".to_string(),
            position: 7,
            head: 10,
            lag: 3,
            next_position: Some(9),
            missing_position: Some(8),
            last_checkpoint_at: now,
            dead_letter_count: 2,
            last_dead_letter_at: Some(now),
            condition: PolicyCondition::from_fields(3, 2, Some(8)),
        };
        assert_eq!(blocked.condition, PolicyCondition::Blocked);
        assert_eq!(blocked.missing_position, Some(8));
        assert_eq!(blocked.next_position, Some(9));
        assert_eq!(blocked.condition.as_str(), "Blocked");
        assert_eq!(blocked.condition.to_string(), "Blocked");
    }

    /// Exhaustive precedence table for [`PolicyCondition::from_fields`].
    #[test]
    fn policy_condition_precedence() {
        assert_eq!(
            PolicyCondition::from_fields(0, 0, None),
            PolicyCondition::CaughtUp
        );
        assert_eq!(
            PolicyCondition::from_fields(5, 0, None),
            PolicyCondition::Working
        );
        assert_eq!(
            PolicyCondition::from_fields(0, 3, None),
            PolicyCondition::Degraded
        );
        // Dead letters win even when the policy is also behind.
        assert_eq!(
            PolicyCondition::from_fields(10, 2, None),
            PolicyCondition::Degraded
        );
        // A hole in front of the cursor outranks everything.
        assert_eq!(
            PolicyCondition::from_fields(5, 0, Some(7)),
            PolicyCondition::Blocked
        );
        assert_eq!(
            PolicyCondition::from_fields(5, 4, Some(7)),
            PolicyCondition::Blocked
        );
    }

    /// The missing position is derived, not stored: it exists only when the
    /// position immediately after the cursor is absent *and* a later one exists.
    #[test]
    fn missing_position_is_the_hole_in_front_of_the_cursor() {
        // Next event is the very next position: no hole.
        assert_eq!(missing_position(5, Some(6)), None);
        // Next event is further out: the hole is position 6.
        assert_eq!(missing_position(5, Some(9)), Some(6));
        // Nothing past the cursor at all: caught up, not blocked.
        assert_eq!(missing_position(5, None), None);
        // A cursor at 0 with the log starting at 1 is the bootstrap case.
        assert_eq!(missing_position(0, Some(1)), None);
    }

    /// Directly verify the `PolicyCondition` string representations.
    #[test]
    fn policy_condition_display_and_as_str() {
        assert_eq!(PolicyCondition::CaughtUp.as_str(), "CaughtUp");
        assert_eq!(PolicyCondition::Working.as_str(), "Working");
        assert_eq!(PolicyCondition::Degraded.as_str(), "Degraded");
        assert_eq!(PolicyCondition::Blocked.as_str(), "Blocked");
        assert_eq!(format!("{}", PolicyCondition::CaughtUp), "CaughtUp");
        assert_eq!(format!("{}", PolicyCondition::Working), "Working");
        assert_eq!(format!("{}", PolicyCondition::Degraded), "Degraded");
        assert_eq!(format!("{}", PolicyCondition::Blocked), "Blocked");
    }
}
