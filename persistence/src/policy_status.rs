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
//!
//! Beside the per-policy snapshot it lists the [`DeadLetter`]s behind each
//! `dead_letter_count`, with the event that triggered each, so a consumer can show
//! "which records failed and why" and act on them through
//! [`crate::PolicyRunner::retry_dead_letter`] / [`crate::PolicyRunner::discard_dead_letter`]
//! without reading Replay's tables (funkode-io/replay#260). The listing is a stream, and
//! its count a call of its own
//! ([ADR-0029](../../docs/adr/0029-the-public-api-is-stream-first.md)).

use std::fmt;

use chrono::{DateTime, Utc};
use futures::{Stream, TryStreamExt};
use serde_json::Value;
use sqlx::{Pool, Postgres, QueryBuilder};
use urn::Urn;
use uuid::Uuid;

use crate::PersistedEvent;

/// How many dead letters one query of [`PolicyStatusStore::dead_letters`] reads.
///
/// Not a page size: the consumer sees one stream and decides how much of it to read.
/// It bounds what the library holds at once and how long one query holds a pool
/// connection and a snapshot, so a consumer that walks the stream slowly keeps neither
/// for the length of its walk (ADR-0029).
const DEAD_LETTER_CHUNK: i64 = 100;

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
/// ([ADR-0026](../../docs/adr/0026-a-policy-tracks-its-position-per-stream.md)).
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
    /// Events written and not yet passed, summed over every stream this Policy tracks and
    /// is behind on.
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
    /// a policy tracks, because no index can answer a comparison between two tables'
    /// columns (ADR-0026). That is affordable for a status endpoint scraped every few
    /// seconds and would not be on every poll, which is why the runner does not use it
    /// that way.
    ///
    /// What a policy tracks is what its runner last recorded in
    /// `policy_cursors.tracked_stream_types`: every stream, or for a policy filtered by
    /// [`StreamFilter::ForStreamTypes`](crate::StreamFilter::ForStreamTypes) the streams of
    /// those types. A stream it does not track is never behind — counted against every
    /// stream, each one with no place would read as behind for ever
    /// ([ADR-0031](https://github.com/funkode-io/replay/blob/main/docs/adr/0031-a-policy-filtered-by-stream-type-tracks-only-those-types.md)).
    ///
    /// **Until a runner records it, every stream counts.** The column is `NULL` — every
    /// stream — on a row written before it existed, so a policy filtered by type reports
    /// the lag of every stream of every other type from the upgrade until a worker next
    /// starts it under this version; that worker records the types, and the lag narrows.
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
                FROM (
                    -- The streams this Policy tracks (ADR-0031). Two branches rather than
                    -- one `IS NULL OR type = ANY` predicate, so each is planned on its own:
                    -- the first is a one-time filter that skips the scan entirely for a
                    -- Policy that tracks some types, and the second is an index scan on
                    -- `idx_streams_type_id` that never reads a row of another type.
                    SELECT id, stream_seq FROM streams
                     WHERE pc.tracked_stream_types IS NULL
                    UNION ALL
                    SELECT id, stream_seq FROM streams
                     WHERE type = ANY(pc.tracked_stream_types)
                ) s
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

    /// Stream every parked row matching `filter`, oldest id first, each with the event
    /// that triggered it.
    ///
    /// A stream, not a page: take what a screen shows (`.take(50)`) or walk all of it.
    /// It reads in short keyset chunks on the row id, so a consumer that holds it while
    /// it works holds no connection or snapshot between chunks, and a row parked while
    /// the walk is under way is met if its id is still ahead. To resume across requests,
    /// pass the last id shown to [`dead_letters_after`](Self::dead_letters_after).
    ///
    /// The filter is taken by value, where [`dead_letter_count`](Self::dead_letter_count)
    /// borrows it: the stream is `'static` — it owns a pool handle and its filter, so a
    /// consumer can return it from a handler or move it into a task — and a borrowed
    /// filter would tie it to the caller's frame. Clone the filter to count and list
    /// with the same one.
    ///
    /// ```rust,ignore
    /// let first_screen: Vec<DeadLetter> = store
    ///     .dead_letters(DeadLetterFilter::new().policy("import_readiness"))
    ///     .take(50)
    ///     .try_collect()
    ///     .await?;
    /// ```
    pub fn dead_letters(
        &self,
        filter: DeadLetterFilter,
    ) -> impl Stream<Item = Result<DeadLetter, replay::Error>> + Send + 'static {
        self.dead_letters_after(filter, 0)
    }

    /// [`dead_letters`](Self::dead_letters), starting after the row whose id is `after`:
    /// the rows matching `filter` whose ids are greater.
    ///
    /// The id need not still be parked — a row retried or discarded since it was shown
    /// is still a place to resume from.
    pub fn dead_letters_after(
        &self,
        filter: DeadLetterFilter,
        after: i64,
    ) -> impl Stream<Item = Result<DeadLetter, replay::Error>> + Send + 'static {
        let pool = self.pool.clone();
        async_stream::try_stream! {
            let mut after = after;
            loop {
                let chunk = read_dead_letter_chunk(&pool, &filter, after).await?;
                let complete = (chunk.len() as i64) < DEAD_LETTER_CHUNK;
                for dead_letter in chunk {
                    after = dead_letter.id;
                    yield dead_letter;
                }
                if complete {
                    break;
                }
            }
        }
    }

    /// How many parked rows match `filter`: one call, which loads none of them.
    ///
    /// Counts what [`dead_letters`](Self::dead_letters) would list for the same filter
    /// at the same moment — parked commands, as [`PolicyStatus::dead_letter_count`]
    /// does, not reactions.
    pub async fn dead_letter_count(&self, filter: &DeadLetterFilter) -> Result<i64, replay::Error> {
        let mut query: QueryBuilder<Postgres> =
            QueryBuilder::new("SELECT COUNT(*) FROM policy_dead_letters dl");
        // The event is joined only for a criterion that reads it: a count by Policy
        // stays on the dead-letter table's own index. An inner join, which selects the
        // rows the listing's LEFT JOIN does — see `read_dead_letter_chunk`.
        if filter.reads_trigger() {
            query.push(" JOIN events e ON e.id = dl.event_id");
        }
        query.push(" WHERE TRUE");
        filter.push_criteria(&mut query);

        query
            .build_query_scalar::<i64>()
            .fetch_one(&self.pool)
            .await
            .map_err(|e| crate::db_error(e).with_operation("dead_letter_count"))
    }

    /// Every parked row matching `filter`, collected.
    ///
    /// A convenience for a consumer that has decided to hold them all; it is
    /// [`dead_letters`](Self::dead_letters) collected, and the library never calls it.
    pub async fn dead_letters_all(
        &self,
        filter: DeadLetterFilter,
    ) -> Result<Vec<DeadLetter>, replay::Error> {
        self.dead_letters(filter).try_collect().await
    }
}

// ── Dead letters ──────────────────────────────────────────────────────────────

/// Which parked rows [`PolicyStatusStore::dead_letters`] and
/// [`PolicyStatusStore::dead_letter_count`] read. Empty, it matches every parked row;
/// each criterion narrows it, and they combine with AND.
///
/// ```rust,ignore
/// let filter = DeadLetterFilter::new()
///     .policy("import_readiness")
///     .triggered_in(import_id)          // the trigger event's stream
///     .triggered_by("RecordRejected");  // the trigger event's type
/// ```
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DeadLetterFilter {
    policy_name: Option<String>,
    trigger_stream_id: Option<Urn>,
    trigger_event_type: Option<String>,
}

impl DeadLetterFilter {
    /// A filter matching every parked row.
    pub fn new() -> Self {
        Self::default()
    }

    /// Only rows parked by the Policy named `policy_name`.
    pub fn policy(mut self, policy_name: impl Into<String>) -> Self {
        self.policy_name = Some(policy_name.into());
        self
    }

    /// Only rows whose triggering event was written to the stream `stream_id` — the
    /// stream the Policy reacted to, not the instance its failed command targeted.
    pub fn triggered_in(mut self, stream_id: impl Into<Urn>) -> Self {
        self.trigger_stream_id = Some(stream_id.into());
        self
    }

    /// Only rows whose triggering event has the type `event_type`, as the store wrote
    /// it ([`PersistedEvent::r#type`]).
    pub fn triggered_by(mut self, event_type: impl Into<String>) -> Self {
        self.trigger_event_type = Some(event_type.into());
        self
    }

    /// Whether a criterion reads the triggering event, so the query must join it.
    fn reads_trigger(&self) -> bool {
        self.trigger_stream_id.is_some() || self.trigger_event_type.is_some()
    }

    /// Append this filter's criteria to a `WHERE` that already holds one predicate.
    fn push_criteria(&self, query: &mut QueryBuilder<Postgres>) {
        if let Some(policy_name) = &self.policy_name {
            query
                .push(" AND dl.policy_name = ")
                .push_bind(policy_name.clone());
        }
        if let Some(stream_id) = &self.trigger_stream_id {
            query
                .push(" AND e.stream_id = ")
                .push_bind(stream_id.to_string());
        }
        if let Some(event_type) = &self.trigger_event_type {
            query.push(" AND e.type = ").push_bind(event_type.clone());
        }
    }
}

/// One parked command — a row behind [`PolicyStatus::dead_letter_count`] — with the
/// event whose reaction parked it.
///
/// The glossary's *Dead letter*: one command per reaction, not one per delivery
/// ([ADR-0024](../../docs/adr/0024-a-parked-command-is-one-row.md)).
///
/// The rows one reaction parked share an [`event_id`](Self::event_id), and the two
/// controls treat them differently. [`crate::PolicyRunner::retry_dead_letter`] on any
/// one of their ids replays the reaction once and settles **every** sibling row with it
/// ([ADR-0021](../../docs/adr/0021-retry-settles-a-reaction-not-a-row.md)), so a listing
/// read before the retry may show siblings that are gone after it.
/// [`crate::PolicyRunner::discard_dead_letter`] archives only the row it names.
#[derive(Debug, Clone)]
pub struct DeadLetter {
    /// The row's id: what [`crate::PolicyRunner::retry_dead_letter`] and
    /// [`crate::PolicyRunner::discard_dead_letter`] take, and the key a listing resumes
    /// after ([`PolicyStatusStore::dead_letters_after`]).
    pub id: i64,
    /// The Policy whose reaction parked it.
    pub policy_name: String,
    /// The kind of the error the command failed with — the error's kind, or
    /// [`crate::PANIC_ERROR_KIND`] / [`crate::TIMEOUT_ERROR_KIND`].
    pub error_kind: String,
    /// The error's message.
    pub error_message: String,
    /// When the command first failed; a redelivery does not move it.
    pub created_at: DateTime<Utc>,
    /// When the last delivery of the triggering event parked it.
    pub last_parked_at: DateTime<Utc>,
    /// Deliveries of the triggering event that parked this command.
    pub deliveries: i32,
    /// Settlements a retry has made on this row.
    pub retry_count: i32,
    /// When the last of them was made; `None` until the row is first retried.
    pub last_retried_at: Option<DateTime<Utc>>,
    /// Rust type name of the aggregate the failed command targeted. `None` on a row
    /// that names no dispatch: parked before the identity migration, or for a panic in
    /// the reaction itself (funkode-io/replay#210).
    pub aggregate_name: Option<String>,
    /// URN of the aggregate instance the failed command was addressed to — the same
    /// type as a [`PersistedEvent`]'s `stream_id` and [`DeadLetterFilter::triggered_in`].
    pub target_stream_id: Option<Urn>,
    /// Rust type name of the failed command (its variant and payload are not recorded).
    pub command_name: Option<String>,
    /// The failed dispatch's index in what the reaction returned; negative on a row
    /// parked before the column existed.
    pub dispatch_ordinal: Option<i32>,
    /// Id of the event whose reaction parked this command.
    pub event_id: Uuid,
    /// That event, as the log holds it now. `None` only when it is no longer in the
    /// log, which the library never causes; the row is listed anyway, so the listing
    /// agrees with [`PolicyStatus::dead_letter_count`].
    pub triggering_event: Option<PersistedEvent<Value>>,
}

impl DeadLetter {
    fn try_from_row(row: sqlx::postgres::PgRow) -> Result<Self, replay::Error> {
        use sqlx::Row as _;
        let id: i64 = row.get("dead_letter_id");
        let policy_name: String = row.get("policy_name");
        let error_kind: String = row.get("error_kind");
        let error_message: String = row.get("error_message");
        let created_at: DateTime<Utc> = row.get("created_at");
        let last_parked_at: DateTime<Utc> = row.get("last_parked_at");
        let deliveries: i32 = row.get("deliveries");
        let retry_count: i32 = row.get("retry_count");
        let last_retried_at: Option<DateTime<Utc>> = row.get("last_retried_at");
        let aggregate_name: Option<String> = row.get("aggregate_name");
        // Written from `Dispatch::target_stream_id`, a `Urn`, so it always parses back.
        let target_stream_id: Option<Urn> = row
            .get::<Option<String>, _>("target_stream_id")
            .map(|raw| {
                Urn::try_from(raw.clone()).map_err(|e| {
                    replay::Error::internal(
                        "failed to parse a dead letter's target_stream_id as URN",
                    )
                    .with_operation("dead_letters")
                    .with_context("target_stream_id", raw)
                    .with_source(e)
                })
            })
            .transpose()?;
        let command_name: Option<String> = row.get("command_name");
        let dispatch_ordinal: Option<i32> = row.get("dispatch_ordinal");
        let event_id: Uuid = row.get("trigger_event_id");
        // The event's columns come from a LEFT JOIN: its `id` is null when the event
        // is gone, and every other column with it.
        let found: Option<Uuid> = row.get("id");
        let triggering_event = match found {
            Some(_) => Some(PersistedEvent::<Value>::try_from(row)?),
            None => None,
        };
        Ok(DeadLetter {
            id,
            policy_name,
            error_kind,
            error_message,
            created_at,
            last_parked_at,
            deliveries,
            retry_count,
            last_retried_at,
            aggregate_name,
            target_stream_id,
            command_name,
            dispatch_ordinal,
            event_id,
            triggering_event,
        })
    }
}

/// Read the chunk of parked rows matching `filter` whose ids follow `after`, in id
/// order: at most [`DEAD_LETTER_CHUNK`] of them.
///
/// The event is LEFT JOINed so a parked command whose triggering event has left the
/// log is still listed (with no event), as [`PolicyStatusStore::dead_letter_count`]
/// counts it. That count joins with an inner JOIN, and only when a criterion reads the
/// event. Both select the same rows: with no `e.*` criterion, the count does not join
/// and the LEFT JOIN drops nothing; with one, the criterion compares a column the LEFT
/// JOIN leaves null for a missing event, which is never true, so the LEFT JOIN drops
/// exactly the rows the inner join does. Change one side and the other must follow.
async fn read_dead_letter_chunk(
    pool: &Pool<Postgres>,
    filter: &DeadLetterFilter,
    after: i64,
) -> Result<Vec<DeadLetter>, replay::Error> {
    let mut query: QueryBuilder<Postgres> = QueryBuilder::new(
        "SELECT dl.id AS dead_letter_id, dl.policy_name, dl.error_kind, dl.error_message, \
                dl.created_at, dl.last_parked_at, dl.deliveries, dl.retry_count, \
                dl.last_retried_at, dl.aggregate_name, dl.target_stream_id, \
                dl.command_name, dl.dispatch_ordinal, dl.event_id AS trigger_event_id, \
                e.id, e.data, e.metadata, e.stream_id, e.type, e.version, e.created, \
                e.aggregate_version \
         FROM policy_dead_letters dl \
         LEFT JOIN events e ON e.id = dl.event_id \
         WHERE dl.id > ",
    );
    query.push_bind(after);
    filter.push_criteria(&mut query);
    query
        .push(" ORDER BY dl.id ASC LIMIT ")
        .push_bind(DEAD_LETTER_CHUNK);

    query
        .build()
        .fetch_all(pool)
        .await
        .map_err(|e| crate::db_error(e).with_operation("dead_letters"))?
        .into_iter()
        .map(DeadLetter::try_from_row)
        .collect()
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
