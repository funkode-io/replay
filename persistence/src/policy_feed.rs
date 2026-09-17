//! The slice of the log a Policy reads on a poll, and the order it reads it in.
//!
//! The order is the pair `(commit_txid, global_position)`: the transaction that wrote
//! an event, then the position it took within the log. The feed reads only events
//! whose writing transaction has certainly ended — `commit_txid` below
//! `pg_snapshot_xmin(pg_current_snapshot())`, the standard Postgres CDC/outbox
//! watermark — and that is what makes the order a Policy may walk with a single
//! cursor (funkode-io/replay#195).
//!
//! Why the pair rather than the position alone. A `global_position` is drawn from a
//! sequence when a write *starts* and becomes visible when it *finishes*, so the
//! positions appear out of order and a position taken by an aborted write never
//! appears at all. Reading in position order therefore needs a theory of holes; reading
//! in transaction order needs none:
//!
//! - every row below the watermark is already visible, and no row below it can appear
//!   later, so nothing ever turns up behind a point the Policy has passed;
//! - a position burned by an aborted write belongs to no row, so it is not a hole in
//!   this order — it is simply not in it;
//! - a write still in flight sits at or above the watermark with everything committed
//!   after it, so its events are delivered in their place once it ends, never skipped
//!   and never early.
//!
//! The cost is stated rather than hidden: a long-running write holds the watermark
//! down and delays the events behind it until it ends. That is bounded by the write and
//! self-healing; [`crate::policy_blocked`] is what says so out loud.
//!
//! Delivery is decided per row and never touches the order: an event the Policy's
//! `stream_filter` excludes, and a compaction snapshot row, advance the cursor and fire
//! nothing (ADR-0004, ADR-0013).

use serde_json::Value;
use sqlx::{Pool, Postgres, QueryBuilder, Row};

use crate::commit_stamp::CommitStamp;
use crate::{PersistedEvent, PostgresEventStore, StreamFilter};

/// A point in the Policy feed: a transaction, and a position within the log.
///
/// Ordered lexicographically, transaction first — the order the feed reads in, and the
/// reason a cursor records both halves (funkode-io/replay#194). Two points are
/// comparable whatever their positions, so a Policy's progress is a single value even
/// though the log's positions arrive out of order.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct FeedPoint {
    pub(crate) commit_txid: CommitStamp,
    pub(crate) position: i64,
}

/// What the log says about the position a stored cursor row carries, read in one
/// statement by [`crate::policy_runner`] and turned into a point by [`resume_point`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct ResumeInputs {
    /// The transaction that wrote the event at that position, when an event is there.
    pub(crate) at_position: Option<CommitStamp>,
    /// The earliest transaction, in feed order, that wrote an event past that position
    /// and is already readable.
    pub(crate) first_past_position: Option<CommitStamp>,
    /// `pg_snapshot_xmin`: every transaction below it has ended, so every event still to
    /// appear was written at or above it.
    pub(crate) watermark: CommitStamp,
}

/// The point a Policy resumes from, given the row `policy_cursors` holds.
///
/// A row this crate wrote names an event: the event at `position` carries exactly
/// `commit_txid`, and the pair is a point in the feed's order. It is taken as written.
///
/// Anything else is a position without a point behind it — a row that predates migration
/// 0022 and carries the sentinel, or the one-column move ADR-0012 gives an operator — and
/// a position is not a cut in `(commit_txid, global_position)` order: the events past it
/// may have been written by transactions older than the one at it. Reading such a row as
/// "processed everything at or before this position" is the only reading that is true of
/// both, so the point derived from it is the greatest one that still delivers every event
/// past that position:
///
/// - the earliest transaction holding such an event, when the feed can already see one;
/// - otherwise one below the watermark, because an event that is still to appear cannot
///   have been written below it.
///
/// Both may re-deliver: an event at or before `position` written by a younger transaction
/// sorts after the derived point. That is the direction the ambiguity is resolved in —
/// delivery is at-least-once and a rewind is visible, while a skip is silent and
/// permanent.
pub(crate) fn resume_point(stored: FeedPoint, log: ResumeInputs) -> FeedPoint {
    if log.at_position == Some(stored.commit_txid) {
        return stored;
    }

    // Position 0 under the sentinel is not an instruction to complete: it is where a
    // Policy that has processed nothing sits, already behind every transaction.
    if stored.position == 0 && stored.commit_txid == CommitStamp::SENTINEL {
        return stored;
    }

    FeedPoint {
        commit_txid: log
            .first_past_position
            .unwrap_or_else(|| log.watermark.previous()),
        position: stored.position,
    }
}

/// One row from the window read past a Policy's cursor. `delivered` is `None` for a
/// compaction snapshot or an event the Policy's filter excludes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct WindowPosition<E> {
    pub(crate) point: FeedPoint,
    pub(crate) delivered: Option<E>,
}

/// Read the events past `cursor` the Policy may advance over, in feed order.
///
/// Unfiltered — every row past the cursor, up to `limit` — because how far the cursor
/// gets belongs to the log, not to the rows the Policy asked for (ADR-0013). `filter`
/// is evaluated per row as `matches_filter` and decides delivery only; an excluded row
/// advances the cursor like a compaction snapshot (`compacted_snapshot = TRUE`,
/// ADR-0004).
///
/// The `WHERE` clause is the whole decision: the row comparison resumes the pair order,
/// and the watermark withholds anything an unfinished write could still be overtaken
/// by. Both are index-ordered columns of `idx_events_commit_txid_position` (migration
/// 0019), so the plan is a forward index scan the `LIMIT` stops early.
pub(crate) async fn read_feed(
    pool: &Pool<Postgres>,
    filter: StreamFilter,
    cursor: FeedPoint,
    limit: u32,
) -> Result<Vec<WindowPosition<PersistedEvent<Value>>>, replay::Error> {
    let mut qb: QueryBuilder<Postgres> = QueryBuilder::new(
        "SELECT id, data, metadata, stream_id, type, version, created, aggregate_version, \
         global_position, commit_txid::text AS commit_txid, compacted_snapshot, COALESCE((",
    );
    // As a predicate NULL means no match; read as a value it must be collapsed.
    PostgresEventStore::add_filters(&mut qb, filter);
    qb.push("), FALSE) AS matches_filter FROM events WHERE (commit_txid, global_position) > (");
    qb.push_bind(cursor.commit_txid.to_string());
    qb.push("::xid8, ");
    qb.push_bind(cursor.position);
    qb.push(") AND commit_txid < pg_snapshot_xmin(pg_current_snapshot())");
    qb.push(" ORDER BY commit_txid, global_position LIMIT ");
    qb.push_bind(limit as i64);

    let rows = qb.build().fetch_all(pool).await.map_err(crate::db_error)?;

    let mut window = Vec::with_capacity(rows.len());
    for row in rows {
        let point = FeedPoint {
            commit_txid: CommitStamp::from_row(&row, "commit_txid")?,
            position: row.get("global_position"),
        };
        let is_snapshot: bool = row.get("compacted_snapshot");
        let matches_filter: bool = row.get("matches_filter");

        // Only delivered rows are parsed; a skipped row's bytes are still fetched.
        let delivered = if is_snapshot || !matches_filter {
            None
        } else {
            Some(PersistedEvent::<Value>::try_from(row)?)
        };

        window.push(WindowPosition { point, delivered });
    }

    Ok(window)
}

#[cfg(test)]
mod tests {
    use super::{resume_point, FeedPoint, ResumeInputs};
    use crate::commit_stamp::CommitStamp;

    fn point(commit_txid: &str, position: i64) -> FeedPoint {
        FeedPoint {
            commit_txid: CommitStamp::parse(commit_txid).expect("a plain counter parses"),
            position,
        }
    }

    fn stamp(commit_txid: &str) -> CommitStamp {
        CommitStamp::parse(commit_txid).expect("a plain counter parses")
    }

    /// A log with nothing past the stored position and nothing running near it.
    fn log(at_position: Option<&str>, first_past_position: Option<&str>) -> ResumeInputs {
        ResumeInputs {
            at_position: at_position.map(stamp),
            first_past_position: first_past_position.map(stamp),
            watermark: stamp("100"),
        }
    }

    /// A row this crate wrote names an event, and resuming from it is what makes a
    /// restart cost nothing: deriving a point for it would rewind the Policy on every
    /// election, because the events past its position include the ones it walked over to
    /// get there.
    #[test]
    fn a_pair_that_names_an_event_is_resumed_as_written() {
        let stored = point("12", 2);

        assert_eq!(
            resume_point(stored, log(Some("12"), Some("11"))),
            stored,
            "the event at position 2 was written by transaction 12, so the row is a point"
        );
    }

    /// The upgrade this rule exists for: a cursor written before the transaction half
    /// existed sits at a position, and the events past that position may belong to older
    /// transactions than the one at it. Completing it to the transaction *at* the
    /// position would sort those events behind the cursor and lose them for good.
    #[test]
    fn a_position_whose_events_are_not_all_behind_it_resumes_before_them() {
        assert_eq!(
            resume_point(point("0", 2), log(Some("12"), Some("11"))),
            point("11", 2),
            "resuming just inside transaction 11 delivers what it wrote past position 2"
        );
    }

    /// An operator's position, against a log whose unprocessed events are all younger
    /// than the one at it. The derived point is still the earliest of them: it costs
    /// nothing, since every transaction below it wrote only events the position covers.
    #[test]
    fn an_operators_position_resumes_at_the_earliest_transaction_past_it() {
        assert_eq!(
            resume_point(point("7", 40), log(Some("12"), Some("13"))),
            point("13", 40)
        );
    }

    /// Nothing past the position is readable yet — a caught-up Policy, or the #164
    /// recovery where the operator moves past a burned position. The events still to come
    /// were written at or above the watermark, so the point sits one below it and lets
    /// every one of them through.
    #[test]
    fn a_position_with_nothing_readable_past_it_resumes_below_the_watermark() {
        assert_eq!(
            resume_point(point("0", 264_786), log(Some("12"), None)),
            point("99", 264_786),
            "an append in flight holds a transaction at or above 100, so 99 precedes it"
        );
    }

    /// A position no event carries names no pair at all, so it is completed like any
    /// other instruction rather than refused.
    #[test]
    fn a_position_no_event_carries_is_completed_too() {
        assert_eq!(
            resume_point(point("7", 500), log(None, None)),
            point("99", 500)
        );
    }

    /// `StartAt::Beginning` before the first poll: the sentinel at position 0 is where a
    /// Policy that has processed nothing sits, and it already precedes every transaction.
    /// Deriving a point for it would move it forward for no reason.
    #[test]
    fn a_policy_that_has_processed_nothing_stays_at_the_sentinel() {
        let nothing_processed = FeedPoint {
            commit_txid: CommitStamp::SENTINEL,
            position: 0,
        };

        assert_eq!(
            resume_point(nothing_processed, log(None, Some("11"))),
            nothing_processed
        );
    }

    #[test]
    fn a_later_position_in_the_same_transaction_comes_later() {
        assert!(point("100", 8) < point("100", 9));
    }

    /// The property the whole read path rests on: what a write commits is delivered
    /// after everything the writes before it committed, whatever positions the two
    /// took. A position that arrives out of order is not out of *this* order.
    #[test]
    fn a_later_transaction_comes_later_whatever_position_it_took() {
        assert!(point("100", 900) < point("101", 8));
    }

    /// Events that predate the stamp (migration 0018) carry the sentinel, and a cursor
    /// that predates 0022 does too: both sit at the head of the order, where the
    /// position order they were written in is the order they are read in.
    #[test]
    fn the_sentinel_comes_before_every_real_transaction() {
        let migrated = FeedPoint {
            commit_txid: CommitStamp::SENTINEL,
            position: 264_786,
        };

        assert!(migrated < point("1", 1));
        assert!(
            migrated
                < FeedPoint {
                    commit_txid: CommitStamp::SENTINEL,
                    position: 264_787,
                }
        );
    }
}
