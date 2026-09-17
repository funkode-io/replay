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
//! in commit order needs none:
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
    use super::FeedPoint;
    use crate::commit_stamp::CommitStamp;

    fn point(commit_txid: &str, position: i64) -> FeedPoint {
        FeedPoint {
            commit_txid: CommitStamp::parse(commit_txid).expect("a plain counter parses"),
            position,
        }
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
