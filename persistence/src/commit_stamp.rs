//! The id of the transaction that wrote an event: `events.commit_txid`, and the
//! transaction half of a Policy's cursor.
//!
//! `xid8` is an unsigned 64-bit counter and sqlx has no codec for it, so it crosses
//! the wire as text in both directions: read as `commit_txid::text`, bound as
//! `$n::xid8`. The type exists so that conversion happens in one place rather than at
//! every query that touches the column.

use std::collections::HashSet;
use std::fmt;
use std::sync::Mutex;

use sqlx::{postgres::PgRow, Row};

/// A transaction id, ordered as Postgres orders `xid8`: numerically, without wraparound.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct CommitStamp(u64);

impl CommitStamp {
    /// `InvalidTransactionId`: never assigned to a transaction, so it names no write and
    /// orders before every real id. Carried by every event that predates migration 0018
    /// and by every cursor that predates 0022.
    pub(crate) const SENTINEL: Self = CommitStamp(0);

    /// Read a stamp out of a `commit_txid::text` column.
    ///
    /// A value that is not a 64-bit counter is a schema this code does not understand,
    /// not a row to skip: it fails the read rather than defaulting to the sentinel,
    /// which would silently order the event before the whole log.
    pub(crate) fn parse(text: &str) -> Result<Self, replay::Error> {
        text.parse().map(CommitStamp).map_err(|_| {
            replay::Error::internal("a transaction id is an unsigned 64-bit counter")
                .with_operation("read_commit_txid")
                .with_context("commit_txid", text)
        })
    }

    /// The stamp in `column`, which the query must have selected as `::text`.
    pub(crate) fn from_row(row: &PgRow, column: &str) -> Result<Self, replay::Error> {
        Self::parse(row.get::<String, _>(column).as_str())
    }

    /// The stamp one below this one: a point the feed reads *before* every transaction
    /// from here up. Saturates at the sentinel, which already precedes them all.
    pub(crate) fn previous(self) -> Self {
        CommitStamp(self.0.saturating_sub(1))
    }
}

/// The two statements that put a restored log back in an order this cluster can read.
///
/// Zeroing is what migration 0018 does for events older than itself: the sentinel is
/// below every id this cluster will issue, so restored events stay ahead of everything
/// appended afterwards and keep the `global_position` order they already have. It is
/// sound because every restored event is, by definition, committed — there is no open
/// transaction left in a cluster that no longer exists.
const REBASE_RESTORED_STAMPS: &str = "\
    UPDATE events SET commit_txid = '0'::xid8 \
    WHERE commit_txid >= pg_snapshot_xmax(pg_current_snapshot()); \
    UPDATE policy_cursors SET commit_txid = '0'::xid8 \
    WHERE commit_txid >= pg_snapshot_xmax(pg_current_snapshot());";

/// Which stored stamp was found to come from another cluster.
#[derive(Clone, Copy, Debug)]
pub(crate) enum StampSource {
    /// The Policy's own cursor: the silent case, where the feed comes back empty.
    Cursor,
    /// The oldest event the watermark holds back: the loud case, where it stalls.
    WithheldEvent,
}

impl fmt::Display for StampSource {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            StampSource::Cursor => write!(f, "cursor"),
            StampSource::WithheldEvent => write!(f, "oldest withheld event"),
        }
    }
}

/// Policies whose foreign stamp has been diagnosed in this process.
///
/// The condition is permanent until an operator acts, and the poll loop keeps polling, so
/// the full diagnosis is written once per Policy rather than once per poll. Its absence
/// afterwards is not silence: every poll still fails, and the worker logs that it failed.
/// Bounded by the number of registered policies.
static DIAGNOSED: Mutex<Option<HashSet<String>>> = Mutex::new(None);

/// Whether this is the first time this process has diagnosed `policy`.
fn first_diagnosis(policy: &str) -> bool {
    let mut diagnosed = DIAGNOSED.lock().unwrap_or_else(|e| e.into_inner());
    diagnosed
        .get_or_insert_with(HashSet::new)
        .insert(policy.to_owned())
}

/// Refuse a stamp this cluster has not issued, and tell the operator how to repair it.
///
/// `next_id` is `pg_snapshot_xmax(pg_current_snapshot())`, the first id not yet assigned:
/// every transaction this cluster has ever run is below it, so a stored stamp at or above
/// it was written somewhere else. That is what a logical restore leaves behind — `xid8`
/// is a cluster-local counter, and `pg_dump`/`pg_restore` or logical replication copies
/// the column into a cluster whose own counter starts again from the beginning.
///
/// It has to stop the Policy rather than warn it: the feed would not deliver something
/// wrong, it would deliver *nothing*. A cursor stamped 50000 sorts above every event a
/// fresh cluster appends, so the Policy reads an empty feed, reports itself idle, and
/// skips every reaction until the counter climbs past the restored value — the one
/// failure this library does not allow itself.
///
/// The diagnosis is logged rather than carried by the error, because the runner logs
/// errors by `Display` and an `Internal` error redacts its message there.
pub(crate) fn reject_foreign_stamp(
    policy: &str,
    what: StampSource,
    stamp: CommitStamp,
    next_id: CommitStamp,
) -> Result<(), replay::Error> {
    if stamp < next_id {
        return Ok(());
    }

    if first_diagnosis(policy) {
        tracing::error!(
        policy,
        stamp = %stamp,
        next_id = %next_id,
        remedy = REBASE_RESTORED_STAMPS,
        "policy {policy} cannot continue: its {what} is stamped with transaction {stamp}, \
         which this database has not issued — its next id is {next_id}. Transaction ids \
         belong to one PostgreSQL cluster, so this log was restored into another one with \
         pg_dump/pg_restore, or replicated into it logically. Reading the feed now would \
         skip every event appended since. Stop the daemon and run the statements in \
         `remedy`, which rebase the restored stamps below this cluster's counter."
        );
    }

    Err(replay::Error::internal(format!(
        "policy {policy} has a {what} stamped by another cluster ({stamp}, this cluster's \
         next id is {next_id}); see the logged remedy"
    ))
    .with_operation("policy_feed_stamp_origin")
    .with_context("policy", policy)
    .with_context("stamp", stamp.to_string())
    .with_context("next_id", next_id.to_string()))
}

impl fmt::Display for CommitStamp {
    /// The form a `$n::xid8` bind takes.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::{reject_foreign_stamp, CommitStamp, StampSource, REBASE_RESTORED_STAMPS};

    fn stamp(id: &str) -> CommitStamp {
        CommitStamp::parse(id).expect("a plain counter parses")
    }

    /// The boundary is exact: `pg_snapshot_xmax` is the first id *not* assigned, so the
    /// id below it is this cluster's newest transaction and must pass.
    #[test]
    fn the_newest_transaction_this_cluster_issued_is_not_foreign() {
        assert!(
            reject_foreign_stamp("orders", StampSource::Cursor, stamp("41"), stamp("42")).is_ok()
        );
    }

    #[test]
    fn a_stamp_this_cluster_has_not_issued_is_refused() {
        let refused = reject_foreign_stamp("orders", StampSource::Cursor, stamp("42"), stamp("42"))
            .expect_err("a stamp at the next id was issued elsewhere");

        assert!(
            format!("{refused:?}").contains("orders"),
            "the error names the policy an operator has to fix: {refused:?}"
        );
        assert!(
            REBASE_RESTORED_STAMPS.contains("UPDATE events SET commit_txid")
                && REBASE_RESTORED_STAMPS.contains("UPDATE policy_cursors SET commit_txid"),
            "and the logged remedy repairs both tables: {REBASE_RESTORED_STAMPS}"
        );
    }

    /// The sentinel is what the remedy writes, and what migration 0018 left behind.
    #[test]
    fn the_sentinel_belongs_to_every_cluster() {
        assert!(reject_foreign_stamp(
            "orders",
            StampSource::Cursor,
            CommitStamp::SENTINEL,
            stamp("42")
        )
        .is_ok());
    }

    #[test]
    fn a_stamp_survives_the_round_trip_through_text() {
        let stamp = CommitStamp::parse("4294967296").expect("a plain counter parses");

        assert_eq!(stamp.to_string(), "4294967296");
    }

    /// Why `xid8` and not `xid`: the counter is 64 bits wide, and the top half of it
    /// does not fit in the `i64` a bigint column would offer.
    #[test]
    fn a_stamp_past_the_signed_range_is_still_a_stamp() {
        let past_i64 = u64::MAX.to_string();

        let stamp = CommitStamp::parse(&past_i64).expect("the counter is unsigned");

        assert_eq!(stamp.to_string(), past_i64);
    }

    #[test]
    fn the_sentinel_orders_before_every_real_transaction() {
        assert!(CommitStamp::SENTINEL < CommitStamp::parse("1").unwrap());
        assert_eq!(CommitStamp::SENTINEL.to_string(), "0");
    }

    #[test]
    fn the_stamp_before_one_is_below_it_and_never_below_the_sentinel() {
        assert_eq!(
            CommitStamp::parse("100").unwrap().previous().to_string(),
            "99"
        );
        assert_eq!(CommitStamp::SENTINEL.previous(), CommitStamp::SENTINEL);
    }

    #[test]
    fn a_value_that_is_not_a_counter_fails_the_read() {
        let error = CommitStamp::parse("-1").expect_err("a transaction id is unsigned");

        // `internal` keeps its message out of `Display`; the diagnosis is in `Debug`.
        let reported = format!("{error:?}");
        assert!(
            reported.contains("64-bit counter") && reported.contains("-1"),
            "the error says what the column held and what it should have: {reported}"
        );
    }
}
