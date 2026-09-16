//! The id of the transaction that wrote an event: `events.commit_txid`, and the
//! transaction half of a Policy's cursor.
//!
//! `xid8` is an unsigned 64-bit counter and sqlx has no codec for it, so it crosses
//! the wire as text in both directions: read as `commit_txid::text`, bound as
//! `$n::xid8`. The type exists so that conversion happens in one place rather than at
//! every query that touches the column.

use std::fmt;

/// A transaction id, ordered as Postgres orders `xid8`: numerically, without wraparound.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct CommitStamp(u64);

impl CommitStamp {
    /// `InvalidTransactionId`: never assigned to a transaction, so it names no write and
    /// orders before every real id. Carried by every event that predates migration 0018
    /// and by every cursor that predates 0020.
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
}

impl fmt::Display for CommitStamp {
    /// The form a `$n::xid8` bind takes.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

#[cfg(test)]
mod tests {
    use super::CommitStamp;

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
