//! Whether the hole a [Policy](crate::Policy)'s feed stopped at can still be filled.
//!
//! A `global_position` is taken from a sequence at `INSERT` and published at
//! `COMMIT`, so a missing one is normally an append about to land and the feed is
//! right to wait for it (ADR-0003 skip-safety). `nextval` is not transactional,
//! though: a value taken by a transaction that then aborts is burned, and the feed
//! waits for it forever — one burned position stopped nineteen policies for three
//! days (funkode-io/replay#164).
//!
//! The two cases are told apart exactly, with no timeout. A hole at `p` can only be
//! filled by a transaction that has *already* taken `p` from the sequence: the
//! sequence is past `p`, so no later `nextval` can return it. Such a transaction
//! holds `RowExclusiveLock` on the sequence until it ends, and `pg_locks` lists that
//! lock to any role. So the transactions that could fill the hole are enumerable at
//! the moment the hole is seen, and once none of them is still running the hole is
//! permanent.
//!
//! The lock is the oracle, not the transaction snapshot `pg_current_snapshot()`
//! offers, for two reasons. A transaction acquires an `xid` only when it first
//! writes, so one that has taken a sequence value and not yet inserted is in no
//! snapshot's `xip` list at all; and a snapshot's `xip` only lists running
//! transactions below its `xmax` (`latestCompletedXid + 1`), so the newest running
//! transaction is routinely absent from it too. A watermark built from `xmin`/`xmax`
//! would therefore call a hole permanent while the append that fills it is still
//! running, and skipping *that* is the silent event loss the feed exists to prevent.
//! The lock appears with the transaction's first statement and is released only
//! after its commit is published, which is what makes the ordering in
//! [`verdict`](BurnedPositions::verdict) safe.
//!
//! That is a claim about Postgres, so it is tested rather than asserted:
//! `a_transaction_snapshot_cannot_prove_a_position_is_burned_postgres_test` drives
//! the snapshot rule to a false verdict about a position a running append is holding.
//! Read it before replacing the oracle here with a cheaper-looking one.

use std::collections::HashMap;
use std::sync::Mutex;

use sqlx::{Pool, Postgres, Row};

/// Most sequence-lock holders one observation carries.
///
/// A holder is a transaction that has taken a value from the events sequence, so
/// the real population is bounded by the server's `max_connections` and is normally
/// a handful. The cap is the memory bound this module promises: past it an
/// observation is refused rather than truncated, because a *partial* candidate set
/// is worse than none — a missing candidate is an append that could still land, and
/// skipping its position loses the event.
const MAX_HOLDERS: i64 = 1024;

/// A transaction holding a value of the events sequence, named by its virtual
/// transaction id (`backend/local`).
///
/// Virtual rather than real: a transaction has a virtual id from its first
/// statement, but acquires an `xid` only when it first writes, and a transaction
/// that has taken a sequence value and not yet inserted has none. Prepared
/// transactions have no virtual id and are named by their `xid` instead.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Holder(String);

impl From<String> for Holder {
    fn from(id: String) -> Self {
        Self(id)
    }
}

#[cfg(test)]
impl From<&str> for Holder {
    fn from(id: &str) -> Self {
        Self(id.to_owned())
    }
}

/// The transactions that could still fill a hole, as one poll saw them.
///
/// Empty is the ordinary answer on a healthy system and the strongest one: nothing
/// holds a sequence value, so nothing can fill the hole.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct Holders(Vec<Holder>);

impl Holders {
    fn holds_any_of(&self, others: &Holders) -> bool {
        others.0.iter().any(|holder| self.0.contains(holder))
    }
}

impl<H: Into<Holder>> FromIterator<H> for Holders {
    fn from_iter<I: IntoIterator<Item = H>>(ids: I) -> Self {
        Self(ids.into_iter().map(Into::into).collect())
    }
}

/// What a poll concluded about the hole in front of a Policy.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Permanence {
    /// A transaction that could fill the hole is still running. Keep waiting: this
    /// is the in-flight append the feed is designed to wait for.
    Fillable,
    /// Every transaction that could have filled the hole has ended. The position
    /// can never appear, so the Policy may be moved past it — after confirming, in
    /// a snapshot taken *later than this verdict*, that it is still missing.
    Permanent,
}

/// One Policy's candidate set for one hole.
struct Candidates {
    /// The hole the set was collected for. A different one is a different question.
    missing_position: i64,
    /// Transactions holding the sequence when the hole was first observed. Only
    /// these can ever fill it, so the set is never widened by a later poll.
    holders: Holders,
}

/// Which holes are permanent, per Policy, in memory.
///
/// One entry per Policy, each bounded by [`MAX_HOLDERS`], so the whole structure is
/// bounded by the code that registers Policies. Nothing is persisted: the state is
/// a cache of an answer the database can always be asked again, and a restart or a
/// leadership change simply re-observes.
pub(crate) struct BurnedPositions {
    seen: Mutex<HashMap<String, Candidates>>,
}

impl BurnedPositions {
    pub(crate) fn new() -> Self {
        Self {
            seen: Mutex::new(HashMap::new()),
        }
    }

    /// Record `policy` parked in front of `missing_position` while `holders` hold
    /// the sequence, and say whether the hole can still be filled.
    ///
    /// The first sighting of a hole fixes its candidate set; later polls only ever
    /// check whether any of *those* transactions is still running. Collecting the
    /// set after the read that found the hole is safe, and collecting it before the
    /// read would not be: a candidate that ended in between either committed — and
    /// the confirming read then finds the position present — or aborted, and could
    /// never fill it.
    ///
    /// A holder list that is empty the first time is already an answer: the hole is
    /// permanent on the poll that saw it, which is the common case for a burned
    /// position discovered long after the abort that burned it.
    pub(crate) fn verdict(
        &self,
        policy: &str,
        missing_position: i64,
        holders: Holders,
    ) -> Permanence {
        let mut seen = self.lock();
        let candidates = seen.entry(policy.to_owned()).or_insert_with(|| Candidates {
            missing_position,
            holders: holders.clone(),
        });
        if candidates.missing_position != missing_position {
            *candidates = Candidates {
                missing_position,
                holders: holders.clone(),
            };
        }

        if candidates.holders.holds_any_of(&holders) {
            Permanence::Fillable
        } else {
            Permanence::Permanent
        }
    }

    /// Forget `policy`: it is no longer parked where it was, so its next hole is a
    /// fresh question. Dropping the set matters — an operator who rewinds the cursor
    /// can put the Policy back in front of a position that is now being appended,
    /// and a candidate set collected before that is answering about other
    /// transactions entirely.
    pub(crate) fn cleared(&self, policy: &str) {
        self.lock().remove(policy);
    }

    /// A poisoned lock is not worth failing a drain over: the worst a recovered one
    /// does is re-observe a hole whose candidates it already knew.
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<String, Candidates>> {
        self.seen.lock().unwrap_or_else(|e| e.into_inner())
    }
}

/// Where a Policy that is parked in front of burned positions resumes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Resume {
    /// The hole is gone, or nothing exists past the cursor at all: there is nothing
    /// to skip, and the feed's next poll is the whole answer.
    NothingToSkip,
    /// The stored cursor is no longer where this process left it — an operator
    /// moved it. The hole this verdict was about belongs to a position the Policy
    /// has already abandoned.
    CursorMoved,
    /// Positions `cursor + 1 ..= next_position - 1` can never appear. The Policy
    /// resumes at `next_position`, so the whole burned run is crossed at once
    /// rather than one poll per position.
    Skip { next_position: i64 },
}

/// Read the sequence's lock holders: every transaction that has taken a value from
/// it and not yet ended.
///
/// `Ok(None)` when more than [`MAX_HOLDERS`] hold it — the candidate set would be
/// incomplete, and an incomplete set must never be used to declare a hole permanent.
///
/// The sequence is resolved through `pg_get_serial_sequence` rather than named, so
/// a renamed or non-default sequence is still the one probed. A column that owns no
/// sequence is an error and not an empty answer, for the same reason: "nothing holds
/// the sequence" is the verdict that unblocks a skip.
///
/// `pg_locks` is cluster-wide while a relation's identifier is only meaningful inside
/// one database, so the rows are scoped to this one. Without that, a lock held in
/// another database on a relation that happens to share the identifier counts as a
/// candidate, and a long-lived transaction over there keeps a Policy waiting for an
/// append that cannot exist.
pub(crate) async fn sequence_holders(
    pool: &Pool<Postgres>,
) -> Result<Option<Holders>, replay::Error> {
    let row = sqlx::query(
        "SELECT pg_get_serial_sequence('events', 'global_position') AS sequence_name, \
         (SELECT COALESCE(array_agg(held.holder), ARRAY[]::text[]) FROM ( \
            SELECT COALESCE(l.virtualtransaction, l.transactionid::text) AS holder \
            FROM pg_locks l \
            WHERE l.locktype = 'relation' \
              AND l.database = (SELECT d.oid FROM pg_database d \
                                WHERE d.datname = current_database()) \
              AND l.relation = pg_get_serial_sequence('events', 'global_position')::regclass \
            LIMIT $1 \
         ) held) AS holders",
    )
    // One over the cap, so a full page is recognisable as "too many" rather than
    // passing for a complete set of exactly `MAX_HOLDERS`.
    .bind(MAX_HOLDERS + 1)
    .fetch_one(pool)
    .await
    .map_err(crate::db_error)?;

    if row.get::<Option<String>, _>("sequence_name").is_none() {
        return Err(replay::Error::internal(
            "events.global_position owns no sequence, so a hole in the policy feed \
             cannot be told from an append still in flight",
        )
        .with_operation("policy_drain"));
    }

    let holders: Vec<String> = row.get("holders");
    if holders.len() as i64 > MAX_HOLDERS {
        return Ok(None);
    }

    Ok(Some(holders.into_iter().collect()))
}

/// Confirm `policy` is still parked at `cursor` and find where it resumes.
///
/// **Must be called after [`sequence_holders`] has cleared the hole**, never
/// before or in the same statement. Postgres publishes a transaction's commit
/// before releasing its locks, so a position still missing in a snapshot taken
/// after the lock has gone is a position no transaction will ever write. The
/// reverse order proves nothing: a commit can land between reading the row and
/// reading the locks.
///
/// Two index probes, no scan of the event log.
pub(crate) async fn resume_after_burned(
    pool: &Pool<Postgres>,
    policy: &str,
    cursor: i64,
) -> Result<Resume, replay::Error> {
    let row = sqlx::query(
        "SELECT (SELECT MIN(global_position) FROM events WHERE global_position > $2) \
         AS next_position FROM policy_cursors WHERE name = $1 AND position = $2",
    )
    .bind(policy)
    .bind(cursor)
    .fetch_optional(pool)
    .await
    .map_err(crate::db_error)?;

    Ok(match row {
        None => Resume::CursorMoved,
        Some(row) => resume_from(cursor, row.get("next_position")),
    })
}

/// The decision [`resume_after_burned`] makes once the database has answered.
fn resume_from(cursor: i64, next_position: Option<i64>) -> Resume {
    match next_position {
        Some(next_position) if next_position > cursor + 1 => Resume::Skip { next_position },
        _ => Resume::NothingToSkip,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const POLICY: &str = "import_started";
    const HOLE: i64 = 264_786;

    fn holders(ids: &[&str]) -> Holders {
        ids.iter().copied().collect()
    }

    /// The incident's shape: the abort that burned the position is long over, so
    /// nothing holds the sequence and the first poll to look already knows.
    #[test]
    fn a_hole_no_transaction_can_fill_is_permanent_at_once() {
        let burned = BurnedPositions::new();

        assert_eq!(
            burned.verdict(POLICY, HOLE, Holders::default()),
            Permanence::Permanent
        );
    }

    /// The case the wait exists for: an append still running holds the sequence, so
    /// its position is not a hole to skip but an event about to arrive.
    #[test]
    fn a_hole_a_running_transaction_could_fill_is_not_permanent() {
        let burned = BurnedPositions::new();

        assert_eq!(
            burned.verdict(POLICY, HOLE, holders(&["4/731"])),
            Permanence::Fillable
        );
    }

    #[test]
    fn a_hole_becomes_permanent_once_its_candidates_have_ended() {
        let burned = BurnedPositions::new();

        assert_eq!(
            burned.verdict(POLICY, HOLE, holders(&["4/731"])),
            Permanence::Fillable
        );
        assert_eq!(
            burned.verdict(POLICY, HOLE, Holders::default()),
            Permanence::Permanent
        );
    }

    /// Transactions that started *after* the hole was seen cannot fill it: the
    /// sequence is already past the missing position, so their `nextval` returns a
    /// higher one. Only the original candidates count.
    #[test]
    fn transactions_that_arrived_after_the_hole_do_not_prolong_it() {
        let burned = BurnedPositions::new();

        burned.verdict(POLICY, HOLE, holders(&["4/731"]));

        assert_eq!(
            burned.verdict(POLICY, HOLE, holders(&["9/12", "11/4"])),
            Permanence::Permanent
        );
    }

    /// One candidate still running is enough, however many others have ended.
    #[test]
    fn one_surviving_candidate_keeps_the_hole_open() {
        let burned = BurnedPositions::new();

        burned.verdict(POLICY, HOLE, holders(&["4/731", "5/2", "6/9"]));

        assert_eq!(
            burned.verdict(POLICY, HOLE, holders(&["5/2", "9/12"])),
            Permanence::Fillable
        );
    }

    /// A Policy that moves on meets a new hole, and a new hole is a new question:
    /// the candidate set is collected again rather than inherited.
    #[test]
    fn a_different_hole_collects_a_new_candidate_set() {
        let burned = BurnedPositions::new();

        burned.verdict(POLICY, HOLE, Holders::default());

        assert_eq!(
            burned.verdict(POLICY, HOLE + 9, holders(&["4/731"])),
            Permanence::Fillable
        );
    }

    /// Nineteen policies stopped at the same hole in the incident, and each asks
    /// independently.
    #[test]
    fn policies_are_judged_independently() {
        let burned = BurnedPositions::new();

        burned.verdict(POLICY, HOLE, holders(&["4/731"]));

        assert_eq!(
            burned.verdict("price_fanout", HOLE, holders(&["4/731"])),
            Permanence::Fillable
        );
    }

    /// An operator rewinding the cursor can park a Policy in front of a position
    /// that is being appended right now. The verdict recorded before the rewind was
    /// about other transactions, so it must not be reused.
    #[test]
    fn clearing_a_policy_forgets_its_candidate_set() {
        let burned = BurnedPositions::new();

        burned.verdict(POLICY, HOLE, holders(&["4/731"]));
        burned.cleared(POLICY);

        assert_eq!(
            burned.verdict(POLICY, HOLE, holders(&["4/731"])),
            Permanence::Fillable
        );
    }

    #[test]
    fn clearing_a_policy_that_was_never_parked_is_a_no_op() {
        let burned = BurnedPositions::new();

        burned.cleared("never_ran");

        assert_eq!(
            burned.verdict("never_ran", HOLE, Holders::default()),
            Permanence::Permanent
        );
    }

    /// The whole burned run is crossed at once: a hundred positions burned by one
    /// aborted batch cost one poll, not a hundred.
    #[test]
    fn a_run_of_burned_positions_is_crossed_in_one_move() {
        assert_eq!(
            resume_from(264_785, Some(264_886)),
            Resume::Skip {
                next_position: 264_886
            }
        );
    }

    #[test]
    fn a_hole_that_filled_between_the_read_and_the_verdict_is_not_skipped() {
        assert_eq!(resume_from(264_785, Some(264_786)), Resume::NothingToSkip);
    }

    /// Nothing past the cursor at all: a caught-up Policy, not a blocked one.
    #[test]
    fn a_cursor_with_nothing_past_it_skips_nothing() {
        assert_eq!(resume_from(264_785, None), Resume::NothingToSkip);
    }
}
