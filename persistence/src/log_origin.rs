//! Which PostgreSQL cluster issued this log's transaction stamps.
//!
//! The Policy feed orders by `events.commit_txid`, an `xid8` drawn from a counter one
//! cluster owns ([ADR-0022](../../docs/adr/0022-policy-feed-reads-below-the-commit-watermark.md)).
//! Move the log to another cluster and that order is not wrong in a visible way — it is
//! silent: a cursor carrying a restored stamp sorts above every event the new cluster
//! appends, so the feed reads empty, the Policy reports itself idle, and every reaction
//! is dropped.
//!
//! Migration 0025 records the cluster's `system_identifier` in the log. A copy that
//! carries the identifier carries the counter too (PITR, promoting a replica); one that
//! does not was made logically, and the stamps came with it.

use std::collections::HashSet;
use std::sync::Mutex;

/// The rebase a logically restored log needs before it can be read, in batches.
///
/// Every restored event is committed — there is no open transaction left in a cluster
/// that no longer exists — so the sentinel is the honest stamp for all of them: it orders
/// below every id the new cluster will issue, and the restored events keep the
/// `global_position` order they already have. It is the state migration 0018 leaves for
/// events older than itself.
const REBASE_RESTORED_STAMPS: &str = "\
    UPDATE events SET commit_txid = '0'::xid8 WHERE global_position IN \
    (SELECT global_position FROM events WHERE commit_txid <> '0'::xid8 \
     ORDER BY global_position LIMIT 50000); -- repeat until it reports 0 rows, then: \
    UPDATE policy_cursors SET commit_txid = '0'::xid8;";

/// What tells the log it is where it is now, once the stamps are sound.
const ADOPT_THIS_CLUSTER: &str = "\
    UPDATE event_log_origin SET system_identifier = \
    (SELECT system_identifier FROM pg_control_system());";

/// Policy feeds whose foreign log has been diagnosed in this process.
///
/// The condition is permanent until an operator acts and the poll loop keeps polling, so
/// the diagnosis is written once rather than once per poll. Its absence afterwards is not
/// silence: every poll still fails, and the worker logs that it failed.
///
/// Keyed by the cluster as well as the database and the policy, because one process can
/// run two runners against two clusters and they are two separate outages — and the case
/// this whole check exists for, a blue/green cutover, is exactly the one where both
/// clusters hold a database of the same name. Bounded by the policies registered against
/// each.
static DIAGNOSED: Mutex<Option<HashSet<(i64, String, String)>>> = Mutex::new(None);

/// Whether this is the first time this process has diagnosed `policy` in `database` on
/// this cluster.
fn first_diagnosis(cluster_now: i64, database: &str, policy: &str) -> bool {
    let mut diagnosed = DIAGNOSED.lock().unwrap_or_else(|e| e.into_inner());
    diagnosed.get_or_insert_with(HashSet::new).insert((
        cluster_now,
        database.to_owned(),
        policy.to_owned(),
    ))
}

/// Refuse a log this cluster did not write, and tell the operator how to repair it.
///
/// Identity rather than arithmetic: comparing stamps against this cluster's own counter
/// catches a restore only while that counter is still behind them. Once it has passed —
/// a backfill between the restore and the first poll is enough — restored and local
/// stamps overlap, and no comparison can separate them while the feed silently skips the
/// local ones.
///
/// The two repairs are not interchangeable, so the message names both and leaves the
/// choice to whoever knows which operation was performed. `pg_upgrade` changes the
/// identifier while carrying the transaction counter: its stamps are sound and only the
/// identity has to be adopted. A logical restore carries neither: its stamps have to be
/// rebased first.
/// `wrote_the_log` is `None` when the origin row is missing, which migration 0025 creates
/// and nothing in the library deletes: a log that cannot say where it was written is
/// refused on the same grounds as one that says elsewhere.
pub(crate) fn reject_foreign_log(
    database: &str,
    policy: &str,
    wrote_the_log: Option<i64>,
    cluster_now: i64,
) -> Result<(), replay::Error> {
    if wrote_the_log == Some(cluster_now) {
        return Ok(());
    }
    let wrote_the_log = wrote_the_log.unwrap_or_default();

    if first_diagnosis(cluster_now, database, policy) {
        tracing::error!(
            database,
            policy,
            wrote_the_log,
            cluster_now,
            rebase = REBASE_RESTORED_STAMPS,
            adopt = ADOPT_THIS_CLUSTER,
            "policy {policy} cannot continue: this log was written by PostgreSQL cluster \
             {wrote_the_log} and is being read in cluster {cluster_now}. Events are \
             ordered by the transaction that wrote them, and transaction ids belong to \
             the cluster that issued them, so reading the feed here would skip events \
             rather than deliver them late. With the daemon stopped: after an in-place \
             upgrade (`pg_upgrade`), which carries the transaction counter, the stamps \
             are sound and only the identity has to be adopted — run `adopt`. After a \
             logical restore (`pg_dump`/`pg_restore`, logical replication), the stamps \
             came from the old cluster — run `rebase`, then `adopt`."
        );
    }

    Err(replay::Error::internal(format!(
        "policy {policy} reads a log written by cluster {wrote_the_log} in cluster \
         {cluster_now}; see the logged remedy"
    ))
    .with_operation("policy_feed_log_origin")
    .with_context("policy", policy)
    .with_context("database", database)
    .with_context("wrote_the_log", wrote_the_log.to_string())
    .with_context("cluster_now", cluster_now.to_string()))
}

#[cfg(test)]
mod tests {
    use super::{reject_foreign_log, ADOPT_THIS_CLUSTER, REBASE_RESTORED_STAMPS};

    #[test]
    fn a_log_this_cluster_wrote_is_read() {
        let cluster = 7_687_535_857_959_821_350;

        assert!(reject_foreign_log("app", "orders", Some(cluster), cluster).is_ok());
    }

    /// A log with no recorded origin vouches for nothing, and is refused like one that
    /// names another cluster.
    #[test]
    fn a_log_that_does_not_say_where_it_was_written_is_refused() {
        assert!(reject_foreign_log("app", "orders", None, 42).is_err());
    }

    /// The case arithmetic on stamps cannot see: the log is foreign whatever the
    /// identifiers happen to be, including when one is lower than the other.
    #[test]
    fn a_log_another_cluster_wrote_is_refused_either_way_round() {
        for (wrote, now) in [(9_000_i64, 10_i64), (10, 9_000)] {
            let refused = reject_foreign_log("app", "orders", Some(wrote), now)
                .expect_err("a log from another cluster cannot be read");

            assert!(
                format!("{refused:?}").contains("orders"),
                "the error names the policy an operator has to fix: {refused:?}"
            );
        }
    }

    /// Both repairs are named, because only the operator knows which move was made.
    #[test]
    fn the_remedies_cover_an_upgrade_and_a_logical_restore() {
        assert!(ADOPT_THIS_CLUSTER.contains("UPDATE event_log_origin"));
        assert!(
            REBASE_RESTORED_STAMPS.contains("LIMIT 50000"),
            "the rebase is batched: it rewrites every row of a table that can be huge"
        );
    }
}
