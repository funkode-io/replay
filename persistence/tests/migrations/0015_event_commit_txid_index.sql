-- no-transaction
--
-- Index for reading the log in commit order: transaction first, then position within it
-- (funkode-io/replay#193). Without it, `ORDER BY commit_txid, global_position` is a scan
-- plus a sort of the whole table; with it, it is a forward index scan a cursor can page.
--
-- Not partial on `compacted_snapshot` the way `idx_events_policy_feed` is: the reader
-- this index is for does not exist yet (funkode-io/replay#171), and the feed advances its
-- cursor across snapshot rows even though it delivers none of them (ADR-0013).
--
-- CONCURRENTLY, hence `-- no-transaction` above and one statement in this file: a plain
-- CREATE INDEX holds a lock that blocks appends for the length of the build, and
-- CONCURRENTLY cannot run inside a transaction block.
CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_events_commit_txid_position
    ON events (commit_txid, global_position);
