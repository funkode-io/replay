-- no-transaction
--
-- Index for reading the log in commit order: transaction first, then position within it
-- (funkode-io/replay#193). Without it, `ORDER BY commit_txid, global_position` is a scan
-- plus a sort of the whole table; with it, it is a forward index scan a cursor can page.
--
-- Same shape as 0015, for the same reasons and with the same recovery: built
-- CONCURRENTLY so appends keep working, hence `-- no-transaction` and a single statement
-- in this file, and deliberately not `IF NOT EXISTS`, so a rerun stops at "relation
-- already exists" instead of stepping over an invalid index. 0015 records which leftover
-- is which and what to do about each; nothing here differs.
--
-- Nothing constrains the data, so there is no pre-check migration of the kind 0014 is for
-- 0015: this index is not unique and cannot fail on the rows it finds.
CREATE INDEX CONCURRENTLY idx_events_commit_txid_position
    ON events (commit_txid, global_position);
