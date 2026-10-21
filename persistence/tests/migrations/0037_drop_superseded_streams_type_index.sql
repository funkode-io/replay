-- no-transaction
-- Redundant now: 0036's `idx_streams_type_id` leads with the same column and serves every
-- lookup by type this one did — `StreamFilter::ForStreamTypes`'s
-- `SELECT id FROM streams WHERE type IN (...)` among them.
--
-- Dropped after its replacement exists, not before, so no moment runs without an index
-- on `type`. CONCURRENTLY so the drop takes no lock that would block appends; that cannot
-- run inside a transaction, hence `-- no-transaction` above.
DROP INDEX CONCURRENTLY IF EXISTS idx_streams_type;
