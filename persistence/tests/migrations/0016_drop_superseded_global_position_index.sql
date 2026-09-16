-- no-transaction
-- Redundant now: 0015's unique index has the same column list and serves the policy
-- feed's `WHERE global_position > $cursor ORDER BY global_position` scan identically.
--
-- Dropped after the unique index exists, not before, so a live deployment never runs a
-- moment without an index on the column the feed reads. `CONCURRENTLY` for the same
-- reason the build is concurrent — a plain `DROP INDEX` takes an ACCESS EXCLUSIVE lock
-- on `events` and appends queue behind whatever read is already scanning it — and so,
-- like 0015, this file holds one statement outside a transaction block.
DROP INDEX CONCURRENTLY IF EXISTS idx_events_global_position;
