-- no-transaction
-- Drop the keyset index 0013 built for `ORDER BY created, version, id`. No read issues
-- that order any more (ADR-0018); `idx_events_created` from 0020 covers what is left.
--
-- CONCURRENTLY so the drop takes no lock that would block appends; that cannot run
-- inside a transaction, hence `-- no-transaction` above.
DROP INDEX CONCURRENTLY IF EXISTS idx_events_created_version_id;
