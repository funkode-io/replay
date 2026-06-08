-- Mark synthetic events inserted by compaction so the Policy feed can skip them.
--
-- `compact` archives the original live events (UPDATE ... SET aggregate_version = n)
-- and inserts brand-new rows that represent the compacted state snapshot.  Those
-- synthetic rows must never be delivered to a Policy, because the real originals
-- are still present in the log and will (or already have) been delivered.
--
-- Design (see ADR 0004):
--   - compacted_snapshot = FALSE (default): a real event — written by normal `append`
--     or by archiving during compaction.  The policy feed processes these.
--   - compacted_snapshot = TRUE: a synthetic snapshot row written in step 6 of
--     `compact`.  The policy runner advances its cursor past these but does NOT
--     deliver them to a policy's `react` handler.
--
-- The partial index below accelerates the most common policy-feed scan pattern:
--   SELECT ... FROM events WHERE global_position > $cursor AND compacted_snapshot = false
--   ORDER BY global_position
ALTER TABLE events
    ADD COLUMN IF NOT EXISTS compacted_snapshot BOOLEAN NOT NULL DEFAULT FALSE;

CREATE INDEX IF NOT EXISTS idx_events_policy_feed
    ON events (global_position)
    WHERE compacted_snapshot = FALSE;
