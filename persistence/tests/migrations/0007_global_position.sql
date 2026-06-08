-- Global position: a single monotonic, store-wide ordering for events.
--
-- Per-stream `version` orders events *within* one aggregate stream, but Policies
-- need a *total* order over the whole log to checkpoint against. We add a
-- `BIGSERIAL` so every appended event gets a globally increasing position, and
-- index it so the policy feed can scan `WHERE global_position > $cursor ORDER BY
-- global_position` cheaply.
--
-- NOTE on visibility: BIGSERIAL values are assigned at INSERT time but become
-- visible at COMMIT time, so under concurrent appends a higher position can
-- commit before a lower one. The policy reader compensates with a high-water-mark
-- scan (it only advances across a contiguous, gap-free prefix), so an
-- assigned-but-not-yet-committed position is never skipped.
ALTER TABLE events
    ADD COLUMN IF NOT EXISTS global_position BIGSERIAL;

CREATE INDEX IF NOT EXISTS idx_events_global_position
    ON events (global_position);
