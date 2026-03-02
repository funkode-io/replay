-- Add aggregate_version column to events table.
-- NULL  = current (live) event stream for this aggregate.
-- n > 0 = events archived during the nth compaction run.
ALTER TABLE events
    ADD COLUMN IF NOT EXISTS aggregate_version INTEGER DEFAULT NULL;

-- Drop the existing unique constraint on (stream_id, version) because after
-- compaction the archived events keep their original version numbers while the
-- freshly inserted compacted events restart from 1 — so (stream_id, version)
-- is no longer globally unique.
--
-- We cannot use a single UNIQUE constraint on (stream_id, version, aggregate_version)
-- because Postgres treats NULLs as distinct in UNIQUE constraints, which would allow
-- multiple live events with the same (stream_id, version) when aggregate_version IS NULL.
--
-- Instead we use two partial unique indexes:
--   1. Live stream  (aggregate_version IS NULL):
--        (stream_id, version) must be unique — enforces the concurrency guarantee
--        that no two current events share the same sequence number.
--   2. Archived streams (aggregate_version IS NOT NULL):
--        (stream_id, version, aggregate_version) must be unique — each archived
--        snapshot version has its own contiguous event sequence.
ALTER TABLE events
    DROP CONSTRAINT IF EXISTS events_stream_and_version;

CREATE UNIQUE INDEX IF NOT EXISTS uidx_events_live_version
    ON events (stream_id, version)
    WHERE aggregate_version IS NULL;

CREATE UNIQUE INDEX IF NOT EXISTS uidx_events_archived_version
    ON events (stream_id, version, aggregate_version)
    WHERE aggregate_version IS NOT NULL;

-- Index to speed up compaction queries and version-scoped replays.
CREATE INDEX IF NOT EXISTS idx_events_aggregate_version
    ON events (stream_id, aggregate_version);
