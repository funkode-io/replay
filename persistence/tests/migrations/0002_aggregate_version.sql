-- Add aggregate_version column to events table.
-- NULL  = current (live) event stream for this aggregate.
-- n > 0 = events archived during the nth compaction run.
ALTER TABLE events
    ADD COLUMN IF NOT EXISTS aggregate_version INTEGER DEFAULT NULL;

-- Drop the existing unique constraint on (stream_id, version) because after
-- compaction the archived events keep their original version numbers while the
-- freshly inserted compacted events restart from 1 — so (stream_id, version)
-- is no longer globally unique. The new uniqueness scope is
-- (stream_id, version, aggregate_version).
ALTER TABLE events
    DROP CONSTRAINT IF EXISTS events_stream_and_version;

ALTER TABLE events
    ADD CONSTRAINT events_stream_version_agg
        UNIQUE (stream_id, version, aggregate_version);

-- Index to speed up compaction queries and version-scoped replays.
CREATE INDEX IF NOT EXISTS idx_events_aggregate_version
    ON events (stream_id, aggregate_version);
