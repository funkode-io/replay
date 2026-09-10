-- Indexes supporting inline-projection rebuild (replay) and the general event query API.
--
-- Both `PostgresEventStore::stream_events` and the projection drift-rebuild replay
-- (`load_events_for_replay`) read events with:
--
--   SELECT ... FROM events WHERE <filter> ORDER BY created, version ASC
--
-- 1. events(created, version)
--    Supports the ORDER BY for replays that scan a broad slice of history
--    (e.g. StreamFilter::All or stream-type filters), letting Postgres return rows
--    in order via an index scan instead of a full sequential scan + sort.
CREATE INDEX IF NOT EXISTS idx_events_created_version
    ON events (created, version);

-- 2. streams(type)
--    A projection's natural replay filter is `for_stream_type` -> ForStreamTypes,
--    whose SQL resolves stream ids via `SELECT id FROM streams WHERE type IN (...)`.
--    This index avoids scanning the whole streams table for that lookup.
CREATE INDEX IF NOT EXISTS idx_streams_type
    ON streams (type);
