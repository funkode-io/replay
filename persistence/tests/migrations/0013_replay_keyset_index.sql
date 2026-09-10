-- Index supporting the keyset cursor an inline-projection rebuild pages through.
--
-- `PostgresEventStoreBuilder::build` replays a projection's history in bounded chunks,
-- reissuing one query per page inside the rebuild transaction:
--
--   SELECT ... FROM events
--    WHERE (<filter>) AND (created, version, id) > ($1, $2, $3)
--    ORDER BY created, version, id
--    LIMIT  $4
--
-- The cursor carries `id` because `(created, version)` is not unique — versions restart
-- per stream and separate transactions can share a `created` instant — so a `>` cursor on
-- that pair alone would skip every row after the first of a tied group.
--
-- 0005 indexed `(created, version)`, which is only the leading prefix of that key: it
-- seeks to the right place, but Postgres must then re-sort each tied group to satisfy the
-- ORDER BY, and re-reads it on the following page. Indexing the whole key makes each page
-- a plain forward index scan.
CREATE INDEX IF NOT EXISTS idx_events_created_version_id
    ON events (created, version, id);

-- Redundant now: its column list is a prefix of the index above, which serves the same
-- `ORDER BY created, version` reads (`PostgresEventStore::stream_events`) equally well.
DROP INDEX IF EXISTS idx_events_created_version;
