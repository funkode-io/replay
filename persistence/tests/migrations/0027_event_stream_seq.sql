-- Give every event its place in its own stream (funkode-io/replay#224).
--
-- `version` cannot be that place. Compaction renumbers a stream's live events from 1
-- (`compact`, steps 5-7), so `(stream_id, version)` names two different events over a
-- stream's lifetime. That renumbering is deliberate and stays: hydrating a compacted
-- aggregate always reads `1..N`, which is the point of the fast path. So the two axes
-- separate — `version` replays and restarts, `stream_seq` delivers and never restarts
-- (docs/adr/0023-a-stream-is-numbered-twice.md).
--
-- This is not an online migration: it rewrites every event row, and the `ADD COLUMN`
-- statements below hold ACCESS EXCLUSIVE on `streams` and `events` until the whole file
-- commits. The README ("What compaction does to an event's numbers") states what that
-- costs an operator.
--
-- The counter lives on `streams` rather than being derived from `MAX(events.stream_seq)`
-- so that funkode-io/replay#195's "which streams are behind" query reads one row per
-- stream. A constant default is catalog-only in PostgreSQL 11+, so this ALTER is cheap
-- where the one on `events` is not.
ALTER TABLE streams
    ADD COLUMN IF NOT EXISTS stream_seq BIGINT NOT NULL DEFAULT 0;

-- No default, and `NOT NULL` once the backfill has run: an insert that does not name a
-- place fails, rather than taking one that is already held or leaving a hole. That is
-- what makes `append_event` the only way in without a trigger standing guard.
ALTER TABLE events
    ADD COLUMN IF NOT EXISTS stream_seq BIGINT;

-- Number what is already there, in the order it was written. `global_position` is that
-- order within a stream even across a compaction that has already happened: the snapshot
-- rows were inserted after the originals they archived, so they sort after them.
UPDATE events AS e
   SET stream_seq = numbered.seq
  FROM (
        SELECT id,
               row_number() OVER (PARTITION BY stream_id ORDER BY global_position) AS seq
          FROM events
       ) AS numbered
 WHERE e.id = numbered.id
   AND e.stream_seq IS NULL;

-- One grouped pass rather than a lookup per stream: the index that would serve
-- `MAX(stream_seq)` does not exist yet, and is not worth ordering this migration around.
UPDATE streams AS s
   SET stream_seq = COALESCE(numbered.last_seq, 0)
  FROM (
        SELECT stream_id, MAX(stream_seq) AS last_seq FROM events GROUP BY stream_id
       ) AS numbered
 WHERE numbered.stream_id = s.id;

ALTER TABLE events
    ALTER COLUMN stream_seq SET NOT NULL;

-- A place in a stream holds one event, for good. The index also serves
-- funkode-io/replay#195's read: the events of one stream from a point, in delivery order,
-- without a sort.
--
-- Built in the transaction rather than CONCURRENTLY as migrations 0015 and 0019 build
-- theirs: `CREATE INDEX CONCURRENTLY` cannot run inside one, and the numbers and the index
-- that keeps them unique have to arrive together or not at all.
CREATE UNIQUE INDEX IF NOT EXISTS uidx_events_stream_seq
    ON events (stream_id, stream_seq);

-- One implementation writes an event, so that a stream's version and its place are
-- decided together and cannot drift apart (ADR-0023). Compaction's synthetic snapshot
-- rows come through it too — before this they were a second `INSERT INTO events` in
-- `compact`, and a second copy of the numbering rules.
--
-- `write_event` rather than an eighth argument on `append_event`: a default argument
-- would make every existing seven-argument call ambiguous between the two candidates, and
-- dropping the seven-argument form would fail every append from a process that has not
-- been rolled yet. `append_event` stays exactly as it was and delegates, so the migration
-- can be applied before or after a deployment, as 0018 was.
--
-- The counter is read from the row the function already holds `FOR UPDATE` and written by
-- the `UPDATE` it already performs, so an append costs what it cost before: one row
-- version of the streams row, not two.
CREATE OR REPLACE FUNCTION write_event(
    p_id uuid,
    p_data jsonb,
    p_metadata jsonb,
    p_type text,
    p_stream_id text,
    p_stream_type text,
    p_expected_stream_version bigint,
    p_compacted_snapshot boolean
) RETURNS TABLE(id uuid, version bigint, created timestamp with time zone)
  LANGUAGE plpgsql
  AS $$
  DECLARE
    stream_version bigint;
    next_stream_seq bigint;
    persisted_created timestamp with time zone;
  BEGIN
    SELECT s.version, s.stream_seq INTO stream_version, next_stream_seq
    FROM streams as s
    WHERE s.id = p_stream_id FOR UPDATE;

    -- if stream doesn't exist - create new one with version 0
    IF stream_version IS NULL THEN
      stream_version := 0;
      next_stream_seq := 0;

      INSERT INTO streams
      (id, type, version, stream_seq)
      VALUES
      (p_stream_id, p_stream_type, stream_version, next_stream_seq);
    END IF;

    -- check optimistic concurrency
    IF p_expected_stream_version IS NOT NULL AND stream_version != p_expected_stream_version THEN
        RETURN;
    END IF;

    stream_version := stream_version + 1;
    next_stream_seq := next_stream_seq + 1;

    INSERT INTO events
        (id, data, metadata, stream_id, type, version, stream_seq, compacted_snapshot)
    VALUES
        (p_id, p_data, p_metadata, p_stream_id, p_type, stream_version, next_stream_seq,
         p_compacted_snapshot)
    RETURNING events.created INTO persisted_created;

    UPDATE streams as s
        SET version = stream_version,
            stream_seq = next_stream_seq
    WHERE
        s.id = p_stream_id;

    RETURN QUERY SELECT p_id, stream_version, persisted_created;
  END;
$$;

CREATE OR REPLACE FUNCTION append_event(
    p_id uuid,
    p_data jsonb,
    p_metadata jsonb,
    p_type text,
    p_stream_id text,
    p_stream_type text,
    p_expected_stream_version bigint default null
) RETURNS TABLE(id uuid, version bigint, created timestamp with time zone)
  LANGUAGE sql
  AS $$
    SELECT * FROM write_event(p_id, p_data, p_metadata, p_type, p_stream_id, p_stream_type,
                              p_expected_stream_version, FALSE);
$$;
