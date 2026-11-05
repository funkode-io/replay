-- Which lane each event was appended on (funkode-io/replay#301,
-- docs/adr/0034-a-bulk-append-reaches-policies-through-a-work-queue.md).
--
-- `online`, the default, is every event appended so far and every event appended through
-- `Cqrs::execute`: the lane the Policy feed reads — sweep, late-write lookup,
-- reconciliation (ADR-0026, ADR-0030). `bulk` is an event a bulk append wrote; the online
-- lane never delivers one, and a Policy's place advances past it as past a row its stream
-- filter excludes (ADR-0013).
--
-- A constant default is catalog-only in PostgreSQL 11+, so this ALTER rewrites nothing
-- and takes its ACCESS EXCLUSIVE lock for a moment, not for a scan. The CHECK is
-- `NOT VALID` for the same reason: validating it would read every row under that lock,
-- to confirm what the default already guarantees. New rows are checked either way; an
-- operator who wants the constraint marked valid runs
-- `ALTER TABLE events VALIDATE CONSTRAINT events_lane_known`, which takes a lock that
-- lets appends continue (release notes for 0.16.0).
ALTER TABLE events
    ADD COLUMN IF NOT EXISTS lane TEXT NOT NULL DEFAULT 'online';

ALTER TABLE events
    ADD CONSTRAINT events_lane_known CHECK (lane IN ('online', 'bulk')) NOT VALID;

-- `write_event` takes the lane as a ninth argument, defaulting to `online`.
--
-- Dropped and created rather than replaced: PostgreSQL keys a function on its argument
-- list, so `CREATE OR REPLACE` with a ninth argument would add an overload beside the
-- eight-argument one, and every eight-argument call would then be ambiguous between the
-- two. With the old one gone, an eight-argument call — `append_event`'s, compaction's,
-- and any from a process that has not been rolled yet — resolves to this one and its
-- default, and writes an online event exactly as before.
--
-- The body is 0033's — the stream-creation race fix — with the lane written alongside.
DROP FUNCTION IF EXISTS write_event(uuid, jsonb, jsonb, text, text, text, bigint, boolean);

CREATE FUNCTION write_event(
    p_id uuid,
    p_data jsonb,
    p_metadata jsonb,
    p_type text,
    p_stream_id text,
    p_stream_type text,
    p_expected_stream_version bigint,
    p_compacted_snapshot boolean,
    p_lane text DEFAULT 'online'
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
      INSERT INTO streams
      (id, type, version, stream_seq)
      VALUES
      (p_stream_id, p_stream_type, 0, 0)
      -- Named rather than `ON CONFLICT (id)`: a bare `id` here resolves to this
      -- function's OUT parameter of that name, which plpgsql refuses as ambiguous.
      ON CONFLICT ON CONSTRAINT streams_pkey DO NOTHING;

      -- Whoever ends up with the row, the re-read finds one: this insert either created
      -- it, or waited out the transaction that did and then did nothing. The version it
      -- carries is that transaction's, which is the number this append must follow.
      SELECT s.version, s.stream_seq INTO stream_version, next_stream_seq
      FROM streams as s
      WHERE s.id = p_stream_id FOR UPDATE;
    END IF;

    -- check optimistic concurrency
    IF p_expected_stream_version IS NOT NULL AND stream_version != p_expected_stream_version THEN
        RETURN;
    END IF;

    stream_version := stream_version + 1;
    next_stream_seq := next_stream_seq + 1;

    INSERT INTO events
        (id, data, metadata, stream_id, type, version, stream_seq, compacted_snapshot, lane)
    VALUES
        (p_id, p_data, p_metadata, p_stream_id, p_type, stream_version, next_stream_seq,
         p_compacted_snapshot, p_lane)
    RETURNING events.created INTO persisted_created;

    UPDATE streams as s
        SET version = stream_version,
            stream_seq = next_stream_seq
    WHERE
        s.id = p_stream_id;

    RETURN QUERY SELECT p_id, stream_version, persisted_created;
  END;
$$;
