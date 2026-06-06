-- Evolve append_event to return persisted event metadata used by inline projections.
--
-- Previous signature:
--   append_event(...) RETURNS boolean
-- New signature:
--   append_event(...) RETURNS TABLE(id uuid, version bigint, created timestamptz)
--
-- Behavior:
-- - On successful append: returns exactly one row with persisted id/version/created.
-- - On optimistic-concurrency mismatch: returns zero rows.

-- The original append_event (0001) returns boolean. Postgres cannot change a
-- function's return type with CREATE OR REPLACE, so drop it first. Functions are
-- identified by argument types, so the parameter-name change below is irrelevant here.
DROP FUNCTION IF EXISTS append_event(uuid, jsonb, jsonb, text, text, text, bigint);

CREATE OR REPLACE FUNCTION append_event(
    p_id uuid,
    p_data jsonb,
    p_metadata jsonb,
    p_type text,
    p_stream_id text,
    p_stream_type text,
    p_expected_stream_version bigint default null
) RETURNS TABLE(id uuid, version bigint, created timestamp with time zone)
  LANGUAGE plpgsql
  AS $$
  DECLARE
    stream_version bigint;
    persisted_created timestamp with time zone;
  BEGIN
    -- get stream version
    SELECT
      s.version INTO stream_version
    FROM streams as s
    WHERE
      s.id = p_stream_id FOR UPDATE;

    -- if stream doesn't exist - create new one with version 0
    IF stream_version IS NULL THEN
      stream_version := 0;

      INSERT INTO streams
      (id, type, version)
      VALUES
      (p_stream_id, p_stream_type, stream_version);
    END IF;

    -- check optimistic concurrency
    IF p_expected_stream_version IS NOT NULL AND stream_version != p_expected_stream_version THEN
        RETURN;
    END IF;

    -- increment event_version
    stream_version := stream_version + 1;

    -- append event
    INSERT INTO events
        (id, data, metadata, stream_id, type, version)
    VALUES
        (p_id, p_data, p_metadata, p_stream_id, p_type, stream_version)
    RETURNING events.created INTO persisted_created;

    -- update stream version
    UPDATE streams as s
        SET version = stream_version
    WHERE
        s.id = p_stream_id;

    RETURN QUERY SELECT p_id, stream_version, persisted_created;
  END;
$$;
