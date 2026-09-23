-- A stream row is created by whoever gets there first (funkode-io/replay#232).
--
-- `SELECT ... FOR UPDATE` locks nothing when the row is absent, so two first appends to
-- one new stream both used to insert it and the loser died on `streams_pkey` — an
-- `Internal` error where a caller expects either a version or a conflict.
--
-- `ON CONFLICT DO NOTHING` defers to the winner, and the re-read is what makes it a fix
-- rather than a silencing: without it the loser carries on with the version 0 it assumed
-- while the winner is writing version 1, and the two events take the same place.
--
-- The re-read sees the winner's row because the write path runs at READ COMMITTED,
-- whose statement-level snapshot is taken after the winner committed. An append a
-- consumer wraps in a REPEATABLE READ transaction of its own never reaches the re-read:
-- the insert below is refused as a serialization failure (40001), which is retryable and
-- is not the collision this replaces.
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
