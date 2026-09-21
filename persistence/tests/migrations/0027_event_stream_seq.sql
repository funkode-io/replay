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
-- below holds ACCESS EXCLUSIVE on `events` until the whole file commits. The README
-- ("What compaction does to an event's numbers") states what that costs an operator.
--
-- The counter lives on `streams` rather than being derived from `MAX(events.stream_seq)`
-- so that funkode-io/replay#195's "which streams are behind" query reads one row per
-- stream. A constant default is catalog-only in PostgreSQL 11+, so this ALTER is cheap
-- where the one on `events` is not.
ALTER TABLE streams
    ADD COLUMN IF NOT EXISTS stream_seq BIGINT NOT NULL DEFAULT 0;

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
-- theirs: `CREATE INDEX CONCURRENTLY` cannot run inside one, and the numbers, the trigger
-- that keeps assigning them and the index that keeps them unique have to arrive together
-- or not at all. A failed build here rolls back with the rest of the file instead of
-- leaving an invalid index to drop by hand.
CREATE UNIQUE INDEX IF NOT EXISTS uidx_events_stream_seq
    ON events (stream_id, stream_seq);

-- A trigger rather than an argument to `append_event`, so every insert path carries the
-- number: appends, compaction's synthetic snapshot rows (step 6 of `compact` inserts into
-- `events` directly), and any row a deployment writes itself. A supplied value is
-- overwritten rather than trusted — a wrongly chosen place is a hole or a duplicate that
-- no later write can repair, so nothing outside this function chooses one.
--
-- The `UPDATE … RETURNING` is what makes a stream's places contiguous: it takes the
-- streams row's lock itself, so two inserts racing for one stream are serialised here
-- whether or not the caller took that lock first, and each reads the counter its
-- predecessor left.
CREATE OR REPLACE FUNCTION assign_stream_seq() RETURNS trigger
  LANGUAGE plpgsql
  AS $$
  BEGIN
    UPDATE streams
       SET stream_seq = stream_seq + 1
     WHERE id = NEW.stream_id
    RETURNING stream_seq INTO NEW.stream_seq;

    RETURN NEW;
  END;
$$;

DROP TRIGGER IF EXISTS events_assign_stream_seq ON events;

CREATE TRIGGER events_assign_stream_seq
    BEFORE INSERT ON events
    FOR EACH ROW EXECUTE FUNCTION assign_stream_seq();

-- A place is permanent, and assigning it is the trigger's business alone. Moving an event
-- to a free place or to another stream leaves a hole behind it and strands the counter:
-- the next append then collides on the unique index and that stream stops accepting
-- events — a corruption that looks like a successful statement. Rewinding the counter by
-- hand does the same. A migration that really has to renumber disables these triggers
-- first, which is the intended friction.
CREATE OR REPLACE FUNCTION reject_place_rewrite() RETURNS trigger
  LANGUAGE plpgsql
  AS $$
  BEGIN
    RAISE EXCEPTION
      'an event keeps the place it was given: % #% cannot become % #%',
      OLD.stream_id, OLD.stream_seq, NEW.stream_id, NEW.stream_seq;
  END;
$$;

DROP TRIGGER IF EXISTS events_place_is_permanent ON events;

CREATE TRIGGER events_place_is_permanent
    BEFORE UPDATE ON events
    FOR EACH ROW
    WHEN (NEW.stream_seq IS DISTINCT FROM OLD.stream_seq
          OR NEW.stream_id IS DISTINCT FROM OLD.stream_id)
    EXECUTE FUNCTION reject_place_rewrite();

-- `pg_trigger_depth()` is 2 or more when the counter is moved by `assign_stream_seq`
-- above, and 1 when a statement moves it directly. That is the difference between the
-- one writer allowed to touch it and every other.
CREATE OR REPLACE FUNCTION reject_counter_rewrite() RETURNS trigger
  LANGUAGE plpgsql
  AS $$
  BEGIN
    IF pg_trigger_depth() < 2 THEN
      RAISE EXCEPTION
        'the place counter of stream % is the assigning trigger''s: % cannot become %',
        OLD.id, OLD.stream_seq, NEW.stream_seq;
    END IF;

    RETURN NEW;
  END;
$$;

DROP TRIGGER IF EXISTS streams_counter_is_assigned_not_written ON streams;

CREATE TRIGGER streams_counter_is_assigned_not_written
    BEFORE UPDATE ON streams
    FOR EACH ROW
    WHEN (NEW.stream_seq IS DISTINCT FROM OLD.stream_seq)
    EXECUTE FUNCTION reject_counter_rewrite();
