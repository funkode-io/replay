-- A fourth way out of the active dead-letter set: 'stream_deleted', for a parked
-- command whose triggering event was deleted with its stream
-- (funkode-io/replay#292, docs/adr/0033-a-deleted-stream-takes-what-replay-keeps-about-it.md).
--
-- Not a [Retry] or a [Discard] — nobody judged the failure — and not 'superseded',
-- which names a duplicate. A row whose event is gone can never be retried, so it
-- leaves the active set with the stream, and is archived rather than destroyed like
-- every other row that leaves it.
--
-- NOT VALID, as 0028 adds the constraint this one replaces: a catalogue write, not a
-- scan of the archive. The rows already there passed a constraint whose values this
-- one is a superset of. Additive for a running release, which never writes the new
-- value.
ALTER TABLE discarded_dead_letters
    DROP CONSTRAINT IF EXISTS discarded_dead_letters_reason_check;

ALTER TABLE discarded_dead_letters
    ADD CONSTRAINT discarded_dead_letters_reason_check
    CHECK (reason IN ('retried', 'discarded', 'superseded', 'stream_deleted')) NOT VALID;
