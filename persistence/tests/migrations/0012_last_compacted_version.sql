-- Compaction watermark: the stream `version` through which the stream was last
-- compacted.  A blanket maintenance job uses it as a cheap pre-check
-- (`needs_compaction`) to skip `compact` entirely for streams that have not
-- changed since their last compaction — avoiding the transaction, lock, and fold.
--
-- `compact` sets this to the post-compaction `streams.version` on completion.
-- `needs_compaction` is then a pure streams-row scalar test:
--
--   SELECT version > COALESCE(last_compacted_version, 0) FROM streams WHERE id = $1
--
-- NULL means "never compacted": any stream with events (version >= 1) is eligible.
-- We key off `streams.version` (bumped on every append, reset by compaction) rather
-- than `global_position` so the same watermark works for the in-memory store, which
-- models no global position — and it needs no events query, only a scalar read.
ALTER TABLE streams
    ADD COLUMN IF NOT EXISTS last_compacted_version BIGINT DEFAULT NULL;
