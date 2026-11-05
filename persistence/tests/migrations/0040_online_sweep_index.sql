-- no-transaction
--
-- The online sweep's index: `global_position` over online-lane rows only
-- (funkode-io/replay#301, ADR-0034). The sweep reads
-- `WHERE global_position > $from AND lane = 'online' ORDER BY global_position LIMIT $n`,
-- so a bulk append's rows are not in the index it walks, and a sweep past a bulk of any
-- size reads the online rows after it and nothing else.
--
-- `idx_events_global_position_unique` (0015) stays: it is what keeps a position unique,
-- and every read of the log in position order, bulk rows included, walks it.
--
-- CONCURRENTLY, so appends keep running while the index is built on a populated table;
-- that cannot run inside a transaction, hence `-- no-transaction` above and a single
-- statement in this file. No IF NOT EXISTS: a concurrent build that fails leaves an
-- *invalid* index behind, and IF NOT EXISTS would step over it and report success with
-- no usable index. What to do about that leftover is in 0015.
CREATE INDEX CONCURRENTLY idx_events_online_global_position
    ON events (global_position) WHERE lane = 'online';
