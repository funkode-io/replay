-- no-transaction
--
-- A parked command is one row, whatever redelivers its event.
--
-- Parking was an unconditional INSERT and nothing in the schema said otherwise,
-- so every delivery of an event left a fresh generation of rows for one
-- reaction: same command, same error, its own id (funkode-io/replay#220). The
-- park is now an ON CONFLICT DO UPDATE against this key, which refreshes the
-- error and stamps the delivery instead of inserting.
--
-- The key is what identifies a parked command: the reaction (`policy_name`,
-- `event_id`) and the dispatch within it (`aggregate_name`, `target_stream_id`,
-- `command_name`, `dispatch_ordinal`). `global_position` is left out as
-- redundant — one event has one position. The ordinal is what keeps a reaction's
-- own repeats apart: two commands of one type to one instance are two parked
-- commands, and must stay two rows.
--
-- NULLS NOT DISTINCT (PostgreSQL 15, the crate's floor) so the key also covers
-- the rows whose identity is null — parked before 0024's migration, or parked
-- for a panic in `react`, which fails before any dispatch exists. Those collapse
-- per `(policy_name, event_id)`, which is exactly right: that case parks one row
-- per delivery. Without it Postgres treats every null as distinct and those rows
-- would keep duplicating.
--
-- CONCURRENTLY, because the table is the daemon's write path and a plain build
-- would lock out parking for as long as the migration runs; that cannot run
-- inside a transaction, hence `-- no-transaction` and one statement in this
-- file. No IF NOT EXISTS: a concurrent build that fails leaves an *invalid*
-- index behind, and IF NOT EXISTS would step over it and report success with no
-- key enforced (0019, 0020, 0026).
CREATE UNIQUE INDEX CONCURRENTLY idx_dead_letters_parked_command
    ON policy_dead_letters (policy_name, event_id, aggregate_name, target_stream_id,
                            command_name, dispatch_ordinal)
    NULLS NOT DISTINCT;
