-- What a parked dead letter failed on: the dispatch, not just the trigger.
--
-- Before this, n commands failing on one event parked n rows that differed only in
-- free text, so "which customer is stuck" could not be answered from the table.
-- These columns name the dispatch each row is about, captured where it was built
-- (funkode-io/replay#210).
--
-- Columns (both tables):
--   aggregate_name    — Rust type name of the target aggregate, e.g. "app::BankAccount".
--   target_stream_id  — URN of the aggregate instance the command was addressed to.
--   command_name      — Rust type name of the command, e.g. "app::BankAccountCommand".
--                       The variant and payload are not recorded: `Aggregate::Command`
--                       carries no `Debug`/`Serialize` bound.
--
-- Nullable, for the two cases that have no dispatch to name: a row parked before
-- this migration, and a panic in `react` itself, which fails before any dispatch
-- exists. Both stay listable, retryable and discardable.
ALTER TABLE policy_dead_letters
    ADD COLUMN IF NOT EXISTS aggregate_name   TEXT,
    ADD COLUMN IF NOT EXISTS target_stream_id TEXT,
    ADD COLUMN IF NOT EXISTS command_name     TEXT;

ALTER TABLE discarded_dead_letters
    ADD COLUMN IF NOT EXISTS aggregate_name   TEXT,
    ADD COLUMN IF NOT EXISTS target_stream_id TEXT,
    ADD COLUMN IF NOT EXISTS command_name     TEXT;
