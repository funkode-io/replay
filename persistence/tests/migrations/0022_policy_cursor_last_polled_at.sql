-- The durable half of the liveness signal.
--
-- A worker publishes its last poll in memory, which only the process running it
-- can read. This column is what a consumer's UI reads from a replica that is not
-- the Leader: the Leader stamps it with the database's clock as it polls, at most
-- once per poll interval.
--
-- Owned by the consumer's schema, not by the crate: the runner writes it when it
-- is here and treats its absence as a no-op, so this migration may be applied
-- before or after the crate version that writes it.
ALTER TABLE policy_cursors
    ADD COLUMN IF NOT EXISTS last_polled_at TIMESTAMP WITH TIME ZONE;
