-- The durable half of the liveness axis.
--
-- A worker publishes its state in memory, which only the process running it can
-- read. These columns are what a consumer reads from anywhere else: a beat on a
-- fixed cadence, written by the replica that holds the Policy's advisory lock.
--
-- `last_beat_at` stale  ⇒ no live Leader (the beat does not slow down for work).
-- `liveness`            ⇒ what that Leader's supervisor knows about the worker.
-- `last_polled_at` old  ⇒ alive but not finishing polls: busy, or wedged.
-- `led_by`              ⇒ which replica wrote it, i.e. whose logs to read.
--
-- Owned by the consumer's schema, not by the crate: the runner writes them when
-- they are here and turns the durable heartbeat off for the process when they are
-- not, so this migration may be applied before or after the crate version that
-- writes it. Add all four or none — a partial set turns the whole beat off.
ALTER TABLE policy_cursors
    ADD COLUMN IF NOT EXISTS last_beat_at   TIMESTAMP WITH TIME ZONE,
    ADD COLUMN IF NOT EXISTS liveness       TEXT,
    ADD COLUMN IF NOT EXISTS last_polled_at TIMESTAMP WITH TIME ZONE,
    ADD COLUMN IF NOT EXISTS led_by         TEXT;
