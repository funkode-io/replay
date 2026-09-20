-- What has already been tried on a parked dead letter.
--
-- A retry used to leave no trace but the error it overwrote, so "has anyone
-- tried this since the outage?" was unanswerable from the table
-- (funkode-io/replay#211). Both columns are stamped on every settlement a retry
-- makes on a row that already existed — archiving a row that resolved as much as
-- re-parking one that did not — and never by `discard_dead_letter`, which re-runs
-- nothing. A row a retry parks for the first time is untried, as a row the drain
-- parks is: the columns count retries made on a row, not executions of a command.
--
-- Columns (both tables):
--   retry_count     — settlements a retry has made on this row. 0 means untried.
--   last_retried_at — when the last one was made. NULL means untried.
ALTER TABLE policy_dead_letters
    ADD COLUMN IF NOT EXISTS retry_count     INTEGER     NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_retried_at TIMESTAMPTZ;

ALTER TABLE discarded_dead_letters
    ADD COLUMN IF NOT EXISTS retry_count     INTEGER     NOT NULL DEFAULT 0,
    ADD COLUMN IF NOT EXISTS last_retried_at TIMESTAMPTZ;
