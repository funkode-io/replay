-- Archive of resolved/discarded policy dead letters.
--
-- A row in `policy_dead_letters` represents an *active* parked failure that the
-- policy status read model aggregates.  When a dead letter leaves the active
-- set — resolved by a successful Retry, or Discarded by an operator for
-- bookkeeping — the runner MOVES it here in the same statement
-- (`DELETE ... RETURNING` → `INSERT`) rather than dropping it.  This keeps the
-- active table (and the status query that scans it) lean while the audit
-- history is never lost.
--
-- Columns:
--   id              — surrogate key for the archive row.
--   dead_letter_id  — id the row had in `policy_dead_letters` (traceability).
--   policy_name     — stable policy name (cursor key), e.g. "fee_policy".
--   global_position — position of the triggering event in the global feed.
--   event_id        — UUID of the triggering event.
--   error_kind      — last ErrorKind text recorded while the row was active.
--   error_message   — last human-readable error detail.
--   created_at      — when the dead letter was originally written.
--   reason          — why it left the active set: 'retried' or 'discarded'.
--   discarded_at    — when it was archived.
CREATE TABLE IF NOT EXISTS discarded_dead_letters (
    id               BIGSERIAL   PRIMARY KEY,
    dead_letter_id   BIGINT      NOT NULL,
    policy_name      TEXT        NOT NULL,
    global_position  BIGINT      NOT NULL,
    event_id         UUID        NOT NULL,
    error_kind       TEXT        NOT NULL,
    error_message    TEXT        NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL,
    reason           TEXT        NOT NULL CHECK (reason IN ('retried', 'discarded')),
    discarded_at     TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_discarded_dead_letters_policy
    ON discarded_dead_letters (policy_name, discarded_at DESC);

-- A dead letter is moved here at most once (its BIGSERIAL id is never reused),
-- so dead_letter_id is unique; the index keeps audit lookups by original id
-- from degrading into sequential scans as the archive grows.
CREATE UNIQUE INDEX IF NOT EXISTS idx_discarded_dead_letters_dead_letter_id
    ON discarded_dead_letters (dead_letter_id);
