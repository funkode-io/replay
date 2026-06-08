-- Dead-letter store for policy reactions that fail permanently.
--
-- When a policy dispatch exhausts its retry budget (or fails with a non-retryable
-- error) the runner writes one row here and advances the cursor so the policy is
-- never wedged by a single bad event.  The record is queryable so operators can
-- triage, replay, or discard failed reactions.
--
-- Columns:
--   id              — surrogate key, auto-generated.
--   policy_name     — stable policy name (cursor key), e.g. "fee_policy".
--   global_position — position of the triggering event in the global feed.
--   event_id        — UUID of the triggering event (for direct lookup / replay).
--   error_kind      — ErrorKind text (Permanent, Conflict, …) for filtering.
--   error_message   — human-readable error detail for triage.
--   created_at      — when the dead-letter was written.
CREATE TABLE IF NOT EXISTS policy_dead_letters (
    id               BIGSERIAL   PRIMARY KEY,
    policy_name      TEXT        NOT NULL,
    global_position  BIGINT      NOT NULL,
    event_id         UUID        NOT NULL,
    error_kind       TEXT        NOT NULL,
    error_message    TEXT        NOT NULL,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dead_letters_policy
    ON policy_dead_letters (policy_name, created_at DESC);
