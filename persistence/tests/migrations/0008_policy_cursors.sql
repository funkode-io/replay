-- Per-policy catch-up cursor.
--
-- Each Policy is a checkpointed background subscriber over the event log. Unlike
-- inline projections (which run inside the append transaction and are never
-- behind, so store no position), a Policy stores the `global_position` of the
-- last event whose command it has durably processed. On restart it resumes from
-- here — a code/version change never rewinds it.
--
-- `name` is the Policy's stable identity (NOT its Rust type name). `position` is
-- the last processed `global_position` (0 means "nothing processed yet").
CREATE TABLE IF NOT EXISTS policy_cursors (
    name        TEXT                        NOT NULL    PRIMARY KEY,
    position    BIGINT                      NOT NULL    DEFAULT 0,
    updated_at  TIMESTAMP WITH TIME ZONE    NOT NULL    DEFAULT (now())
);
