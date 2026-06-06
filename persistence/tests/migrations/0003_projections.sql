-- Registry of inline projections and their applied code version.
--
-- One row per inline projection, keyed by the projection's stable `name()`
-- (NOT the Rust type name). `version` is the author-managed code version that
-- was last applied to this projection's view. `updated_at` records when the
-- row was last written.
--
-- This migration only creates the registry table. Version-drift handling
-- (reset + replay) and the version guard are added by later slices.
CREATE TABLE IF NOT EXISTS projections (
    name        TEXT                        NOT NULL    PRIMARY KEY,
    version     INTEGER                     NOT NULL,
    updated_at  TIMESTAMP WITH TIME ZONE    NOT NULL    DEFAULT (now())
);
