-- Schema for the `global_position` inline projection (README walkthrough).
--
-- The projection's view tables are owned here, in a migration, rather than being
-- created from `InlineProjection::init`. Driving schema from your migration
-- history (instead of DDL in `init`) keeps table creation, evolution, and
-- rollback in one auditable place and avoids coupling schema lifecycle to
-- projection registration. `init` is therefore left as a no-op in the example.
--
-- * `gp_account_owner` maps an account URN to its owning user URN.
-- * `gp_user_position` holds each user's name and running total balance.
CREATE TABLE IF NOT EXISTS gp_account_owner (
    account_urn text PRIMARY KEY,
    user_urn    text NOT NULL
);

CREATE TABLE IF NOT EXISTS gp_user_position (
    user_urn      text             PRIMARY KEY,
    name          text             NOT NULL DEFAULT '',
    total_balance double precision NOT NULL DEFAULT 0
);
