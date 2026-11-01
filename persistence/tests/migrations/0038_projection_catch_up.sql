-- Where each inline projection has caught up to, and which transactions above that
-- point it has already applied (funkode-io/replay#289,
-- docs/adr/0032-an-inline-projection-catches-up-on-transactions-that-did-not-apply-it.md).
--
-- `caught_up_below` is the catch-up horizon: every transaction whose id is below it has
-- been accounted for, by applying the projection itself or by a catch-up. It is the
-- `xmin` of the snapshot the last catch-up read under, so it never passes a transaction
-- that was still running. `caught_up_version` is the version it was sealed at: a horizon
-- holds for that version only, so one left behind when a release that does not know
-- these columns changes `version` reads as none. No horizon for the stored version — a
-- row registered before these columns existed, or rebuilt by such a release — is adopted
-- by the first catch-up: the log is taken as it stands rather than replayed.
--
-- `projection_applied_transactions` holds the applied marks above the horizon: this
-- transaction's events were applied to this version of this projection. The append
-- writes one in its own transaction for each projection it registered; a catch-up or a
-- rebuild writes them for the transactions it accounted for. Keyed by version so an older version's live writes during a rolling deploy
-- never count for the newer one. Marks below the horizon, and marks of older versions,
-- are deleted each time the horizon moves, so the table holds only recent transactions.
--
-- Additive: an earlier release reads `projections` by name and version and never sees
-- the column or the table, so it runs unchanged against this schema.
ALTER TABLE projections
    ADD COLUMN IF NOT EXISTS caught_up_below xid8,
    ADD COLUMN IF NOT EXISTS caught_up_version INTEGER;

CREATE TABLE IF NOT EXISTS projection_applied_transactions (
    projection TEXT    NOT NULL,
    version    INTEGER NOT NULL,
    txid       xid8    NOT NULL,
    PRIMARY KEY (projection, version, txid)
);
