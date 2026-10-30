-- Where each inline projection has caught up to, and which transactions above that
-- point it has already applied (funkode-io/replay#289,
-- docs/adr/0032-an-inline-projection-catches-up-on-transactions-that-did-not-apply-it.md).
--
-- `caught_up_below` is the catch-up horizon: every transaction whose id is below it has
-- been accounted for, by applying the projection itself or by a catch-up. It is the
-- `xmin` of the snapshot the last catch-up read under, so it never passes a transaction
-- that was still running. NULL means "no horizon yet": a row registered before this
-- column existed, or by a release that does not know it. The first catch-up of such a
-- row adopts the log as it stands rather than replaying it.
--
-- `projection_applied_transactions` holds the applied marks above the horizon: this
-- transaction's events were applied to this version of this projection. The append
-- writes one in its own transaction; a catch-up or a rebuild writes them for what it
-- applied. Keyed by version so an older version's live writes during a rolling deploy
-- never count for the newer one. Marks below the horizon, and marks of older versions,
-- are deleted each time the horizon moves, so the table holds only recent transactions.
--
-- Additive: an earlier release reads `projections` by name and version and never sees
-- the column or the table, so it runs unchanged against this schema.
ALTER TABLE projections
    ADD COLUMN IF NOT EXISTS caught_up_below xid8;

CREATE TABLE IF NOT EXISTS projection_applied_transactions (
    projection TEXT    NOT NULL,
    version    INTEGER NOT NULL,
    txid       xid8    NOT NULL,
    PRIMARY KEY (projection, version, txid)
);
