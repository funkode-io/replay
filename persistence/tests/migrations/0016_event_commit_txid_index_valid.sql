-- Refuse to finish the migration while 0015's index is missing or invalid.
--
-- 0015 builds CONCURRENTLY and so cannot run in a transaction, which leaves its
-- `IF NOT EXISTS` unable to tell "already built" from "left behind by a build that
-- failed": an invalid index is present, is maintained on every write, and is used by no
-- query. This migration is the half that can tell the difference, and it is the reason
-- 0015 may adopt what it finds.
--
-- Failing here leaves 0016 unapplied and 0015 recorded, which is the state a retry wants:
-- `REINDEX INDEX CONCURRENTLY idx_events_commit_txid_position` (or a `DROP INDEX` and a
-- re-run of 0015 by hand) then re-run, and nothing rebuilds an index that is already good.
DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_index
         WHERE indexrelid = to_regclass('idx_events_commit_txid_position')
           AND indisvalid
    ) THEN
        RAISE EXCEPTION
            'idx_events_commit_txid_position is missing or invalid: the CREATE INDEX '
            'CONCURRENTLY in migration 0015 did not finish. REINDEX INDEX CONCURRENTLY '
            'it (or DROP INDEX it and re-run 0015) before re-running the migrations.';
    END IF;
END $$;
