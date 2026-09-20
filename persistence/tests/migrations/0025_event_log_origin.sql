-- Record which PostgreSQL cluster issued this log's transaction stamps
-- (funkode-io/replay#195).
--
-- `events.commit_txid` is an `xid8`: a counter one cluster owns, and the Policy feed
-- orders by it. Copy the log into another cluster and the stamps mean nothing there —
-- silently, because a cursor carrying a restored stamp sorts above everything the new
-- cluster appends, so the feed reads empty and the Policy reports itself idle.
--
-- Comparing the stamps against the target's own counter only catches the restore while
-- that counter is still behind them; once it has passed, restored and local stamps
-- overlap and no arithmetic can tell them apart. The cluster has to be identified, not
-- inferred.
--
-- `system_identifier` is generated at initdb from a timestamp and a pid. Physical copies
-- carry it (PITR, promoting a replica); `pg_dump`/`pg_restore` and logical replication do
-- not. Neither does `pg_upgrade`, which *does* carry the transaction counter, so the two
-- cases have different repairs and the runner names both. It is readable by any role
-- (verified on PostgreSQL 13, this crate's floor).
--
-- One row: the primary key is a constant.
CREATE TABLE IF NOT EXISTS event_log_origin (
    only_row         boolean PRIMARY KEY DEFAULT true CHECK (only_row),
    system_identifier bigint NOT NULL
);

-- Seeding declares "this log belongs to the cluster it is being migrated in", which is
-- true for every deployment that migrates in place. It is false for a log restored from
-- an older schema and migrated afterwards, so the seed refuses to certify stamps this
-- cluster could not have issued.
--
-- That test is one-directional, and this is the one place the residual can be stated: a
-- log restored from a cluster whose counter was *behind* this one's carries stamps that
-- look local at migration time, and nothing here can see it. It is why the runbook puts
-- the rebase before the upgrade rather than after it.
DO $$
DECLARE
    stamped_ahead bigint;
BEGIN
    SELECT COUNT(*) INTO stamped_ahead
    FROM events
    WHERE commit_txid >= pg_snapshot_xmax(pg_current_snapshot());

    IF stamped_ahead > 0 THEN
        RAISE EXCEPTION 'this log carries % event(s) stamped with transactions this '
                        'cluster has not issued, so it was restored here from another '
                        'cluster. Migrating now would record this cluster as the log''s '
                        'origin and certify stamps it did not issue. Rebase the stamps '
                        'first; the procedure is in the crate README, under moving a '
                        'database between clusters (funkode-io/replay#195).',
                        stamped_ahead;
    END IF;
END
$$;

INSERT INTO event_log_origin (system_identifier)
SELECT system_identifier FROM pg_control_system()
ON CONFLICT DO NOTHING;
