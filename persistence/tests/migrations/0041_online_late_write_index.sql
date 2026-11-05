-- no-transaction
--
-- The online late-write lookup's index: `(commit_txid, global_position)` over online-lane
-- rows only (funkode-io/replay#301, ADR-0034). The lookup pages an ended transaction's
-- events, and the ids above the previous snapshot, in `(commit_txid, global_position)`
-- order (ADR-0030); a bulk segment's transaction is not in this index, so it nominates
-- nothing and costs the lookup nothing however many rows it wrote.
--
-- `idx_events_commit_txid_position` (0019) stays: the inline-projection catch-up reads
-- every transaction, bulk ones included (ADR-0032).
--
-- CONCURRENTLY, single statement, no IF NOT EXISTS: as 0040, for the same reasons.
CREATE INDEX CONCURRENTLY idx_events_online_commit_txid_position
    ON events (commit_txid, global_position) WHERE lane = 'online';
