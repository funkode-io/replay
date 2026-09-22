-- A Policy's position, per stream (funkode-io/replay#195).
--
-- Why per stream rather than over the log, what the two discovery paths cost and how a
-- write that commits below the hint is still delivered:
-- docs/adr/0024-a-policy-tracks-its-position-per-stream.md. What to stop before running
-- this: README, "Upgrading a running Policy to per-stream cursors".

CREATE TABLE IF NOT EXISTS policy_stream_cursors (
    policy      TEXT                        NOT NULL,
    stream_id   TEXT                        NOT NULL,
    stream_seq  BIGINT                      NOT NULL,
    updated_at  TIMESTAMP WITH TIME ZONE    NOT NULL    DEFAULT (now()),
    PRIMARY KEY (policy, stream_id)
);

-- Carry every running Policy over at exactly what it has processed: for each stream, the
-- place it had reached by the position its cursor stopped at. A Policy at 0 has processed
-- nothing and seeds nothing, so it starts every stream at the beginning, which is where
-- it was.
--
-- The join is over the whole log once per Policy — the one expensive statement in this
-- file, and the reason it belongs in the same window as the deployment rather than on a
-- live system.
INSERT INTO policy_stream_cursors (policy, stream_id, stream_seq)
SELECT pc.name, e.stream_id, MAX(e.stream_seq)
FROM policy_cursors pc
JOIN events e ON e.global_position <= pc.position
GROUP BY pc.name, e.stream_id
ON CONFLICT DO NOTHING;

-- `position` no longer records progress; the rows above do. What it records now is how
-- far discovery has scanned the log for streams with new events — a hint, which may run
-- past a write that had not committed when it swept by, and which decides nothing about
-- delivery. Renamed rather than redefined in place: a column that means something else
-- under the same name is the kind of change nobody notices until it has misled them.
ALTER TABLE policy_cursors RENAME COLUMN position TO discovered_through;

-- The transaction half of a cursor point (0022, funkode-io/replay#194) existed to make
-- the log's global order total. A Policy that reads no global order has nothing for it to
-- qualify.
ALTER TABLE policy_cursors DROP COLUMN IF EXISTS commit_txid;

-- Where the reconciliation left off. It examines one batch of streams at a time, so it
-- resumes after the last id it looked at and wraps at the end: without that it restarts
-- at the lowest id every time, and a permanently-behind batch of low ids would hide a
-- quiet stream from it for good.
ALTER TABLE policy_cursors
    ADD COLUMN IF NOT EXISTS reconciled_through TEXT NOT NULL DEFAULT '';
