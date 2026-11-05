-- The bulk lane's work queue (funkode-io/replay#302,
-- docs/adr/0034-a-bulk-append-reaches-policies-through-a-work-queue.md).
--
-- A bulk append reaches the Policies that consume the bulk lane through these two tables,
-- never through the online feed. Both are new and empty, so this file costs a populated
-- database nothing but their creation.

-- Which Policies consume the bulk lane, and which stream types each tracks: the record
-- ADR-0031 keeps for the online lane in `policy_cursors.tracked_stream_types`, kept here
-- for the bulk lane. NULL is every stream.
--
-- A runner writes a Policy's row when it starts, before it competes for the Policy's
-- leadership. A bulk append fans out over the rows that are here when it inserts its
-- work, so a Policy registered after a bulk append committed never receives it — as
-- `StartAt::Now` on the online lane.
CREATE TABLE IF NOT EXISTS policy_bulk_registrations (
    policy               TEXT        PRIMARY KEY,
    tracked_stream_types TEXT[],
    registered_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- One row per bulk-lane event per Policy that consumes it, inserted by the append in its
-- own transaction and deleted by the worker that reacted to it. Only pending work is
-- here, so the table is sized by what is owed, never by history, and an idle queue costs
-- one empty read of the primary key.
--
-- The primary key is also the claim's index: a worker reads its Policy's lowest
-- positions in order, `FOR UPDATE SKIP LOCKED`, and walks no other Policy's rows.
--
-- No foreign key to `events`: a stream deleted with its work pending (ADR-0033) leaves a
-- row the worker finds no event for, and deletes like any other it has handled.
CREATE TABLE IF NOT EXISTS policy_bulk_work (
    policy          TEXT   NOT NULL,
    global_position BIGINT NOT NULL,
    PRIMARY KEY (policy, global_position)
);
