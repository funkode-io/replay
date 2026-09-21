-- Make every parked command unique, without retiring one the library cannot
-- prove is a duplicate.
--
-- Before the next migration can forbid a second row for one parked command
-- (funkode-io/replay#220), no two rows may share the key it builds. Two shapes
-- get there, and they are not the same problem:
--
--   1. A row that names a dispatch but not its place in the reaction — parked
--      after 0024 and before 0027. Two such siblings may be a redelivery's
--      duplicate *or* a reaction that legitimately emitted the same command type
--      to the same instance twice, both failing. Nothing recorded tells them
--      apart. So they are made unique, not collapsed: a synthetic ordinal keeps
--      both rows, and the worst case is the triage noise the table already has.
--      Retiring one would take an active failure out of the table on a guess,
--      which is the one thing a dead letter must never do (0014 refuses to clean
--      `events` positions for the same reason).
--
--   2. A row that names no dispatch at all — parked before 0024, or parked for a
--      panic in `react`, which fails before any dispatch exists. That case parks
--      exactly one row per delivery *by construction*, so siblings are
--      duplicates, and the second phase collapses them.
--
-- Its own file, ahead of the index, because the index cannot run in a
-- transaction and this must (0014/0015 split for the same reason): a dedupe that
-- half-ran would leave the table in a state neither the old code nor the new one
-- describes.

-- Phase 1. A legacy row's place in its reaction was never recorded, so it takes
-- a negative one: unique within its group, ordered by the order the rows were
-- parked in, and never equal to a dispatch's index, which counts from 0. That
-- is also what a negative ordinal means when an operator reads one — "parked
-- before the column existed", not "the -2nd command".
--
-- Every such row is numbered, not only the ones with siblings, so a null ordinal
-- goes back to meaning exactly one thing: no dispatch to name.
UPDATE policy_dead_letters dl
SET dispatch_ordinal = legacy.ordinal
FROM (
    SELECT
        id,
        -row_number() OVER (
            PARTITION BY policy_name, event_id, aggregate_name, target_stream_id,
                         command_name
            ORDER BY id
        )::int AS ordinal
    FROM policy_dead_letters
    WHERE dispatch_ordinal IS NULL
      AND aggregate_name IS NOT NULL
) legacy
WHERE dl.id = legacy.id;

-- Phase 2. What is left duplicating the key is a reaction parked once per
-- delivery with nothing to name. The newest generation survives, carrying what
-- the group knew between them: the earliest `created_at` (when the reaction
-- first failed), its own newest error, the summed `deliveries`, the greatest
-- `retry_count` and the latest `last_retried_at` (a retry settles every
-- generation, but an older generation may have been settled by a retry the
-- newest one missed). The losers are archived `superseded`, not deleted: a
-- parked failure has never left this schema without a record of it.
--
-- Grouped on the whole key rather than on the null-identity rows alone, so a
-- duplicate written by hand is collapsed here too rather than failing the index
-- build with nothing but a constraint name to go on.
WITH grouped AS (
    SELECT
        policy_name,
        event_id,
        aggregate_name,
        target_stream_id,
        command_name,
        dispatch_ordinal,
        max(id)                AS keep_id,
        min(created_at)        AS first_parked_at,
        sum(deliveries)::int   AS deliveries,
        max(retry_count)       AS retry_count,
        max(last_retried_at)   AS last_retried_at
    FROM policy_dead_letters
    GROUP BY policy_name, event_id, aggregate_name, target_stream_id,
             command_name, dispatch_ordinal
    HAVING count(*) > 1
),
superseded AS (
    DELETE FROM policy_dead_letters dl
    USING grouped g
    WHERE dl.policy_name = g.policy_name
      AND dl.event_id = g.event_id
      AND dl.aggregate_name IS NOT DISTINCT FROM g.aggregate_name
      AND dl.target_stream_id IS NOT DISTINCT FROM g.target_stream_id
      AND dl.command_name IS NOT DISTINCT FROM g.command_name
      AND dl.dispatch_ordinal IS NOT DISTINCT FROM g.dispatch_ordinal
      AND dl.id <> g.keep_id
    RETURNING dl.id, dl.policy_name, dl.global_position, dl.event_id, dl.error_kind,
              dl.error_message, dl.created_at, dl.aggregate_name, dl.target_stream_id,
              dl.command_name, dl.retry_count, dl.last_retried_at, dl.dispatch_ordinal,
              dl.deliveries, dl.last_parked_at
),
archived AS (
    INSERT INTO discarded_dead_letters
        (dead_letter_id, policy_name, global_position, event_id, error_kind,
         error_message, created_at, reason, aggregate_name, target_stream_id,
         command_name, retry_count, last_retried_at, dispatch_ordinal, deliveries,
         last_parked_at)
    SELECT id, policy_name, global_position, event_id, error_kind, error_message,
           created_at, 'superseded', aggregate_name, target_stream_id, command_name,
           retry_count, last_retried_at, dispatch_ordinal, deliveries, last_parked_at
    FROM superseded
)
UPDATE policy_dead_letters dl
SET created_at      = g.first_parked_at,
    deliveries      = g.deliveries,
    retry_count     = g.retry_count,
    last_retried_at = g.last_retried_at
FROM grouped g
WHERE dl.id = g.keep_id;
