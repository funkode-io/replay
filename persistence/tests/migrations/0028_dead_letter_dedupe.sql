-- Collapse the duplicate generations a redelivery left behind.
--
-- Before the next migration can forbid a second row for one parked command
-- (funkode-io/replay#220), the rows that already exist must become one per
-- command. These duplicates are the library's own — an unconditional INSERT on
-- a table with no key — unlike the hand-written `events` positions 0014 refuses
-- to clean, so this cleans them.
--
-- Its own file, ahead of the index, because the index cannot run in a
-- transaction and this must (0014/0015 split for the same reason): a dedupe that
-- half-ran would leave the table in a state neither the old code nor the new one
-- describes.
--
-- What survives is the newest generation, carrying what the group knew between
-- them: the earliest `created_at` (when the command first failed), its own
-- newest error, the summed `deliveries`, the greatest `retry_count` and the
-- latest `last_retried_at` (a retry settles every generation, but an older
-- generation may have been settled by a retry the newest one missed). The losers
-- are archived `superseded`, not deleted: a parked failure has never left this
-- schema without a record of it.
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
