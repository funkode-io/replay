-- Make every parked command unique, without retiring one the library cannot
-- prove is a duplicate.
--
-- Before the next migration can forbid a second row for one parked command
-- (funkode-io/replay#220), no two rows may share the key it builds. Which of
-- them are duplicates is a question the table can only answer for one shape:
--
--   * A row parked after 0024 and before 0028 names its command but not its
--     place in the reaction. Two of them are *either* a redelivery's duplicate
--     or a reaction that legitimately emitted that command twice.
--   * A row parked before 0024 names nothing at all, and n commands failing on
--     one event parked n such rows, differing only in free text (0024's own
--     header). Two of them are *either* a redelivery's duplicate or two
--     different commands.
--   * A row parked for a **panic** — `error_kind = 'Panic'` (`PANIC_ERROR_KIND`)
--     with no dispatch named — is the exception: the unwind settles the
--     delivery, so that shape parks exactly one row per delivery in every
--     release that has contained a panic at all (ADR-0016). Siblings of it are
--     therefore duplicates, and nothing else is.
--
-- So the ambiguous rows are made unique rather than collapsed (phase 1), and
-- only the provable duplicates are collapsed (phase 2). The cost of keeping an
-- ambiguous pair is the triage noise the table already has; the cost of
-- collapsing one is an active failure retired on a guess, which is the one thing
-- a dead letter must never do — 0014 refuses to clean `events` positions for the
-- same reason.
--
-- Its own file, ahead of the index, because the index cannot run in a
-- transaction and this must (0014/0015 split for the same reason): a dedupe that
-- half-ran would leave the table in a state neither the old code nor the new one
-- describes.

-- Phase 1. A row whose place in its reaction was never recorded takes a negative
-- one: unique within its group, ordered by the order the rows were parked in,
-- and outside the range a dispatch's index can take, which counts from 0. That
-- is also what a negative ordinal means when an operator reads one — "parked
-- before the column existed", not "the -2nd command".
--
-- Numbered *below* the lowest synthetic ordinal the group already carries, so
-- the statement is re-runnable by hand. A replica still running the old code
-- parks null-ordinal rows after this migration is recorded as applied, and a
-- second pair of them is a duplicate 0030's build then fails on; the recovery
-- 0030's header describes is to run this again once those writers are gone,
-- which a fixed -1 would answer with the ordinal an earlier run already used.
-- On the first run no group has one, `lowest` is null, and the numbering is
-- -1, -2, … as it reads.
--
-- A panic's row is left alone, null ordinal and all: it is what phase 2
-- collapses, and it is how the running code parks the same panic again, so the
-- key must go on matching it.
WITH unplaced AS (
    SELECT
        id,
        policy_name,
        event_id,
        aggregate_name,
        target_stream_id,
        command_name,
        row_number() OVER (
            PARTITION BY policy_name, event_id, aggregate_name, target_stream_id,
                         command_name
            ORDER BY id
        ) AS place
    FROM policy_dead_letters
    WHERE dispatch_ordinal IS NULL
      AND NOT (aggregate_name IS NULL
               AND target_stream_id IS NULL
               AND command_name IS NULL
               AND error_kind = 'Panic')
),
numbered AS (
    SELECT
        policy_name,
        event_id,
        aggregate_name,
        target_stream_id,
        command_name,
        min(dispatch_ordinal) AS lowest
    FROM policy_dead_letters
    WHERE dispatch_ordinal < 0
    GROUP BY policy_name, event_id, aggregate_name, target_stream_id, command_name
)
UPDATE policy_dead_letters dl
SET dispatch_ordinal = (COALESCE(n.lowest, 0) - u.place)::int
FROM unplaced u
LEFT JOIN numbered n
       ON n.policy_name = u.policy_name
      AND n.event_id = u.event_id
      AND n.aggregate_name IS NOT DISTINCT FROM u.aggregate_name
      AND n.target_stream_id IS NOT DISTINCT FROM u.target_stream_id
      AND n.command_name IS NOT DISTINCT FROM u.command_name
WHERE dl.id = u.id;

-- Phase 2. The generations a redelivery left of a panicking reaction. The newest
-- *parking* survives — by `last_parked_at`, not by `id`: a row's id is taken when
-- it is inserted and its timestamps when its transaction began, so the two can
-- disagree — carrying what the group knew between them: the earliest `created_at`
-- (when the reaction first failed), its own newest error, the latest
-- `last_parked_at` (so the recency signal `PolicyStatus` reads cannot move
-- backwards over the collapse), the summed `deliveries`, the greatest
-- `retry_count` and the latest `last_retried_at` (a retry settles every
-- generation, but an older generation may have been settled by a retry the
-- newest one missed).
--
-- The losers are archived `superseded`, not deleted: a parked failure has never
-- left this schema without a record of it.
WITH grouped AS (
    SELECT
        policy_name,
        event_id,
        (array_agg(id ORDER BY last_parked_at DESC, id DESC))[1] AS keep_id,
        min(created_at)      AS first_parked_at,
        max(last_parked_at)  AS last_parked_at,
        sum(deliveries)::int AS deliveries,
        max(retry_count)     AS retry_count,
        max(last_retried_at) AS last_retried_at
    FROM policy_dead_letters
    WHERE aggregate_name IS NULL
      AND target_stream_id IS NULL
      AND command_name IS NULL
      AND dispatch_ordinal IS NULL
      AND error_kind = 'Panic'
    GROUP BY policy_name, event_id
    HAVING count(*) > 1
),
superseded AS (
    DELETE FROM policy_dead_letters dl
    USING grouped g
    WHERE dl.policy_name = g.policy_name
      AND dl.event_id = g.event_id
      AND dl.aggregate_name IS NULL
      AND dl.target_stream_id IS NULL
      AND dl.command_name IS NULL
      AND dl.dispatch_ordinal IS NULL
      AND dl.error_kind = 'Panic'
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
    last_parked_at  = g.last_parked_at,
    deliveries      = g.deliveries,
    retry_count     = g.retry_count,
    last_retried_at = g.last_retried_at
FROM grouped g
WHERE dl.id = g.keep_id;
