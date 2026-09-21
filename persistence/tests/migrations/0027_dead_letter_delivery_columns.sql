-- What a parked command is unique over, and what a redelivery of it stamps.
--
-- A dead letter is written before the batched cursor checkpoint, so a crash in
-- between — or an operator rewinding the cursor — redelivers the event and parks
-- the same command again (funkode-io/replay#220). The next migrations collapse
-- those generations and forbid new ones; these columns are what the collapse
-- must not lose, and the last of the identity it is unique over.
--
-- Columns (both tables):
--   dispatch_ordinal — the dispatch's index in the vector the reaction returned.
--                      Completes the identity `(policy_name, event_id,
--                      aggregate_name, target_stream_id, command_name)` for a
--                      reaction that emits the same command type to the same
--                      instance twice: those are two parked commands, not one
--                      parked twice. NULL where there is no dispatch to name (a
--                      panic in `react`) and on rows parked before this
--                      migration.
--   deliveries       — how many deliveries of the event parked this command.
--                      Counts what the collapse would otherwise hide: a command
--                      that failed on fifty deliveries must not read like one
--                      that failed once. Distinct from `retry_count` (0025),
--                      which counts the operator's retries.
--   last_parked_at   — when the last of those deliveries parked it.
--                      `PolicyStatus.last_dead_letter_at` reads this rather than
--                      `created_at`, which an in-place refresh never moves.
--
-- Existing rows are one delivery each and were parked when they were created,
-- which is what the backfill says.
--
-- The backfill and the `SET NOT NULL` it feeds scan both tables under ACCESS
-- EXCLUSIVE, unlike the concurrent index builds around them (0026, 0029): the
-- scan is over the parked backlog an outage leaves, not over `events`, and the
-- alternative — a nullable column — would put "parked, time unknown" in the
-- column a Policy's status reads its recency from.
--
-- The archive goes first, and the active table last, because this migration is
-- one transaction and every lock it takes is held to the end of it.
-- `discarded_dead_letters` retains history and can be far larger than the active
-- backlog, so scanning it after locking `policy_dead_letters` would hold the
-- daemon's parking path shut for the size of the audit trail. Reversed, the
-- parking table's ACCESS EXCLUSIVE spans the backlog-sized backfill only; what
-- the archive's own lock blocks meanwhile is [Retry] and [Discard], which an
-- operator invokes and a migration window may refuse.
ALTER TABLE discarded_dead_letters
    ADD COLUMN IF NOT EXISTS dispatch_ordinal INTEGER,
    ADD COLUMN IF NOT EXISTS deliveries       INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS last_parked_at   TIMESTAMPTZ;

UPDATE discarded_dead_letters SET last_parked_at = created_at WHERE last_parked_at IS NULL;

ALTER TABLE discarded_dead_letters
    ALTER COLUMN last_parked_at SET DEFAULT now(),
    ALTER COLUMN last_parked_at SET NOT NULL;

-- A third way out of the active set: 'superseded', for a duplicate generation
-- the next migration retires. Not a [Retry] and not a [Discard] — nobody
-- invoked either — so it is named apart from both.
ALTER TABLE discarded_dead_letters
    DROP CONSTRAINT IF EXISTS discarded_dead_letters_reason_check;

ALTER TABLE discarded_dead_letters
    ADD CONSTRAINT discarded_dead_letters_reason_check
    CHECK (reason IN ('retried', 'discarded', 'superseded'));

ALTER TABLE policy_dead_letters
    ADD COLUMN IF NOT EXISTS dispatch_ordinal INTEGER,
    ADD COLUMN IF NOT EXISTS deliveries       INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS last_parked_at   TIMESTAMPTZ;

UPDATE policy_dead_letters SET last_parked_at = created_at WHERE last_parked_at IS NULL;

ALTER TABLE policy_dead_letters
    ALTER COLUMN last_parked_at SET DEFAULT now(),
    ALTER COLUMN last_parked_at SET NOT NULL;
