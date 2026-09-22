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
-- The active table is backfilled and the archive is not, which is the whole of
-- the difference between them. `policy_dead_letters.last_parked_at` is what a
-- Policy's status reads its recency from, so a null there would mean "parked,
-- time unknown" in a signal a consumer polls; its backfill and the `SET NOT
-- NULL` it feeds scan the parked backlog an outage leaves, under ACCESS
-- EXCLUSIVE, and that is the lock this migration is worth.
--
-- `discarded_dead_letters` retains history and can be far larger than that
-- backlog, and nothing reads the three columns back out of it — so it takes
-- `dispatch_ordinal` and `last_parked_at` nullable and `deliveries` NOT NULL
-- DEFAULT 1, all three a catalogue write and no scan at all (PostgreSQL 11 and
-- up store a non-volatile default rather than rewriting the table). A null in
-- the first two says "archived before the column existed", which is true; the
-- default in `deliveries` reads every such row as one delivery, which is what a
-- row archived before the column was counted as. Backfilling instead would hold
-- ACCESS EXCLUSIVE on the archive for the size of the audit trail, and this
-- migration is one transaction: taking that lock *before* the active table's
-- would also invert the order `move_dead_letter_to_archive` takes them in
-- (active first, then archive) and let a concurrent [Retry] deadlock the
-- migration. Same order as the runtime path, no long scan on the archive.
ALTER TABLE policy_dead_letters
    ADD COLUMN IF NOT EXISTS dispatch_ordinal INTEGER,
    ADD COLUMN IF NOT EXISTS deliveries       INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS last_parked_at   TIMESTAMPTZ;

UPDATE policy_dead_letters SET last_parked_at = created_at WHERE last_parked_at IS NULL;

ALTER TABLE policy_dead_letters
    ALTER COLUMN last_parked_at SET DEFAULT now(),
    ALTER COLUMN last_parked_at SET NOT NULL;

ALTER TABLE discarded_dead_letters
    ADD COLUMN IF NOT EXISTS dispatch_ordinal INTEGER,
    ADD COLUMN IF NOT EXISTS deliveries       INTEGER NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS last_parked_at   TIMESTAMPTZ DEFAULT now();

-- A third way out of the active set: 'superseded', for a duplicate generation
-- the next migration retires. Not a [Retry] and not a [Discard] — nobody
-- invoked either — so it is named apart from both.
--
-- NOT VALID, so the constraint is a catalogue write rather than a scan of the
-- archive under the lock this transaction already holds on the active table. It
-- is enforced for every row written from here on; the rows already there passed
-- the constraint it replaces, whose values this one is a superset of, so there
-- is nothing for a validation pass to find.
ALTER TABLE discarded_dead_letters
    DROP CONSTRAINT IF EXISTS discarded_dead_letters_reason_check;

ALTER TABLE discarded_dead_letters
    ADD CONSTRAINT discarded_dead_letters_reason_check
    CHECK (reason IN ('retried', 'discarded', 'superseded')) NOT VALID;
