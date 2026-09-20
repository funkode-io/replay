-- Stamp every event with the transaction that wrote it (funkode-io/replay#193).
--
-- `global_position` is a sequence value: taken before commit, burned for good if the
-- transaction aborts, and made visible out of order. Ordering the policy feed by it
-- therefore needs a theory of holes (ADR-0013, ADR-0015). A transaction id can instead
-- be compared against a snapshot of transactions that have ended, which is the standard
-- Postgres CDC/outbox watermark. This migration only puts the stamp in the data;
-- funkode-io/replay#171 carries the read-path change, and nothing reads the column yet.
--
-- `xid8` rather than `xid`: 64 bits wide and never wrapping, available since
-- PostgreSQL 13, which was the crate's documented floor when this landed (README
-- "Requirements"; the floor is now 15).
--
-- The stamp is a column DEFAULT rather than an argument to `append_event`, so every
-- insert path carries it: normal appends, compaction's synthetic snapshot rows (step 6
-- of `compact` inserts into `events` directly), and any row a deployment writes itself.
--
-- Both statements are one transaction, in this order, so the change is safe to apply to
-- a live deployment:
--
--   1. ADD COLUMN with the constant `'0'::xid8`. A non-volatile default is catalog-only
--      in PostgreSQL 11+: no table rewrite, so the ACCESS EXCLUSIVE lock is momentary
--      and concurrent appends queue behind it instead of failing. Defaulting straight to
--      `pg_current_xact_id()` would be a volatile default and rewrite every row.
--   2. SET DEFAULT to `pg_current_xact_id()`. Because it commits with step 1, there is
--      no window in which an append commits carrying the sentinel.
--
-- `0` is InvalidTransactionId: never assigned to a transaction, and ordering before
-- every real id. Events that predate the migration therefore sort ahead of everything
-- written after it, and among themselves keep the `global_position` order they have
-- today.
ALTER TABLE events
    ADD COLUMN IF NOT EXISTS commit_txid xid8 NOT NULL DEFAULT '0'::xid8;

ALTER TABLE events
    ALTER COLUMN commit_txid SET DEFAULT pg_current_xact_id();
