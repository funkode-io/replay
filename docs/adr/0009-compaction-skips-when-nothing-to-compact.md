# Compaction skips no-op rewrites via two complementary guards

**Status:** accepted

`compact` unconditionally archived the current live events and inserted a fresh
synthetic snapshot every time it ran — even when the stream had not changed since
its last compaction. A daily maintenance job over 2000 rarely-changing streams
therefore rewrote all 2000 snapshots every day: 30 days of writes for zero actual
change, with the archived-original churn that ADR-0004 already makes unprunable.
We make a no-op compaction write **nothing**, detected by two independent guards.

## Decisions

- **Storage guard (cheap, universal): skip when nothing has been appended since
  the last compaction.** After acquiring the stream's `FOR UPDATE` lock (so an
  append cannot race in), `compact` checks for the existence of a *live real*
  event —

  ```sql
  SELECT 1 FROM events
  WHERE stream_id = $1 AND aggregate_version IS NULL AND compacted_snapshot = FALSE
  LIMIT 1
  ```

  If the only live rows are a prior synthetic snapshot (`compacted_snapshot = TRUE`),
  re-folding them yields an equivalent snapshot — pure churn — so we skip before
  even reading the stream. This is a *sufficient* condition: it can only ever skip
  a genuine no-op (given the fixpoint contract below), never real work. It kills the
  repeat-daily multiplication, but only *after* a first compaction has laid down a
  snapshot.

- **Domain guard (author judgment): `compacted_events` may report the stream is
  already minimal.** The storage guard cannot skip a stream that has *never* been
  compacted but is already in minimal form (e.g. a lone `BrandCreated`), because
  its events are still live and real. Only the aggregate author knows the minimal
  shape, so `Compactable::compacted_events` now returns

  ```rust
  pub enum Compaction<E> { Rewrite(Vec<E>), AlreadyCompacted }
  ```

  Returning `AlreadyCompacted` on the branch where the author recognises the
  already-compacted shape makes `compact` skip the archive+insert entirely. This
  is the only guard that eliminates even the *first* write for a never-changing
  stream.

- **`AlreadyCompacted` is a distinct variant, never an overloaded empty `Vec`.**
  `Ok(vec![])` already means "archive everything, leave the stream empty" (*compact
  to nothing*), which is the opposite write from *don't compact*. A sentinel would
  conflate them; the enum keeps them separate. `impl From<Vec<E>> for Compaction<E>`
  (⇒ `Rewrite`) keeps unchanged impls a one-token `.into()` migration.

- **The two guards run in cost order** inside `compact`: lock → storage guard
  (skip, no fold) → fold via `compacted_events` → domain guard (`AlreadyCompacted`
  ⇒ skip, no archive/insert) → archive + insert.

- **`compact` returns an outcome, not a bare version.** The signature changes from
  `Result<i32>` to a `Compacted { archive_version: i32 } | Skipped` outcome, so a
  maintenance job can count how many streams actually compacted versus were skipped.
  Returning a fabricated version on skip would be a lie.

- **The fixpoint contract is now load-bearing and explicit.** Both guards assume
  `compacted_events(snapshot) == snapshot`: folding an already-folded stream must
  reproduce it. This was always implicitly required for compaction to be idempotent
  under redelivery (ADR-0004); the skip optimisation depends on it, so it is
  documented as a `Compactable` contract rather than left implicit.

## Consequences

- **Breaking trait/store change, taken deliberately at `0.x`.** Both
  `Compactable::compacted_events` (enum return) and `Store::compact` (outcome return)
  change signature. The blast radius is ~4 impls and ~3 callers; pre-1.0 is the right
  window, and `From<Vec<E>>` makes the impl side near-mechanical.
- A stateless daily/blanket compaction sweep is now safe and cheap to run over every
  stream: unchanged streams cost one indexed existence check (storage guard) or one
  author decision (domain guard) and write nothing.
- An author whose `compacted_events` is **not** a fixpoint will now observe drift or
  redundant rewrites the guards were meant to prevent — the contract violation
  surfaces as churn rather than corruption, but it is a contract violation.
