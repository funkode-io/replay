# Compaction skips no-op rewrites via a stream watermark and two layered guards

**Status:** accepted

`compact` unconditionally archived the current live events and inserted a fresh
synthetic snapshot every time it ran — even when the stream had not changed since
its last compaction. A daily maintenance job over 2000 rarely-changing streams
therefore rewrote all 2000 snapshots every day: 30 days of writes for zero actual
change, with the archived-original churn that ADR-0004 already makes unprunable.
We make a no-op compaction do **nothing** — ideally without even entering
`compact` — detected by two layered guards over a durable per-stream watermark.

## Decisions

- **A per-stream compaction watermark is the durable answer to "anything new since
  we last settled this stream?"** We add `streams.last_compacted_position` — the
  `global_position` through which the stream was last compacted. `compact` sets it
  on completion, for **every** outcome (a rewrite *and* an already-minimal skip).
  This is the single source of truth both guards read; it replaces the earlier,
  rejected idea of inferring "already compacted" from the `compacted_snapshot` event
  flag (see *Rejected* below).

- **Guard 1 — cheap, caller-side pre-check (`needs_compaction`), non-breaking.**
  `Store::needs_compaction(stream_id) -> bool` answers "does a live event exist with
  `global_position > last_compacted_position`?" A blanket maintenance job guards the
  call:

  ```rust
  if store.needs_compaction(&id).await? { store.compact(&agg, meta).await?; }
  ```

  When false we never enter `compact` — no transaction, no lock, no fold — saving CPU
  and memory, not just rows. The pre-check takes **no lock**: the race is benign
  because compaction is best-effort idempotent maintenance, so an append that lands
  just after a false read is simply picked up next cycle. `compact` keeps its current
  signature; Guard 1 adds only a column, an `UPDATE` in `compact`, and a read method.
  No append hook is needed — appends get a higher `global_position` for free.

- **Guard 2 — in-`compact` domain guard (author judgment), deferred.** Guard 1 alone
  never skips the *first* compaction of a stream that was **never** compacted but is
  already minimal (a lone `BrandCreated`): its watermark is unset, so Guard 1 sends
  it into `compact`. Only the aggregate author knows the minimal shape, so
  `Compactable::compacted_events` gains a no-op variant:

  ```rust
  pub enum Compaction<E> { Rewrite(Vec<E>), AlreadyCompacted }
  impl<E> From<Vec<E>> for Compaction<E> { /* ⇒ Rewrite */ }
  ```

  Returning `AlreadyCompacted` makes `compact` skip the archive+insert **and still
  advance the watermark**, so the fold runs once and Guard 1 skips it ever after.
  This layer requires the fold (hence it lives inside `compact`, not the pre-check)
  and is the only guard that eliminates even the one-time write for a born-minimal
  stream.

- **`AlreadyCompacted` is a distinct variant, never an overloaded empty `Vec`.**
  `Ok(Rewrite(vec![]))` already means "archive everything, leave the stream empty"
  (*compact to nothing*) — the opposite write from *don't compact*. The enum keeps
  them separate; `From<Vec<E>>` keeps unchanged impls a one-token `.into()` migration.

- **`compact` returns an outcome, not a bare version (Guard 2 only).** With Guard 2
  the signature changes from `Result<i32>` to `Compacted { archive_version: i32 } |
  Skipped`, so a maintenance job can count real compactions versus skips. A fabricated
  version on skip would be a lie. Guard 1 needs none of this — which is why it ships
  first and non-breaking.

- **The fixpoint contract is now load-bearing and explicit.** Both guards assume
  `compacted_events(snapshot) == snapshot`: folding an already-folded stream must
  reproduce it. Always implicitly required for compaction to be idempotent under
  redelivery (ADR-0004); the skip optimisation depends on it, so it is documented as
  a `Compactable` contract rather than left implicit.

## Rejected alternatives

- **Deriving Guard 1 from the `compacted_snapshot` event flag** ("skip when no live
  `compacted_snapshot = FALSE` event exists"). It only self-clears because a *rewrite*
  converts real rows → synthetic; an `AlreadyCompacted` skip does no such conversion,
  so the predicate stays true and Guard 2 re-folds the same stream **on every run**,
  forever. It answered the real question ("anything new since?") by a proxy that only
  correlated in the rewrite path. The watermark answers it directly and uniformly.

- **Marking the event rows on `AlreadyCompacted`** (flip `compacted_snapshot = TRUE`,
  or add a per-event "minimised" flag). Reusing `compacted_snapshot` would hide *real*
  events from the policy feed — lagging or `start_at: Beginning` policies would skip
  them (ADR-0004), i.e. silent reaction data loss. Any per-event marking is also O(N)
  writes, defeating the whole point of an `AlreadyCompacted` that writes nothing. The
  determination is recorded once, at the stream grain, instead.

- **Putting Guard 1 inside `compact`.** Keeping the cheap "should we?" decision inside
  the expensive mechanism forced `compact` to grow a `Skipped` return (a breaking
  change) for the common case. Hoisting it to a caller-side pre-check keeps that case
  non-breaking and skips the transaction/lock/fold entirely.

## Consequences

- **Sequenced delivery.** Guard 1 (watermark + `needs_compaction`) ships first and is
  **non-breaking**, and on its own solves the recurring daily multiplication: a
  rewrite advances the watermark, so an unchanged stream is compacted at most once and
  skipped thereafter. Guard 2 (the domain guard, `Compaction`/`Skipped` types, and the
  `AlreadyCompacted` watermark advance) is a later, deliberately breaking layer taken
  at `0.x`; it **depends on Guard 1** because its skip path writes Guard 1's watermark.
  Blast radius of the breaking half is ~4 impls and ~3 callers; `From<Vec<E>>` makes it
  near-mechanical.
- A blanket daily compaction over every stream is safe and cheap: unchanged streams
  cost one indexed watermark check and never enter `compact`.
- An author whose `compacted_events` is **not** a fixpoint will observe drift or
  redundant rewrites the guards were meant to prevent — surfaced as churn rather than
  corruption, but a contract violation nonetheless.
