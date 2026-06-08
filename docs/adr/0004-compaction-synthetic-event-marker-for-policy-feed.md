# Compaction marks synthetic events so a lagging Policy reads the true log

**Status:** accepted

A [Policy](0003-policies-as-checkpointed-background-subscribers.md) must process
every *real* event exactly once, in global order, even when it lags behind a
compaction. Compaction (`compact`) does **not** delete history: it **archives**
the originals in place (`UPDATE events SET aggregate_version = n`, keeping their
`id`, `created`, and `global_position`) and **inserts brand-new synthetic events**
for the compacted tail (new ids, new `global_position`, `aggregate_version = NULL`).

This breaks both naive policy-feed reads:

- Reading **all rows** by `global_position` walks the archived originals *and* the
  synthetic copies of the post-checkpoint tail → some real events are processed
  **twice**.
- Reading only **live rows** (`aggregate_version IS NULL`) sees only the synthetic
  snapshot → the lagging policy **skips** all pre-compaction history.

## Decision

Tag the synthetic rows inserted by step 6 of `compact` with a boolean marker (e.g.
`compacted_snapshot = true`). The **policy feed reads the true log**:

```sql
WHERE compacted_snapshot = false ORDER BY global_position
```

— which includes archived originals (they are real) and excludes synthetic
snapshots, *without* filtering on `aggregate_version`. Aggregate replay is
unchanged and still reads the compacted fast path (`aggregate_version IS NULL`,
including the synthetics). The same rows are thus read through two opposite lenses:
aggregate replay wants the compacted snapshot; a Policy wants the true granular log.

Policies and compaction stay **fully decoupled**: we deliberately do **not** gate
compaction on the slowest policy's cursor, because a stuck or dead policy must
never freeze write-model maintenance or grow the live stream unbounded.

## Consequences

- Archived originals become **load-bearing for policies** and cannot be pruned while
  any policy (especially a `start_at: Beginning` one) might still need them.
  Compaction already retains them, so this is no new storage cost — but "compaction
  frees space" is now false for the policy log.
- The lagging-policy-across-compaction scenarios (walk pre-compaction originals once,
  never double-fire synthetics, continue past the checkpoint) are covered by
  Docker-gated Postgres integration tests that document the intent.
