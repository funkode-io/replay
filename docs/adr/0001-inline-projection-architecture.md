# Inline projections run atomically inside the append transaction and are store-specific

**Status:** accepted

We add a first kind of persisted read-model projection — the *inline projection* —
whose write is applied **inside the same transaction that appends the events**,
against the **same event store instance**. The projection loop lives inside
`EventStore::store_events`: after appending a command's events the store offers
them to each registered projection and only commits if every projection's
`handle` succeeds; any error rolls the whole transaction back. This makes a
projection and the events it derives from strongly consistent — they commit
together or not at all — and means a projection is never behind the log, so no
checkpoint or catch-up position is ever stored (the `projections` table records
only a version, for rebuild detection). This "no stored position" property is
specific to *inline* projections; a [Policy](0003-policies-as-checkpointed-background-subscribers.md),
being an eventually-consistent background subscriber, necessarily does store a
catch-up position.

The registry of projections is **fixed at construction** via a builder
(`PostgresEventStore::builder(pool).register(..).build().await?`). `build()` runs
each projection's `init` / version-drift check / replay once at startup and then
freezes the set, so the append hot path reads it lock-free and projections can
never appear or rebuild under live traffic. On version drift the projection is
reset and all (filtered) events are replayed in a single transaction with the new
version written last; a stored version *newer* than the code version is a hard
error (a likely bad/rolled-back deploy) rather than a silent backward rebuild.

## Considered options

- **Store-agnostic inline projections** (associated transaction/executor type on
  the `EventStore` trait, or a generic write-port abstraction). Rejected: it
  leaks `sqlx` transaction lifetimes into a public generic trait (GAT pain),
  forces the in-memory store to model a transaction it lacks, and the generic
  write API just re-exposes SQL through a leaky abstraction. Inline projections
  are treated as an extension of a concrete store instead; the in-memory store
  runs them best-effort for tests, but true atomicity is a Postgres guarantee.
- **After-commit, best-effort handling** (projection writes through its own
  connection after the events commit). Rejected for the primary use case: it has
  a real correctness hole (event committed, projection write lost, never
  reconciled because rebuilds only fire on version drift). This eventual,
  external-target model is deferred to a future *async projection*.
- **Runtime registration on a live store** (lock-guarded registry, async
  `register_projection`). Rejected: projection versions only change on recompile,
  so all upgrade/replay work belongs at startup; freezing the set removes hot-path
  locking and the possibility of projections changing under load.

## Consequences

- Inline projections require Postgres; the in-memory store supports them only
  best-effort for testing.
- `store_events` gains a second responsibility (append + project) and
  `PostgresEventStore` becomes stateful beyond its pool, but the write path stays
  single so projections can't be accidentally bypassed.
- Inline projections are **native-only (non-WASM)**. They live in the
  `persistence` crate, which depends on `sqlx`/`tokio`/Postgres and does not
  target `wasm32`, so the `InlineProjection` trait is free to use `Send` bounds
  and `BoxFuture`. WASM consumers use the core `es`/`macros` crates (which keep
  their dual `cfg(target_arch = "wasm32")` trait arms) for the write side; a
  WASM-facing read model would be served by the future *async projection*, which
  must honour the no-`Send` WASM pattern.
