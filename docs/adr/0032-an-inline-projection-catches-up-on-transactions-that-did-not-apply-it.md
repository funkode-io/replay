# 32. An inline projection catches up on transactions that did not apply it

Date: 2026-10-30

## Status

Accepted. Amends [ADR-0001](0001-inline-projection-architecture.md), which held that an
inline projection "is never behind the log, so no checkpoint or catch-up position is ever
stored". Replaces the caveat [ADR-0011](0011-inline-projections-flushed-in-bounded-chunks.md)
records about a rebuild missing appends committed past its cut
([#162](https://github.com/funkode-io/replay/issues/162)). Decides
[#289](https://github.com/funkode-io/replay/issues/289).

## Context

An [Inline projection] is applied by the process that appends, inside the append
transaction, and only if that process registered it. The registry keeps a name and a
version and nothing else. That is enough while every writer runs the same code, and it is
not during a rolling deploy:

- A process that has not registered a new projection appends events the projection never
  sees.
- A process running an older version of a projection applies the old logic to the view
  the new version has just rebuilt.
- `build()` replays the snapshot it started from; anything committed after that snapshot
  is applied by nobody (#162).

Nothing repairs any of these until the next version bump. #162 was closed on the
assumption that the window is the few seconds of a rebuild. Downstream (dxp-data-loom, on
PostgreSQL 18) the window was the whole rollout overlap, and the missing rows were user
visible.

The fix has to answer which events a projection has not seen, and the obvious answer — a
`global_position` checkpoint — is wrong. Positions are drawn before commit and become
visible out of order (a [Late write]): a checkpoint at 11 written while position 10 is
still in flight skips 10 for ever. Policies met the same problem and solved it twice
(ADR-0026 per stream, ADR-0030 by commit stamp).

## Decision

**Every transaction is accounted for per projection version: either it applied the
projection itself, or a catch-up applies its events later. A transaction-safe horizon
bounds what has to be remembered.**

### What is recorded

- **Applied mark** — a row `(projection, version, txid)` in
  `projection_applied_transactions`, meaning "this transaction's events were applied to
  this version of this projection". The append path writes one, in the append
  transaction, for each projection whose `handle` received an event in that append. A
  catch-up and a rebuild write them for the transactions they apply above the horizon.
  The mark is keyed by version so that an older version's live writes during a rollout
  never count for the newer one.
- **Catch-up horizon** — `projections.caught_up_below`, an `xid8`: every transaction with
  a lower id has been accounted for. It is the `xmin` of the snapshot the last catch-up or
  rebuild read under, so it never passes a transaction that was still running. Marks below
  it are no longer needed and are deleted when it moves.

The transaction id is the event's [Commit stamp], already written on every row and
indexed by `(commit_txid, global_position)` (migrations 0018, 0019). Ids are compared
against a snapshot, so nothing is inferred from a missing position.

### Catch-up

One catch-up of one projection is one transaction:

1. Take the projection's advisory lock (transaction-scoped, keyed by name). A periodic
   catch-up that finds it held skips the round: another process is doing the same work.
2. Read the registry row and `pg_current_snapshot()` (`S`). If the stored version is not
   this process's code version, stop: a process must never apply its logic to a view
   another version owns.
3. Page through the events with `commit_txid >= caught_up_below`, visible in `S`, with no
   applied mark for this version, matching the projection's filter — in `global_position`
   order (ADR-0018), compaction snapshots excluded (ADR-0004), at most
   `projection_flush_size` per page. Hand each page to `handle` and mark the page's
   transactions at or above `xmin(S)`.
4. Seal: set `caught_up_below = xmin(S)` and delete the marks below it and the marks of
   older versions.

Every transaction below `xmin(S)` had ended when `S` was taken, so its events, if it
committed, are visible in `S`: either it was marked, or step 3 applied it. Transactions at
or above `xmin(S)` that `S` already shows as committed were applied and marked in step 3,
so the next catch-up skips them. Transactions `S` could not see are left for the next one.
Paging reads under `S` (`pg_visible_in_snapshot`) at `READ COMMITTED`, so a commit
landing between two pages is never half-read.

### When it runs

- **At startup**, inside `build()`. A projection whose stored version equals its code
  version is caught up in the build transaction. A first registration or a version-drift
  rebuild replays as before and marks what it replayed above the horizon, then seals.
  `build()` takes the advisory locks of every projection it registers, as session locks on
  its own connection, **before** its `REPEATABLE READ` transaction takes its snapshot, so
  a rebuild never races a catch-up and never reads a snapshot older than one that
  committed. The connection is closed rather than returned to the pool, which is what
  releases the locks on every path, cancellation included. Two processes building the same
  projection now queue instead of failing on a serialization error or a duplicate key.
- **On a cadence**, from a background task `build()` starts when at least one projection is
  registered: `REPLAY_PROJECTION_CATCH_UP_SECS`, overridden by
  `PostgresEventStoreBuilder::projection_catch_up_interval`, default 60 s, zero disables
  it. The task holds a weak reference to the store and stops when the store is dropped.
  The first round runs one interval after `build()`, so an append committed while the build
  ran is applied within one interval.
- **On demand**, through `PostgresEventStore::catch_up_projections()`, which waits for the
  lock rather than skipping.

A caught-up projection costs one indexed lookup of the events above its horizon and a
registry write per round.

### Event types

`InlineProjection::event_types()` lets a projection declare the `type` values it handles.
When it does, the rebuild replay, the catch-up and the live path all skip events of any
other type before deserializing them, so the three deliver the same events. Undeclared
(the default) keeps deserialize-or-skip routing over every event the filter admits
(ADR-0002).

## Delivery contract

Between processes running this release, each event reaches each projection version
exactly once. What a projection must still tolerate, and the docs say so:

- **Late arrival.** A caught-up event can land after a later event of the same stream was
  applied live. Handlers that care about order guard on a value the view keeps.
- **Re-application across versions.** An event an older version applied during a rollout
  is applied again by the newer version: the newer logic must win, and an upsert lets it.
- **The upgrade to this release.** Processes of an earlier release write no marks, so the
  events they append — with the projection registered or not — are caught up by the first
  processes of this one. A projection whose `handle` is not idempotent should take this
  upgrade as a stop-then-start deploy. A registry row written before this release has no
  horizon; the first catch-up **adopts** the log as it stands, marking every transaction
  visible above `xmin(S)` without applying it, so history before the upgrade is not
  replayed. A version bump is still how a projection repairs history it missed before.

## Considered options

- **A `global_position` checkpoint advanced in the append transaction**, as #289 first
  proposed. Skips any position that commits after a higher one, and every append through
  a store with the projection would update one registry row, serialising appends.
- **A horizon alone, re-applying everything above it.** Correct only if every handler is
  idempotent, and every process would re-apply every live append once per round.
- **Lock the registry row in `REPEATABLE READ`.** The rebuild's snapshot predates the wait,
  so a catch-up committing during the wait turns into a serialization failure at startup.
- **Catch up the appended stream first, on the live path.** Would keep per-stream order,
  but costs a read of the stream's history per append and needs the catch-up to take stream
  locks to avoid applying an event twice.
- **Declare event types on `Event` and derive them.** Gives filtering for free, but widens
  the core `es` and `macros` crates for a persistence concern; an opt-in on
  `InlineProjection` is enough for #289.

## Consequences

- **Every append with a matching projection writes one mark per projection.** The table
  holds marks above the horizon only; each round deletes the rest.
- **A long transaction holds the horizon back, not the catch-up.** Commits after it are
  still applied, by their marks; the horizon moves once it ends. A bulk lane
  ([#288](https://github.com/funkode-io/replay/issues/288)) is not blocked by a catch-up,
  which takes no stream lock.
- **A catch-up holds the projection's in-process lock while it runs.** Appends from the
  same process that reach that projection wait for it. The catch-up transaction bounds its
  row-lock waits by the stream lock wait (ADR-0022), so a wait on a view row cannot hang it.
- **Schema.** Migration 0038 adds the column and the table. It is additive: an earlier
  release runs unchanged against it, and its rows read as "no horizon yet".
- The in-memory store is single-process and test-only; it has nothing to catch up.

[Inline projection]: ../../CONTEXT.md#inline-projection
[Late write]: ../../CONTEXT.md#late-write
[Commit stamp]: ../../CONTEXT.md#commit-stamp
