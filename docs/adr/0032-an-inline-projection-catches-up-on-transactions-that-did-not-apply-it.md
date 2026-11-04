# 32. An inline projection catches up on transactions that did not apply it

Date: 2026-10-30

## Status

Accepted. Amends [ADR-0001](0001-inline-projection-architecture.md), which held that an
inline projection "is never behind the log, so no checkpoint or catch-up position is ever
stored". Replaces the caveat [ADR-0011](0011-inline-projections-flushed-in-bounded-chunks.md)
records about a rebuild missing appends committed past its cut
([#162](https://github.com/funkode-io/replay/issues/162)). Decides
[#289](https://github.com/funkode-io/replay/issues/289). Amended by
[#306](https://github.com/funkode-io/replay/issues/306): where the gap starts is found by a
read no plan can turn into a walk of the log, and the order a catch-up promises is per
stream only (*Where the gap starts*, *Delivery contract*).

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
  transaction, for every projection it registered — each was offered the whole append,
  matched or not, and an unmarked append would be read again by the next catch-up for
  nothing. A catch-up and a rebuild write them when they seal. The mark is keyed by
  version so that an older version's live writes during a rollout never count for the
  newer one.
- **Catch-up horizon** — `projections.caught_up_below`, an `xid8`: every transaction with
  a lower id has been accounted for. It is the `xmin` of the snapshot the last catch-up or
  rebuild read under, so it never passes a transaction that was still running. Marks below
  it are no longer needed and are deleted when it moves. `caught_up_version` records the
  version it was sealed at: a release that does not know these columns can rebuild and
  change `version`, and the horizon it leaves behind says nothing about the new version,
  so it reads as none.

The transaction id is the event's [Commit stamp], already written on every row and
indexed by `(commit_txid, global_position)` (migrations 0018, 0019). Ids are compared
against a snapshot, so nothing is inferred from a missing position.

### Catch-up

One catch-up of one projection is one transaction:

1. Take the projection's advisory lock (transaction-scoped, keyed by name). A periodic
   catch-up that finds it held skips the round: another process is doing the same work.
   Only then bound the transaction's row-lock waits (ADR-0022): `lock_timeout` bounds
   advisory waits too, and waiting for another process's catch-up or rebuild is waiting
   for work in progress.
2. Read the registry row and `pg_current_snapshot()` (`S`). If the stored version is not
   this process's code version, stop: a process must never apply its logic to a view
   another version owns.
3. Page through the events with `commit_txid >= caught_up_below`, visible in `S`, with no
   applied mark for this version, of the projection's declared event types — in
   `global_position` order (ADR-0018), compaction snapshots excluded (ADR-0004), at most
   `projection_flush_size` per page, and hand each page to `handle`. The walk starts at the
   lowest position the catch-up owes (*Where the gap starts*). With no horizon for the
   stored version, read nothing: the log is **adopted** as it stands.

   Not narrowed by `stream_filter()`: a catch-up stands in for the live append, which has
   never applied that filter, so it reads what the live path would have been offered. An
   append from a process without the projection must end up as one from a process with
   it. Reading the gap through that filter would also let a concurrent compaction move an
   archived row out of a `WithAggregateVersion` page before it was read, while the seal
   accounted for its transaction.
4. Seal: mark every transaction at or above `xmin(S)` that `S` shows as committed, set
   `caught_up_below = xmin(S)` for this version, and delete the marks below it and the
   marks of older versions.

Every transaction below `xmin(S)` had ended when `S` was taken, so its events, if it
committed, are visible in `S`: either it was marked, or step 3 applied it. Transactions at
or above `xmin(S)` that `S` shows as committed were marked already, applied in step 3, or
held nothing the projection reads; step 4 marks them, so the next catch-up skips them.
Transactions `S` could not see are left for the next one. Paging reads under `S`
(`pg_visible_in_snapshot`) at `READ COMMITTED`, so a commit landing between two pages is
never half-read.

The marks are written once the read is over, never page by page: a transaction sees its
own writes, so a mark written after the first page of a large append would hide the rest
of it from the catch-up applying it. Sealing is one `INSERT … SELECT` over the events
since the oldest transaction still running when `S` was taken; nothing of it comes back to
the process.

### Where the gap starts

The pages walk `global_position` from the lowest position the catch-up owes: above the
horizon, visible in `S`, unmarked. Finding it must cost a read of the range above the
horizon, on every round, however long the connection has been running the statement.

The first release looked it up with a bare `MIN(global_position)` above the horizon, cached
on the connection. Planned generically, as a cached statement is after five executions, or
with the real horizon once the rows above it are a large share of the log, that `MIN`
becomes a walk of `global_position` from the start of the log: the whole log on every round
that owes nothing. On a consumer's log it was a third of the database's disk reads (#306).

So the lookup reads the horizon's range of `(commit_txid, global_position)` in a fenced
subquery, which no plan can turn into a walk of `global_position`; it is planned with the
horizon every time (an unnamed statement); and it asks only for rows the catch-up owes.
The last part matters under a long transaction: it holds the horizon back, every round
reads what committed above it, and those rows are marked, so a round that owes nothing
stops at the lookup instead of walking them page by page. The seal's range read is unnamed
for the same reason.

The order this keeps is **per stream, and nothing across streams**: `global_position` is
drawn under the stream's row lock, so it rises with each stream's places. The positions of
two streams say nothing a handler may rely on.

### When it runs

- **At startup**, in `build()`. A first registration or a version-drift rebuild replays
  in the build transaction as before, then seals at its snapshot. A projection whose
  stored version equals its code version is caught up once that transaction commits, in a
  `READ COMMITTED` transaction of its own: inside the `REPEATABLE READ` build, a view row
  that a live append of another process also writes would end the build on a
  serialization failure, for as long as the rolling deploy keeps writing.
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

A caught-up projection costs, per round, an indexed read of the events above its horizon
and the seal, both proportional to the events committed since the oldest transaction still
running when the round began. Under a long transaction that is a rescan, every round, of
everything committed while it runs; a seal that remembered what it had already marked would
avoid it, and is left for when a deployment measures it.

### Event types

`InlineProjection::event_types()` lets a projection declare the `type` values it handles.
When it does, the rebuild replay, the catch-up and the live path all skip events of any
other type before deserializing them, so the three agree on the types they deliver.
Undeclared (the default) keeps deserialize-or-skip routing (ADR-0002). The live path still
ignores `stream_filter()`, as it always has, and so does the catch-up: that filter narrows
what a replay reads.

## Delivery contract

Between processes running this release, each event reaches each projection version
exactly once. What a projection must still tolerate, and the docs say so:

- **Late arrival.** A caught-up event can land after a later event of the same stream was
  applied live. Handlers that care about order guard on a value the view keeps.
- **Order per stream only.** Caught-up events reach `handle` in each stream's own order
  and in no promised order across streams.
- **Re-application across versions.** An event an older version applied during a rollout
  is applied again by the newer version: the newer logic must win. An upsert that sets
  values lets it; one that accumulates (a running balance) counts the event twice, so a
  projection whose `handle` is not idempotent should take every version bump as a
  stop-then-start deploy, as it had to before.
- **The upgrade to this release.** Processes of an earlier release write no marks, so the
  events they append — with the projection registered or not — are caught up by the first
  processes of this one. A projection whose `handle` is not idempotent should take this
  upgrade as a stop-then-start deploy. A registry row written before this release has no
  horizon; the first catch-up **adopts** the log as it stands, marking every transaction
  visible above `xmin(S)` without applying it, so history before the upgrade is not
  replayed. That includes an append committed by a process without the projection between
  the first upgraded process starting and its adoption: the guarantee begins at the
  adoption, so such writers should be quiesced until the first upgraded `build()` has
  returned. A version bump is still how a projection repairs history it missed before.

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
- **Page the gap by commit stamp, `(commit_txid, global_position)`, from the horizon**
  (#306), which removes the lookup and any reliance on `global_position` across
  transactions — or by `(commit_txid, stream_id, stream_seq)` behind a new index, which
  also drops it as a tie-break but costs every consumer a large index build on `events`.
  Rejected: a commit stamp is not a stream's order. A transaction can take
  its id before it takes the stream's row, so under concurrent appends a later place in a
  stream carries a lower stamp — 32 of 60 rounds of eight appenders to one stream on
  PostgreSQL 18, 47 of 60 when they also race to create it. A catch-up in stamp order would
  hand a stream's events to `handle` out of order.
- **Page the gap by `(stream_id, stream_seq)`**, a stream's own order. No index serves it
  from the horizon: every page would re-read the whole range above it, and the planner
  could walk the stream index from the start of the log instead.
- **Declare event types on `Event` and derive them.** Gives filtering for free, but widens
  the core `es` and `macros` crates for a persistence concern; an opt-in on
  `InlineProjection` is enough for #289.

## Consequences

- **Every append through a store with projections writes one mark per projection.** The table
  holds marks above the horizon only; each round deletes the rest.
- **A long transaction holds the horizon back, not the catch-up.** Commits after it are
  still applied, by their marks; the horizon moves once it ends. A bulk lane
  ([#288](https://github.com/funkode-io/replay/issues/288)) is not blocked by another
  process's catch-up, which takes no stream lock; in the catch-up's own process the next
  bullet holds.
- **A catch-up holds the projection's in-process lock while it runs.** Every append in the
  same process offers its events to every projection, so all of them wait for it. A
  catch-up that waits on a view row an append of this process holds, while that append
  waits for the lock, is a deadlock Postgres cannot see; the catch-up's row-lock waits are
  bounded by the stream lock wait (ADR-0022), which breaks it — by the 30 s default when a
  consumer sets that wait to zero, since zero cannot mean "forever" for a wait half of
  which Postgres cannot see.
- **Schema.** Migration 0038 adds the column and the table. It is additive: an earlier
  release runs unchanged against it, and its rows read as "no horizon yet".
- **Transaction ids are cluster-local.** A logical restore into another cluster carries
  commit stamps, horizons and marks the new cluster's counter can fall below or repeat, so
  a restore resets the stamps to the pre-stamp sentinel and clears horizons and marks (the
  README gives the statements), and the next catch-up adopts the log.
  A physical restore or promotion keeps the counter. Commit stamps already carry this
  assumption for the policy feed (ADR-0030).
- The in-memory store is single-process and test-only; it has nothing to catch up.

[Inline projection]: ../../CONTEXT.md#inline-projection
[Late write]: ../../CONTEXT.md#late-write
[Commit stamp]: ../../CONTEXT.md#commit-stamp
