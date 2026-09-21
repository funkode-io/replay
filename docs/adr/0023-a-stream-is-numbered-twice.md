# A stream is numbered twice: `version` replays, `stream_seq` delivers

**Status:** accepted

Compaction renumbers a stream's live events from 1
([ADR-0004](0004-compaction-synthetic-event-marker-for-policy-feed.md), steps 5-7 of
`compact`). Hydrating a compacted aggregate therefore always reads `1..N`, which is the
point of the fast path — and it means `(stream_id, version)` names two different events
over a stream's lifetime: the originals run `1…100`, the snapshot rows restart at `1`,
and the next append is `4` again.

A [Policy] needs the opposite. To ask "have I got every event of this stream?" it needs a
number that names one event for good and rises without holes, so that the last number it
delivered plus one is the next it expects
([#195](https://github.com/funkode-io/replay/issues/195)). `version` cannot answer that
question, and `global_position` cannot either: a position is handed out when a write
starts and becomes visible when it finishes, so a hole in it may be an append still in
flight, a [Burned position], or another stream's event entirely.

## Decision

Number every event twice, on two axes that answer different questions:

| | axis | restarts at compaction | answers |
|---|---|---|---|
| `version` | replay | yes | where in the current live stream, for hydration and optimistic concurrency |
| `stream_seq` | delivery | never | which place in this stream, for good |

`stream_seq` is assigned by `write_event`, the one function that writes an event
(migration [0027](../../persistence/tests/migrations/0027_event_stream_seq.sql)), from a
counter on the `streams` row it already holds `FOR UPDATE`. Both callers go through it:
`append_event` is now a thin delegation, and `compact` stopped writing its snapshot rows
with an `INSERT` of its own — the numbering rules exist once, so a stream's version and
its place cannot drift apart. A unique index on `(stream_id, stream_seq)` holds a place to
one event.

A stream's places are contiguous by construction, not by convention. The function reads
the counter from the locked row and writes it back in the `UPDATE` it already performed
for `version`, so two appends racing for one stream are serialised as they always were,
and an append costs exactly what it cost before: one row version of the `streams` row.
Holes *between* streams are not this axis's problem: there is no order between streams to
break.

A place is also permanent, and nothing enforces that beyond the code that writes it. The
log is written by this library — `write_event` and the migrations — and by nothing else,
so the numbering is maintained the way `global_position`'s uniqueness and a stream's
`version` contiguity are maintained: by the one writer there is.
[ADR-0012](0012-policy-cursor-is-an-operator-writable-control-surface.md) makes
`policy_cursors` an operator-writable control surface precisely because the log is not
one. The column has **no default**, which catches the narrower failure: a write path that
*omits* the place fails on the spot. A path that supplies a *wrong* one would not be
caught, where a trigger would have overwritten it — the trade taken below.

Rejected alternatives:

- **Stop resetting `version` and use it as the delivery axis.** It works, and it is a
  breaking change to a public field's meaning (`PersistedEvent.version`) for every
  consumer that has ever compacted. A column costs less than a contract.
- **Derive the number from `MAX(events.stream_seq)` instead of a counter on `streams`.**
  A concurrent writer would then collide on the unique index rather than wait on the row
  lock, and [#195](https://github.com/funkode-io/replay/issues/195)'s "which streams are
  behind this Policy" query would have to aggregate over `events` rather than read one
  row per stream.
- **Assign it in a `BEFORE INSERT` trigger.** It covers every insert path without anyone
  having to remember, including ones that do not exist yet, and it overwrites a wrong
  value as readily as a missing one — a strictly wider guarantee. It costs a second
  `UPDATE` of the `streams` row per event, because the trigger cannot reach the one the
  append already performs, and it stands on the hot path forever to defend against a third
  write path that does not exist. With two writers in one file, `NOT NULL` catches the
  realistic failure — someone adding a third path and forgetting — and a wrong value is
  caught by the unique index the moment it collides. A third path must reimplement the
  read-lock-increment, not merely supply a number.
- **Add an eighth argument to `append_event` instead of a new function.** A defaulted
  argument makes every existing seven-argument call ambiguous between the two candidates,
  and dropping the seven-argument form fails every append from a process that has not
  rolled yet. Delegation keeps the migration applicable before or after a deployment, as
  0018 was.
- **Defend the numbering with triggers that reject an `UPDATE` or a `DELETE`.** They
  cannot tell dropping a stream from dropping one event out of the middle, and they would
  make every future migration that renumbers disable them first. The log has never been
  defended this way, and one column is not the place to start.

## Consequences

- One `BIGINT` per event and one unique index: an index write per append, and a place
  that is never reused even when an event is archived.
- Every insert now updates the `streams` row. An append already holds that row's lock and
  already updates it, and `write_event` folds the counter into that same `UPDATE`, so an
  append pays no extra write. Compaction pays one row update per snapshot row where it
  previously paid one for the whole run — it holds the lock throughout, and a snapshot is
  a handful of rows.
- A test fixture that writes to `events` directly now has to name a place, and six of them
  did — the `NOT NULL` doing its job. It does not settle `streams.stream_seq`, though, so
  those fixtures also call `common::places::settle`: a hand-written insert owes the
  counter the same update the store performs, or the next append through the store asks
  for a place the fixture has taken.
- **The migration needs writers quiesced, not merely blocked.** An append already inside
  the old `append_event` when it commits keeps that body, which writes no place, and is
  refused; compaction fails from either side of a mixed-version fleet, since an old
  process's snapshot `INSERT` names no place and a new one calls a function an un-migrated
  database lacks. All of it fails loudly and none of it corrupts anything — an append from
  an old process is safe once the migration has landed, because `append_event` keeps its
  signature — but the window is a quiet one. The README carries the rollout order.
- Migration 0027 takes `events` offline for its duration. The README ("What compaction
  does to an event's numbers") states the cost; the decision here is that a log small
  enough to migrate in a window is worth an axis a Policy can trust, and that an online
  variant — batched backfill, `CONCURRENTLY` index, `NOT VALID` constraint — can be built
  later without changing what the column means.
- Editing `events` by hand now breaks one more invariant than it used to: a stream's
  places would stop being contiguous, and a Policy reading them would wait for one that is
  never coming, or collide on a place the counter has already passed. This is the same
  contract the log has always had — ADR-0004 already rules out pruning archived events for
  a different reason — stated once more because
  [#195](https://github.com/funkode-io/replay/issues/195) will depend on it.
- Nothing reads the column yet. It is the expand half of
  [#171](https://github.com/funkode-io/replay/issues/171); the read is #195.

[Policy]: ../../CONTEXT.md#policy
[Burned position]: ../../CONTEXT.md#burned-position
