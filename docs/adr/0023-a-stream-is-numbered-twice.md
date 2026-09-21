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

`stream_seq` is assigned by a `BEFORE INSERT` trigger from a counter on the `streams` row
(migration [0027](../../persistence/tests/migrations/0027_event_stream_seq.sql)), and a
unique index on `(stream_id, stream_seq)` holds a place to one event. A supplied value is
overwritten rather than trusted: a wrongly chosen place is a hole or a duplicate that no
later write can repair, so nothing outside the trigger chooses one.

A stream's places are contiguous by construction, not by convention. The trigger numbers
a row by incrementing the counter on the `streams` row and reading back what it wrote, in
one statement, so two inserts racing for one stream are serialised on that row whether or
not the caller took its lock first — an insert that bypasses `append_event` entirely is
numbered as correctly as one that does not. Holes *between* streams are not this axis's
problem: there is no order between streams to break.

A place is also permanent, which takes a second pair of triggers. An `UPDATE` that moved
an event to a free place, or to another stream, would leave a hole behind it and strand
the counter, so the next append would collide on the unique index and that stream would
stop accepting events — a corruption that looks like a successful statement. Both are
rejected, as is a write to `streams.stream_seq` from anywhere but the assigning trigger
itself.

Rejected alternatives:

- **Stop resetting `version` and use it as the delivery axis.** It works, and it is a
  breaking change to a public field's meaning (`PersistedEvent.version`) for every
  consumer that has ever compacted. A column costs less than a contract.
- **Derive the number from `MAX(events.stream_seq)` instead of a counter on `streams`.**
  A concurrent writer would then collide on the unique index rather than wait on the row
  lock, and [#195](https://github.com/funkode-io/replay/issues/195)'s "which streams are
  behind this Policy" query would have to aggregate over `events` rather than read one
  row per stream.
- **Assign it in `append_event`.** That covers the appends and misses the other insert
  paths — compaction's synthetic rows, and anything a deployment writes itself.

## Consequences

- One `BIGINT` per event and one unique index: an index write per append, and a place
  that is never reused even when an event is archived.
- The trigger updates the `streams` row for every inserted event. Appends already hold
  that row's lock, so they pay nothing new; a bulk insert of many rows into one stream now
  performs one row update each.
- Migration 0027 takes `events` offline for its duration. The README ("What compaction
  does to an event's numbers") states the cost; the decision here is that a log small
  enough to migrate in a window is worth an axis a Policy can trust, and that an online
  variant — batched backfill, `CONCURRENTLY` index, `NOT VALID` constraint — can be built
  later without changing what the column means.
- Deleting events by hand now breaks a guarantee rather than just losing data: a stream's
  places would no longer be contiguous, and a Policy reading them would wait for one that
  is never coming. `DELETE` is deliberately **not** rejected the way an `UPDATE` is —
  dropping a stream entirely is legitimate, and a row trigger cannot tell that from
  dropping one event out of the middle. [#195](https://github.com/funkode-io/replay/issues/195)
  therefore cannot treat a missing place as "not written yet" indefinitely; it needs a way
  to tell a gap that will fill from one that never will. ADR-0004 already rules out
  pruning archived events for a different reason; this is a second.
- A migration that needs to renumber has to disable the triggers to do it
  (`ALTER TABLE events DISABLE TRIGGER events_place_is_permanent`), which is the intended
  friction: nothing renumbers a stream by accident.
- Nothing reads the column yet. It is the expand half of
  [#171](https://github.com/funkode-io/replay/issues/171); the read is #195.

[Policy]: ../../CONTEXT.md#policy
[Burned position]: ../../CONTEXT.md#burned-position
