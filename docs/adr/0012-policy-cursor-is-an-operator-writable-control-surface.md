# The persisted policy cursor is an operator-writable control surface

**Status:** accepted; the surface moved. Since [ADR-0025](0025-a-policy-tracks-its-position-per-stream.md) a Policy's position is a row per stream in `policy_stream_cursors`, and that is what an operator writes — finer than what this ADR describes, since one stream can be redelivered without rewinding the Policy over the others. The decision below stands unchanged: the stored value is authoritative and a running leader adopts it. The machinery it needed does not, because places are read fresh every poll rather than held in memory between them.

The leader loaded a Policy's cursor once per leadership term and kept it in
memory for the whole term. `policy_cursors` was therefore a crash-recovery
checkpoint and nothing else: an `UPDATE policy_cursors SET position = …` against
a live deployment changed a row no running process would ever read again, and the
stale in-memory value was written back over the correction at the next
checkpoint.

That made the only safe repair procedure "scale every replica to zero, move an
integer, scale back up" — which is what recovering the permanent-gap incident in
funkode-io/replay#164 actually required. We make the stored cursor authoritative
at runtime instead: the row is a **control surface** an operator may write while
the daemon runs, and the in-memory position is a lease on it.

## Decisions

- **The leader re-reads the stored cursor whenever its feed comes back empty.**
  An empty feed is exactly the state a wedged or idle Policy sits in — there is
  nothing to process either because the log has caught up or because the next
  position can never exist — so it is both the state an operator corrects and the
  only moment the extra query is free. A busy Policy never re-reads: it pays one
  query per *idle* poll and nothing per event.

- **Checkpointing is a compare-and-set, not an upsert.** `UPDATE policy_cursors
  SET position = $new WHERE name = $1 AND position = $expected`, where `$expected`
  is the value this process last observed. Zero rows affected means someone moved
  the row underneath us; the runner adopts the stored position and abandons the
  rest of its batch rather than reinstating a position that predates the
  correction. This is the same optimistic-concurrency shape `Cqrs::execute` uses
  on aggregate versions, applied to the cursor.

- **The instruction is a position; the transaction half is derived from it.** The
  cursor became a pair — `(commit_txid, position)` — when events started carrying
  the transaction that wrote them (funkode-io/replay#194). An operator still writes
  the position alone, and the runner completes the pair from the log: the
  transaction that belongs with a position is the one that wrote the last event at
  or before it, which is exactly what the runner would have stored itself. It
  writes the completed pair back, so the row shows the point the Policy resumes
  from rather than the half-instruction it was given. The compare-and-set covers
  both halves.

- **The cursor may move in either direction.** Nothing clamps the adopted value
  to be greater than the in-memory one. Moving forward skips events (the #164
  recovery); moving backward re-delivers them, which the at-least-once contract
  and the causation guard already make safe. Clamping to "forward only" would
  make a rewind livelock — the runner would keep reinstating the higher value it
  still held.

- **A deleted row is recreated at the in-memory position.** Dropping the row is
  not a documented way to rewind a Policy, and treating it as one would silently
  replay history from the `StartAt` bootstrap.

## Rejected

- **Re-reading on every drain.** Correct, but it charges a busy Policy a query per
  batch to serve an event that happens a few times a year. The empty feed is the
  cheap, sufficient trigger.

- **A monotonic checkpoint (`SET position = GREATEST(current, new)`).** It
  satisfies "never overwrite a newer value with a stale one" only while
  corrections move forward, and permanently defeats a deliberate rewind.

- **An admin API (`runner.move_cursor(policy, position)`).** A nicer interface,
  but it does not help the operator in the incident: they have a psql prompt
  against the database, not a handle on the running process. SQL is the surface
  that already exists; this ADR makes it honest rather than replacing it.

- **Honouring a transaction half an operator writes by hand.** It cannot be told
  apart from the stale one left in the row by a position-only move, and the move is
  the documented instruction. A pair the runner wrote survives derivation unchanged,
  so nothing is lost by treating the position as the whole instruction.

## Consequences

- An operator recovers a stuck Policy with a single `UPDATE` against a running
  deployment. The change takes effect within one poll interval, with no restart
  and no leadership change.
- Anything that writes `policy_cursors` — a migration, a second tool, an
  overlapping leader mid-failover — is now a participant in the compare-and-set
  rather than a racer against an in-memory value. The loser re-reads; nobody
  silently wins.
- A drain that is superseded mid-batch stops early and returns the reactions it
  had already executed. The events between the abandoned position and the new one
  are skipped deliberately: that is what the operator asked for.
