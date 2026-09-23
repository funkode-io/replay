# The persisted policy cursor is an operator-writable control surface

**Status:** accepted; the surface moved. Since [ADR-0026](0026-a-policy-tracks-its-position-per-stream.md) a Policy's position is a row per stream in `policy_stream_cursors`, and that is what an operator writes — finer than what this ADR describes, since one stream can be redelivered without rewinding the Policy over the others. The decision below stands unchanged: the stored value is authoritative and a running leader adopts it. The machinery it needed does not, because places are read fresh every poll rather than held in memory between them.

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

- ~~**The leader re-reads the stored cursor whenever its feed comes back empty.**~~
  *Replaced by [ADR-0026](0026-a-policy-tracks-its-position-per-stream.md): a poll
  reads the places of the streams it is about to look at, every time. There is no
  in-memory copy to go stale, so there is no trigger to pick for refreshing it, and
  the "empty feed" heuristic this decision turned on has no equivalent.*

- **Checkpointing is a compare-and-set, not an upsert.** Still true, per stream and
  against the row rather than the number: a place is written only where the row is
  the one the poll read, which `xmin` says and a value cannot
  ([ADR-0026](0026-a-policy-tracks-its-position-per-stream.md), funkode-io/replay#234).
  A poll that loses abandons **that stream** and leaves the place where its new owner
  put it; the rest of its batch carries on, because one operator moving one stream is
  no longer a statement about the Policy's whole position.

- ~~**The instruction is a position; the transaction half is derived from it.**~~
  *Deleted with funkode-io/replay#197: the transaction half existed to make the log's
  global order total, and a Policy reads no global order. An operator writes a place,
  which is whole on its own.*

- **The place may move in either direction.** Unchanged, and now per stream: moving
  forward skips that stream's events, moving backward re-delivers them, and the
  at-least-once contract with the causation guard is what makes the second safe. The
  write refuses a *stale* value, never a backwards one — clamping to "forward only"
  would make a rewind livelock.

- **A deleted row is the documented way to redeliver a stream whole.** *Reversed by
  [ADR-0026](0026-a-policy-tracks-its-position-per-stream.md).* When the position was
  one number for the whole Policy, dropping the row meant replaying history from the
  `StartAt` bootstrap, which nobody wants by accident. A place is one stream, absence
  is its first event, and that is a useful thing to ask for — so the checkpoint
  refuses to recreate a row it did not read rather than reinstating it.

## Rejected

- ~~**Re-reading on every drain.** Correct, but it charges a busy Policy a query per
  batch to serve an event that happens a few times a year. The empty feed is the
  cheap, sufficient trigger.~~ *This is what the runner does now, and the objection
  died with the global cursor: a poll reads the places of the streams it is about to
  look at, which it needs anyway to know what they are owed. Re-reading costs nothing
  extra once the read is per stream rather than per Policy.*

- **A monotonic checkpoint (`SET position = GREATEST(current, new)`).** It
  satisfies "never overwrite a newer value with a stale one" only while
  corrections move forward, and permanently defeats a deliberate rewind.

- **An admin API (`runner.move_cursor(policy, position)`).** A nicer interface,
  but it does not help the operator in the incident: they have a psql prompt
  against the database, not a handle on the running process. SQL is the surface
  that already exists; this ADR makes it honest rather than replacing it.

- ~~**Honouring a transaction half an operator writes by hand.**~~ *Moot with the
  transaction half itself (funkode-io/replay#197).*

## Consequences

- An operator recovers a stuck Policy with a single `UPDATE` against a running
  deployment, with no restart and no leadership change. **When** it takes effect is
  no longer "one poll interval": a poll reads the places of the streams it is looking
  at, so the move lands as soon as discovery nominates that stream — immediately for
  one still being written to, at the next reconciliation for a quiet one
  ([ADR-0026](0026-a-policy-tracks-its-position-per-stream.md) has the bound).
- Anything that writes `policy_stream_cursors` — a migration, a second tool, an
  overlapping leader mid-failover — is a participant in the compare-and-set rather
  than a racer against an in-memory value. The loser re-reads; nobody silently wins.
- A poll that is superseded in one stream abandons that stream and returns the
  reactions it had already executed. The events between the abandoned place and the
  new one are skipped deliberately: that is what the operator asked for.
