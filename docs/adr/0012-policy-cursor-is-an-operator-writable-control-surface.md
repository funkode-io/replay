# The persisted policy cursor is an operator-writable control surface

**Status:** accepted

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
