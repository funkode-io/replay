# A Policy crosses a `global_position` no transaction can fill

**Status:** superseded by [ADR-0025](0025-a-policy-tracks-its-position-per-stream.md). A Policy no longer reads the log by position, so there is no burned number in front of it to cross and no `pg_locks` probe to tell one kind of hole from another. The implementation this ADR describes was deleted with funkode-io/replay#197.

**Amends** [ADR-0003](0003-policies-as-checkpointed-background-subscribers.md)'s
*Global cursor* decision: "advance only across a contiguous, gap-free prefix (with a
short visibility grace), so a sequence value that is assigned but not yet committed
by a concurrent append can never be skipped."

That rule is right about what it forbids and wrong about what it assumes. It assumes
every hole eventually fills. `nextval` is not transactional: a `global_position`
taken by a transaction that then aborts is burned, and the hole it leaves is
permanent. The feed waits for it anyway, forever. In a live deployment one burned
position stopped nineteen Policies for three days, with the log growing by ~170
events behind it and nothing written at any level
([#164](https://github.com/funkode-io/replay/issues/164)). Aborted appends are
routine — a producer that yields an error mid-stream, a pod killed during a rolling
deploy, a statement timeout — so this is a matter of when, not if.

## Decision

A Policy crosses a hole once the hole is **provably** permanent, and never before.
No timeout, no grace period, no heuristic: those would trade one silent failure for
another, because skipping a position that later commits is the silent event loss the
gap-free-prefix rule exists to prevent.

The proof rests on the sequence itself. A hole at `p` can only be filled by a
transaction that has *already* taken `p` from the events sequence — the sequence is
past `p`, so no later `nextval` can return it. A transaction that has taken a value
holds `RowExclusiveLock` on the sequence relation until it ends, and `pg_locks` lists
that lock to any role. So the transactions that could fill a hole are enumerable at
the moment the hole is seen, and once none of them is still running the hole can
never fill.

Each Policy therefore records, the first time it sees a hole, the transactions then
holding the sequence. A later poll that finds none of them running crosses the hole:
it logs a `warn` naming the Policy and the range skipped, moves the cursor to the
last burned position, and checkpoints. The whole burned run is crossed in one move,
so an aborted batch that burned a hundred thousand positions costs one poll rather
than a hundred thousand.

**One holder escapes the candidate set, by necessity.** A prepared transaction
outlives the session that created it, and Postgres re-issues its lock as `-1/<xid>`
after a server restart, so a candidate recorded before that restart looks like one
that has ended while `COMMIT PREPARED` can still publish the missing event. There is
no identity to record that survives the transition, so the verdict refuses to settle
while *any* prepared transaction holds the sequence — including one that took a later
position and therefore cannot fill this hole. That over-waits by construction, and
the over-wait is bounded: such a transaction holds a position ahead of the hole, so
the feed parks in front of that position the moment this one is crossed. Only the
events in between are delayed, and only until an operator resolves a prepared
transaction that is a half-finished append in its own right.

**The order of the two reads is the argument, not an implementation detail.** The
lock is read first; only then is the position confirmed missing, in a snapshot taken
afterwards. Postgres publishes a transaction's commit before releasing its locks, so
a position still absent after its holders are gone is a position that will never
exist. The reverse order proves nothing: a commit can land between reading the row
and reading the locks.

### Why not the transaction snapshot

The obvious mechanism — record `pg_current_snapshot()` when the hole appears and wait
until every transaction in flight then has ended — is unsound here, in two
independent ways:

- A transaction acquires an `xid` only when it first writes. One that has taken a
  sequence value and not yet inserted appears in no snapshot's `xip` list at all.
- A snapshot's `xip` only lists running transactions below its `xmax`, which is
  `latestCompletedXid + 1`. The newest running transaction is routinely above it, so
  `xmin` can and does exceed the `xid` of a transaction that is still running.

Either one lets an `xmin`/`xmax` watermark call a hole permanent while the append
that fills it is still running. The lock has neither problem: it exists from the
transaction's first statement, whatever the transaction has written.

The rejected rule is not only argued against here, it is executed:
`a_transaction_snapshot_cannot_prove_a_position_is_burned_postgres_test` stages a
running append, records the watermark that rule would record, and shows the rule
reaching its verdict while the append is still holding the position. If Postgres ever
changes what a snapshot reports, that test fails and this decision deserves a fresh
look — which is the only circumstance in which it should be revisited.

## Consequences

- **The prepared-transaction guard defends a case the library cannot reach on its
  own.** The store owns every transaction it writes in, begins and commits it itself,
  and never prepares one; nor does it accept an externally-managed transaction to
  write into. So a prepared transaction can only come to hold a `global_position` if
  something outside the library appends to `events` under a transaction manager that
  uses two-phase commit — which is also outside every other guarantee made here. The
  guard costs nothing when that never happens: two-phase commit is disabled by
  default in PostgreSQL, and the check only counts prepared transactions holding the
  events sequence specifically, so an unrelated one elsewhere in the database is not
  seen. It is kept because the failure it prevents is a silently undelivered event,
  and the alternative to keeping it is documenting a trap.
- **No schema change, no cursor-format change, no migration.** The candidate set is
  per Policy and in memory, bounded by the number of transactions that can hold the
  sequence at once, which is bounded by the server's `max_connections`. A restart or
  a leadership change re-observes; nothing is persisted because nothing needs to be.
  This is what makes the fix releasable as a patch.
- **The manual repair is no longer the only exit.** Moving a parked cursor by hand
  ([ADR-0012](0012-policy-cursor-is-an-operator-writable-control-surface.md)) remains
  supported, and remains the tool for a hole the runner is right to wait at. It is no
  longer the only way out of a burned one.
- **The blocked-Policy `warn` now reports only legitimate waits**: a long-running
  append, or a cursor an operator parked in front of a position that does not exist
  yet. A hole that cannot fill is crossed long before that warning's threshold, and
  says so in its own record.
- **`PolicyCondition::Blocked` becomes a transient reading rather than a terminal
  one.** It still means "the feed stops at a position that does not exist", but the
  Policy now leaves that state on its own unless an append really is in flight.
- **This machinery is meant to be deleted.** Ordering the feed by commit visibility
  instead of sequence contiguity
  ([#171](https://github.com/funkode-io/replay/issues/171)) removes the hazard by
  construction, and replaces this decision rather than extending it. That change
  needs a migration, a backfill and a new cursor format; this one needed none, which
  is why it ships first.
