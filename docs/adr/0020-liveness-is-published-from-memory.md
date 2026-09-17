# Liveness is published from memory; the cursor row carries only a stamp

**Status:** accepted

A service could not answer "is this Policy's worker running". `PolicyRunnerDaemon`
exposed `shutdown` and nothing else, so monitoring inferred liveness from cursor
movement — which cannot tell an idle healthy Policy from a dead one, nor a
[Standby](../../CONTEXT.md#standby) from a [Leader](../../CONTEXT.md#leader) that
died. The crate's own health API answered `Working` for a Policy whose worker no
longer existed (funkode-io/replay#164, funkode-io/replay#187).

The daemon now publishes [Liveness](../../CONTEXT.md#liveness) per worker:
`Leading`, `StandingBy`, `Restarting`, `Stopped`, `Unknown`.

## Decisions

- **Liveness is process state, never a database derivation.** The worker and its
  supervisor write it; `daemon.liveness()` reads it. Deriving it from
  `policy_cursors` is what produced the wrong answer in the first place, and the
  tables cannot see a task.

- **[Policy status](../../CONTEXT.md#policy-status) gains no liveness field.** It
  stays the [Progress](../../CONTEXT.md#progress) axis, derived from the
  operational tables and readable from any replica
  ([ADR-0006](0006-policy-status-read-only-operational-snapshot.md)). Neither axis
  implies the other: a Standby is live and advances nothing, and the Leader of a
  [Blocked policy](../../CONTEXT.md#blocked-policy) is live and advances nothing
  either.

- **An unelected worker is `StandingBy`, and a silent one is `Unknown`.**
  `Stopped` is claimed only for a worker that is down for good — it spent its
  [Restart budget](../../CONTEXT.md#restart-budget) or its lock manager stopped —
  because a replica that leads nothing is the common case, and reading it as down
  would make every multi-replica deployment look like a fleet of dead policies.

- **The last poll is published as a monotonic `Instant`, recorded after the drain
  returns.** A worker held inside one long reaction therefore reads as `Leading`
  with an ageing stamp, which is what tells it from an idle one; a stamp taken
  before the drain would say the opposite.

- **The durable half is one nullable column the consumer owns**,
  `policy_cursors.last_polled_at`, stamped with the database's clock by the
  Leader. It is what a UI reads from a replica that leads nothing. The crate
  writes it when it exists and, on Postgres `42703`, stops attempting it for the
  life of the process and says so once — so the column may be added before or
  after the crate version that writes it, and a consumer who never adds it pays
  one failed statement.

- **The stamp is rate-limited to one write per poll interval.** A `NOTIFY` wakes a
  worker per append, so an unthrottled stamp would add a write per event to the
  row the checkpoint already updates.

- **The durable stamp is a reading, not the verdict.** Liveness itself is never
  read back out of the database: a `last_polled_at` that stops moving says the
  Leader stopped polling, and which of the five states it is in is a question only
  its own process can answer.

## Consequences

- A consumer's UI can show a Policy's Leader as alive from any replica, at the
  price of one column and one `UPDATE` per poll interval per led Policy.
- A test asserting liveness has to reach two runners against one database, because
  `StandingBy` only exists where another replica holds the lock
  (`policy_liveness.rs`, on the harness of
  [ADR-0014](0014-policy-daemon-tests-assert-through-operator-visible-observations.md)).
- The five states are the vocabulary the transition logging of
  funkode-io/replay#188 narrates; nothing else publishes them.

## Rejected

- **Deriving liveness from `last_polled_at` alone.** One column cannot separate
  standing by from stopped — a Standby never polls, and neither does a corpse —
  and it would reintroduce the guess this work removes.
- **A `Liveness` variant on `PolicyCondition`.** It would make adding a state to
  one axis silently change the meaning of the other, and would force a read model
  over the tables to answer a question the tables cannot see.
- **Writing the stamp inside the checkpoint `UPDATE`.** Free, but it would couple
  a column the consumer owns to the statement that advances the cursor: a schema
  without the column would fail the checkpoint, not the heartbeat.
