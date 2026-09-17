# Liveness is published from memory and beaten out on a fixed cadence

**Status:** accepted

A service could not answer "is this Policy's worker running". `PolicyRunnerDaemon`
exposed `shutdown` and nothing else, so monitoring inferred liveness from cursor
movement — which cannot tell an idle healthy Policy from a dead one, nor a
[Standby](../../CONTEXT.md#standby) from a [Leader](../../CONTEXT.md#leader) that
died. The crate's own health API answered `Working` for a Policy whose worker no
longer existed (funkode-io/replay#164, funkode-io/replay#187).

The daemon now publishes [Liveness](../../CONTEXT.md#liveness) per worker —
`Leading`, `StandingBy`, `Restarting`, `Stopped`, `Unknown` — and a process-level
task beats it out to the cursor row for consumers that are not in that process.

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

- **The durable signal is a beat on a fixed cadence, not a stamp on the poll
  path.** A stamp written by the worker as it polls goes silent for every reason
  the worker is busy — a restart backoff of up to 30s, a hung reaction, standing
  by — so staleness meant "busy or dead" and answered nothing. The beat keeps its
  cadence whatever the workload, so a stale beat means one thing: the Leader's
  process is gone.

- **The beat cannot come from the worker, so it comes from a sibling task.** A
  worker awaiting a hung dispatch cannot write anything, and that is the case the
  beat exists for. `run_heartbeat` is supervised alongside the lock manager and
  the NOTIFY listener, owns no Policy, and reads the same in-memory registry
  `daemon.liveness()` reads. Its guarantee is exact: *this process is alive, and
  here is its supervisor's knowledge of each worker.* Every way a worker task can
  end is observed through its `JoinHandle` before the next beat goes out; the one
  thing that leaves no trace there — a reaction that hangs — is what the poll
  stamp in the same row covers. Between the two fields there is no gap.

- **Three facts, kept apart**: `last_beat_at` stale ⇒ no live Leader;
  `liveness` ⇒ what that Leader's supervisor knows; `last_polled_at` old against a
  fresh beat ⇒ alive but not finishing polls, busy or wedged.

- **A replica beats only for the Policies whose advisory lock it holds**, read
  from the same leadership channels the workers are elected by. One row per Policy
  is shared fleet-wide, so a Standby that beat would overwrite the Leader's beat
  with its own idleness. A Leader whose worker has *stopped* still holds the lock
  and still beats — `Stopped` against a fresh beat is the half-dead state
  funkode-io/replay#164 could not see, and silence cannot express it.

- **`led_by` names the replica**, because triage starts with "whose logs do I
  read". It defaults to `HOSTNAME` and is left null rather than invented.

- **Both timestamps come from the database's clock** (`now()`, and
  `now() - make_interval(...)` for the poll age), so `last_beat_at -
  last_polled_at` is an exact age taken from one clock and a skewed replica cannot
  report a poll in the future. The price is that `last_polled_at` is recomputed
  each beat and wobbles by the round trip instead of standing still; it carries
  beat-level precision, which is all a threshold of several beats can use.

- **The columns are the consumer's, absent by default.** The crate writes them
  when they exist and, on Postgres `42703`, turns the durable heartbeat off for
  the life of the process, reporting any failed beat once rather than once per
  beat. The migration may land before or after the crate version that writes it.

- **The beat takes `SET LOCAL lock_timeout = '1s'` and skips a tick rather than
  queueing.** It updates the row the checkpoint writes; a beat that waited would
  arrive late for the same reason the worker is busy, which is the coupling the
  fixed cadence exists to remove.

- **Cadence defaults to 5s** (`HEARTBEAT_CADENCE`, configurable), deliberately
  unrelated to the poll interval: a deployment polling every 30s still wants its
  liveness answered in seconds. One statement per beat per replica covers every
  Policy it leads.

## Consequences

- A consumer alerts on `last_beat_at` older than three beats and on
  `liveness = 'Stopped'`; both are unambiguous. `last_polled_at` is a warning
  channel, not a page: one hung dispatch legitimately costs `dispatch_timeout` ×
  four attempts, so a poll stamp minutes old can be correct behaviour the runner
  is already parking as a [Dead letter](../../CONTEXT.md#dead-letter).
- Liveness costs one connection and one statement per replica per cadence, and no
  work at all on the worker's path.
- A test asserting liveness has to reach two runners against one database, because
  `StandingBy` only exists where another replica holds the lock
  (`policy_liveness.rs`, on the harness of
  [ADR-0014](0014-policy-daemon-tests-assert-through-operator-visible-observations.md)).
- A test whose worker is wedged cannot shut the daemon down — joining it means
  joining the reaction — so the harness gained `abandon()`, which is what a real
  process does when it exits with a worker still inside one.

## Rejected

- **A stamp written by the worker as it polls** (the first implementation). It
  cannot beat while the worker is doing what it is supposed to do, and it goes
  quiet during restarts and while standing by — making every hang look like a dead
  process.
- **A per-replica liveness table** (`policy_workers` keyed by `(policy,
  replica_id)`). It answers failover readiness — "is a Standby ready to take
  over?" — but that is a fleet question answered by scraping each pod's own
  `daemon.liveness()`, since the consumer hosts the runner. It costs a stable
  replica identity, row growth bounded only by how many replica ids ever existed,
  and pruning of replicas that were scaled away. The cursor row already answers
  the question that pages someone: is anything reacting for this Policy.
- **Deriving liveness from `last_beat_at` alone.** One timestamp cannot separate
  standing by from stopped — neither polls anything — and it would reintroduce the
  guess this work removes.
- **A `Liveness` variant on `PolicyCondition`.** It would make adding a state to
  one axis silently change the meaning of the other, and would force a read model
  over the tables to answer a question the tables cannot see.
- **Writing the beat inside the checkpoint `UPDATE`.** Free, but it would couple
  columns the consumer owns to the statement that advances the cursor: a schema
  without them would fail the checkpoint, not the beat — and a checkpoint only
  happens when the worker is working, which is not when the signal is needed.
