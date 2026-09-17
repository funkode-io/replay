# A worker that is down for good escalates through a consumer hook, defaulting to process exit

**Status:** accepted

A worker that spent its [Restart budget](../../CONTEXT.md#restart-budget) was
stopped and listed by `PolicyRunnerDaemon::stopped_workers()`
([ADR-0017](0017-dead-policy-worker-restarted-on-a-budget.md)) — a poll nobody was
obliged to make. The library cannot judge what should happen next, because that
depends on how the consumer is deployed, but it must not simply stop: that is the
failure mode of funkode-io/replay#164.

Exhaustion now invokes an escalation hook owned by the consumer
(funkode-io/replay#186).

## Decisions

- **The default is process exit** (`ESCALATION_EXIT_CODE`, `EX_SOFTWARE`).
  Leadership is held per Policy by the replica's lock-manager session
  ([ADR-0008](0008-policy-runner-shared-connection-leadership.md)), so a stopped
  worker's replica keeps the advisory lock and no
  [Standby](../../CONTEXT.md#standby) takes over. Exiting is the only outcome that
  releases it. A service that configures nothing therefore recovers by being
  restarted rather than lingering half-dead.

- **A hook that returns leaves the Policy stopped fleet-wide** and the
  documentation says so. Overriding the default means taking on the ending —
  failing a probe, draining, paging — or the Policy is down until a human notices.

- **The stop is recorded before the hook runs.** The default never returns and a
  supplied one may be defective, so `stopped_workers()` cannot depend on either. A
  hook that panics is caught and logged: the worker stays stopped and the
  supervisor's own reporting survives.

- **A worker abandoned by a dead lock manager escalates too**
  (`EscalationReason::Abandoned`), without a restart. It is as absent as one that
  spent its budget, and exiting is even more clearly the answer: nothing in this
  process can elect it again.

- **Nothing escalates while the daemon is shutting down.** A worker racing the
  lock manager's dropped leadership channel reads the shutdown as abandonment;
  escalating there would exit the process on every clean shutdown.

- **The hook is synchronous and per Policy.** It runs on that worker's supervisor
  task, which has nothing left to do, and is called once — a supervisor escalates
  and then stops supervising. A consumer needing async work spawns it.

- **Escalating one Policy does not stop the others.** The supervisors are
  independent; only what the hook itself does (exiting, by default) reaches them.

## Consequences

- A test must install its own hook, or the default ends the test process. The
  daemon harness installs a recording one before the test's own configuration
  runs, so the escalation path is asserted through the hook rather than by
  observing a dead process.
- `stopped_workers()` remains, now as the record behind an escalation rather than
  the only signal. A consumer reads a non-empty list only when the default has
  been replaced by a hook that returns.
- Still no schema change and nothing persisted: the hook is process state, like
  the budget it fires from.
