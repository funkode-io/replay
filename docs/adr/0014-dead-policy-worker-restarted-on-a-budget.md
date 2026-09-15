# A dead worker is restarted on a budget; exhausting it is published, not silent

**Status:** accepted

A Policy worker that panics for a reason the per-event path cannot contain — in
the drain loop, in cursor I/O, in the feed read — unwound inside `tokio::spawn`
and was simply gone. The daemon held every `JoinHandle` privately and discarded
every `JoinError` on shutdown, so nothing observed the death, nothing restarted
it, and no consumer could supervise from outside. Replicas did not help:
leadership is held per Policy by a process's lock-manager session
([ADR-0008](0008-policy-runner-shared-connection-leadership.md)), not by the
worker task, so a dead worker's process keeps the advisory lock and no
[Standby](../../CONTEXT.md#standby) ever takes over.

Each worker now runs under a supervisor that restarts it, bounded by a
[Restart budget](../../CONTEXT.md#restart-budget) (funkode-io/replay#185).

## Decisions

- **A restart is safe because the cursor is durable.** A restarted worker resumes
  from its last durable checkpoint and re-delivers at most a checkpoint's worth
  of events. That is the at-least-once contract reactions are already written
  against, so restarting costs re-delivery, never a skipped event. Nothing about
  supervision would work without this: it is why "restart it" is a legitimate
  answer at all.

- **The budget is per worker, not per process.** One Policy's restarts charge one
  Policy's budget, and a restart re-reads only that Policy's cursor. A process
  running twenty Policies where one is defective must not be one where the other
  nineteen are one death away from stopping.

- **Bounded by count within a sliding window, with exponential backoff.** The
  backoff keeps a worker failing against a struggling database from spinning
  against it; the window keeps "restarting forever" from passing for "running".
  Both, and both backoff bounds, are configurable with defaults
  (`WorkerSupervision`), because the right numbers depend on what the deployment
  considers transient.

- **Exhaustion is published, not merely logged.** A worker that spends its budget
  is recorded in `PolicyRunnerDaemon::stopped_workers()`. The incident behind
  this work (funkode-io/replay#164) was invisible precisely because a stopped
  Policy produced no signal at any level; a stop that only a log line knows about
  repeats it.

- **Exhaustion does not end the process — yet.** Only a process exit releases the
  advisory lock and lets a Standby take over, so escalation must stay reachable;
  it is a consumer-owned hook (funkode-io/replay#186). Until it lands the library
  stops the worker and says so, rather than choosing to exit on a consumer's
  behalf.

- **The shared tasks are supervised too, but report through the Policies they
  abandon.** The lock manager and the NOTIFY listener are spawned tasks that can
  die the same way, so they carry the same budget. They own no Policy, so they
  are not stopped *workers*: their leadership senders are owned by the caller
  rather than by the task, which is what lets a restarted manager resume
  publishing on the same channels instead of closing them under the workers. If
  the manager does spend its budget, those channels close and every worker
  reports itself stopped — which is what an operator acts on, a list of Policies
  rather than a piece of plumbing.

- **A worker whose lock manager is gone is not restarted.** Restarting cannot
  help a worker that can never be elected again, so it is recorded as stopped
  immediately instead of burning a budget to reach the same place.

## Consequences

- A panic outside the reaction costs re-delivery and a `warn`, not a stopped
  Policy. A panic *inside* the reaction still kills the worker today; containing
  it at the event and parking a [Dead letter](../../CONTEXT.md#dead-letter) is
  funkode-io/replay#183, and will make the two boundaries distinct so that a
  poison event cannot consume a restart budget.
- Nothing is persisted: the budget and the stopped list are in-process state, so
  a restart of the process resets both and no schema change is involved.
- `stopped_workers()` is a poll, not a notification. Turning it into something a
  consumer is told about is the escalation hook's job (funkode-io/replay#186).
