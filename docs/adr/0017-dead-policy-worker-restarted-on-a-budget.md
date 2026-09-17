# A dead worker is restarted on a budget; exhausting it is published, not silent

**Status:** accepted

A Policy worker that panicked outside the per-event path unwound inside
`tokio::spawn` and was gone: the daemon held every `JoinHandle` privately and
discarded every `JoinError`. Replicas did not help — leadership is held per
Policy by a process's lock-manager session
([ADR-0008](0008-policy-runner-shared-connection-leadership.md)), not by the
worker task, so the dead worker's process keeps the advisory lock and no
[Standby](../../CONTEXT.md#standby) takes over.

Each worker now runs under a supervisor that restarts it, bounded by a
[Restart budget](../../CONTEXT.md#restart-budget) (funkode-io/replay#185).

## Decisions

- **A restart is safe because the cursor is durable.** It re-delivers at most a
  checkpoint's worth of events, which the at-least-once contract already covers.
  Without this, "restart it" would not be a legitimate answer at all.

- **The budget is per worker, not per process.** A process running twenty
  Policies where one is defective must not be one where the other nineteen are
  one death away from stopping.

- **Bounded by count within a sliding window, with exponential backoff.** The
  backoff keeps a worker failing against a struggling database from spinning; the
  window keeps "restarting forever" from passing for "running". Both, and both
  backoff bounds, are configurable (`WorkerSupervision`).

- **Exhaustion is published, not merely logged**, in
  `PolicyRunnerDaemon::stopped_workers()`. The incident behind this work
  (funkode-io/replay#164) was invisible because a stopped Policy produced no
  signal at any level.

- **Exhaustion does not end the process here.** Only a process exit releases the
  advisory lock and lets a Standby take over, so ending it is a consumer-owned
  hook ([ADR-0019](0019-escalation-is-a-consumer-hook-that-exits-by-default.md)),
  which this layer invokes rather than deciding for itself.

- **The shared tasks carry the same budget but report through the Policies they
  abandon.** Their leadership senders are owned by the caller, not the task, so a
  restarted manager resumes publishing on the same channels instead of closing
  them under the workers. A manager that does spend its budget closes those
  channels and every worker reports itself stopped — a list of Policies, which is
  what an operator acts on.

- **A worker whose lock manager is gone is not restarted**, since it can never be
  elected again; it is recorded as stopped immediately.

- **The lock manager's pinned connection is ended, not returned, when it lets go
  of it.** Postgres releases a session advisory lock only when the session ends,
  and sqlx returns a dropped pool connection to the idle queue with its session
  state intact. A panicking manager would otherwise leave every Policy it led
  locked by an idle connection — unleadable in every replica, and invisible,
  because a worker waiting to be elected looks exactly like a healthy
  [Standby](../../CONTEXT.md#standby).

## Consequences

- A panic *inside* the reaction never reaches supervision: it is contained at the
  event and parked as a [Dead letter](../../CONTEXT.md#dead-letter)
  ([ADR-0016](0016-panicking-reaction-parked-as-a-permanent-failure.md)), so a
  poison event cannot consume a restart budget.
- **A dispatch that fails is never supervision's business.** Restarting on a
  failed dispatch would re-deliver the same event forever, which is the loop the
  dead-letter contract exists to prevent, so no test here asserts it.
- **An election logs where it resumes from**, at `info`. It is the only trace a
  process killed from outside leaves: an OOM-killed pod reprinting the same
  `next_position` is being killed by one event, one whose position advances is
  leaking.
- Nothing is persisted: budget and stopped list are in-process state, so a
  process restart resets both and no schema change is involved.
- `stopped_workers()` is a poll; what a consumer is *told* about is the escalation
  hook ([ADR-0019](0019-escalation-is-a-consumer-hook-that-exits-by-default.md)).
