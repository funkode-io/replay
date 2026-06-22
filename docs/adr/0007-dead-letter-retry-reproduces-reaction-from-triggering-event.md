# Dead-letter retry reproduces the reaction from the triggering event

**Status:** accepted

Policies dead-letter-and-advance: when a reaction fails permanently the runner
records a [`policy_dead_letters`](../../persistence/tests/migrations/0010_policy_dead_letters.sql)
row pinning the **triggering event** (`event_id` + `global_position`) and moves
the cursor past it, so one bad event never wedges the Policy (ADR-0003).
ADR-0006 added **policy status** to *observe* those failures but deliberately
deferred *controlling* them. This ADR adds the controlling half — **retry** and
**discard** of a dead letter — the capability the `Dead letter` glossary entry
always promised ("queryable so an operator can later inspect them and either
retry or discard them").

The motivating case is a side effect that was transiently unavailable: a Policy
reaction dispatches a command whose handler writes to an external system (e.g. a
CDN) via its registered `Services`; the system is down, the command fails
permanently, a dead letter is parked. Hours later the system is restored and an
operator wants the parked reactions to run — without re-importing source data
they may no longer hold.

## Decisions

- **Retry re-runs the reaction; it does not replay a stored command.** To retry,
  the runner loads the triggering event (already pinned by the dead letter) and
  calls `Policy::react` **again** — a pure function of the event (ADR-0003), so the
  event reproduces the reaction — then pushes the resulting `Dispatch` back through
  the normal execution path. We rejected serializing and replaying the failed
  command payload: it would force every command to be `Serialize`, persist opaque
  payloads (today `Box<dyn Any + Send>`), and replay a command the current Policy
  code might no longer produce. Re-react needs nothing the dead letter does not
  already store. The schema pins the *event*, not the command, precisely because
  the reaction is reproducible.

- **Retry runs against current state and current code.** Because `react` is
  re-evaluated now and the command is re-judged by the target [Aggregate] against
  **today's** state, a retry can legitimately resolve differently than the original
  attempt — succeeding now that the side effect is reachable, or being declined
  with `BusinessRuleViolation` if the reaction has gone stale. This is the desired
  semantics for an event-sourced system: retry means "re-do this event's reaction
  as the Policy defines it **now**," not "replay a fossilised attempt."

- **The library does not detect or prevent stale reactions; the Aggregate
  guards staleness.** A retried reaction steps outside the global-order guarantee
  the forward drain preserves (a parked "publish image v1" can be retried after a
  newer "publish v2" already succeeded). The library **cannot** know which side
  effects are order-sensitive, so it neither silently reorders nor silently
  refuses: it re-judges against current Aggregate state, and an Aggregate that
  models the relevant state declines the stale command via `BusinessRuleViolation`
  (a clean resolution, no clobber). Guarding order-sensitive effects is the target
  Aggregate's responsibility, exposed through the current-state re-judgment hook.

- **The cursor is never touched.** Dead-letter-and-advance already moved the cursor
  past the parked event so the Policy was not wedged; retry reaches back to that
  single event out of band. Retry advances nothing.

- **Retry does not require leadership.** The per-policy `pg_advisory_lock` exists
  solely to keep the forward cursor a single sequential consumer of `global_position`
  (ADR-0003). Retry moves no cursor, and re-execution is already made safe by the
  **causation guard** (idempotent re-apply keyed on the triggering event's identity)
  plus the existing optimistic-concurrency check in `Cqrs::execute`. Gating retry on
  leadership would only force the admin call to be routed to the current leader, for
  no correctness gain.

- **Row lifecycle: delete-on-success, update-in-place-on-repeat-failure.** A retry
  that resolves (`Ok` or `BusinessRuleViolation`) **deletes** the dead-letter row; a
  retry that fails permanently again **updates the existing row in place** (refreshed
  error) rather than inserting a second one. This keeps `dead_letter_count` meaning
  *"events currently parked"* so [policy status] recovers from `Degraded` truthfully
  (ADR-0006). The required ordering is **execute first, then mutate the row** — never
  delete optimistically before executing. Because every row op is a single
  primary-key-scoped `DELETE`/`UPDATE`, concurrent retries of the same row are
  idempotent and commutative.

- **Discard ships paired with retry.** An operator who judges a reaction permanently
  unrecoverable can **discard** the dead letter — drop the row without re-executing.
  It shares all of retry's plumbing, and without it the only escape from a genuinely
  dead `Degraded` is hand-rolled SQL, exactly the opacity ADR-0006 set out to remove.

- **Controlling lives on `PolicyRunner`, not `PolicyStatusStore`.** Retry needs the
  live policies, executors, and `Cqrs`; the read-only status store has none of them.
  This keeps ADR-0006's *observing vs. controlling* split clean: status observes,
  the runner controls. The surface is a `retry_dead_letter(id)` primitive, a
  `retry_policy_dead_letters(name)` convenience built on it, and their `discard`
  counterparts.

## Consequences

- **No schema change.** The feature is built entirely on the existing
  `policy_dead_letters` columns; retry is a read of the pinned event plus a
  PK-scoped row mutation.

- Retry is **not** a verbatim reuse of the drain path: the drain *inserts* a dead
  letter on failure, whereas retry must *update in place*, so it is a sibling
  executor that owns its own outcome handling.

- An order-sensitive side effect with **no** staleness guard in its Aggregate can be
  resurrected stale by a retry. This is a documented contract, not a defect: the
  same current-state re-judgment that makes retry useful is the seam where authors
  opt into staleness protection.

- Like status, controlling is **native-only**, living in the server-only
  `persistence` crate alongside the runner and its tables.

[Aggregate]: ../../CONTEXT.md#aggregate
[policy status]: ../../CONTEXT.md#policy-status
