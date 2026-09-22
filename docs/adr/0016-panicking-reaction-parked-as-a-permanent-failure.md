# A panicking reaction is a permanent failure, contained at the event

**Status:** accepted; the duplicate row a crash inside the checkpoint window
leaves is no longer accepted —
[ADR-0024](0024-a-parked-command-is-one-row.md) makes a parked command unique per
reaction and turns the second park into a refresh. Everything else below stands.

A `Policy` reaction is arbitrary user code the runner calls on the worker's own
task. A panic in it — an unwrapped `None`, an index out of range, a malformed
payload — unwound the whole worker: nothing caught it, nothing restarted it,
nothing logged it, and that Policy simply stopped reacting for the rest of the
process's life. One bad event cost an entire pipeline, silently.

The runner already had the answer for every *returned* failure: a permanent one
parks a [Dead letter] and the cursor advances, so a single bad event never wedges
a Policy. A panic is exactly that — a permanent failure — and was only
unreachable because the absorption sat inside an `async fn` rather than behind a
catch. We make the delivery of one event the containment boundary.

## Decisions

- **The boundary is the event, not the worker.** `react_to_event` runs the whole
  per-event reaction (`react` plus the dispatches it returns) behind
  `catch_unwind`, one level outside `execute_event_reactions` so it wraps the
  retry loop as well as the reaction. Containing at the worker instead — letting
  the task die and restarting it — would re-deliver the same event to the same
  reaction and panic again, spending the worker's restart budget on one bad event
  that will never succeed. Restarting a worker is a separate concern, for
  failures that are *about* the worker.

- **A panic is permanent on first occurrence and is never retried.** Re-running a
  reaction that panicked deterministically panics again, so there is nothing for
  a back-off to fix. The row is parked immediately; the retry classification of
  returned errors (`Unavailable`, `RateLimited`, `Conflict`) is untouched.

- **A parked panic says it was a panic.** The dead letter records
  `error_kind = 'Panic'` (`PANIC_ERROR_KIND`) rather than an `ErrorKind`
  rendering, because the two demand different responses: a returned error is
  usually data — the domain refused the command, or a dependency was down — and a
  panic is usually a defect in the reaction. `WHERE error_kind = 'Panic'`
  separates the operator's triage queue from the developer's bug list. The
  `error_message` carries the panic's own message, which is the only description
  of the defect that exists.

- **Parking a panic logs at `error`,** naming the Policy, the event and the
  position. A returned permanent failure already does; a panic is at least as
  serious, and the point of the change is that it stops being silent.

- **The retry path catches too.** `retry_dead_letter` replays the same reaction,
  so a row parked for a panic panics again when an operator retries it. It is
  contained there as well: the row is updated in place to `Panic` with the fresh
  message and reported `StillFailing`, instead of unwinding the operator's call
  and — in `retry_policy_dead_letters` — abandoning every row after it.

## Rejected alternatives

- **Catching around `react` alone.** It contains the panic the ticket named and
  leaves the one a dispatched command's handler raises, on the same worker task,
  through the same await. The per-event boundary covers both for the same catch.

- **Treating a panic as retryable.** It costs three back-offs per panicking
  reaction and reaches the same dead letter, delaying the Policy for a failure
  that is deterministic by construction.

- **Storing the panic under `ErrorKind::Internal`.** It reads as "a returned
  internal error" and is indistinguishable, in the table an operator queries,
  from a store failure. The distinction is the point.

## Consequences

- A Policy survives a panicking reaction: it reacts to every later event, its
  cursor advances past the event that panicked, and a restart resumes past it
  rather than re-delivering it.
- **A crash between parking and the next checkpoint parks the row again.** The
  dead letter commits immediately, the cursor is written every
  `checkpoint_batch_size` events, so a process that dies in between re-delivers
  the event and a deterministic failure parks a second row. Accepted, not fixed,
  and not specific to panics — it is at-least-once delivery (ADR-0003) showing
  through on the path that was already there for returned permanent failures. A
  dead letter is an extraordinary event an operator reads by hand; showing the
  same one twice costs a moment's triage, while de-duplicating it costs a
  uniqueness constraint over every parking path.
  **Superseded by [ADR-0024](0024-a-parked-command-is-one-row.md)**: the constraint
  turned out to cost one `ON CONFLICT` in the one function that parks, and a retry
  that settles every generation of a row (ADR-0021) made the duplicates the
  library's bookkeeping rather than only an operator's reading.
- Two things remain uncontained, and both are stated in `CONTEXT.md`'s
  non-guarantees: a panic inside a task the reaction **spawns itself**, which
  unwinds in its own task outside this boundary, and any panic in a binary built
  with `panic = "abort"`, where the process ends before a catch can run.
- A reaction with interior mutable state may be left inconsistent by its own
  panic. `AssertUnwindSafe` is deliberate: the runner's own bookkeeping and the
  database are untouched by the unwind, and the reaction's private state is the
  reaction's to keep consistent.

[Dead letter]: ../../CONTEXT.md#dead-letter
