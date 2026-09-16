# A hung dispatch is cut loose by a timeout, and parked as a timeout

**Status:** accepted

Nothing bounded the dispatch path in time. A command handler that blocked — a
stalled connection, a lock it would never get, a read that never returned — held
its worker indefinitely while every other Policy carried on normally, so the
deployment looked healthy while one pipeline was frozen. Unlike a panic, the
failure produced no error, no log line and no cursor movement: it was
indistinguishable from a Policy with nothing to do.

The runner already knows what to do with a retryable failure: back off, retry,
park a [Dead letter] when the budget runs out. A timeout is how a hang becomes
one.

## Decisions

- **The bound is on one dispatch, not on the event.** `tokio::time::timeout`
  wraps the single `execute_dispatch` call, inside the retry loop, so each
  attempt gets a fresh budget and the timeout is the same number whichever
  attempt is running. Bounding the whole event instead would make the effective
  per-attempt limit depend on how many dispatches the reaction returned and how
  many attempts had already been spent.

- **A timeout is retryable, not permanent.** The runner has no evidence the
  command *cannot* succeed — only that it did not succeed in time, which is what
  a dependency that is coming back looks like. It therefore takes the existing
  `Unavailable` path: three back-offs, then a parked [Dead letter]. This is the
  opposite classification from a panic (ADR-0016), because the evidence is the
  opposite: a panic reproduces deterministically, a hang may not.

- **A parked timeout says it was a timeout.** The dead letter records
  `error_kind = 'Timeout'` (`TIMEOUT_ERROR_KIND`), distinct from every
  `ErrorKind` rendering and from `'Panic'`. The three demand different responses:
  a returned error is usually data, a panic is usually a defect in the reaction,
  a timeout is a question about what the command was waiting for. Nothing else
  could carry it — a hung command returns no error to derive a kind from.

- **The limit is per Policy, with a default.** `Policy::dispatch_timeout()`, then
  `REPLAY_DISPATCH_TIMEOUT_MS`, then 30s — the same precedence every other Policy
  setting uses. The default is generous because the mechanism exists to cut loose
  a reaction that has *stopped*, not to enforce a latency budget: a limit chosen
  for fast reactions would park slow ones as failures. `0` reads as unset rather
  than as "abandon everything immediately".

- **Timing out logs at `warn`,** naming the Policy, the position, the aggregate,
  the limit and the elapsed time. The elapsed time is the one fact the parked row
  cannot carry, and the difference between "wedged" and "just over the line".

- **The operator's retry is bounded too.** `retry_dead_letter` replays the
  reaction through the same bounded call, so a bulk retry of rows parked for a
  hang comes back instead of wedging the operator's own process on the first row.

## Rejected alternatives

- **A new `ErrorKind::Timeout` in `es`.** It would put a runner concern in the
  portable error type every aggregate returns, and be indistinguishable in the
  table from a timeout an aggregate's own client returned. The kind is a runner
  classification, so it lives where `PANIC_ERROR_KIND` already lives.

- **Treating a timeout as permanent.** It parks a dependency's ten-second
  hiccup as a failure an operator must retry by hand, for exactly the class of
  fault the back-off was built for.

- **Bounding the reaction's whole delivery with one timeout.** A reaction
  returning several dispatches would get an ever-shrinking budget per command,
  and the number in the log would no longer be the number in the Policy.

## Consequences

- A hung reaction costs one parked row and at most
  `(1 + MAX_DISPATCH_RETRIES) × dispatch_timeout` plus the back-offs, after which
  the worker continues with the next event.
- **The timeout cannot interrupt work the reaction moved onto another task.**
  Dropping the future cancels the command at its next suspension point only;
  a `tokio::spawn`, a blocking pool or a request already in flight in a detached
  client keeps running, unobserved, after the runner stops waiting. Stated in
  `CONTEXT.md`'s non-guarantees.
- A cancelled dispatch is cancelled *mid-command*: the dropped connection rolls
  its transaction back, so an abandoned dispatch normally commits nothing, and
  the positions its append consumed are burned (funkode-io/replay#164).
- **A dispatch abandoned while committing may still have committed.** The
  runner cannot tell a command that hung from one that returned a microsecond
  after the deadline, so a retry can re-execute work that landed. That is the
  at-least-once contract (ADR-0003) the retry path already relies on: the
  causation guard plus optimistic concurrency is what makes re-execution safe,
  and it is unchanged here.
- Every Policy is now bounded by default, including ones written before this
  existed. A reaction that legitimately takes more than 30s must say so with
  `dispatch_timeout`, or it will be parked as a timeout.

[Dead letter]: ../../CONTEXT.md#dead-letter
