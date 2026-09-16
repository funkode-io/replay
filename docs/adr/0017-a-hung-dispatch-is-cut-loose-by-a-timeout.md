# A hung dispatch is cut loose by a timeout, and parked as a timeout

**Status:** accepted

Nothing bounded the dispatch path in time. A command handler that blocked held
its worker indefinitely while every other Policy carried on, and produced no
error, no log line and no cursor movement — indistinguishable from a Policy with
nothing to do. The runner already knows what to do with a retryable failure; a
timeout is how a hang becomes one.

## Decisions

- **The bound is on one dispatch, inside the retry loop.** Each attempt gets a
  fresh budget, so the limit in the log is the number the Policy declared.
  Bounding the whole event would make the effective per-attempt limit depend on
  how many dispatches the reaction returned and how many attempts were spent.

- **A timeout is retryable, not permanent** — the opposite of a panic
  (ADR-0016), because the evidence is the opposite: a panic reproduces
  deterministically, a hang looks like a dependency that is coming back. It takes
  the existing `Unavailable` path: three back-offs, then a parked [Dead letter].

- **A parked timeout says it was a timeout.** `error_kind = 'Timeout'`
  (`TIMEOUT_ERROR_KIND`): a returned error is usually data, a panic usually a
  defect, a timeout a question about what the command was waiting for. Nothing
  else could carry it — a hung command returns no error to derive a kind from.

- **The limit is per Policy, with a default:** `Policy::dispatch_timeout()` →
  `REPLAY_DISPATCH_TIMEOUT_MS` → 30s. Generous, because it cuts loose a reaction
  that has *stopped* rather than enforcing a latency budget. `0` reads as unset.

- **Timing out logs at `warn`** with the elapsed time — the one fact the parked
  row cannot carry, and the difference between "wedged" and "just over the line".

- **The operator's retry is bounded too,** so a bulk retry of rows parked for a
  hang returns instead of wedging the operator's own process on the first row.

## Rejected alternatives

- **A new `ErrorKind::Timeout` in `es`.** A runner classification in the portable
  error type every aggregate returns, indistinguishable in the table from a
  timeout an aggregate's own client reported.

- **Treating a timeout as permanent.** It parks a dependency's ten-second hiccup
  as a failure an operator must clear by hand.

## Consequences

- **The bound is per dispatch, not per event.** One hung dispatch costs at most
  `dispatch_timeout`, and a single-dispatch reaction therefore costs at most
  `(1 + MAX_DISPATCH_RETRIES) × dispatch_timeout` plus back-offs. A reaction
  returning several commands costs more: every attempt re-runs the dispatches
  before the one that hung, and the final attempt carries on into the ones after
  it, which can each time out in turn. An event's worst case scales with how many
  dispatches the reaction returns and where the hung one sits in the list.
- A hung reaction parks one row **per failing dispatch** — as it already does for
  returned permanent errors — and retrying any of them replays the whole reaction
  (funkode-io/replay#204).
- The timeout cannot interrupt work the reaction moved onto another task; see
  `CONTEXT.md`'s non-guarantees.
- An abandoned dispatch is cancelled mid-command: its transaction rolls back and
  the positions its append consumed are burned (ADR-0015). The rollback is not
  immediate. Cancelling the future stops this process waiting; it sends Postgres
  nothing, so a dispatch abandoned inside `append_event`'s
  `SELECT ... FOR UPDATE` holds its pool connection until the lock it was waiting
  for clears — and each retry takes another. A reaction that hangs in its own code
  (the common case) holds no connection at all: the command handler runs before
  the append opens a transaction. See funkode-io/replay#205.
- **A dispatch abandoned while committing may still have committed**, so a retry
  can re-execute work that landed. Unchanged at-least-once behaviour (ADR-0003),
  made safe by the causation guard.
- Every Policy is bounded by default, including ones written before this existed:
  a reaction that legitimately runs longer than 30s must say so.

[Dead letter]: ../../CONTEXT.md#dead-letter
