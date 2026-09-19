# A stream lock wait is bounded on the server

**Status:** accepted

Both transactions that take a stream's row — the append path's
`SELECT id FROM streams WHERE id = $1 FOR UPDATE`, inside `append_event`, and
`compact`, which holds the same row across the fold, the snapshot insert and the
archive — waited indefinitely. Nothing bounded the wait server-side.

The [Dispatch timeout](../../CONTEXT.md#dispatch-timeout) does not reach it.
Cancelling a future sends Postgres nothing: the statement keeps waiting, sqlx
cannot return the connection to the pool until it finishes, and an abandoned
dispatch therefore held a pool connection for as long as the blocker lasted —
four of them per hung event once the retries are counted
(funkode-io/replay#205, ADR-0018).

Each of those transactions now sets `lock_timeout` for its own duration. A wait
that exceeds it fails on the server, the transaction ends, and the connection
goes back to the pool while the blocker is still holding the row.

## Decisions

- **One knob for both sites.** A single value — how long anything in this library
  waits for a stream row — applied in the append transaction and the compaction
  transaction. They wait on the same row for the same reason; the asymmetry
  between them is on the *holding* side, which a waiter-side bound does not
  address and a second knob would not either.

- **Resolution follows `projection_flush_size`**: per-store builder override
  (`stream_lock_wait`) → `REPLAY_STREAM_LOCK_WAIT_MS` → 30s.

- **The default is 30s, matched to `REPLAY_DISPATCH_TIMEOUT_MS`'s**, so the server
  releases the row at roughly the moment the runner walks away from the dispatch:
  the strand is then bounded by one timeout instead of by the blocker.

- **Zero means disabled, not unset.** The value goes verbatim to `lock_timeout`,
  where zero already means "wait forever", so zero is the documented opt-out and
  restores the behaviour this replaced. It is written out as `'0'` rather than
  left unset, so it also overrides a `lock_timeout` the consumer's pool or role
  carries — a bound that merely skipped its own statement would inherit that one
  and report it as a wait this library never made. It deliberately diverges from
  `REPLAY_DISPATCH_TIMEOUT_MS`, whose zero reads as *unset* because that value
  goes to a tokio timer rather than to Postgres. Unset, negative and unparseable
  values fall back to the default: a typo must not silently remove a bound. A
  value past what `lock_timeout` can express — an integer of milliseconds, so
  about 24.8 days — is clamped to that ceiling rather than sent: an unclamped one
  makes the `set_config` itself fail, turning a bound on one statement into a
  failure of every append and compaction.

- **`55P03` is `Unavailable`, not `Conflict`.** `Error::conflict` is this
  codebase's optimistic-concurrency failure and arrives carrying an expected and
  an actual version; a lock wait that ran out says nothing about versions. Both
  are temporary and take the same retry path, so the runner behaves identically
  with no new classification branch — what changes is what a consumer matching on
  `kind()` concludes. The error names the stream and the limit it exceeded.

- **`SET LOCAL`, via `set_config(…, true)`.** The bound dies with its transaction,
  so a pooled connection carries nothing into its next use — and `set_config`
  takes a bind parameter, which `SET` does not. It is written on every
  transaction, including a disabled one, so the setting the transaction runs under
  is always this library's and never the session's leftovers. The same pattern the
  liveness beat already uses on the cursor row
  ([ADR-0020](0020-liveness-is-published-from-memory-and-beaten-on-a-cadence.md)),
  shared with it in `lock_wait.rs`.

- **It bounds the transaction, not one statement.** Everything the append
  transaction does — including writes by registered inline projections — is
  bounded by the same value. That is the intent: the bound is on this library's
  waiting, not on one row. Which is why `55P03` is classified in `db_error`, the
  function a projection handler maps its own failures with, and not only where
  this crate takes the stream row: a contended projection write would otherwise
  reach the runner as `Internal` and be parked without a retry. A handler that
  maps sqlx errors its own way owns that classification, as it owns every other.

- **`statement_timeout` stays out.** A consumer can already set it, and any other
  pool-level option, through `PgConnectOptions`, and neither of them bounds a lock
  wait specifically.

## Consequences

- **An append queued behind a legitimately long compaction of a large stream now
  fails after 30s instead of waiting, and parks a [Dead
  letter](../../CONTEXT.md#dead-letter) once its retries are spent.** The lower
  bound on any value chosen here is the longest *honest* hold, and in this library
  that is `compact` — O(stream length), with no number in the code. That is the
  argument for a generous default and against tightening it.
- A spike of these means a long holder, not a broken append. Diagnosis starts with
  `pg_locks`/`pg_stat_activity` on the stream the error names, not with the
  appending service.
- ADR-0018's consequence — an abandoned dispatch holding a pool connection until
  the lock clears — is now bounded by this value rather than by the blocker, and
  disabled only by opting out.
- Shortening how long `compact` *holds* the row is untouched: that is the blocker
  side, and it is the subject of funkode-io/replay#214.

## Rejected

- **A knob per site** (one for appends, one for compaction). The two waits are the
  same wait; a consumer who wants an append to give up sooner than a compaction is
  expressing a preference about the *holder*, which neither knob can act on.
- **Pool- or session-level `lock_timeout`.** It would bound every statement on the
  connection, including ones this library does not own, and it leaks across the
  pool. `SET LOCAL` scopes the claim to the transaction that makes it.
- **Classifying the timeout as `Conflict`** so it reads as "somebody else has the
  stream". It would arrive without the versions every other conflict carries, and
  make `kind()` useless for telling a version mismatch from a busy row.
- **Deriving the wait from the caller's remaining dispatch budget.** It would be
  the sharper bound, but the store has no access to it: `Cqrs::execute` is called
  from inside the timeout, not with it, and plumbing a deadline through the
  `EventStore` trait for one backend's one statement is a wide change for a value
  a default already approximates.
