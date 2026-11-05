# 35. A command passes through middleware before it runs and before it commits

Date: 2026-11-04

## Status

Accepted. Decides [#308](https://github.com/funkode-io/replay/issues/308). The bulk lane
([#288](https://github.com/funkode-io/replay/issues/288)), which landed after it, follows it.

## Context

A consumer that needs to run code around every command had no hook for it, so it borrowed
the only one Replay runs inside every append's transaction: an [Inline projection].
dxp-data-loom's maintenance mode is registered as one. It writes nothing and matches no
event; its `handle` reads a flag and fails the append. Replay treats it as a read model in
every other respect, and that leaks:

- Every append records an applied mark per registered projection, and the catch-up looks
  for transactions without one (ADR-0032). A store built without the guard leaves every
  one of its appends owed to it for ever, and each catch-up round holds the projection's
  in-process lock while it pays that debt.
- The guard has to tell a live append from a catch-up, from Replay implementation details.
- A rule about commands lives in a read-model registry, so two stores executing the same
  commands behave differently because one carries an extra "projection".

Maintenance mode is the motivating case, not the only one: auditing, metrics and
enriching metadata (the user, a tenant id) all want to run around every command.

## Decision

**A command middleware is registered on `Cqrs`, runs on every command it executes, and
is never a projection.**

### The contract

A middleware has three hooks, each with a default that does nothing:

| Hook | Runs | Receives | Returns |
|---|---|---|---|
| `before` | Before the aggregate is loaded | the command context, mutably | `Next` |
| `before_commit` | Inside the append transaction, after the events are inserted and every inline projection has run, immediately before the commit | the command context | `Next` |
| `after` | Once the command has finished, whatever its outcome | the command context and the outcome | nothing |

```rust
enum Next {
    Continue,
    /// Not now; the same command may succeed later. Not a failure.
    Busy(String),
    /// The command is refused or the middleware failed.
    Error(replay::Error),
}
```

- **`before` can stop or enrich.** `Busy` or `Error` stops the command: nothing is loaded,
  handled or appended. Taking the context mutably lets a middleware add metadata, which the
  appended events then carry.
- **`before_commit` can roll back.** Anything but `Continue` makes the store roll the
  append back instead of committing. It receives no transaction handle: a middleware
  decides, the store commits or rolls back. So a middleware works with any store.
- **`after` only observes.** It cannot change the outcome. The outcome says whether the
  append committed, was stopped as `Busy`, or failed, with the error.
- **Order.** Middlewares run in registration order, as in a web server. The first one
  that does not return `Continue` stops the chain; later ones do not run. `after` runs in
  reverse order, and only for the middlewares whose `before` returned `Continue`.

At the public edge `Cqrs::execute` still returns the aggregate's error: `Error` is
returned as it is, and `Busy` becomes a `replay::Error` of kind `Busy` (ADR-0036).

### The command context

Registered once, a middleware cannot be typed by the aggregate, so it does not see the
command's payload. It sees the aggregate type, the stream type, the stream id, the
command's type name, the metadata, and the [Causation] when a [Policy] dispatched the
command. The runner already stamps causation into the metadata, so an application command
is one without it. A middleware that must decide on the content of a command decides on
the metadata, which is where a consumer puts what such rules need.

### Where it runs

On every command `Cqrs` executes: an application's call, a Policy dispatch (the runner
executes through the `Cqrs` it was built with, so it shares its middleware) and a bulk
execution (`Cqrs::execute_bulk`), where `before` runs once and `before_commit` runs before
every segment's commit: a middleware that stops a segment rolls back that segment only, and
the segments before it stay committed.

Not on what is not a command: a direct append through the store, compaction, stream
deletion, a rebuild or a catch-up. A middleware writes no row to the projection registry
and no applied mark. A `Cqrs` without middleware behaves as before.

### The store's part: a commit gate

`EventStore::store_events_stream` takes a **commit gate**: an async callback returning
`Next`, beside the existing sink. A store calls it once per transaction, after the
inserts and the inline projections, and commits only on `Continue`. `Cqrs` passes its
middleware chain; a direct append, and a `Cqrs` with no middleware, pass a gate that
always continues (`OpenGate`) and says so (`CommitGate::always_continues`), so a store
may skip the lock below for it. Every store implements the same three steps, so a new
store needs nothing else to support middleware.

### No gap between the check and the commit

A gate that returns `Continue` and a commit that follows are two steps. A flag turned on
between them would let one append commit after the operator believed writes had stopped.

The Postgres store closes it. For any gate but the one that always continues it takes a
shared, transaction-scoped advisory lock on a key Replay owns, before calling the gate, and
holds it until the transaction ends. `PostgresEventStore::await_in_flight_commits()` takes
the same lock exclusively and releases it. Each attempt at that exclusive lock is bounded on
the server by the stream lock wait (ADR-0022), and retried until it succeeds, so a call
dropped by its caller leaves at most one bounded wait behind rather than one that holds a
connection, and queues every gated append, for as long as the appends in flight last. Maintenance mode is then: commit the flag, call
`await_in_flight_commits()`. Every append that checked before the flag was set still holds
its lock, so the call waits for it; every append that takes the lock afterwards checks
after the flag was set. The middleware must read the flag as it is, not from a cache.

**Every store must give the same guarantee.** A store that cannot is not a complete
`EventStore`; it documents the gap instead.

## Considered options

- **Keep the inline projection.** The leaks above are the reason for this ADR.
- **Let an inline projection opt out of catch-up and applied marks.** Cheapest, and ends
  the catch-up debt, but keeps a command rule in a read-model registry and gives no hook
  before the aggregate is loaded.
- **Register middleware on the store.** Would also cover direct appends, but a store has
  no notion of "before the command" and two stores would again need configuring alike.
- **`in_transaction` at the start of the transaction, with the connection.** Cannot see a
  flag set while the transaction runs, and ties the middleware to one store's connection
  type. `before_commit` without a connection plus the store's lock does both jobs.
- **Return `Result` from the hooks.** Leaves "not now" indistinguishable from a failure,
  which is the distinction the [Policy runner] needs (ADR-0036).
- **Hand the middleware the typed command.** Not possible for one middleware registered
  for every aggregate; a serialized payload would need a `Serialize` bound on every
  command.

## Consequences

- **Breaking:** `store_events_stream` takes the commit gate, which changes it for direct
  callers and for third-party `EventStore` implementations. The release notes carry a
  migration.
- **Each append through a `Cqrs` with middleware takes one shared advisory lock.**
  `await_in_flight_commits()` waits behind every append in flight and, while it waits,
  new appends queue behind it, each for at most the stream lock wait before it fails as
  `Unavailable`; it is meant for a switch, not a request path. A direct append and a
  `Cqrs` without middleware take no lock and are never waited for.
- **A generic transaction abstraction** (one append flow, each store implementing begin,
  commit and rollback) would let a new store inherit the gate and the lock. It is left
  for its own issue.

[Inline projection]: ../../CONTEXT.md#inline-projection
[Causation]: ../../CONTEXT.md#causation
[Policy]: ../../CONTEXT.md#policy
[Policy runner]: ../../CONTEXT.md#policy-runner
