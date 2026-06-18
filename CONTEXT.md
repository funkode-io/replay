# Context: replay

The ubiquitous language for the `replay` event-sourcing library.

## Glossary

### Projection

An umbrella term for a derived read model built from events. "Projection" on its
own never names a single mechanism — always qualify it as a Live, Inline, or
Async projection. Distinct from an [Aggregate], which is the write-side state
rebuilt from a stream to make command decisions.

### Live projection

A read model computed on demand by folding events in memory, without persisting
any state or progress. This is the existing [Query] mechanism. The caller drives
it; nothing is stored between runs.

### Inline projection

A projection whose write is applied **inside the same transaction that appends
the events**, against the **same event store instance**. It is strongly
consistent with the events: the events and the projection write commit together
or not at all. The projection does not begin or commit the transaction — it only
contributes writes to a transaction the store owns.

### Async projection

A projection that updates an **external** system (for example a search index)
**eventually**, decoupled from the append transaction, driven by a background
process that compares projection progress against the event stream. Eventually
consistent rather than strongly consistent. (Planned; not yet implemented.)

### Policy

An event-driven reaction in the event-sourcing domain: an appended event
triggers a Policy, which issues a command that may raise further events. A
Policy is **not** a [Projection] — it derives no read model; its output is a
command and its effects are side effects on the write side. It runs in the
background and is eventually consistent. Because a Policy re-executes side
effects when it processes an event, it cannot be safely rebuilt by replaying
history the way a versioned [Projection] can. (Planned; not yet implemented.)
_Avoid_: reactor, saga, process manager, automation, trigger, reaction.

### Causation

The link from the event that triggered a [Policy] to the command and resulting
events the Policy raises in response. The triggering event's identity is the
stable key a target [Aggregate] uses to recognise a reaction it has already
applied, and the chain of causation is what bounds how deep one event may
cascade into further reactions.
_Avoid_: trigger, cause, origin.

### Dead letter

A recorded failure of a [Policy] reaction that could not be completed — a
_recorded skip_, never a silent one. When a reaction fails permanently (or
exhausts its retries) the runner stores a dead letter and advances past the
triggering event so a single bad event never wedges the Policy. Dead letters are
queryable so an operator can later inspect them and either [Retry] or [Discard]
them.
_Avoid_: poison message, failed event, error queue.

### Retry

The _controlling_ act of re-running a parked [Policy] reaction: the triggering
event recorded by a [Dead letter] is re-evaluated through the Policy **as it is
defined now** and the command it raises is re-executed, judged against **current**
[Aggregate] state. A retry reaches back to a single parked event out of band and
never moves the Policy's cursor. Because it re-runs against today's state, a
reaction that is now stale or no longer valid is legitimately declined rather than
replayed blindly — guarding order-sensitive side effects is the target Aggregate's
responsibility, not the runner's. Distinct from a [Rebuild], which resets and
replays a whole [Projection].
_Avoid_: reprocess, requeue, redrive.

### Discard

The _controlling_ act of an operator judging a [Dead letter]'s reaction
permanently unrecoverable and dropping the record **without** re-executing it. The
counterpart to [Retry]; together they are the controlling actions over a Policy's
failures that [Policy status] only observes.
_Avoid_: dismiss, drop, ignore.

### Policy status

A point-in-time, read-only snapshot of a [Policy]'s operational progress and
health — how far behind the event log it is and whether its reactions are
failing — intended for a human monitoring the system. It is **not** domain
state (an [Aggregate]'s rebuilt-from-stream state) and **not** a [Projection]
(it derives no read model from the event log; it reports on a Policy's runtime).
_Observing_ a Policy's status (read-only) is a separate concern from
_controlling_ a Policy (acting on its failures by [Retry] or [Discard] of a
[Dead letter]).
_Avoid_: state, policy state, health check.

### Query

The existing on-demand, in-memory fold over filtered events. It is the
realisation of a [Live projection].

### Projection version

A number declared in projection code that identifies the shape/logic of a
projection. When the version recorded in the store is older than the version in
code, the projection is rebuilt: its state is reset and all events are replayed.

### Rebuild

Discarding a projection's current state (reset) and replaying the full event
history through it to reconstruct it. Triggered when a projection's code version
is newer than the version recorded in the store.

### WASM target

`replay` supports WebAssembly. The core `es` crate and the `macros` crate are
WASM-compatible: they provide **dual, cfg-gated definitions** of the core traits —
`Send`-bounded under `cfg(not(target_arch = "wasm32"))` for multi-threaded native
runtimes, and `Send`-free under `cfg(target_arch = "wasm32")` for the
single-threaded WASM environment (generated services use
`cfg_attr(target_arch = "wasm32", async_trait(?Send))`). Any change to traits in
`es`/`macros` must preserve both arms. The `persistence` crate (Postgres + sqlx +
tokio) is **server-only / non-WASM**, so every projection mechanism that lives
there — including the [Inline projection] — is a native-only feature and is free
to use `Send` bounds. A [Policy] is likewise native-only: its contract (the
reaction itself) is portable, but its runtime is an irreducibly server-side
background process. A future [Async projection] aimed at client/edge targets
would need to honour the WASM dual-cfg pattern.

[Aggregate]: #aggregate
[Policy]: #policy
[Dead letter]: #dead-letter
[Retry]: #retry
[Discard]: #discard
[Rebuild]: #rebuild
[Policy status]: #policy-status
[Query]: #query
[Live projection]: #live-projection
[Inline projection]: #inline-projection
[Async projection]: #async-projection
[Projection]: #projection
