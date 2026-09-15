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

### Policy feed

The slice of the event log one [Policy] reads on a poll: every `global_position`
past its cursor, up to its read batch size, **before** its `stream_filter` is
applied. Contiguity is decided on those unfiltered positions; an excluded position
advances the cursor and fires nothing, like a compaction snapshot
([ADR-0012](docs/adr/0012-policy-feed-contiguity-on-unfiltered-positions.md)). A
`stream_filter` decides what a Policy *reacts to*, never how far it *gets*.
_Avoid_: subscription, stream, queue, backlog.

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
permanently unrecoverable and retiring the record from the active set
**without** re-executing it. The record is archived rather than destroyed, so
the failure history is never lost. The counterpart to [Retry]; together they are
the controlling actions over a Policy's failures that [Policy status] only
observes.
_Avoid_: dismiss, drop, ignore.

### Cursor move

The _controlling_ act of an operator repositioning a [Policy]'s stored cursor —
the `policy_cursors` row that records how far it has processed — by writing the
row directly while the system runs. The third controlling action alongside
[Retry] and [Discard], and the coarsest: it moves the Policy itself rather than
one parked reaction, skipping events when it moves forward and re-delivering
them when it moves backward. The running leader adopts the new position when its
feed is empty, and never writes a position that predates the move
([ADR-0012](docs/adr/0012-policy-cursor-is-an-operator-writable-control-surface.md)).
_Avoid_: reset, seek, rewind (as a name for the act; a rewind is one direction of
it).

### Policy status

A point-in-time, read-only snapshot of a [Policy]'s operational progress and
health — how far behind the event log it is and whether its reactions are
failing — intended for a human monitoring the system. It is **not** domain
state (an [Aggregate]'s rebuilt-from-stream state) and **not** a [Projection]
(it derives no read model from the event log; it reports on a Policy's runtime).
_Observing_ a Policy's status (read-only) is a separate concern from
_controlling_ a Policy (acting on its failures by [Retry] or [Discard] of a
[Dead letter]). It reports the [Progress] axis only and says nothing about
[Liveness]. It also tells a healthy idle Policy from a [Blocked policy].
_Avoid_: state, policy state, health check.

### Blocked policy

A [Policy] whose cursor sits in front of a `global_position` that does not exist
while a later one does, so its feed yields nothing and it reacts to nothing.
Distinct from _lagging_ (a backlog that is draining) and from `Degraded`
(reactions parked while the Policy still advances): blocked means zero
throughput. It is a [Progress] verdict: a blocked Policy's worker is usually
perfectly live. Whether it clears is not observable from one reading
([ADR-0006](docs/adr/0006-policy-status-read-only-operational-snapshot.md)).
_Avoid_: stuck, wedged, hung, stalled.

### Policy runner

The set of background workers that drive every [Policy] in a process — one
worker per Policy, each owning that Policy's durable cursor, sharing the
process's listener and lock-manager connections
([ADR-0008](docs/adr/0008-policy-runner-shared-connection-leadership.md)). The
Policy is what reacts; the runner is what makes it run, restarts it and reports
on it.
_Avoid_: policy engine, subscriber, dispatcher, scheduler, worker pool.

### Leader

The single worker, across every replica, that currently drives a given [Policy]
and holds its advisory lock. Leadership is held per Policy, not per process: one
replica is routinely Leader for some Policies and [Standby] for others, and
leadership moves only when the lock is released.
_Avoid_: primary, master, owner, active node.

### Standby

A worker that exists for a [Policy] another replica leads, holds no lock and
therefore processes nothing. A Standby is healthy and deliberately idle — it is
not a stopped worker and not a lagging one — and becomes [Leader] when the
current Leader releases the lock.
_Avoid_: secondary, passive replica, follower, spare.

### Liveness

The axis reporting whether a [Policy]'s worker exists and is running — leading,
standing by, restarting, stopped or unknown. Only the process running the
[Policy runner] knows it, so it is published from memory and never derived from
the operational tables. Independent of [Progress] in both directions: a
[Standby] is live and advances nothing, and a [Leader] can be live while its
Policy is a [Blocked policy].
_Avoid_: uptime, availability, aliveness, worker status.

### Progress

The axis reporting how far a [Policy] has advanced through its feed and whether
its reactions are completing — the axis [Policy status] observes, on which
[Blocked policy] and [Caught up] are verdicts. Derived from the operational
tables alone, so any replica can read it, including one whose worker is a
[Standby]. Independent of [Liveness].
_Avoid_: advancement, catch-up rate, freshness.

### Caught up

The transition of a [Policy] from a backlog to zero lag: the moment its cursor
reaches the end of its feed, announced once with how many events it took and how
long. It is not a terminal state: a Policy that remains at zero lag is simply
idle, and the next appended event returns it to working. Nothing is caught up
for a stretch of time — only at the instant it arrives.
_Avoid_: up to date, in sync, complete, finished.

### Scoped URN

A stream identifier carrying its owner as a suffix:
`urn:bank-account:acct-1@branch:london`. The embedded URN is the **scope**, the
part before the `@` the **base**. Composed with `at`, taken apart with
`extract_scope` (the scope), `unscoped` (the base) and `to_slug` (the base's NSS,
borrowed, for a caller who wants the identity rather than a typed URN). Scopes
nest — `urn:watchlist:main@user:0x78@wallet-type:evm` — and `extract_scope` peels
one level at a time. `at` refuses to scope an already-scoped URN, which is what
makes the left-most `@` the outermost scope
([ADR-0010](docs/adr/0010-nested-scoped-urns-parse-at-the-first-at-sign.md)). It
is an identity, not a [Query]: the event store treats the whole string as an
opaque stream id.
_Avoid_: qualified URN, namespaced URN, parent/child URN, compound key.

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

## Non-guarantees

What the [Policy runner] does not promise, stated here so no consumer builds on
it:

- **A panic inside a task the reaction spawns itself is not contained.** The
  runner's containment boundaries are the per-event dispatch and the worker; a
  task the reaction hands to a runtime or a blocking pool unwinds in its own
  task, outside both, and neither parks a [Dead letter] nor restarts anything.
- **An OOM kill is not containable in-process.** The kernel ends the process; no
  supervision layer can catch it. The only defences are bounding what a reaction
  loads and bounding how long a dispatch may run.

[Aggregate]: #aggregate
[Policy]: #policy
[Policy feed]: #policy-feed
[Dead letter]: #dead-letter
[Retry]: #retry
[Discard]: #discard
[Cursor move]: #cursor-move
[Rebuild]: #rebuild
[Policy status]: #policy-status
[Blocked policy]: #blocked-policy
[Policy runner]: #policy-runner
[Leader]: #leader
[Standby]: #standby
[Liveness]: #liveness
[Progress]: #progress
[Caught up]: #caught-up
[Query]: #query
[Scoped URN]: #scoped-urn
[Live projection]: #live-projection
[Inline projection]: #inline-projection
[Async projection]: #async-projection
[Projection]: #projection
