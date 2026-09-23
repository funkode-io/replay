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

### Global position

The sequencing key of the event log: a `BIGSERIAL` on `events`, drawn inside the
same `streams … FOR UPDATE` section that hands out a stream `version`, so within a
stream it rises with `version` and across streams it is a total order. Every event
read sorts on it and nothing else
([ADR-0018](docs/adr/0018-every-event-read-is-ordered-by-global-position.md)).
`created` is a wall-clock audit stamp a time-travel read may *filter* on; it orders
nothing. A position may be missing (a [Burned position]) but never repeated — a
unique index enforces that (migration 0015).
_Avoid_: offset, sequence number, event time.

### Stream place

The place an event holds in its own stream: `stream_seq`, assigned by `write_event` from
a counter on the `streams` row and never reset
([0027](persistence/tests/migrations/0027_event_stream_seq.sql)). Contiguous, unique per
stream, and permanent — unlike a stream `version`, which compaction restarts at 1 so
hydration reads `1..N`, making `(stream_id, version)` name two different events over time
([ADR-0023](docs/adr/0023-a-stream-is-numbered-twice.md)). Permanence is the library's to
keep, as it is for [Global position], and unlike a [Cursor move]
([ADR-0012](docs/adr/0012-policy-cursor-is-an-operator-writable-control-surface.md)). It is
what a [Policy feed] is read and checkpointed over
([ADR-0026](docs/adr/0026-a-policy-tracks-its-position-per-stream.md)).
_Avoid_: stream sequence, stream version, offset, per-stream position.

### Policy feed

What one [Policy] reads on a poll: for each stream it is behind on, that stream's events
past its [Stream place] in that stream, in the stream's own order, up to its read batch
size. Nothing orders one stream against another, and nothing waits — a stream's places
arrive in order, so the feed has no holes to reason about
([ADR-0026](docs/adr/0026-a-policy-tracks-its-position-per-stream.md)).

Which streams to read is found two ways: a sweep of the log by [Global position] every
poll, which is fast and may pass a write that had not committed yet, and a reconciliation
on a cadence, which compares every stream's head with the Policy's place and catches what
the sweep passed. The sweep nominates streams; it never decides what is owed. Both take a
batch and resume where the last one stopped, so "bounded" costs a rotation rather than a
stream nobody looks at.

Which streams a poll reads, in what order, how far its event budget gets and where that
leaves the rotation is **one decision, taken over what the two nomination queries
returned and before a single event is read** (`PollPlan`): the poll asks it what to read
next and tells it what each read yielded. It spans three sources, a candidate cap, a
rotation cursor, a shared budget and a carried queue that truncates, so it is decided
over its states by simulation rather than corner by corner (funkode-io/replay#243).

A `stream_filter` decides what a Policy *reacts to*, never how far it *gets*: an excluded
event advances the place and fires nothing, like a compaction snapshot
([ADR-0013](docs/adr/0013-policy-feed-contiguity-on-unfiltered-positions.md),
[ADR-0004](docs/adr/0004-compaction-synthetic-event-marker-for-policy-feed.md)).
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
triggering event so a single bad event never wedges the Policy. A reaction that
**panics** is one of these: the panic is contained at the event it was reacting
to, parked on first occurrence without a retry, and recorded as kind `Panic` so
an operator can tell a defect in the reaction from a command the domain refused
([ADR-0016](docs/adr/0016-panicking-reaction-parked-as-a-permanent-failure.md)).
A reaction the runner **abandoned on its [Dispatch timeout]** is another: retried
like any transient failure and, once the retries are exhausted, recorded as kind
`Timeout`.
A delivery parks what its settling attempt failed on: a command that fails
permanently is recorded once, however many attempts a retryable sibling forces.
A row is one parked **command per reaction**, not per delivery: an event
delivered again — after a crash between the park and the cursor checkpoint, or
after a [Cursor move] — refreshes the row its command already has with the new
error and counts the delivery, rather than parking a second copy
([ADR-0024](docs/adr/0024-a-parked-command-is-one-row.md)). It is not a
[Retry] and does not touch what one has tried.
A row names the dispatch it is about — the [Aggregate] type, the URN of the
instance the command was addressed to, the command's **type** (its variant
and payload are not recorded, as `Aggregate::Command` carries no `Debug` or
`Serialize` bound), and its place in the reaction, which is what keeps two
commands of one type to one instance apart — so "which customer is stuck" is
answerable from the table.
A reaction that panicked before building a dispatch has nothing to name, and its
identity columns are null.
A row also records what has been tried on it: how many times a [Retry] has
settled it, and when the last one did.
Dead letters are queryable so an operator can later inspect them and either
[Retry] or [Discard] them.
_Avoid_: poison message, failed event, error queue.

### Retry

The _controlling_ act of re-running a parked [Policy] reaction: the triggering
event recorded by a [Dead letter] is re-evaluated through the Policy **as it is
defined now** and the commands it raises are re-executed, judged against
**current** [Aggregate] state. A retry reaches back to a single parked event out
of band and never moves the Policy's cursor. Because it re-runs against today's
state, a reaction that is now stale or no longer valid is legitimately declined
rather than replayed blindly — guarding order-sensitive side effects is the
target Aggregate's responsibility, not the runner's.
Its unit is the **reaction**, not the row: the reaction — identified by the
Policy and the event it reacted to — is replayed **once** however many of its
commands are parked, the replay carries on past a failure as the forward drain
does, and each row is settled by its own command's outcome, resolved rows
archived and still-failing ones left retryable with their own error
([ADR-0021](docs/adr/0021-retry-settles-a-reaction-not-a-row.md)). A retry
summary therefore counts reactions where `dead_letter_count` counts parked
commands.
Distinct from a [Rebuild], which resets and replays a whole [Projection].
_Avoid_: reprocess, requeue, redrive.

### Discard

The _controlling_ act of an operator judging a [Dead letter]'s reaction
permanently unrecoverable and retiring the record from the active set
**without** re-executing it. The record is archived rather than destroyed, so
the failure history is never lost. The only way a row leaves the active set for
good on purpose: a failed [Retry] always leaves it retryable, because what makes
another attempt worth making is a change outside the library.
The counterpart to [Retry]; together they are
the controlling actions over a Policy's failures that [Policy status] only
observes.
_Avoid_: dismiss, drop, ignore.

### Cursor move

The _controlling_ act of an operator repositioning a [Policy] by writing its
`policy_stream_cursors` row directly while the system runs. The third controlling action
alongside [Retry] and [Discard], and the coarsest: it moves the Policy itself rather than
one parked reaction, skipping events when it moves forward and re-delivering them when it
moves backward. The instruction is a [Stream place] in one stream, so a redelivery can be
forced for that stream alone. The running leader adopts it on the next poll, because it
reads its places fresh every poll rather than holding them between polls
([ADR-0012](docs/adr/0012-policy-cursor-is-an-operator-writable-control-surface.md),
[ADR-0026](docs/adr/0026-a-policy-tracks-its-position-per-stream.md)).
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
[Liveness]. Its `lag` is an exact count of events not yet passed, summed over the streams
the Policy is behind on, and `streams_behind` says how widely that is spread — one stream
a million events behind and a million streams one event behind are the same lag and very
different problems.
_Avoid_: state, policy state, health check.

### Blocked policy

Retired. A Policy used to be able to sit in front of a `global_position` that did not
exist and read nothing for ever (funkode-io/replay#164). It reads each stream over that
stream's own sequence now, which has no holes, so there is no number it can be parked in
front of and no `Blocked` condition to report
([ADR-0026](docs/adr/0026-a-policy-tracks-its-position-per-stream.md)). A Policy that is
not moving is lagging, [Degraded](#policy-status), or not alive — three conditions with
three different readings.
_Avoid_: stuck, wedged, hung, stalled.

### Dispatch timeout

How long the [Policy runner] awaits one command a [Policy] dispatched before it
abandons it — per Policy, defaulting to 30s
([ADR-0018](docs/adr/0018-a-hung-dispatch-is-cut-loose-by-a-timeout.md)). It cuts
loose a reaction that has *stopped*; a merely slow one raises the limit rather
than being parked by it. Exceeding it is retryable, so a hang reaches the same
[Dead letter] a dependency outage does. A Policy held inside one reaction has a healthy
feed in front of it and is not lagging for any reason the feed can see, which is why
[Liveness] and not [Policy status] is what reports it.
_Avoid_: deadline, SLA, watchdog, timeout (unqualified).

### Stream lock wait

How long a transaction of this library waits for a stream's row before the server
abandons it — one value for both transactions that take that row, the append and
`compact`, defaulting to 30s and disabled by zero
([ADR-0022](docs/adr/0022-a-stream-lock-wait-is-bounded-on-the-server.md)). It
bounds *this* library's waiting, not the holder: a wait that runs out is
`Unavailable` and retryable, and a spike of them names a long holder rather than a
broken append. Distinct from the [Dispatch timeout], which is a client-side bound
that Postgres never hears about; this one is the server's, which is what lets an
abandoned append hand its connection back.
_Avoid_: lock timeout (unqualified), statement timeout, deadlock detection.

### Burned position
A `global_position` taken from the sequence by a transaction that then aborted.
`nextval` is not transactional, so the value is never returned to the sequence and no
event can ever carry it. It is a number nothing will ever hold, and since
[ADR-0026](docs/adr/0026-a-policy-tracks-its-position-per-stream.md) nothing reads it as
anything else: a [Policy feed] is ordered per stream, and a stream's places are handed
back by a write that fails rather than burned. A [Stream place] has no equivalent.
_Avoid_: gap, hole (as a name for the permanent kind), lost position, skipped
position.

### Commit stamp

The id of the transaction that wrote an event, carried on the event as `commit_txid`
and written by every insert path
([0018](persistence/tests/migrations/0018_event_commit_txid.sql)). Its purpose is an
ordering the [Policy feed] can trust without reasoning about holes: a transaction id
can be compared against a snapshot of transactions that have ended, whereas a
`global_position` can be a [Burned position]. Events that predate the stamp carry the
sentinel `0`, which orders before every real id. Nothing in the library reads it: the
ordering it was carried for was the [Policy feed]'s, and that feed is ordered per stream
now ([ADR-0026](docs/adr/0026-a-policy-tracks-its-position-per-stream.md)). It stays on
the event as an operator's diagnostic — which write wrote this row, and what else that
write wrote.
_Avoid_: commit id, transaction number, xmin, sequence.

### Policy runner

The set of background workers that drive every [Policy] in a process — one
worker per Policy, each owning that Policy's durable cursor, sharing the
process's listener and lock-manager connections
([ADR-0008](docs/adr/0008-policy-runner-shared-connection-leadership.md)). The
Policy is what reacts; the runner is what makes it run, restarts it within its
[Restart budget], [Escalates](#escalation) it when that runs out, and reports on it.
_Avoid_: policy engine, subscriber, dispatcher, scheduler, worker pool.

### Leader

The single worker, across every replica, that currently drives a given [Policy].
Leadership is decided per Policy, not per process — the replica whose shared
lock-manager session holds that Policy's advisory lock leads it
([ADR-0008](docs/adr/0008-policy-runner-shared-connection-leadership.md)) — so
one replica is routinely Leader for some Policies and [Standby] for others.
_Avoid_: primary, master, owner, active node.

### Standby

A worker that exists for a [Policy] another replica leads, whose replica holds no
lock for it and which therefore processes nothing. A Standby is healthy and
deliberately idle — it is not a stopped worker and not a lagging one — and
may become [Leader]: when the lock for that Policy is released every Standby
competes for it and one of them wins it.
_Avoid_: secondary, passive replica, follower, spare.

### Restart budget

How many times the [Policy runner] may restart one worker within a sliding
window, and how long it waits between attempts
([ADR-0017](docs/adr/0017-dead-policy-worker-restarted-on-a-budget.md)). It is the
answer to a worker that *dies*, never to a reaction that *fails* (that is a
[Dead letter]). Per worker: spending one leaves every other Policy's worker,
cursor and the process's leadership untouched. A worker that spends its budget is
stopped and [Escalated](#escalation) rather than restarted again.
_Avoid_: retry policy, circuit breaker, restart limit, backoff policy.

### Escalation

What the [Policy runner] does about a worker that is down for good — it spent its
[Restart budget], or the lock manager that elects it stopped. A consumer-supplied
hook is called once, naming the Policy and the reason, and defaults to exiting the
process
([ADR-0019](docs/adr/0019-escalation-is-a-consumer-hook-that-exits-by-default.md)).
Exiting is what releases the Policy's advisory lock, so a hook that returns leaves
the Policy stopped in every replica.
_Avoid_: alert, failover, panic, giving up.

### Liveness

The axis reporting whether a [Policy]'s worker exists and is running — leading,
standing by, restarting, stopped or unknown. Only the process running the
[Policy runner] knows it, so it is published from memory and never derived from
the operational tables
([ADR-0020](docs/adr/0020-liveness-is-published-from-memory-and-beaten-on-a-cadence.md)).
Its durable form is the [Heartbeat], which carries it to whoever is not in that
process. It implies nothing about [Progress], and nothing about it can be
inferred from Progress.
_Avoid_: uptime, availability, aliveness, worker status.

### Heartbeat

The beat a replica writes on a **fixed cadence** for each [Policy] it leads,
carrying that worker's [Liveness], when it last finished a poll, and which
replica wrote it. Fixed is the whole property: a signal that slowed down when a
worker got busy could not tell busy from gone. It is written by a task of its own
rather than by the worker, because a worker inside a reaction that never returns
cannot write anything — which is the case the beat is for. A stale beat means no
live [Leader]; a fresh beat carrying an old poll means a worker that is alive and
not finishing polls. A [Standby] writes none: the row belongs to whoever holds the
lock.
_Avoid_: ping, keepalive, health check, liveness probe.

### Progress

The axis reporting how far a [Policy] has advanced through its feed and whether
its reactions are completing — the axis [Policy status] observes, on which
[Caught up] is a transition. Derived from the operational
tables alone, so any replica can read it, including one whose worker is a
[Standby]. Independent of [Liveness] in both directions: a [Standby] is live and
advances nothing, and a [Leader] whose reaction is wedged is live and advances
nothing either.
_Avoid_: advancement, catch-up rate, freshness.

### Caught up

The transition of a [Policy] from a backlog to zero lag: the moment its cursor
reaches the end of its feed, announced once with how many events it took and how
long. It is not a terminal state: a Policy that remains at zero lag is simply
idle, and the next appended event returns it to working. Nothing is caught up
for a stretch of time — only at the instant it arrives.
_Avoid_: up to date, in sync, complete, finished.

### Narration

What a [Policy] writes to the log: an edge, never an event. A burst is bracketed
by a record when work appears and the [Caught up] record that ends it, with a
bounded progress record in between while a backlog is still draining
([ADR-0021](docs/adr/0021-a-policy-narrates-its-transitions.md)). Output is
proportional to how often a Policy changes state, not to how much work it does,
which is what keeps an idle Policy's silence readable as a signal. Per-dispatch
detail exists at `debug` and is off by default.
_Avoid_: logging, tracing, audit trail, telemetry.

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
  runner's containment boundaries are the reaction to one event and the worker; a
  task the reaction hands to a runtime or a blocking pool unwinds in its own
  task, outside both, and neither parks a [Dead letter] nor restarts anything.
- **No panic is contained under `panic = "abort"`.** Containment is unwinding: a
  binary that aborts on panic ends the process before the runner's catch can park
  a [Dead letter].
- **A [Dispatch timeout] cannot interrupt work the reaction moved onto another
  task.** It bounds the future the runner awaits; a `tokio::spawn`, a blocking
  pool or a request already in flight keeps running, unobserved, after the runner
  has stopped waiting for it.
- **A [Dispatch timeout] cannot cut loose a reaction that never yields.**
  Cancellation happens at a suspension point, so a command that blocks the thread
  — `std::thread::sleep`, a synchronous client, a tight CPU loop — runs past its
  limit and holds the worker until it returns on its own. Bounding that needs a
  thread or process boundary the runner does not impose; a reaction that must
  block belongs on `spawn_blocking`, where the timeout at least stops the runner
  waiting on it.
- **A [Dispatch timeout] does not cancel work already running in Postgres.** It
  ends this process's wait and sends the server nothing, so a dispatch abandoned
  inside a statement holds its pool connection until that statement finishes. The
  case that reaches this is an append blocked on another transaction's stream
  lock, and the server — not the timeout — is what ends it, after the
  [Stream lock wait] ([ADR-0022](docs/adr/0022-a-stream-lock-wait-is-bounded-on-the-server.md)).
  A consumer who sets that wait to zero is back to holding the connection until
  the blocker clears. A reaction hanging in its own code holds no connection,
  because the command handler runs before the append opens its transaction.
- **An OOM kill is not containable in-process.** The kernel ends the process; no
  supervision layer can catch it. The only defences are bounding what a reaction
  loads and bounding how long it may run.

[Aggregate]: #aggregate
[Policy]: #policy
[Policy feed]: #policy-feed
[Dead letter]: #dead-letter
[Dispatch timeout]: #dispatch-timeout
[Stream lock wait]: #stream-lock-wait
[Retry]: #retry
[Discard]: #discard
[Cursor move]: #cursor-move
[Rebuild]: #rebuild
[Policy status]: #policy-status
[Blocked policy]: #blocked-policy
[Burned position]: #burned-position
[Commit stamp]: #commit-stamp
[Global position]: #global-position
[Policy runner]: #policy-runner
[Leader]: #leader
[Standby]: #standby
[Restart budget]: #restart-budget
[Escalation]: #escalation
[Liveness]: #liveness
[Heartbeat]: #heartbeat
[Progress]: #progress
[Caught up]: #caught-up
[Narration]: #narration
[Query]: #query
[Scoped URN]: #scoped-urn
[Live projection]: #live-projection
[Inline projection]: #inline-projection
[Async projection]: #async-projection
[Projection]: #projection
