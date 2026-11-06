# Context digest: replay

<!-- built from d9372e8 fingerprint c6c4f1e46443f5f9 on 2026-11-05; refresh: run the context-digest skill -->

A map, not a summary: one line per term, decision, section and tunable, each pointing at
the authoritative text. Read this, then only the sections your ticket touches.

## Read this first

- `replay` is a Rust event-sourcing and CQRS library: aggregates fold events, commands
  emit events as a stream, Postgres stores them, Policies react, projections read.
- Four crates, one domain: `es` (core traits, WASM-safe), `macros` (derives),
  `persistence` (stores, projections, Policies), `macros-tests` (not published).
- Rules: [`AGENTS.md`](../../AGENTS.md). Vocabulary: [`CONTEXT.md`](../../CONTEXT.md).
  Decisions: [`docs/adr/`](../adr/). Tutorial and reference: [`README.md`](../../README.md).
- "Projection" never stands alone: Live, Inline or Async. A Policy is not a projection.

## Rules (AGENTS.md)

- [PR Titles and Squash Merge](../../AGENTS.md#pr-titles-and-squash-merge)
- [Preserving Review History](../../AGENTS.md#preserving-review-history)
- [Bounded memory: no unbounded collections in library code](../../AGENTS.md#bounded-memory-no-unbounded-collections-in-library-code)
- [Events record facts, not state](../../AGENTS.md#events-record-facts-not-state)
- [Agent skills](../../AGENTS.md#agent-skills)

## Crate and module map

| Path | KB | What it holds | Tests |
| --- | --: | --- | --- |
| `es/src/aggregate.rs` | 32 | `Aggregate`, `Compactable`, `Compaction` | `es/tests/`, `macros-tests/tests/` |
| `es/src/stream.rs` | 35 | `EventStream`, `ScopedUrn`, `WithId` | `es/tests/urn_slug_allocations.rs` |
| `es/src/error.rs` | 33 | `Error`, `ErrorKind`, `ErrorStatus` (Busy lives here) | |
| `es/src/policy.rs` | 6 | `Policy`, `AggregatePolicy`, `TargetedCommand` | `es/tests/policy_declared_in_core.rs`, `single_target_policy.rs` |
| `es/src/dispatch.rs`, `observed_event.rs`, `metadata.rs`, `event.rs` | 22 | `Dispatch`, `ObservedEvent`, `Metadata`, `Event` | `es/tests/metadata_allocations.rs` |
| `macros/src/lib.rs`, `define_aggregate_macro.rs`, `merge_events_macro.rs` | 42 | `#[derive(Urn)]`, `define_aggregate!`, `query_events!` | `macros-tests/tests/*.rs`, `wasm.rs` |
| `persistence/src/cqrs.rs` | 32 | `Cqrs`: `execute`, `execute_bulk`, compaction, deletion entry points | `persistence/tests/it/` |
| `persistence/src/command_middleware.rs` | 53 | Command middleware, commit gate, Busy | `it/command_middleware.rs`, `command_middleware_allocations.rs` |
| `persistence/src/store.rs`, `filters.rs`, `stream_scope.rs`, `persisted_event.rs` | 42 | `EventStore`, `StreamFilter`, `StreamScope`, `PersistedEvent` | `it/bounded_queries.rs`, `read_path_allocations.rs` |
| `persistence/src/infrastructure/postgres.rs` | 91 | The Postgres store: append, hydrate, compact, locks | `it/integration_tests.rs`, `event_*.rs`, `stream_*.rs` |
| `persistence/src/infrastructure/in_memory_store.rs` | 74 | The in-memory store | `it/integration_tests.rs` |
| `persistence/src/infrastructure/projection_catch_up.rs`, `inline_projection.rs` | 32 | Inline projections, catch-up, rebuild | `it/inline_projection_*.rs`, `inline_projection_*_allocations.rs` |
| `persistence/src/infrastructure/stream_deletion.rs`, `lock_wait.rs`, `advisory_keys.rs` | 16 | Stream deletion, stream lock wait, every advisory-lock key | `it/stream_deletion.rs`, `stream_lock_wait.rs` |
| `persistence/src/policy_runner.rs` | 453 | The native Policy runner: feed, sweep, late-write lookup, reconciliation, dead letters, leadership | `it/policy_*.rs`, `policy_retry_allocations.rs` |
| `persistence/src/policy_runner/bulk_lane.rs` | 34 | The bulk lane worker and registrations | `it/policy_bulk_lane*.rs`, `policy_online_ignores_bulk.rs`, `bulk_segment_allocations.rs` |
| `persistence/src/policy_frontier.rs` | 65 | `PollPlan`: which streams a poll reads and where the rotation stops | `it/policy_per_stream_feed.rs`, `policy_backlog.rs` |
| `persistence/src/policy.rs`, `policy_status.rs`, `policy_liveness.rs`, `policy_narration.rs` | 103 | `PolicySettings`, `Lanes`, `StartAt`; status; liveness; narration | `it/policy_liveness.rs`, `policy_daemon.rs` |
| `persistence/tests/migrations/*.sql` | | Migrations 0001 to 0042, numbered independently of ADRs | `tests/common/migrations.rs` |

Test harness: `persistence/tests/common/` (Postgres via testcontainers, policy harness,
allocation report). Fixtures: `persistence/tests/fixtures/`.

## Glossary (CONTEXT.md)

Each line: term, the glossary's first sentence, its _Avoid_ words, the ADRs it cites.

- [Event](../../CONTEXT.md#event): a fact, named in the past tense after the act and the actor; never state the domain derives.
- [Projection](../../CONTEXT.md#projection): umbrella term for a derived read model; never unqualified; distinct from an Aggregate.
- [Live projection](../../CONTEXT.md#live-projection): computed on demand by folding events in memory; the Query mechanism.
- [Inline projection](../../CONTEXT.md#inline-projection): written inside the append transaction, same store instance; other transactions reach it by Catch-up. ADR-0001, 0032.
- [Async projection](../../CONTEXT.md#async-projection): updates an external system eventually. Planned, not implemented.
- [Policy](../../CONTEXT.md#policy): an event-driven reaction: an event triggers it, it issues a command; not a Projection, cannot be rebuilt. Avoid: reactor, saga, process manager, automation, trigger, reaction. ADR-0027, 0028.
- [Observed event](../../CONTEXT.md#observed-event): an event as a rule sees it: `data`, `stream_id`, `metadata`, `created`; no id. Avoid: event envelope, event view, policy event.
- [Global position](../../CONTEXT.md#global-position): the sequencing key of the log, a `BIGSERIAL` drawn under the stream lock; total order. Avoid: offset, sequence number, event time. ADR-0018 (ordering).
- [Stream place](../../CONTEXT.md#stream-place): `stream_seq`, contiguous and never reset; ends with a Stream deletion. Avoid: stream sequence, stream version, offset, per-stream position. ADR-0023, 0026, 0033.
- [Policy feed](../../CONTEXT.md#policy-feed): what one Policy reads on a poll, per stream it is behind on, in the stream's order; streams nominated by sweep, late-write lookup, reconciliation; the filter decides delivery, never how far it gets. Avoid: subscription, stream, queue, backlog. ADR-0026, 0030, 0031, 0013, 0004.
- [Lane](../../CONTEXT.md#lane): which of two delivery paths an event takes to a Policy: online (the feed) or bulk (a work queue); stamped at append, declared at registration, no order across lanes. Avoid: channel, priority, queue (online), batch mode. ADR-0034.
- [Bulk work](../../CONTEXT.md#bulk-work): one `policy_bulk_work` row per bulk event per consuming Policy; inserted by the append, deleted by the worker; holds only pending work. Avoid: outbox, job, task, pending event. ADR-0034.
- [Causation](../../CONTEXT.md#causation): the link from triggering event to the command and events a Policy raises; metadata, not offered to the Aggregate. Avoid: trigger, cause, origin.
- [Command middleware](../../CONTEXT.md#command-middleware): code registered once to run around every command: before it runs, before its append commits, after it finished; on nothing that is not a command. Avoid: interceptor, hook, filter, guard, plugin. ADR-0035.
- [Commit gate](../../CONTEXT.md#commit-gate): the point where the store asks the middleware whether to commit, after events and inline writes are in place. Avoid: pre-commit hook, commit check. ADR-0035.
- [Busy](../../CONTEXT.md#busy): the answer "not now": neither wrong nor failed; the runner holds it, never parks it. Avoid: unavailable, paused, deferred, refused. ADR-0036.
- [Dead letter](../../CONTEXT.md#dead-letter): a recorded failure of a reaction, never a silent skip; one row per parked command per reaction. Avoid: poison message, failed event, error queue. ADR-0016, 0024, 0029.
- [Retry](../../CONTEXT.md#retry): the controlling act of re-running a parked reaction as the Policy is defined now, against current state; unit is the reaction. Avoid: reprocess, requeue, redrive. ADR-0021 (retry).
- [Discard](../../CONTEXT.md#discard): the controlling act of retiring a Dead letter from the active set without re-executing it; archived, not destroyed. Avoid: dismiss, drop, ignore.
- [Cursor move](../../CONTEXT.md#cursor-move): the controlling act of repositioning a Policy by writing its `policy_stream_cursors` row; one Stream place in one stream. Avoid: reset, seek, rewind. ADR-0012, 0026.
- [Stream deletion](../../CONTEXT.md#stream-deletion): the controlling act of removing streams for good with everything the library keeps about them; deleted, not archived. Avoid: purge, tombstone, hard delete. ADR-0033.
- [Policy status](../../CONTEXT.md#policy-status): a point-in-time read-only snapshot of a Policy's progress and health; `lag`, `streams_behind`, `bulk_pending`; not Liveness. Avoid: state, policy state, health check.
- [Blocked policy](../../CONTEXT.md#blocked-policy): retired; no `Blocked` condition since the per-stream feed. Avoid: stuck, wedged, hung, stalled. ADR-0026.
- [Dispatch timeout](../../CONTEXT.md#dispatch-timeout): how long the runner awaits one dispatched command before abandoning it; 30s; retryable. Avoid: deadline, SLA, watchdog, timeout (unqualified). ADR-0018 (dispatch).
- [Stream lock wait](../../CONTEXT.md#stream-lock-wait): how long a transaction waits for a stream's row before the server abandons it; 30s, zero disables; `Unavailable`, retryable. Avoid: lock timeout, statement timeout, deadlock detection. ADR-0022, 0033.
- [Late write](../../CONTEXT.md#late-write): an append not committed when the sweep passed its position; found by its Commit stamp; late, never lost. Avoid: missed write, lost event, gap, hole. ADR-0030.
- [Burned position](../../CONTEXT.md#burned-position): a `global_position` taken by a transaction that aborted; nothing will ever hold it. Avoid: gap, hole, lost position, skipped position. ADR-0026.
- [Commit stamp](../../CONTEXT.md#commit-stamp): the writing transaction's id, `commit_txid`, on every event; `0` for events that predate it. Avoid: commit id, transaction number, xmin, sequence. ADR-0026, 0030.
- [Policy runner](../../CONTEXT.md#policy-runner): the background workers driving every Policy in a process, one per Policy, sharing listener and lock connections. Avoid: policy engine, subscriber, dispatcher, scheduler, worker pool. ADR-0008.
- [Leader](../../CONTEXT.md#leader): the single worker across replicas that drives a given Policy; per Policy, by advisory lock. Avoid: primary, master, owner, active node. ADR-0008.
- [Standby](../../CONTEXT.md#standby): a worker for a Policy another replica leads; healthy, idle, may become Leader. Avoid: secondary, passive replica, follower, spare.
- [Restart budget](../../CONTEXT.md#restart-budget): how many restarts one worker gets in a sliding window and the wait between them; per worker. Avoid: retry policy, circuit breaker, restart limit, backoff policy. ADR-0017.
- [Escalation](../../CONTEXT.md#escalation): what the runner does about a worker down for good: a consumer hook, called once, defaulting to process exit. Avoid: alert, failover, panic, giving up. ADR-0019.
- [Liveness](../../CONTEXT.md#liveness): whether a Policy's worker exists and is running; published from memory, never from tables. Avoid: uptime, availability, aliveness, worker status. ADR-0020.
- [Heartbeat](../../CONTEXT.md#heartbeat): the beat a replica writes on a fixed cadence for each Policy it leads; Standby writes none. Avoid: ping, keepalive, health check, liveness probe.
- [Progress](../../CONTEXT.md#progress): how far a Policy has advanced through its feed; derived from tables; independent of Liveness. Avoid: advancement, catch-up rate, freshness.
- [Caught up](../../CONTEXT.md#caught-up): the transition from a backlog to zero lag, announced once; not a terminal state. Avoid: up to date, in sync, complete, finished.
- [Narration](../../CONTEXT.md#narration): what a Policy writes to the log: an edge, never an event. Avoid: logging, tracing, audit trail, telemetry. ADR-0021 (narration).
- [Scoped URN](../../CONTEXT.md#scoped-urn): a stream id carrying its owner as a suffix after `@`; left-most `@` is the outermost scope. Avoid: qualified URN, namespaced URN, parent/child URN, compound key. ADR-0010.
- [Stream scope](../../CONTEXT.md#stream-scope): which streams a stream-level read covers, by the id's text: all, prefix or suffix; selects ids, never events. Avoid: stream filter, tenant filter.
- [Query](../../CONTEXT.md#query): the on-demand in-memory fold over filtered events; realises a Live projection.
- [Projection version](../../CONTEXT.md#projection-version): a number in projection code; older stored version triggers a rebuild.
- [Rebuild](../../CONTEXT.md#rebuild): reset a projection and replay the true log through it, archived originals included, never a snapshot. ADR-0004.
- [Catch-up](../../CONTEXT.md#catch-up): what an Inline projection does about transactions that did not apply it; per stream, by Commit stamp against the horizon. Avoid: reconcile, backfill, resync, checkpoint. ADR-0032.
- [Catch-up horizon](../../CONTEXT.md#catch-up-horizon): `projections.caught_up_below`, the transaction id below which everything is accounted for; the snapshot `xmin`. Avoid: checkpoint, high-water mark, offset.
- [WASM target](../../CONTEXT.md#wasm-target): `es` and `macros` are WASM-compatible with dual cfg-gated traits; `persistence` and Policies are native-only.
- [Non-guarantees](../../CONTEXT.md#non-guarantees): what the runner does not promise about panics, timeouts and OOM.

## Decisions (docs/adr)

Numbers 0018 and 0021 are each used twice; cite by file name.

- [ADR-0001](../adr/0001-inline-projection-architecture.md) Inline projections run atomically inside the append transaction and are store-specific. Accepted; amended by 0032.
- [ADR-0002](../adr/0002-projection-event-routing.md) Projection event routing by deserialize-or-skip over JSON. Accepted; amended by 0011.
- [ADR-0003](../adr/0003-policies-as-checkpointed-background-subscribers.md) Policies are a checkpointed background subscriber, distinct from projections. Accepted; narrowed by 0027.
- [ADR-0004](../adr/0004-compaction-synthetic-event-marker-for-policy-feed.md) Compaction marks synthetic events so a lagging Policy reads the true log. Accepted.
- [ADR-0005](../adr/0005-stream-first-event-emission.md) Commands emit events as a stream, the store ingests it under one transaction. Accepted.
- [ADR-0006](../adr/0006-policy-status-read-only-operational-snapshot.md) Policy status is a read-only operational snapshot, not a projection. Accepted; its fields replaced by 0026.
- [ADR-0007](../adr/0007-dead-letter-retry-reproduces-reaction-from-triggering-event.md) Dead-letter retry reproduces the reaction from the triggering event. Accepted; retry unit and row lifecycle superseded by 0021 (retry).
- [ADR-0008](../adr/0008-policy-runner-shared-connection-leadership.md) Policy runner shares one lock connection and one listener across all policies. Accepted.
- [ADR-0009](../adr/0009-compaction-skips-when-nothing-to-compact.md) Compaction skips no-op rewrites via a stream watermark and two layered guards. Accepted.
- [ADR-0010](../adr/0010-nested-scoped-urns-parse-at-the-first-at-sign.md) Nested scoped URNs: the left-most `@` is the outermost scope. Accepted.
- [ADR-0011](../adr/0011-inline-projections-flushed-in-bounded-chunks.md) Inline projections are flushed in bounded chunks, not once per append. Accepted; amends 0002; amended by 0018 (ordering); its rebuild caveat replaced by 0032.
- [ADR-0012](../adr/0012-policy-cursor-is-an-operator-writable-control-surface.md) The persisted policy cursor is an operator-writable control surface. Accepted; the surface moved to `policy_stream_cursors` with 0026.
- [ADR-0013](../adr/0013-policy-feed-contiguity-on-unfiltered-positions.md) A Policy's feed decides contiguity on unfiltered positions. Superseded by 0026; "the filter decides delivery only" survives.
- [ADR-0014](../adr/0014-policy-daemon-tests-assert-through-operator-visible-observations.md) Policy daemon tests assert through operator-visible observations. Accepted.
- [ADR-0015](../adr/0015-policy-crosses-a-position-no-transaction-can-fill.md) A Policy crosses a `global_position` no transaction can fill. Superseded by 0026; implementation deleted.
- [ADR-0016](../adr/0016-panicking-reaction-parked-as-a-permanent-failure.md) A panicking reaction is a permanent failure, contained at the event. Accepted; duplicate-row consequence superseded by 0024.
- [ADR-0017](../adr/0017-dead-policy-worker-restarted-on-a-budget.md) A dead worker is restarted on a budget; exhausting it is published, not silent. Accepted.
- [ADR-0018 (dispatch)](../adr/0018-a-hung-dispatch-is-cut-loose-by-a-timeout.md) A hung dispatch is cut loose by a timeout, and parked as a timeout. Accepted.
- [ADR-0018 (ordering)](../adr/0018-every-event-read-is-ordered-by-global-position.md) Every event read is ordered by `global_position`. Accepted, narrowed by 0026: the Policy feed is excluded; amends 0011.
- [ADR-0019](../adr/0019-escalation-is-a-consumer-hook-that-exits-by-default.md) A worker that is down for good escalates through a consumer hook, defaulting to process exit. Accepted.
- [ADR-0020](../adr/0020-liveness-is-published-from-memory-and-beaten-on-a-cadence.md) Liveness is published from memory and beaten out on a fixed cadence. Accepted.
- [ADR-0021 (narration)](../adr/0021-a-policy-narrates-its-transitions.md) A Policy narrates its transitions, and nothing else. Accepted.
- [ADR-0021 (retry)](../adr/0021-retry-settles-a-reaction-not-a-row.md) The unit of retry is the reaction, not the parked row. Accepted; supersedes 0007's retry unit.
- [ADR-0022](../adr/0022-a-stream-lock-wait-is-bounded-on-the-server.md) A stream lock wait is bounded on the server. Accepted; extended to deletion by 0033.
- [ADR-0023](../adr/0023-a-stream-is-numbered-twice.md) A stream is numbered twice: `version` replays, `stream_seq` delivers. Accepted; permanence narrowed to the stream's life by 0033.
- [ADR-0024](../adr/0024-a-parked-command-is-one-row.md) A parked command is one row, and the table enforces it. Accepted; supersedes 0016's consequence; its per-row settlement guard superseded by 0025.
- [ADR-0025](../adr/0025-a-retry-settles-the-group-it-locked.md) A retry settles the group it locked, a page at a time. Accepted; supersedes 0024's per-row guard.
- [ADR-0026](../adr/0026-a-policy-tracks-its-position-per-stream.md) A Policy tracks its position per stream. Accepted; supersedes 0013 and 0015; amended by 0030 and 0031.
- [ADR-0027](../adr/0027-the-policy-rule-is-core-vocabulary.md) The policy rule is core vocabulary; the machinery stays in persistence. Accepted; narrows 0003 and supersedes its dedup key.
- [ADR-0028](../adr/0028-a-single-target-policy-declares-its-target.md) A single-target policy declares its target, and a blanket impl keeps the runner erased. Accepted.
- [ADR-0029](../adr/0029-the-public-api-is-stream-first.md) The public API is stream-first. Accepted.
- [ADR-0030](../adr/0030-a-late-write-is-found-by-its-commit-stamp.md) A late write is found by its commit stamp. Accepted; amends 0026: reconciliation becomes a safety net.
- [ADR-0031](../adr/0031-a-policy-filtered-by-stream-type-tracks-only-those-types.md) A Policy filtered by stream type tracks only those types. Accepted; amends 0026.
- [ADR-0032](../adr/0032-an-inline-projection-catches-up-on-transactions-that-did-not-apply-it.md) An inline projection catches up on transactions that did not apply it. Accepted; amends 0001; replaces 0011's rebuild caveat; amended by #306.
- [ADR-0033](../adr/0033-a-deleted-stream-takes-what-replay-keeps-about-it.md) A deleted stream takes what Replay keeps about it. Accepted; narrows 0023 and 0026, extends 0022.
- [ADR-0034](../adr/0034-a-bulk-append-reaches-policies-through-a-work-queue.md) A bulk append reaches Policies through a work queue. Accepted 2026-11-05; decides #288.
- [ADR-0035](../adr/0035-a-command-passes-through-middleware-before-it-runs-and-before-it-commits.md) A command passes through middleware before it runs and before it commits. Accepted; decides #308.
- [ADR-0036](../adr/0036-a-busy-command-is-held-not-dead-lettered.md) A busy command is held, not dead-lettered. Accepted; the runner half of #308.

## README map

- [Requirements](../../README.md#requirements): PostgreSQL 15 or later.
- [Example](../../README.md#example): the banking tutorial. [Defining the aggregates](../../README.md#defining-the-aggregates) for the trait shape; [Events record facts, not state](../../README.md#events-record-facts-not-state) before modelling any event (cited by `AGENTS.md`); [Compaction](../../README.md#compaction); [A cross-aggregate read model](../../README.md#a-cross-aggregate-read-model); [Driving it with CQRS](../../README.md#driving-it-with-cqrs).
- [Using Macros](../../README.md#using-macros): `#[derive(Urn)]`, URN helper methods, services for external dependencies, URN namespace configuration, generic type parameters, URNs in collections, [Composing Scoped URNs](../../README.md#composing-scoped-urns), [Prelude](../../README.md#prelude).
- [WASM Support](../../README.md#wasm-support): when touching `es` or `macros` traits. [Aggregate Trait](../../README.md#aggregate-trait), [Async Services in WASM](../../README.md#async-services-in-wasm), [Testing WASM](../../README.md#testing-wasm).
- [Filtering Events with `StreamFilter`](../../README.md#filtering-events-with-streamfilter): [Available filters](../../README.md#available-filters), [Combining filters](../../README.md#combining-filters), per-filter examples, use in a `Query` and directly with the store.
- [Inline Projections (Postgres)](../../README.md#inline-projections-postgres): [When to use it](../../README.md#when-to-use-it), [Migrations](../../README.md#migrations) (how consumers apply `persistence/tests/migrations`), [Lowest-ceremony path: register a Postgres event handler](../../README.md#lowest-ceremony-path-register-a-postgres-event-handler), [Full control: implement `InlineProjection`](../../README.md#full-control-implement-inlineprojection), [Runtime behavior](../../README.md#runtime-behavior), [Batching: a single append may arrive as several calls](../../README.md#batching-a-single-append-may-arrive-as-several-calls), [Rolling deploys: catching up on appends that did not apply it](../../README.md#rolling-deploys-catching-up-on-appends-that-did-not-apply-it).
- [Querying Events from Multiple Aggregates](../../README.md#querying-events-from-multiple-aggregates): `query_events!`. [What the Macro Generates](../../README.md#what-the-macro-generates), [Use Cases](../../README.md#use-cases), [Example: Building a User Activity Log](../../README.md#example-building-a-user-activity-log).
- [Stream Compaction](../../README.md#stream-compaction): [The `Compactable` trait](../../README.md#the-compactable-trait), the bank-account example, [Running compaction via `Cqrs`](../../README.md#running-compaction-via-cqrs), [What compaction does to an event's numbers](../../README.md#what-compaction-does-to-an-events-numbers), [Skipping unchanged streams](../../README.md#skipping-unchanged-streams-needs_compaction), [Compacting a whole scope](../../README.md#compacting-a-whole-scope-streams_needing_compaction).
- [Deleting streams](../../README.md#deleting-streams): when a stream must be gone, not shortened.
- [Policies](../../README.md#policies): [Implementing `Policy`](../../README.md#implementing-policy), [A policy that targets one aggregate](../../README.md#a-policy-that-targets-one-aggregate), [Closure shortcut](../../README.md#closure-shortcut), [Building and starting the runner](../../README.md#building-and-starting-the-runner), [How a policy tracks where it is](../../README.md#how-a-policy-tracks-where-it-is) (feed, sweep, late writes, reconciliation bounds), [Moving a policy on a running system](../../README.md#moving-a-policy-on-a-running-system), [The bulk lane](../../README.md#the-bulk-lane), [Advisory-lock leader election](../../README.md#advisory-lock-leader-election), [LISTEN/NOTIFY latency optimisation](../../README.md#listennotify-latency-optimisation), [Loop prevention](../../README.md#loop-prevention), [Batching](../../README.md#batching), [Dispatch timeout](../../README.md#dispatch-timeout), [Stream lock wait](../../README.md#stream-lock-wait), [Bulk segment size](../../README.md#bulk-segment-size), [Failure handling](../../README.md#failure-handling) (the resilience table, dead letters, retry, discard, Busy), [Reading each worker's liveness](../../README.md#reading-each-workers-liveness), [What a policy logs](../../README.md#what-a-policy-logs), [Monitoring policy status](../../README.md#monitoring-policy-status), [Upgrading a running Policy to per-stream cursors](../../README.md#upgrading-a-running-policy-to-per-stream-cursors), [A write that is slow, stuck or rolled back](../../README.md#a-write-that-is-slow-stuck-or-rolled-back).

## Tunables

| Env var | Builder | Default | README |
| --- | --- | --- | --- |
| `REPLAY_PROJECTION_FLUSH_SIZE` | `builder(pool).projection_flush_size(n)` | 500 | [Batching: a single append…](../../README.md#batching-a-single-append-may-arrive-as-several-calls) |
| `REPLAY_PROJECTION_CATCH_UP_SECS` | `builder(pool).projection_catch_up_interval(d)` | 60 s; 0 disables the task | [Rolling deploys](../../README.md#rolling-deploys-catching-up-on-appends-that-did-not-apply-it) |
| `REPLAY_POLICY_RECONCILE_SECS` | | 300 s; cadence, never whether | [How a policy tracks where it is](../../README.md#how-a-policy-tracks-where-it-is) |
| | `PolicyRunnerBuilder::reconcile_window(NonZeroU32)` | 10,000 stream rows per statement | same |
| `REPLAY_MAX_CAUSATION_DEPTH` | `PolicySettings::new().with_max_causation_depth(n)` | 10 | [Loop prevention](../../README.md#loop-prevention) |
| `REPLAY_READ_BATCH_SIZE` | `with_read_batch_size(u32)` | 100 | [Batching](../../README.md#batching) |
| `REPLAY_CHECKPOINT_BATCH_SIZE` | `with_checkpoint_batch_size(u32)` | 100 | [Batching](../../README.md#batching) |
| `REPLAY_DISPATCH_TIMEOUT_MS` | `with_dispatch_timeout(Duration)` | 30 s; 0 reads as unset | [Dispatch timeout](../../README.md#dispatch-timeout) |
| `REPLAY_STREAM_LOCK_WAIT_MS` | `builder(pool).stream_lock_wait(d)` | 30 s; 0 disables the bound | [Stream lock wait](../../README.md#stream-lock-wait) |
| `REPLAY_BULK_SEGMENT_SIZE` | `builder(pool).bulk_segment_size(n)` | 1000 events per segment | [Bulk segment size](../../README.md#bulk-segment-size) |
| `REPLAY_NOTIFY_CHANNEL` | re-exported constant, not a knob | `replay_events` | [LISTEN/NOTIFY](../../README.md#listennotify-latency-optimisation) |
| `REPLAY_BLOCKED_WARN_AFTER_SECS` | retired with the `Blocked` status | | [A write that is slow…](../../README.md#a-write-that-is-slow-stuck-or-rolled-back) |

## Workflow

- Lint: `cargo fmt --all --check` and `cargo clippy --workspace --all-targets -- -D warnings` (pre-commit hook).
- Test: `cargo test --workspace`, needs Docker (testcontainers starts Postgres); pre-push hook, `SKIP_TESTS=1` skips. `make test` is `cargo watch`.
- Hooks: `make install-hooks` sets `core.hooksPath` to `.githooks`. CI (`.github/workflows/ci.yml`) is `workflow_dispatch` only; the hooks are the gate.
- WASM: `make wasm-test` (`wasm-pack test --headless --chrome macros-tests`) when `es` or `macros` change; CI builds `es-replay` for `wasm32-unknown-unknown`.
- Allocation budgets: `persistence/tests/*_allocations.rs` pin paths in bytes; a release never ships with them failing.
- Release: [`docs/agents/release.md`](release.md): version policy, the four `Cargo.toml`s, publish order `es-replay`, `es-replay-macros`, `es-replay-persistence`; notes in `docs/releases/<version>.md`.
- Issues: [`docs/agents/issue-tracker.md`](issue-tracker.md). Labels: [`docs/agents/triage-labels.md`](triage-labels.md). Domain docs: [`docs/agents/domain.md`](domain.md). Review checklists: [`docs/agents/review-standards.md`](review-standards.md). Copilot: [`docs/agents/copilot-review.md`](copilot-review.md).
- Digest check: `python3 scripts/digest_check.py check . docs/agents/context-digest.md` (pre-commit hook and CI). Skills: installed from `skills-lock.json` (`make install-skills`), never vendored.

## Traps

- `origin` (`carlos-verdes/replay`) has issues disabled: every `gh` call needs `--repo funkode-io/replay` (`docs/agents/issue-tracker.md`).
- `CLAUDE.md` is a symlink to `AGENTS.md`: edit `AGENTS.md` only.
- ADR numbers 0018 and 0021 are each taken twice; link by file name, not number. The next ADR number is 0037.
- Migrations are numbered independently of ADRs (`persistence/tests/migrations/0034_…` is not ADR-0034) and live under `tests/`, where consumers copy them from (README, Migrations).
- A zero `REPLAY_STREAM_LOCK_WAIT_MS` disables the bound; a zero `REPLAY_DISPATCH_TIMEOUT_MS` reads as unset and keeps the 30 s default (README, Stream lock wait).
- `REPLAY_BLOCKED_WARN_AFTER_SECS` and the `Blocked` status are gone; README names them only to say so.
- A Policy tracks only its stream types when its whole filter is `ForStreamTypes`; any other filter tracks every stream (ADR-0031).
- An online Policy never sees a bulk-lane event; `Lanes::Online` is the default at registration (CONTEXT, Lane).
- The `Makefile` targets `gitRebase` and `gitAmmend` force-push: forbidden on a branch with an open PR (`AGENTS.md`, Preserving Review History).
- `Cargo.lock` is gitignored in this library workspace; do not commit it (`docs/agents/release.md`).
- `es-replay-macros-tests` is never published; the `0.1.1` on crates.io is a historical accident.
- `[profile.test]` keeps `debug = "line-tables-only"` on purpose (`Cargo.toml`, #247).
