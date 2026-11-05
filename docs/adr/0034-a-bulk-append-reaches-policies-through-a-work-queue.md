# 34. A bulk append reaches Policies through a work queue

Date: 2026-11-03

## Status

Proposed. Decides [#288](https://github.com/funkode-io/replay/issues/288). Leaves
[ADR-0026](0026-a-policy-tracks-its-position-per-stream.md) and
[ADR-0030](0030-a-late-write-is-found-by-its-commit-stamp.md) as the design of the
**online lane**; this ADR adds a second lane beside it and changes nothing on the first.

## Context

Events record the decisions a domain makes. External facts — a supplier's rows, a file's
contents, another system's candles — belong in tables the domain reads when it decides,
and that principle is now stated in the README. Some decisions are nonetheless bulk by
design: a file import accepts thousands of records, each acceptance is a decision the
same Policies react to as a record accepted from the UI, and a consumer rightly wants
one Policy serving both.

Written through the online path, such a bulk is the worst case of everything the
[Policy feed] is engineered around. One long transaction holds the online commit-stamp
watermark back for every Policy and projection until it commits (ADR-0030, ADR-0032).
Split into short appends, it pushes chunk events, continuation commands and exactly-once
bookkeeping into the domain. Either way, 24 Policies each sweep, nominate, read and place
12k events that only a few of them want, and dxp-data-loom measured the cost of that
(#258). The online lane's machinery — per-stream places, the sweep, the late-write
lookup, the reconciliation and its rotation — exists to find a handful of late writes
cheaply among a large stream table. A bulk has no late writes to find: the writer knows
exactly which events it wrote and for whom.

## Decision

**A bulk append writes its events to the event store and, in the same transaction, one
row of work per event per Policy that consumes the bulk lane. A worker claims that work
in batches, reacts, dispatches and deletes it. The online lane never sees a bulk event.**

### The append

`Cqrs::execute_bulk` runs the aggregate's `handle_stream` and commits its events in
**segments** of a bounded size, each segment one transaction: the events, their
[Inline projection] rows, and the work rows. Peak memory is the segment. A failure rolls
back the segment it is in; earlier segments stand, which is the same outcome as the
domain issuing them as separate commands. Every event of a bulk execution is stamped
`lane = bulk` on `events`.

Fan-out happens at the append: the store reads the registered bulk Policies and their
tracked stream types (the record ADR-0031 keeps per Policy) and inserts a row into
`policy_bulk_work (policy, global_position)` for each Policy whose types include the
event's stream. A bulk Policy registered after a bulk append does not receive it, exactly
as `StartAt::Now` on the online lane: a Policy cannot be rebuilt from history ([Policy]).

### The worker

The [Policy runner]'s [Leader] runs one worker task per Policy registered on the bulk
lane. A worker opens a transaction, claims the lowest `batch` rows of its Policy with
`FOR UPDATE SKIP LOCKED`, reads those events, calls `react` on each, dispatches the
commands grouped by target stream so one aggregate load serves every command aimed at
it, deletes the claimed rows and commits. The claim transaction stays open while it
dispatches: a worker that dies releases its locks and the rows are simply there again
for the next claim. At-least-once, as the online lane; a command that fails is a
[Dead letter] for its triggering event, and the row is deleted with it, so one bad event
never wedges the queue.

One worker per Policy, in the leader, claiming in `global_position` order, gives a bulk
Policy the online lane's guarantee: a stream's events in that stream's order. Nothing
orders one stream against another. A consumer may ask for more workers on one Policy and
in return gives up per-stream order; the setting says so.

### The online lane is unchanged

A Policy declares its lane at registration: online, the default, or bulk, or both. The
online sweep and late-write lookup read only `lane = online` rows, served by partial
indexes, so a bulk segment costs them nothing however large it is. When the online feed
reads a stream that holds bulk events it treats them as a `stream_filter` does: the
[Stream place] advances past them and nothing fires (ADR-0013). Existing registrations,
settings, places, dead letters and the reconciliation behave as before.

## Considered options

- **Make the online lane survive a bulk.** A key window on the reconciliation (#298), a
  lane-aware watermark, bulk commits in sub-transactions the sweep could see. Each makes
  the online runner more complex to serve the one workload for which its central
  question — "which stream might be behind that nothing nominated?" — has a trivial
  answer the writer already knows.
- **Bulk rows outside the event store** (a staging table and commands). Right for external
  facts, and the README now says so; wrong for decisions, which an aggregate must rebuild
  from and an operator must find in the log.
- **A cursor over the work table** instead of claim-and-delete. Cheaper rows, but a
  serial id assigned inside a long segment is a [Late write] again, and the table grows
  with history. Claim-and-delete holds only pending work, so an idle queue costs one
  empty indexed read and needs no reconciliation.
- **A stale-claim timestamp** instead of holding the claim transaction open. Fewer
  connections held, but a reclaim timeout to tune and a second path to get wrong. The
  cost of one connection per bulk Policy for the length of a batch is acceptable.
- **A batch `react`** handed the whole claim, so a Policy can coalesce commands. Deferred:
  `react` per event keeps the rule pure and unchanged across lanes, and grouping the
  dispatch by target stream recovers most of the saving. It can be added as a default
  method without touching this decision.

## Consequences

- **A bulk append costs one work row per event per bulk Policy**, deleted when reacted.
  The table is sized by pending work, never by history.
- **Completion is the domain's.** When `execute_bulk` returns, every segment is committed
  and its work queued. The aggregate records that the bulk is done with an ordinary online
  event if its consumers need to know; a crash before that event leaves a bulk whose
  segments stand and whose rerun must be idempotent at the command, as any command must.
- **Delivery on the bulk lane is bounded by the queue depth**, not by a cadence: a worker
  runs until its queue is empty. An event that is both bulk and of a type no bulk Policy
  tracks is written and queued for nobody.
- **Migration on a populated database:** a `lane` column with default `online` and the two
  partial indexes, built `CONCURRENTLY`; the release notes carry the runbook.
- **ADR-0032's horizon is not held by a bulk**: each segment ends, and its marks are
  applied per segment, inside the append.
