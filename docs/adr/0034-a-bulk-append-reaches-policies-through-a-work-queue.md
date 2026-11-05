# 34. A bulk append reaches Policies through a work queue

Date: 2026-11-03

## Status

Accepted on 2026-11-05. Decides [#288](https://github.com/funkode-io/replay/issues/288).
Implemented by:

- [#326](https://github.com/funkode-io/replay/pull/326): every event carries a lane, and
  the online feed reads only the online one;
- [#327](https://github.com/funkode-io/replay/pull/327): `Cqrs::execute_bulk`, the
  registration of a bulk Policy and the work queue its worker drains;
- [#330](https://github.com/funkode-io/replay/pull/330): a bulk append commits in bounded
  segments;
- [#329](https://github.com/funkode-io/replay/pull/329): the bulk lane survives a failed
  reaction and a dead worker, and Policy status shows its queue.

Leaves [ADR-0026](0026-a-policy-tracks-its-position-per-stream.md) and
[ADR-0030](0030-a-late-write-is-found-by-its-commit-stamp.md) as the design of the
**online lane**. This ADR adds a second lane beside it and changes nothing on the first.

## Context

Events record the decisions a domain makes. External facts, such as a supplier's rows, a
file's contents or another system's candles, belong in tables the domain reads when it
decides. The README states that principle. Some decisions are nonetheless bulk by design.
A file import accepts thousands of records. Each acceptance is a decision that the same
Policies react to as they would to a record accepted from the UI, and a consumer rightly
wants one Policy serving both.

`Aggregate::handle_stream` already lets a handler produce such a bulk without building a
`Vec`, and `Cqrs::execute` already appends it with bounded memory. Per-stream places
(ADR-0026) and the commit-stamp lookup (ADR-0030) already keep a long transaction from
blocking unrelated streams. What remains is that **a committed bulk backlog competes
with online work inside each Policy's feed.** 24 Policies each sweep, nominate, read and
place 12k events that only a few of them want, and dxp-data-loom measured what that
costs (#258). The online lane's machinery exists to find a handful of late writes cheaply
among a large stream table: per-stream places, the sweep, the late-write lookup, and the
reconciliation and its rotation. A bulk has no late writes to find, because the writer
knows exactly which events it wrote and for whom.

## Decision

**A bulk append writes its events to the event store and, in the same transaction, one
row of work per event per Policy that consumes the bulk lane. A worker claims that work
in batches, reacts, dispatches and deletes it. The online lane never delivers a bulk
event.**

### The append

`Cqrs::execute_bulk` runs the command as `execute` does: the middleware chain, the load
and the aggregate's `handle_stream`. It commits the events in **segments** of
`bulk_segment_size` events (default 1000, `REPLAY_BULK_SEGMENT_SIZE`). Each segment is
one transaction that holds the events, their [Inline projection] rows, their applied
mark and their work rows. Every event of a bulk execution is stamped `lane = bulk` on
`events`.

- **Memory and holds are bounded by the segment.** Peak memory is one segment, and a
  bulk holds the stream row and the commit stamp that online consumers and the
  projection catch-up wait behind (ADR-0030, ADR-0032) for only as long as one segment
  takes to write.
- **A failure rolls back the segment it is in.** The segments before it stand with their
  work queued, which is the same outcome as the domain issuing them as separate
  commands. The error is returned to the caller. Every middleware's `before_commit` is
  asked once per segment.
- **The expected version is checked against the stream's head before the first segment
  only.** Another writer may append to the stream between segments.

The fan-out happens at the append. One statement joins the transaction's bulk events to
`policy_bulk_registrations` and inserts a row into
`policy_bulk_work (policy, global_position)` for each registered Policy whose tracked
stream types include the event's stream. A bulk Policy registered after a segment
commits does not receive that segment, exactly as with `StartAt::Now` on the online
lane, because a Policy cannot be rebuilt from history ([Policy]).

### Registration

A Policy declares its lanes at registration: `PolicySettings::on_lanes(Lanes::Online)`,
the default, or `Lanes::Bulk` or `Lanes::Both`. A runner whose Policy consumes the bulk
lane writes the Policy's row of `policy_bulk_registrations` when its worker starts,
before the leader election. The row is separate from `policy_cursors`, so a bulk-only
Policy gets no online cursor and nothing in the online machinery changes. A registration
only ever **widens**. Every replica merges the stream types it tracks into the row, and
`NULL` (every stream) absorbs any list, so an older replica in a rolling deploy cannot
narrow the **fan-out** at the append. Delivery is narrower: the worker's read applies
the Policy's filter as its own release knows it, and it deletes the claimed work it does
not deliver. While an older leader still runs, work for a stream type that only the
newer release tracks can therefore be deleted undelivered. Widening a bulk Policy's types
needs the older leaders stopped before anything appends a bulk of the new type.

A Policy removed from the code keeps its registration, and bulk appends keep queueing
work for it. Retiring it is an operator's act: delete its registration and its work rows
(release notes 0.16.0).

### The worker

The [Policy runner] runs one worker per Policy on the bulk lane, in that Policy's
[Leader]. It is elected by the Policy's own advisory lock, so a Policy on both lanes is
led by one replica for both. On every wakeup (an append's NOTIFY or the poll interval)
the worker drains its queue until a claim comes back short. A claim opens a
transaction, takes a per-Policy transaction-level advisory lock so that two claims of
one Policy never overlap, and claims the lowest `read_batch_size` rows of that Policy
with `FOR UPDATE SKIP LOCKED`. It reads those events and calls `react` on each in
`global_position` order. It dispatches the commands grouped by target stream, so one
aggregate load serves every command aimed at that stream unless the stream's head moved
in between. It then deletes the claimed rows and commits.

The claim transaction stays open while it dispatches. A worker that dies releases its
locks, and the next claim finds the rows again. Delivery is at-least-once, as on the
online lane. A command that fails for good is a [Dead letter] for its triggering event,
written on the claim transaction, and its row is deleted with the batch, so one bad
event never wedges the queue. A [Busy] command ends the batch: the claim and its dead
letters roll back, and the rows are claimed again on the next poll (ADR-0036).

One worker per Policy, claiming in `global_position` order, gives a bulk Policy the
online lane's guarantee: a stream's events arrive in that stream's order. Nothing orders
one stream against another, and nothing orders a Policy's bulk events against its online
ones. A Policy on both lanes must not rely on either order.

The commands a bulk Policy dispatches are ordinary online commands. A Policy that wants
its reactions on the bulk lane runs `execute_bulk` itself.

### The online lane is unchanged

The online sweep and late-write lookup read only `lane = online` rows, through partial
indexes, so a bulk segment adds nothing to what they scan however large it is. When the
online feed reads a stream that holds bulk events, it treats them as a `stream_filter`
does: the [Stream place] advances past them and nothing fires (ADR-0013). Existing
registrations, settings, places, dead letters and the reconciliation behave as before.

| Path | Online lane | Bulk lane |
| --- | --- | --- |
| Aggregate load, expected version | yes | yes |
| Inline projections, catch-up, rebuild | yes | yes |
| Sweep, late-write lookup, reconciliation | yes | no |
| Feed delivery to an online Policy | yes | no (passed, as a filtered row) |
| Work rows for a bulk Policy | no | yes |

`PolicyStatus::bulk_pending` reports a Policy's queue depth, so an operator can watch an
import drain. Pending bulk work keeps a Policy `Working`.

## Considered options

- **Make the online lane survive a bulk.** This covers a key window on the
  reconciliation (#298), a lane-aware watermark, and bulk commits in sub-transactions
  that the sweep could see. Each makes the online runner more complex to serve the one
  workload for which its central question, "which stream might be behind that nothing
  nominated?", has a trivial answer that the writer already knows.
- **Bulk rows outside the event store** (a staging table and commands). This is right
  for external facts, and the README says so. It is wrong for decisions, which an
  aggregate must rebuild from and an operator must find in the log.
- **A cursor over the work table** instead of claim-and-delete. The rows are cheaper,
  but a serial id assigned inside a long segment is a [Late write] again, and the table
  grows with history. Claim-and-delete holds only pending work, so an idle queue costs
  one empty indexed read and needs no reconciliation.
- **A stale-claim timestamp** instead of holding the claim transaction open. It holds
  fewer connections, but it adds a reclaim timeout to tune and a second path to get
  wrong. One connection per bulk Policy for the length of a batch is an acceptable cost.
- **Several workers per Policy.** `SKIP LOCKED` would let a second worker take a later
  event of a stream while the first still holds an earlier one, which breaks per-stream
  order. One worker in the leader is the only mode.
- **Holding the stream lock across the whole bulk.** That would fence the stream for
  the whole import, and every online command on it would wait out the stream lock wait
  and fail. Segments release it.
- **A batch `react`** that is handed the whole claim, so a Policy can coalesce commands.
  This is deferred. `react` per event keeps the rule pure and unchanged across lanes,
  and grouping the dispatch by target stream recovers most of the saving. It can be
  added later as a default method without touching this decision.

## Consequences

- **A bulk append costs one work row per event per bulk Policy**, deleted when reacted
  to. The table is sized by pending work, never by history.
- **A bulk is not all-or-nothing.** When `execute_bulk` returns, every segment is
  committed and its work queued. A failure leaves the segments before it standing.
- **Completion is the domain's.** The aggregate records that the bulk is done with an
  ordinary online event if its consumers need to know. A crash before that event leaves
  a bulk whose segments stand. Its aggregate is the checkpoint, so a rerun must be
  idempotent at the command, as any command must be, and the continuation logic lives
  in the domain's `handle_stream`.
- **Segments do not fence the stream.** An online write may land between two segments
  of a bulk on the same stream, and the bulk does not fail because of it. A domain that
  must not interleave runs its bulk on a stream that nothing else writes to while the
  bulk runs.
- **Delivery on the bulk lane is bounded by the queue depth**, not by a cadence: a
  worker runs until its queue is empty. A bulk event of a type that no bulk Policy
  tracks is written and queued for nobody.
- **Migration on a populated database:** a `lane` column with default `online` and two
  partial indexes built `CONCURRENTLY` (0039 to 0041), then two empty tables (0042). The
  0.16.0 release notes carry the runbook.
- **ADR-0032's horizon is not held by a bulk**, because each segment ends and its marks
  are applied per segment, inside the append.

[Inline projection]: ../../CONTEXT.md#inline-projection
[Policy]: ../../CONTEXT.md#policy
[Policy runner]: ../../CONTEXT.md#policy-runner
[Leader]: ../../CONTEXT.md#leader
[Dead letter]: ../../CONTEXT.md#dead-letter
[Busy]: ../../CONTEXT.md#busy
[Stream place]: ../../CONTEXT.md#stream-place
[Late write]: ../../CONTEXT.md#late-write
