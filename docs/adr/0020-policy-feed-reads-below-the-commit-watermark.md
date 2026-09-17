# The Policy feed reads below the commit watermark

**Status:** accepted

**Supersedes** the gap-free-prefix half of
[ADR-0013](0013-policy-feed-contiguity-on-unfiltered-positions.md), and the hole-handling
that [ADR-0003](0003-policies-as-checkpointed-background-subscribers.md)'s skip-safety
rule and [ADR-0015](0015-policy-crosses-a-position-no-transaction-can-fill.md) exist to
manage.

`global_position` is drawn from a sequence when a write *starts* and becomes visible when
it *finishes*, so positions arrive out of order, and a position taken by a write that
aborts never arrives at all. Reading in position order therefore needs a theory of holes:
wait at the first missing number in case it is still in flight, then prove that it never
can be. The first half wedges a Policy for good on a position that was burned
([#164](https://github.com/funkode-io/replay/issues/164)); the second half is a proof
that only holds in one direction, and getting it backwards loses events
(ADR-0015).

## Decision

The feed reads events in `(commit_txid, global_position)` order — the transaction that
wrote an event, then the position it took — and reads only events whose transaction has
certainly ended: `commit_txid < pg_snapshot_xmin(pg_current_snapshot())`, the standard
Postgres CDC/outbox watermark. A Policy's cursor is that pair
([#194](https://github.com/funkode-io/replay/issues/194)).

The hazard is gone by construction rather than handled:

- every row below the watermark is already visible and no row below it can appear later,
  so nothing turns up behind a point a Policy has passed;
- a burned position belongs to no row, so it is not a hole in this order — it is not in
  it;
- a write in flight sits at or above the watermark together with everything committed
  after it, so its events are delivered in their place once it ends.

Note the direction the snapshot is used in. Reading *below* the watermark is sound.
Using a watermark to prove a specific missing position can never appear is not, and
ADR-0015 rejects it with a test that makes it be wrong on demand.

## Consequences

- A long-running write delays the events committed after it until it ends. That is
  bounded by the write and self-healing, never permanent; the runner traces the wait at
  `debug` and escalates to `warn` past the threshold, so it is distinguishable from a
  Policy that is simply idle.
- A Policy is no longer stopped by a position, so there is nothing to detect, nothing to
  prove and nothing to skip: the gap machinery, the sequence-lock probe and the skip
  warning have no subject (#197).
- Reads are a forward scan of `idx_events_commit_txid_position` (migration 0019) stopped
  by the `LIMIT`, resumed by a row comparison on the cursor pair. Written as two `AND`ed
  comparisons it would be neither the same set nor an index scan.
- Events written before migration 0018 carry the sentinel stamp `0`, which orders before
  every real transaction, so a migrated log is read in position order at its head and a
  cursor that predates 0022 resumes where it left off.
- `StartAt::Now` starts at the head position under the transaction that wrote it, so a
  write in flight at that moment is history the Policy skips. Starting at the watermark
  instead would catch it and replay every event committed while any transaction was open;
  no point in the order does both.
- The stable-cut API (`contiguous_high_water_mark`) is unchanged and still speaks in
  positions; rebuilding it on the watermark is a separate question.
