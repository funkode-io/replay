# 24. A Policy tracks its position per stream

Date: 2026-09-22

## Status

Accepted. Supersedes [ADR-0013](0013-policy-feed-contiguity-on-unfiltered-positions.md)
and [ADR-0015](0015-policy-crosses-a-position-no-transaction-can-fill.md), whose subject —
a hole in `global_position` — no longer exists. Narrows
[ADR-0018](0018-every-event-read-is-ordered-by-global-position.md) to the reads it was
written about. Moves the control surface of
[ADR-0012](0012-policy-cursor-is-an-operator-writable-control-surface.md) to a new table.

## Context

A Policy stored one number: the `global_position` it had processed. Its feed was the
positions after that number, and it could only advance over them contiguously, because a
position it had not read might be an append still in flight. So a missing number stopped
it.

Numbers go missing. `nextval` is not transactional, so a write that fails leaves its
position empty for good, and a Policy would wait in front of it forever
(funkode-io/replay#164). A write that is merely *slow* does the same thing for as long as
it runs: it holds a position nobody can see, and every Policy — including ones that read
no stream that write touches — stops there. Machinery accumulated around this: telling a
burned position from an in-flight one by reading `pg_locks`, a rate-gated warning for a
Policy that had been parked too long, a status field naming the hole.

All of it existed to answer one question — *is this number ever going to arrive?* — that
only had to be asked because a Policy's position was a number in a global order that it
did not write and could not predict.

The domain has no such order. Events of one stream must be processed in order; two streams
have no order between them. [ADR-0023](0023-a-stream-is-numbered-twice.md) gave every event
a place in its own stream, and that place has properties `global_position` never had:
appends to a stream serialise on its row, so its places arrive in order and cannot be
overtaken, and the counter is a row rather than a sequence, so a failed write hands its
place back instead of burning it.

## Decision

**A Policy records a place per stream, and delivers each stream's events in that stream's
order.** `policy_stream_cursors (policy, stream_id, stream_seq)`, one row per stream the
Policy has delivered from. Nothing orders one stream against another, and no read waits
for a number.

Finding which streams are owed work is where this design has to be engineered rather than
written, because the question "is this stream behind this Policy" compares two tables'
columns and no index can answer it. Two paths, with different costs and different jobs:

- **The sweep, every poll.** `SELECT global_position, stream_id FROM events WHERE
  global_position > swept_through ORDER BY global_position LIMIT batch` — an indexed range
  scan, two columns, proportional to new events. It nominates streams and reads no cursors.
  It advances `swept_through` to the last position it *read*, deliberately passing
  positions it never saw, which is what stops it waiting for anything.
- **The reconciliation, on a cadence.** `streams.stream_seq > COALESCE(cursor, 0)` — one
  row per stream, bounded by the same batch, default every 5 seconds
  (`REPLAY_POLICY_RECONCILE_SECS`). It finds what the sweep swept past: a write that had
  not committed when the sweep passed its position.

The sweep is allowed to be wrong because it decides nothing. It nominates; what a stream
is owed is read from that stream's own sequence, which has no holes.

## Consequences

- **A long write delays the stream it is writing to, and nothing else.** Two streams
  written concurrently are two independent feeds; the one that is waiting is the one being
  written. This is the property the commit watermark (funkode-io/replay#215, closed) could
  not give: it bought the same correctness by making every Policy wait for the oldest
  in-flight write in the whole instance (funkode-io/replay#214).
- **A write that commits below the sweep is delivered late, not never — and the lateness
  is a number.** At most one reconciliation cadence. The watermark's equivalent bound was
  the duration of the longest transaction, which is not a number anyone configures.
- **There is no hole to detect, so the machinery that detected holes is gone**:
  `burned_position.rs` and its `pg_locks` probe, `policy_feed.rs` and its gap truncation,
  `policy_blocked.rs` and its rate gate, `PolicyCondition::Blocked`, and the cursor's
  commit stamp (funkode-io/replay#194), whose job was to make the global order total
  (funkode-io/replay#197).
- **Lag becomes a count of events.** `SUM(streams.stream_seq - place)` over the streams a
  Policy is behind on is exactly the number of events it has not passed, per stream and
  summable per Policy — replacing a subtraction of positions that counted other streams'
  events and burned numbers alike (funkode-io/replay#196).
- **State is one row per stream the Policy has delivered from**, so it grows with the
  number of streams and not with the log. A Policy that starts at `Now` writes one row per
  *existing* stream, once, at bootstrap — worth knowing before pointing a new Policy at a
  database with millions of them.
- **The reconciliation scan is proportional to total streams, not to streams behind.**
  Affordable on a cadence at ten thousand streams; at ten million it is the thing to
  measure first. A frontier table — a row per `(policy, stream)` written on append and
  deleted when caught up — would make it proportional to work instead, and was rejected for
  now because keeping it small means the *write* path has to know the set of deployed
  policies, so an append's cost would scale with how many there are.
- **An operator's control surface moves** from `policy_cursors.position` to a place in
  `policy_stream_cursors`, and gets finer: a redelivery can be forced for one stream
  without rewinding the Policy over every other. The refresh machinery ADR-0012 needed is
  gone with it, because places are read fresh every poll rather than held in memory
  between them — but the compare-and-set stays, per stream: a poll writes a place only
  where it still reads as the value that poll started from. Monotonicity alone would not
  do, because the write it has to refuse is *lower* than the one the poll carries. An
  operator rewinding a stream mid-batch against a monotonic write would see the runner
  reinstate its higher place and undo the rewind silently, which is a control surface in
  name only.
- **`policy_cursors.position` is renamed `discovered_through`** and means where the search
  resumes, not what has been processed. A Policy's progress is no longer one number, and
  no column pretends otherwise.
- **A runner learns it has been superseded one stream at a time**, where the old cursor
  told it once for the whole Policy. A lost compare-and-set abandons that stream for the
  poll and leaves the place where its new owner put it; the other streams in the batch
  carry on. Delivery is at-least-once and reactions are idempotent by contract
  ([ADR-0003](0003-policies-as-checkpointed-background-subscribers.md)), so the overlap
  costs duplicate work rather than correctness.
- **Every bound in the design is a rotation, because every bound is a batch.** The
  reconciliation resumes after the last stream id it examined and wraps; the poll's
  candidate list is capped at the read batch and puts the streams it could not finish at
  the back. Both exist for the same reason: a batch taken from the front of a fixed order
  returns the same entries for ever if the entries at the front never leave, so a
  continuously-busy set of streams would hide a quiet one behind it — a stream whose only
  pending work is a write the sweep passed, which no future event will nominate. Rotating
  turns "hidden while the system is busy" into "examined within one pass", which is
  `ceil(streams / batch)` cadences and a number an operator can compute.
- **`read_batch_size` is a budget for the drain, spent across streams** rather than
  applied to each. A Policy owed work in a hundred streams reads the same number of events
  per poll as one owed work in a single stream, so the knob still bounds how long a poll
  runs — which is what a shutdown, a leadership change and a dispatch-timeout budget all
  wait on.
- **Ordering across streams is no longer promised, because it never held.** A Policy sees
  each stream in that stream's order; between streams it sees whatever the sweep found
  first. Anything that needs two streams ordered against each other needs them to be one
  stream.
