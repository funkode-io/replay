# 30. A late write is found by its commit stamp

Date: 2026-10-21

## Status

Accepted. Amends [ADR-0026](0026-a-policy-tracks-its-position-per-stream.md): the
reconciliation stops being how a late write is normally found and becomes a safety net,
and ADR-0026's delivery bound for such a write is replaced by the one below. Decides
[#268](https://github.com/funkode-io/replay/issues/268), part of
[#258](https://github.com/funkode-io/replay/issues/258).

## Context

A Policy finds the streams it owes work two ways (ADR-0026). The **sweep** reads new
positions every poll and is cheap, but it can pass a **late write**: an append whose
transaction had not committed when the sweep read past its position. The
**reconciliation** catches late writes by comparing every stream's head with the
Policy's place. That comparison costs one row per stream, so it runs on a cadence.

At consumer scale this does not hold. In dxp-data-loom, 24 Policies over 131k streams
made the comparison 98% of the database's execution time (#258), and production will
hold about 2M streams. Making it cheaper per cadence (a key window, #263) stretched the
time to find a late write to minutes or hours. That is the wrong trade for that
workload: a late write there is a whole import, a long `ProcessFile` transaction
overtaken by short ones, and after it commits nothing nominates its stream again.

Narrowing the comparison to a Policy's stream types (#267) shrinks most Policies' scans,
but a Policy over a per-entity type still compares every stream of that type. Finding a
late write has to cost what the new events cost, not what the stream count costs.

Every event already records the transaction that wrote it: its **commit stamp**,
`events.commit_txid`, indexed by `(commit_txid, global_position)` and unused since
ADR-0026. A late write is, by definition, an event whose stamp the sweep's snapshot
could not see.

## Decision

**Each poll, a Policy looks up the events of transactions its sweep could not see, once
those transactions end, and nominates their streams.** The reconciliation remains, as a
safety net, on a slow cadence.

### What the sweep records

The sweep's statement also returns `pg_current_snapshot()`. Within one statement this is
the snapshot the sweep's rows were read under, so it states exactly which transactions
the sweep could not see: the in-progress ids (`xip`), and every id at or above `xmax`.

Each Policy keeps three things **in memory, in its leader, never persisted**:

- **Unseen**: transaction ids a sweep found in progress.
- **Tail**: the previous sweep's `xmax`.
- The previous sweep's `swept_through`.

### The late-write lookup, each poll after the sweep

1. **Ids above the tail.** Events with `tail ≤ commit_txid < xmax(now)`, not in
   progress now, at a position no higher than the previous `swept_through`. These come
   from transactions that already held an id at the previous sweep but sat above its
   `xmax`. That is ADR-0015's second objection to snapshots, and it is closed by
   carrying the range forward rather than trusting `xmax`. The range covers only ids
   assigned since the previous sweep, so the scan is proportional to new events.
2. **Ended unseen ids.** For each id in Unseen that the current snapshot shows as ended,
   look up its events (`commit_txid = id`), nominate their streams, and drop the id. An
   aborted transaction left no rows, so it nominates nothing.
3. Add the current `xip` to Unseen, then set Tail and the previous `swept_through` from
   this sweep.

A leader's first poll has no Tail yet, so it skips step 1. The full comparison at leader
start covers whatever was in flight before it.

Nominations from 1 and 2 join the poll as discovered streams and **lead it**, as the
reconciliation's candidates do (ADR-0026), because nothing else will nominate them again.
They are filtered to the Policy's admitted stream types (#267), and the read and its
place decide what is owed, as for every other nomination. A lookup returns at most the
read batch of stream ids. A transaction that wrote to more streams than that keeps its
id, with a keyset cursor, and continues on the next poll.

### Why it is exact for appends made through Replay

A transaction can hold a position below `swept_through` while invisible to the sweep
only if it drew that position before a later position committed and was read. Two
invariants make such a transaction always visible to steps 1–3:

- **An append has its transaction id before it has its position.** `write_event` locks,
  or creates, the stream's `streams` row before inserting the event, and that write
  assigns the id. So ADR-0015's first objection, a position held by a transaction with
  no id yet, cannot arise for an append made through `write_event`.
- **The commit stamp is the top-level transaction id** (`pg_current_xact_id()`, the
  column default), which is what a snapshot reports.

Every such transaction is therefore either in the sweep's `xip` (step 2) or at or above
its `xmax` (step 1). Nothing outside Replay writes to `events`; a pre-deploy migration
that rewrites events changes no positions and no stamps.

### Unseen is bounded by a number in the code

Unseen holds only ids a snapshot reported as in progress, so its size is bounded by the
transactions open at once (`max_connections` plus prepared transactions). The code
enforces it with a constant cap. If the cap is exceeded, the Policy clears its late-write
state and requests a full comparison. Ids from other databases on the same server (#214)
sit in Unseen until they end, find no events, and delay no delivery.

### The safety net

The full comparison, narrowed by #267, runs:

- **when a leader starts**, because in-memory state does not survive a restart, a
  failover, a restore or a cluster move (subject to the start-up offset of #269);
- **on a manual drain**, as today;
- **on `REPLAY_POLICY_RECONCILE_SECS`**, whose default becomes **300 s**. Its job is
  self-healing after a defect, not delivery. Deployments that set it keep their value.

## Consequences

- **The delivery bound for a late write becomes two bounds.**
  - *Through Replay, with the leader unchanged:* nominated on the first poll after its
    transaction ends, and read in that poll, which it leads.
  - *Otherwise* (leader start, cluster move, cap overflow, a defect): within one
    safety-net cadence, plus ADR-0026's rotation terms.

  The first is what an operator sees; the second is the edge case, slower and still
  correct.
- **The cost of finding a late write is proportional to new events.** Each poll scans
  the stamp range assigned since the previous sweep, plus one indexed lookup per ended
  transaction. The full comparison runs at leader start and every few minutes.
- **The edge cases cost latency, never an event:**

  | Case | What happens |
  |---|---|
  | Restart, failover, leader change | State is lost; the new leader's full comparison finds what was in flight. |
  | PITR, replica promotion | These keep the transaction counter; state is lost with the process; as above. |
  | Logical move (`pg_dump`/`pg_restore`, logical replication, blue/green) | The counter resets (#215). If it moves backwards (`xmax(now) < Tail`), the Policy clears its state and requests a full comparison. Restored stamps from the old cluster can collide with new ids; that causes spurious nominations, each bounded by the read batch, and a nominated stream that is caught up reads nothing. |
  | A long transaction elsewhere on the server (#214) | Held in Unseen, harmless; nothing waits for it. |
  | A transaction open longer than any cadence (a 15-minute import) | Held in Unseen until it ends, then found on the next poll. No time limit is needed. |
  | Too many open transactions | Over the cap: state cleared, full comparison. |

- **This is not the rejected watermark** (#214, #215). Nothing waits for a transaction to
  end, nothing is delivered in transaction-id order, and the transaction id decides
  nothing about what is owed. It only nominates.
- **This is not ADR-0015's rejected rule.** ADR-0015 needed to prove that a *position*
  would never be filled, from a snapshot, for any writer. This needs only that an
  invisible transaction holding a position below the sweep is in `xip` or at or above
  `xmax`. That follows from `write_event`'s order, and a wrong answer costs latency.
- **The commit stamp is load-bearing again.** `events.commit_txid` and
  `idx_events_commit_txid_position` must stay, and every insert path must keep stamping
  the top-level transaction id. CONTEXT.md's "Commit stamp" says so.
- **`write_event`'s order is now an invariant.** Taking the stream row before the
  insert was already needed for the stream's place (ADR-0023). It now also carries this
  ADR, and a test pins it.

### Tests the implementation must carry

- A late write is nominated on the next poll after its transaction commits, with the
  safety net set far away so that it cannot be what finds the write.
- The tail case: a writer whose id is at or above the sweep's `xmax` and whose position
  lies below the sweep is still found.
- An aborted transaction nominates nothing and leaves Unseen.
- A transaction open across many polls is looked up once, when it ends, and Unseen never
  holds more than the transactions open at once.
- An open transaction in another database neither nominates nor delays delivery.
- Leader start runs a full comparison, and a write in flight across a leader change is
  delivered.
- The transaction counter moving backwards, and the cap being exceeded, each clear the
  state and request a full comparison.
- The invariant: a transaction holding a position from `write_event` already has its id
  (it appears in a concurrent snapshot's `xip` or at or above its `xmax`).
- `liveness_simulation` models the new nominations.
