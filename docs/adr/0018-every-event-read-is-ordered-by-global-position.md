# Every event read is ordered by `global_position`

**Status:** accepted, narrowed by
[ADR-0022](0022-policy-feed-reads-below-the-commit-watermark.md): the [Policy feed] is no
longer one of the reads this rule covers. It orders by `(commit_txid, global_position)`,
which is the "order by commit visibility" option rejected below — rejected for *this*
question, and taken up for the feed's own. Everything else here stands.

**Amends** [ADR-0011](0011-inline-projections-flushed-in-bounded-chunks.md)'s keyset
decision: the rebuild cursor's `(created, version, id)` key, and the index behind it.

`events.created` defaults to `now()` — `transaction_timestamp()`, the instant the
transaction *began*. `version` is handed out much later, inside `append_event`, after
`SELECT … FROM streams … FOR UPDATE` has serialised the writers. The two clocks
disagree whenever a transaction starts earlier and wins the lock later:

```
T=0.000000  Tx B begins                     (created = 0.000000)
T=0.000100  Tx A begins                     (created = 0.000100)
T=0.000200  Tx A takes the lock, writes v5  (created = 0.000100)
T=0.000350  Tx B takes the lock, writes v6  (created = 0.000000)

ORDER BY created, version  ->  v6, then v5
```

Replay then applies `Revoked{alice}` before the `Granted{alice}` it revokes, and the
aggregate comes back with Alice still in it. A downstream consumer hit exactly that as a
security bug — a revoked ACL member kept read/write access
([#199](https://github.com/funkode-io/replay/issues/199)).

## Decision

`global_position` is the sort key of every event read: `stream_events`, the
inline-projection rebuild's keyset pages, and compaction's fold over the live stream.
There is one ordering rule, so it cannot drift between read paths.

The [Policy feed] used it too when this was written, and no longer does: it asks "has
every earlier writer finished?", which a position cannot answer, so ADR-0022 moved it to
`(commit_txid, global_position)`. The reads above ask a different question — relative
order within a committed log — and keep this key.

It is a valid key where `created` is not. The `BIGSERIAL` is drawn by `nextval()` at
INSERT, inside the same `FOR UPDATE` critical section that assigns `version`: a second
writer cannot insert until the first commits, so within a stream position rises with
version, and across streams it is a total order — which is what the multi-stream reads
actually need. Being unique, it also needs no tiebreaker, so the rebuild cursor is a
single `WHERE global_position > $1`.

`created` keeps its two honest jobs: it is the audit stamp, and it is what
`StreamFilter::CreatedAfter` / `CreatedBefore` filter on for time-travel reads. Filtering
decides which rows a read returns, never the order they arrive in.

The read-path change itself landed with
[#192](https://github.com/funkode-io/replay/pull/192), which needed the same key for
[ADR-0015](0015-policy-crosses-a-position-no-transaction-can-fill.md); this ADR records
the rule that change established and finishes applying it.

## Considered options

- **`version` alone.** Correct within a stream and meaningless across streams — versions
  restart per stream — so every multi-stream read (`StreamFilter::All`, a projection
  rebuild over a stream type) would still need a second key, and compaction resets it.
- **Keep `(created, version)` and add `id` as a tiebreaker.** What ADR-0011 did. It makes
  the order *deterministic*, which is not the same as *right*: the inversion above is a
  strict `created` difference, not a tie, so `id` never gets a vote.
- **Order by commit visibility (`xid8`).** The right key for the feed's "has every earlier
  writer finished?" question, and it is being pursued there — `events.commit_txid` landed
  with [#193](https://github.com/funkode-io/replay/issues/193), and
  [#195](https://github.com/funkode-io/replay/issues/195) moved the feed onto it
  (ADR-0022). It answers a different question from this one: a point-in-time read of a
  committed stream needs relative order, not a visibility watermark — and it costs a
  backfill and a cursor format change, which this rule does not.

## Consequences

- **Gaps do not matter.** A burned position leaves a hole; relative order is unaffected,
  which is all a full stream read depends on. Contiguity was the feed's problem
  (ADR-0015) until ADR-0022 left it with no holes to have.
- **Uniqueness is load-bearing.** A `>` cursor with no tiebreaker steps over the second
  row of a duplicated position. `BIGSERIAL` implies no constraint; the unique index
  [#200](https://github.com/funkode-io/replay/issues/200) added (migration 0015) is what
  makes the key one.
- **Compaction's write order is load-bearing too.** Snapshot rows take their positions from
  the INSERT loop, so a rewrite emitted out of `version` order would hand a
  non-commutative stream back inverted. Pinned by
  `compaction_writes_snapshot_rows_in_the_order_the_rewrite_returned_postgres_test`.
- **The `(created, version, id)` index is replaced by one on `created`** (migrations 0020
  and 0021). Nothing sorts on that key any more; what survives is the range predicate on
  the leading column. On 50 000 seeded rows: 3 056 kB → 1 112 kB, same plan.
- **`replay_keeps_events_that_share_created_and_version_postgres_test` guards a hazard that
  no longer exists.** It stays as a regression test for the paging it exercises.

[Policy feed]: ../../CONTEXT.md#policy-feed
