# Inline projections are flushed in bounded chunks, not once per append

**Status:** accepted
**Amends:** ADR-0002 (which had the store call `handle` once per append)

`store_events_stream` pulls its producer one event at a time so a bulk append never has
to fit in memory — but it retained every appended event as a `serde_json::Value` so the
[Inline projections] could be applied at the end, which put the whole append back in
memory the moment one was registered. The store now flushes at most `flush_size` events
at a time, on the same transaction, and drops them:

```text
append of B events, flush size N  →  ⌈B/N⌉ calls to handle, in order, one transaction
```

Peak retention becomes O(N) instead of O(B). Atomicity is untouched: every chunk applies
to the transaction the append already owns, so a failure in the last chunk rolls back the
writes of the first.

The rebuild path — the replay `PostgresEventStoreBuilder::build` runs on first
registration or version drift — is bounded the same way and by the same `flush_size`. It
is the worse of the two: an append is bounded by what a caller submits, while a rebuild
loads *all* matching history, at startup, on the deploy that bumped a projection version.
Because the replay must stay in the rebuild transaction (one snapshot, one rollback), the
chunks are paged with a keyset cursor — `WHERE (created, version, id) > (…) ORDER BY
created, version, id LIMIT N` — rather than read from a row stream, which cannot borrow
the transaction while `handle` writes to it. `id` breaks ties in `(created, version)`,
which is not unique, so the paging cannot skip rows.

The cost is a contract change — a projection no longer sees an append in one call. We take
it rather than bound the buffer by bytes or spill to disk, because the events a projection
receives, and their order, are unchanged, and every in-tree projection already folds per
event. `flush_size` follows the crate's tunable convention: store override →
`REPLAY_PROJECTION_FLUSH_SIZE` → 500.

## Considered options

- **Buffer by serialized bytes rather than count.** Bounds memory more directly, but the
  figure a caller can reason about is "events per call", and byte accounting would have to
  guess the retained size of a `Value` tree from its serialized length.
- **Apply each event as it is appended (flush size 1).** Simplest bound, but it costs a
  projection round-trip per event and denies projections any batching at all.
- **Leave it, document the memory cost.** The promise `store_events_stream` makes in its
  own doc comment is bounded memory; a footnote retracting it for anyone with a projection
  is not a fix.
- **Read the rebuild's history from a second connection.** Lets the row stream be consumed
  while `handle` writes, but the read then leaves the rebuild's transaction and sees a
  different snapshot — wrong for a rebuild, which must replay exactly the history its
  transaction committed against.
- **A separate tunable for rebuild chunks.** Both are "events held before `handle` sees
  them"; a second knob would have to be discovered and sized separately to fix the path
  that fails at startup.

## Consequences

- A projection that accumulated state in a local of one `handle` call now sees it reset
  per chunk. Such state belongs in the projection's own fields or its view. Documented on
  `InlineProjection::handle` and in the README.
- **A rebuild costs one round trip per chunk.** Keyset paging reissues a bounded query per
  chunk instead of scanning once, and it reads `(created, version, id)` order, so the
  supporting index matters on a large history.
- **Replay order is now fully determined.** Events that tie on `(created, version)` used to
  arrive in whatever order the scan produced; they now arrive by `id` within the tie.
- The in-memory store applies projections after a whole append by construction and is
  test-only, so it keeps single-call delivery. A projection written against it and deployed
  on Postgres must not rely on that.

[Inline projections]: ../../CONTEXT.md#inline-projection
