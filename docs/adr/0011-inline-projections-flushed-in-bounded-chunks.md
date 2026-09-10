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

## Consequences

- A projection that accumulated state in a local of one `handle` call now sees it reset
  per chunk. Such state belongs in the projection's own fields or its view. Documented on
  `InlineProjection::handle` and in the README.
- **Rebuild replay is not chunked.** `PostgresEventStoreBuilder::build` still loads matching
  history with one `fetch_all` and calls `handle` once. Same unbounded shape, different path;
  not addressed here.
- The in-memory store applies projections after a whole append by construction and is
  test-only, so it keeps single-call delivery. A projection written against it and deployed
  on Postgres must not rely on that.

[Inline projections]: ../../CONTEXT.md#inline-projection
