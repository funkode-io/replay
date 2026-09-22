# A Policy's feed decides contiguity on unfiltered positions

**Status:** superseded by [ADR-0024](0024-a-policy-tracks-its-position-per-stream.md), which removes contiguity from the feed altogether: a Policy reads each stream over that stream's own sequence, which has no holes to be contiguous across. What survives is the half this ADR is named for — **the filter decides delivery only, never how far a Policy gets** — now applied to one stream's events rather than to a window of positions.

A [Policy](0003-policies-as-checkpointed-background-subscribers.md)'s cursor may only
advance across a contiguous prefix of `global_position`: a position that was not read
may belong to an append still in flight (ADR-0003 skip-safety).

The first implementation pushed the Policy's `stream_filter` into the same query that
prefix was computed from, so every event on an excluded stream looked like a hole. Any
filter narrower than `all()` wedged its Policy on the first poll, silently
(funkode-io/replay#166).

## Decision

Contiguity is decided on the **unfiltered** position stream; the filter decides
delivery only.

Each poll reads every `global_position > cursor`, bounded by the read batch size, and
evaluates the filter per row as a value. An excluded position advances the cursor and
fires nothing, like a compaction snapshot
([ADR-0004](0004-compaction-synthetic-event-marker-for-policy-feed.md)).

How far the cursor may then advance is a pure function of that window — the prefix
contiguous from `cursor + 1` — in `policy_feed`. Gap handling
(funkode-io/replay#164) goes there.

The rule rests on one event per position: a cursor that steps one position at a time
steps *over* the second event at a shared position. Since
funkode-io/replay#200 a unique index on `events (global_position)` enforces that
(migration 0015) — `BIGSERIAL` never did, so before then the invariant everything
here relies on was checked by nothing.

## Consequences

- A `stream_filter` changes what a Policy reacts to, not how far it advances. Two
  Policies with different filters walk the log at the same rate.
- The read batch counts positions, not matches, so a selective Policy may need several
  polls to reach its next event. The window is still bounded by the batch, but a
  selective Policy now pays for rows it will not react to; only delivered rows are
  parsed into events, the rest are fetched and dropped.
- Recovering that means projecting payload columns only for matching rows, or reading
  positions and rows as two queries — which would also decouple the position window
  from the row batch. Not done here: the bounded read was never the incident, and two
  queries mean two snapshots, where a concurrent compaction can flip whether a row
  matches. Do it against a measurement.
- The filter now runs in the SELECT list, so a predicate that yields NULL (for example
  `aggregate_version = 3` on a live row) is collapsed with `COALESCE(…, FALSE)`. In a
  `WHERE` clause this was free.
- The plan is a range scan over `global_position` with the filter as a per-row
  qualifier; a filter that used to restrict the scan no longer does. The `LIMIT` is
  what keeps the read small, as before.
