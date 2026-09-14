# A Policy's feed decides contiguity on unfiltered positions

**Status:** accepted

A [Policy](0003-policies-as-checkpointed-background-subscribers.md) reads the log
through a high-water-mark cursor that may only advance across a contiguous,
gap-free prefix of `global_position`: a position that was not read may belong to an
append that is still in flight (BIGSERIAL values are assigned at INSERT and become
visible at COMMIT), and skipping it would lose an event. That is the skip-safety
rule of ADR-0003.

A Policy may also narrow its feed with a `stream_filter`. The first implementation
pushed that filter into the same query the contiguity check ran over, so the feed
asked "are the rows I want contiguous?" instead of "which positions exist?". Every
event on a stream the filter excluded then looked exactly like a hole, and the
Policy stopped in front of it — on its first poll, permanently, with no error and no
log line. `StreamFilter::all()` was the only value that worked; any narrower filter
was a trap (funkode-io/replay#166).

## Decision

Contiguity is a property of the **unfiltered** global position stream, and the
filter is a **delivery** decision applied afterwards.

Each poll reads the window of every `global_position > cursor`, limited by the
policy's read batch size, and evaluates the filter per row as a value rather than as
a `WHERE` predicate. A position whose event does not match advances the cursor
without firing a reaction — the same shape a synthetic compaction snapshot already
uses ([ADR-0004](0004-compaction-synthetic-event-marker-for-policy-feed.md)).

How far the cursor may then advance is a **pure function** of the window: the prefix
that is contiguous from `cursor + 1`, truncated at the first hole. It is the single
place the rule lives, has no database in it, and is where the handling of permanent
holes (funkode-io/replay#164) is to be added.

## Consequences

- A `stream_filter` no longer changes *how far* a Policy advances, only *what it
  reacts to*. Two policies with different filters walk the log at the same rate.
- The read batch is spent on positions, not on matches: a Policy with a highly
  selective filter reads a batch of mostly-skipped positions per poll and may need
  several polls to reach its next event. Memory stays bounded by the batch — a filter
  cannot make the window grow — but the bound is now the same for every Policy: a
  selective one pays for a batch of rows it will not react to, where the old (broken)
  filtered query paid only for its matches. Only delivered rows are parsed into
  events; the row bytes behind a skipped position are still fetched, because one
  query reads the window.
- Projecting the payload columns only for matching rows, or reading positions and
  matching rows as two queries, would recover that and would also let the position
  window be larger than the row batch. Both are deliberately not done here: the
  bounded read was never the incident, and a second query brings a second snapshot
  (a concurrent compaction can change whether a row matches between the two). If a
  selective Policy's catch-up rate ever becomes the problem, that is the change to
  make, with a measurement behind it.
- The filter now runs in the SELECT list, where SQL's three-valued logic is visible:
  a predicate that yields NULL (for example `aggregate_version = 3` on a live row)
  must be read as "no match", so it is collapsed with `COALESCE(…, FALSE)`. As a
  `WHERE` clause this happened for free.
- Postgres plans the query as a range scan over `global_position` with the filter as
  a per-row qualifier. A filter that used to restrict the scan no longer does; the
  bound is the `LIMIT`, which was always what kept the read small.
