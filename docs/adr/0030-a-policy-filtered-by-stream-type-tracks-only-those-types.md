# 30. A Policy filtered by stream type tracks only those types

Date: 2026-10-21

## Status

Accepted. Amends [ADR-0026](0026-a-policy-tracks-its-position-per-stream.md): which streams
a Policy compares, reads and keeps a place for. Narrows the rule in
[ADR-0013](0013-policy-feed-contiguity-on-unfiltered-positions.md) and the
[Policy feed](../../CONTEXT.md#policy-feed) glossary entry that a filter decides delivery
only.

## Context

Under ADR-0026 a Policy's `stream_filter()` decided only what was *delivered*. Everything
before delivery ran over every stream in the database: the reconciliation compared every
row of `streams`, the sweep nominated every stream it saw, and a poll read every nominated
stream and wrote a place in `policy_stream_cursors` for it, whatever its type. The status
read model counted lag over every stream as well.

ADR-0026 named the reconciliation scan as the thing to measure first. The measurement came
from dxp-data-loom (funkode-io/replay#258): 25 Policies, 131k streams, every Policy walking
all of them on every cadence, and about 3.2M place rows. Every one of 24 of those Policies
reads a single stream type. The scan had a multiplier no one had asked about — *every*
stream, including the types a Policy never delivers from — so every new domain made every
existing Policy slower, and every Policy kept a row for every stream of every domain.

A filter that names stream types is the one filter that answers "could this stream ever
matter to this Policy?" from the stream alone, without reading its events: the type is on
the `streams` row, fixed when the stream is created.

## Decision

**When a Policy's filter is `StreamFilter::ForStreamTypes(types)`, the streams it tracks
are the streams whose `streams.type` is in `types`, and every path honours that set:**

- **The reconciliation** compares only those streams:
  `WHERE s.type = ANY($types) AND s.id > $rotation`, served by an index on
  `streams (type, id)` (migration 0036, which supersedes 0005's index on `type` alone;
  migration 0037 drops that one once its replacement exists). Its
  cost is proportional to the tracked streams after the rotation, and it reads no row of
  another type. The rotation rules of ADR-0026 are unchanged: it rotates through the
  tracked streams, and moves only through streams the poll read.
- **The sweep** is exactly as cheap as before. It reads its `LIMIT` of new events by
  position and advances `swept_through` over all of them, then *drops* the nominations
  whose type is not tracked — one primary-key lookup per nominated stream, bounded by the
  same `LIMIT`.
- **A poll** therefore never reads an untracked stream and never writes a place for one.
  That includes the streams the last poll left unfinished: when the tracked set changes,
  the carried queue goes through the same lookup as the sweep's nominations. `StartAt::Now`
  seeds places in tracked streams only.
- **`PolicyStatusStore::list`** counts `lag` and `streams_behind` over the tracked streams.
  The status store has no Policy to ask, so the runner records the tracked types in
  `policy_cursors.tracked_stream_types` (migration 0035) whenever a worker loads the Policy
  and whenever the filter a poll is given differs from what was recorded. `NULL` is every
  stream — which is also what a row written before the column existed holds, so a filtered
  Policy's lag counts every stream until a worker next starts it and records its types.

Only a top-level `ForStreamTypes` narrows what is tracked. `All`, `WithStreamId`,
`WithMetadata` and every combinator — including an `And` that contains a `ForStreamTypes` —
track every stream, as before.

### Why the sweep filters after reading, not in SQL

Filtering in the sweep's own statement — joining `streams` and adding the type to its
`WHERE` — would make it *search ahead*: `LIMIT` would bound the matching rows it returns,
not the rows it reads, and to fill a page past a long run of other types' events it would
read every one of them. The rows it examines would grow with the rows it skips, which is
exactly the defect #258 measured in the reconciliation, moved into the query that runs on
every poll. Filtering after reading keeps the sweep's cost a constant per poll, and lets it
pass a batch that holds nothing tracked in one poll rather than stopping in front of it.

## Consequences

- **A Policy's cost no longer grows with the domains it does not read.** Its reconciliation
  examines its own types' streams; a new aggregate type costs it nothing but the sweep's
  constant per-poll page and a lookup per stream that page names.
- **The reconciliation is still proportional to the tracked streams, not to the streams
  behind.** `LIMIT` bounds what it returns; a caught-up Policy still walks every stream it
  tracks to find nothing. This decision removes the untracked multiplier, not the scan.
  Finding late writes without the full comparison is a separate change.
- **Existing places in untracked streams are left in place.** Deleting them automatically
  is unsafe: if the filter is later widened, a missing row means place 0, and the Policy
  would redeliver those streams' whole history — even a `StartAt::Now` one. The README
  documents a one-off cleanup an operator can choose to run, and states that consequence.
- **Widening a filter starts each newly tracked stream from its first event**, whatever the
  Policy's `StartAt`: it has no place in a stream it never tracked, and a stream with no
  place is at its beginning. That is the same thing that happens today when a new stream is
  created after a Policy starts. A Policy that must not replay a type's history when it
  starts tracking it needs its places in those streams moved before the change is deployed
  (`policy_stream_cursors` is the control surface, ADR-0012).
- **A stream's type is assumed fixed.** It is written when the stream is created and
  nothing in the library changes it; a stream whose type an operator changes by hand moves
  in or out of every Policy's tracked set at the next poll, with the consequence above.
- **The filter's narrowing is a property of one variant**, which `Policy::Event` cannot
  infer: an event does not know its stream type, so a consumer declares the filter. A
  Policy with no filter tracks everything, as it did.
