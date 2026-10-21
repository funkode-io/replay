# 29. The public API is stream-first

**Status:** accepted

Extends [ADR-0005](0005-stream-first-event-emission.md), which made the stream the
canonical path for writes, to every result the library hands a consumer.

## Context

ADR-0005 decided that the canonical event path is a stream and that arrays are a
convenience layered on top — `store_events_stream` is the primitive, `store_events` is
built on it. It decided that for the write path only. The read side already had
`stream_events`, but nothing said a *new* read had to look like it.

The bounded-memory rule in `AGENTS.md` covers the library's internals: no buffer inside
the library may grow with the data. It says nothing about the shape of what a public
function returns, because "a caller's `Vec` is the caller's memory".

That gap showed up as soon as a consumer asked for new reads. Two requests from the
reference application — listing parked reactions to show an import's errors in a UI
([#260](https://github.com/funkode-io/replay/issues/260)), and finding every stream of a
scope that needs compaction ([#261](https://github.com/funkode-io/replay/issues/261)) —
both asked for **pages**. A page size is a decision about the consumer's workload (50 rows
for a screen, everything for a sweep) made inside a library that cannot know it. Meanwhile
the consumer, lacking any read at all, had been querying Replay's tables directly.

## Decision

**A public function whose result grows with the data returns a stream.** The consumer
decides how much of it to read and how to batch it: `.take(50)` for a screen, the whole
stream for a sweep that acts on each item as it passes.

- **The stream is the implementation; a `Vec` is a convenience built on it.** Where
  collecting is useful, a twin collects the stream — one code path, never two queries — and
  is named for what it does, after `sqlx`'s `fetch` / `fetch_all`: the collecting twin
  carries an `_all` suffix, so a caller can see at the call site that they are choosing to
  hold everything. `store_events` / `store_events_stream` predate this and keep their
  names (ADR-0005 kept the existing contract).
- **The library never calls its own collecting twins.** Inside the library everything
  streams, chunks or carries a `LIMIT`, as `AGENTS.md` already requires. A collecting
  twin exists for consumers only.
- **A stream a consumer may hold while it works reads in short keyset chunks**, not
  through one long-lived query. One query behind the whole stream holds a pool connection
  and a database snapshot for as long as the consumer takes — minutes, for a sweep that
  compacts a hundred thousand streams — and a held snapshot stops `VACUUM` from reclaiming
  anything newer than it. Chunking is invisible to the consumer: it still sees one stream.
- **A stream can start after a key**, so a consumer that cannot hold a stream across
  requests (a paged UI behind HTTP) resumes with `after(last_key).take(n)` instead of the
  library offering pages.
- **A count is its own call**, not the length of a collected stream: "312 errors" must not
  cost loading 312 errors.
- **A result whose size is fixed by code, not data, may return a `Vec`.**
  `PolicyStatusStore::list` returns one row per registered Policy; the number of Policies
  is a number in the consumer's source, not in its database.

## Consequences

- New reads — #260 and #261 first — are specified as streams, and a request for "pages"
  is answered with a stream that starts after a key.
- Reviewers have a rule to cite: a new public function returning a `Vec` sized by data, or
  a page size chosen by the library, is a violation of this ADR, not a style note.
- Existing public functions are not changed by this ADR. `stream_events` reads through one
  query for the life of the stream, which predates the chunking rule; bringing it and any
  other existing read in line is follow-up work, each change to be judged on its own.
- Consumers have one more reason not to read Replay's tables: whatever they need to list
  should exist as a stream, and if it does not, that is a missing read to ask for.
