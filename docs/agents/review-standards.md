# Review standards

What the `code-review` skill, and any reviewer, treats as a violation of a documented
standard in `AGENTS.md`, not a style note. The rules themselves are there; this file is
the checklist and the history that made them rules.

## Bounded memory

A `Vec<PersistedEvent>` in `store_events_stream` that grew with the append put 769 MB on
a consuming service's heap and OOM-killed it roughly every nine minutes (#146). ADR-0005
scoped "stream-first" to the command → store write path; the inline-projection paths
were written later and nobody asked whether the rule applied. It did: the rule is the
whole library.

When reviewing `es/` or `persistence/`, these are violations:

- `fetch_all`, `collect::<Vec<_>>()` or `try_collect()` whose length comes from the data;
- a `Vec` accumulating in a loop over a stream without a flush;
- a new internal seam taking `&[T]` or returning `Vec<T>` sized by the data; ask whether
  it should stream, as `Compactable::compacted_events` does;
- a doc comment promising bounded memory over code that buffers;
- a new public `Vec` sized by data, or a page size chosen by the library
  ([ADR-0029](../adr/0029-the-public-api-is-stream-first.md)).

The allocation-budget tests (`persistence/tests/*_allocations.rs`) pin specific paths in
bytes, but they are a point defence: they cannot see a new path, which is why the
checklist exists.

## Events record facts, not state

These are violations:

- an event or command carrying a status, flag or other value the domain derives;
- an event named after the state it produces (`Promoted`, `MarkedFinal`) rather
  than the act that caused it;
- a correction modelled by re-emitting the original event, or carrying the
  previous value the stream already holds;
- a `compacted_events` that writes state into an event instead of emitting the
  facts that rebuild it.

## Rollout effect

A consumer freezes writes for a Replay upgrade only when the release notes say so
(#347); the notes say so because the PR did. When reviewing a PR that touches
`persistence/tests/migrations/`, the `projections` or `projection_applied_transactions`
tables, or the `events` log and its functions, these are violations:

- a PR body without a `Rollout:` line, or one that says `rolling` for a step that is
  unsafe while the previous release serves and appends (a first `build()` of a
  non-idempotent handler, a horizon the old release cannot read);
- a new migration whose header comment does not say whether it is online-safe, or that
  claims it is while rewriting a populated table, validating a constraint with a scan,
  or taking `ACCESS EXCLUSIVE` for longer than an instant;
- a `docs/releases/<version>.md` whose Migration table lacks the Online-safe column.
