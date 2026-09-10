# Agent Conventions

## PR Titles and Squash Merge

This repository uses squash merge as the default merge strategy.

Rule:

- The pull request title MUST follow Conventional Commits format, because the squash commit message is derived from the PR title.

Use:

- `type(scope): short summary`

Examples:

- `feat(persistence): inline projections walking skeleton`
- `fix(es): prevent duplicate event application`
- `docs(readme): clarify wasm test command`

Allowed `type` values (recommended):

- `feat`, `fix`, `docs`, `refactor`, `test`, `chore`

Before opening or updating a PR:

- Ensure the PR title is already in Conventional Commits format.
- If not, update it with `gh pr edit --title "type(scope): ..."`.

## Preserving Review History

Squash merge collapses each PR into a single commit, so messy intermediate
history on a branch is fine — but it MUST stay reviewable while the PR is open.

Rules while a PR is under review:

- NEVER force-push (`git push --force` / `--force-with-lease`) to a branch that
  has an open PR. Force-pushing rewrites history and breaks reviewers'
  incremental "changes since last review" diffs.
- Address review feedback with NEW commits on top, even for fixups. The squash
  merge flattens them at the end, so extra commits cost nothing.
- To bring in changes from `main`/upstream, use a MERGE commit
  (`git merge upstream/main`), never a rebase. A merge commit keeps previously
  reviewed commits unchanged so reviewers only see the new delta.

The only time history may be rewritten is before a branch has ever been pushed
or before any PR exists for it.

## Bounded memory: no unbounded collections in library code

This library streams. A caller may hand us a `Vec` and a caller may ask for one back —
that is their memory. But **inside** the library, no buffer may grow with the size of
the data it is reading or writing.

Every internal path must be one of:

- **streamed** — `.fetch()`, `TryStream`, fold as you go (`stream_events`,
  `fetch_aggregate_at`, `Cqrs::execute`, `compact`);
- **limited** — the SQL carries a `LIMIT` bound by a tunable (`read_feed`);
- **chunked** — a buffer of a fixed maximum, flushed and cleared
  (`store_events_stream`'s projection flush).

"Small in practice" is not a bound. Neither is "our biggest customer only has a
thousand". A bound is a number in the code.

### Why this is a rule and not a preference

It has already cost a production outage. A `Vec<PersistedEvent>` in
`store_events_stream` that grew with the append put 769 MB on the heap of a consuming
service and OOM-killed it roughly every nine minutes (funkode-io/replay#146, symbolized
against the unstripped binary). The buffer was three lines and looked harmless.

ADR-0005 decided "stream-first" but scoped it to the command → store write path. The
inline-projection paths were written later, against a batch-shaped
`InlineProjection::handle(&[PersistedEvent])`, and nobody asked whether the rule
applied to them. It did. **The rule is the whole library, not the path the ADR that
coined it happened to be about.**

### What enforces it

- `persistence/tests/bounded_queries.rs` — every `fetch_all` in `persistence/src` must
  be listed with the bound that makes it safe. A new one fails the build until its
  author writes that sentence. It is a review prompt, not a proof: it cannot tell a
  bounded query from an unbounded one, only whether the question was asked.
- The allocation-budget tests (`*_allocations.rs`) pin specific paths, in bytes, and
  CI publishes the measurements on every pull request.

Both are point defences. Neither can see a new path that allocates in a new way, which
is why this section exists.

### Reviewing for it

When reviewing a change to `es/` or `persistence/`, treat these as findings against a
documented standard, not as style notes:

- `fetch_all`, `collect::<Vec<_>>()`, or `try_collect()` over a query or a stream whose
  length comes from the data;
- a `Vec` that accumulates inside a loop over a stream without a flush;
- a new trait method taking `&[T]` or returning `Vec<T>` on an internal seam, where the
  slice's length is the size of the data — ask whether it should take a stream, as
  `Compactable::compacted_events` does;
- a doc comment promising bounded memory ("never has to live fully in memory") where the
  code beneath it buffers.

And when the answer is a deliberate bounded buffer, say what bounds it — in the code, in
a comment, or in the allowlist.

## Agent skills

The skills themselves are vendored in `.github/skills/` so they travel with the repo
and are picked up by the GitHub Copilot coding agent — no machine-local install
needed. See `.github/skills/README.md`. They read their per-repo configuration from
the files below.

### Issue tracker

Issues live in GitHub Issues on the `upstream` repo `funkode-io/replay`; the `origin`
fork `carlos-verdes/replay` has issues disabled, so every `gh` call needs an explicit
`--repo funkode-io/replay`. See `docs/agents/issue-tracker.md`.

### Triage labels

The default vocabulary: `needs-triage`, `needs-info`, `ready-for-agent`,
`ready-for-human`, `wontfix`. See `docs/agents/triage-labels.md`.

### Domain docs

Single-context: one `CONTEXT.md` and one `docs/adr/` at the repo root, shared by all
workspace crates. See `docs/agents/domain.md`.
