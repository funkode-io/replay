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

A caller's `Vec` is the caller's memory. **Inside** the library, no buffer may grow
with the size of the data it reads or writes. Every internal path must be:

- **streamed** — `TryStream`, fold as you go (`stream_events`, `fetch_aggregate_at`,
  `Cqrs::execute`, `compact`);
- **limited** — SQL carrying a `LIMIT` bound by a tunable (`read_feed`);
- **chunked** — a buffer of fixed maximum, flushed and cleared
  (`store_events_stream`'s projection flush, bounded by `projection_flush_size`).

"Small in practice" is not a bound. A bound is a number in the code, and a deliberate
bounded buffer says what bounds it, in the code. The public API is stream-first for the
same reason: a result that grows with the data is a stream, and a collecting `_all` twin
is a convenience the library never calls itself
([ADR-0029](docs/adr/0029-the-public-api-is-stream-first.md)).

This is a rule, not a preference: an unbounded `Vec` OOM-killed a consuming service
(#146). What counts as a violation when reviewing is in
`docs/agents/review-standards.md`.

## Events record facts, not state

Consumers model their events on this repository's examples, so the examples, tests
and docs must model events correctly. An event names an act in the past tense and
carries that act's data; a status, ownership flag or any other derived value is
decided in `apply`, never stored in the payload. The rules of thumb, including when
a value is attested and therefore part of the fact, are in the README section
*Events record facts, not state*. Once an event is stored it cannot be fixed, which is
why this is a rule; what counts as a violation when reviewing is in
`docs/agents/review-standards.md`.

## Agent skills

Skills are installed, never vendored. `skills-lock.json` at the root names the ones this
repo's workflow uses, from `mattpocock/skills` and `funkode-io/skills`; a global install
of the same skills serves as well. `make install-skills` restores them from the lock, and
the Copilot coding agent installs them itself in
`.github/workflows/copilot-setup-steps.yml`. Their per-repo configuration lives in
`docs/agents/`: `issue-tracker.md` (issues and PRs live on `funkode-io/replay`; the
`origin` fork has issues disabled, so every `gh` call needs `--repo funkode-io/replay`),
`triage-labels.md`, `domain.md` (read `context-digest.md` first), `release.md`,
`review-standards.md` and `copilot-review.md`.
