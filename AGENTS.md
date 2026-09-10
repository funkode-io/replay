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

"Small in practice" is not a bound. A bound is a number in the code.

This is a rule, not a preference: a `Vec<PersistedEvent>` in `store_events_stream` that
grew with the append put 769 MB on a consuming service's heap and OOM-killed it roughly
every nine minutes (#146). ADR-0005 scoped "stream-first" to the command → store write
path; the inline-projection paths were written later and nobody asked whether the rule
applied. It did — the rule is the whole library.

When reviewing `es/` or `persistence/`, treat these as violations of a documented
standard, not style notes:

- `fetch_all`, `collect::<Vec<_>>()` or `try_collect()` whose length comes from the data;
- a `Vec` accumulating in a loop over a stream without a flush;
- a new internal seam taking `&[T]` or returning `Vec<T>` sized by the data — ask whether
  it should stream, as `Compactable::compacted_events` does;
- a doc comment promising bounded memory over code that buffers.

When a buffer is a deliberate bounded one, say what bounds it, in the code. The
allocation-budget tests (`*_allocations.rs`) pin specific paths in bytes, but they are a
point defence: they cannot see a new path, which is why this section exists.

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

### Releases

The upstream slug, the version policy, the four `Cargo.toml`s carrying the version, the
ordered `cargo publish` set and the pre-flight gate. See `docs/agents/release.md`.
