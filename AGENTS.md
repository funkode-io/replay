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
