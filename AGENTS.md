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
