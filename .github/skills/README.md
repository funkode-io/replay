# Vendored agent skills

Skills in this directory are checked into the repo so they are available to every
agent that works on `replay` — including the GitHub Copilot coding agent, which
reads `.github/skills/` — without anyone needing a machine-local install.

## Provenance

| Skill                       | Source                                        |
| --------------------------- | --------------------------------------------- |
| `address-pr-review`         | written for this repo                         |
| `code-review`               | [mattpocock/skills](https://github.com/mattpocock/skills) |
| `diagnosing-bugs`           | [mattpocock/skills](https://github.com/mattpocock/skills) |
| `domain-modeling`           | [mattpocock/skills](https://github.com/mattpocock/skills) |
| `implement`                 | [mattpocock/skills](https://github.com/mattpocock/skills) |
| `resolving-merge-conflicts` | [mattpocock/skills](https://github.com/mattpocock/skills) |
| `tdd`                       | [mattpocock/skills](https://github.com/mattpocock/skills) |
| `to-spec`                   | [mattpocock/skills](https://github.com/mattpocock/skills) |
| `to-tickets`                | [mattpocock/skills](https://github.com/mattpocock/skills) |
| `triage`                    | [mattpocock/skills](https://github.com/mattpocock/skills) |

The vendored `mattpocock/skills` content is MIT licensed, Copyright (c) 2026 Matt
Pocock. See <https://github.com/mattpocock/skills/blob/main/LICENSE>.

## Configuration

These skills read their per-repo configuration from `docs/agents/`:

- `docs/agents/issue-tracker.md` — issues live on `funkode-io/replay`
- `docs/agents/triage-labels.md` — the triage label vocabulary
- `docs/agents/domain.md` — `CONTEXT.md` and `docs/adr/` layout

## Updating

These are vendored copies, so `npx skills update` will not touch them. To refresh one,
re-copy it from upstream and commit the diff:

```sh
npx skills add mattpocock/skills@<name> -y   # into a scratch dir, then copy in
```

Prefer vendoring here over a global (`-g`) install, so the skills travel with the repo.
