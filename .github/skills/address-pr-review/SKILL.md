---
name: address-pr-review
description: 'Review and resolve open review feedback on a pull request against its upstream repository, then keep the branch in sync without rewriting history. Use when asked to "review my PR", address PR/review comments, respond to reviewer feedback, resolve review threads, or sync a feature branch with upstream before review. Covers: resolving the upstream remote (from git remote -v when not configured), challenging vs. fixing each comment, replying to and resolving threads with gh, and detecting/handling a branch that is behind upstream main (conflict check, user confirmation, no force-push).'
---

# Address PR Review

Generic, repo-agnostic workflow for taking a pull request from "has open review
feedback and/or is behind upstream" to "all threads addressed and resolved, branch
in sync" — while preserving a clean, incrementally-reviewable history.

## When to Use
- Asked to review your own PR or act on reviewer feedback.
- Resolving open review comments / review threads on a PR.
- Syncing a feature branch with upstream's default branch before or during review.

## Core Constraints (read first)
- **NEVER force-push** (`git push --force` / `--force-with-lease`) to a branch with
  an open PR. Force-pushing rewrites history and breaks reviewers' incremental
  "changes since last review" diffs. Every update — including upstream syncs — must
  be additive (new commits / merge commits only).
- Sync upstream changes with a **merge commit**, never a rebase.
- When in doubt about intent (which remote is upstream, whether to update a stale
  branch), **ASK the user** rather than guessing.

## 0. Resolve the upstream remote
The "upstream" repo is the canonical repo the PR targets (often distinct from the
contributor's fork `origin`).

1. Check whether an `upstream` remote exists:
   ```bash
   git remote -v
   ```
2. If a remote named `upstream` is configured, use it.
3. If not, inspect the `git remote -v` output and **ask the user which remote is the
   upstream** (e.g. `origin` vs a fork). Do not assume. Record the answer for the
   rest of the session.
4. Identify the PR for the current branch:
   ```bash
   gh pr view --json number,headRefName,baseRefName,url
   ```

## 1. Review the PR title and description
Before touching review threads, make the PR's own metadata correct.

```bash
gh pr view <number> --json title,body,files,commits
```

1. **Title follows Conventional Commits.** Ensure the title matches
   `type(scope): summary` (e.g. `feat(api): add pagination`). Allowed types are
   typically `feat`, `fix`, `docs`, `refactor`, `test`, `chore` — defer to the
   repo's own list if it documents one. Many repos derive the squash-merge commit
   message from the PR title, so this matters.
2. **Check repo guidelines / templates.** Look for a PR template and contribution
   guidelines and validate the description against them:
   - `.github/pull_request_template.md`, `.github/PULL_REQUEST_TEMPLATE/` (or root /
     `docs/` variants).
   - `CONTRIBUTING.md`, `AGENTS.md`, `README.md` for title/description rules.
   If required sections are missing or unfilled, plan an update to satisfy them.
3. **Title/description matches the code.** Compare the stated intent against the
   actual diff (`gh pr diff <number>` / the `files` and `commits` above). Flag any
   mismatch — scope creep not mentioned, claims not backed by the diff, or a stale
   description after later commits.
4. **Confirm before editing.** If the title or body need changes, **notify the user
   with the proposed new title/description and ask for an explicit yes/no**. Do NOT
   modify the PR until the user confirms. On "yes", apply:
   ```bash
   gh pr edit <number> --title "type(scope): summary"
   gh pr edit <number> --body-file <file>   # or --body "..."
   ```
   On "no", leave the PR untouched and continue.

## 2. Collect open review comments
Pull the unresolved review threads for the PR (use the upstream repo with
`gh pr view` / `gh api`). Focus only on threads that are still **open/unresolved**.

```bash
gh pr view <number> --json reviews,comments
# Review threads (resolved state) live on the GraphQL API:
gh api graphql -f query='...reviewThreads(isResolved){...}'   # filter isResolved == false
```

## 3. Process each open comment
For every open comment, iterate one at a time:

1. **Challenge when warranted.** If the comment seems incorrect, unnecessary, or in
   tension with the codebase's conventions, push back with reasoning rather than
   blindly applying it.
2. **Ask for clarity when ambiguous.** If you are not confident what the reviewer
   wants, ask the user a clarifying question before changing code. Do not guess at a
   fix you don't understand.
3. **Fix when clear.** If the request is clear enough, implement the smallest change
   that satisfies it. Keep edits scoped to the comment — no drive-by refactors.
4. **Reply and resolve.** After each comment is handled, use `gh` to reply to that
   thread with the action taken (fixed / why challenged / awaiting clarification),
   and, when the thread is genuinely addressed, mark it resolved.

   ```bash
   # Reply to a review comment thread
   gh api repos/<owner>/<repo>/pulls/<number>/comments/<comment_id>/replies \
     -f body="<action taken>"
   # Resolve the thread (GraphQL)
   gh api graphql -f query='mutation { resolveReviewThread(input:{threadId:"<id>"}) { thread { isResolved } } }'
   ```

   Leave a thread open (reply only, no resolve) when it is still blocked on user
   clarification or remains a point of disagreement.

## 4. Check whether the branch is behind upstream
Fetch and compare the PR branch against the upstream default branch (commonly
`main`):

```bash
git fetch <upstream-remote>
git rev-list --left-right --count <upstream-remote>/<default-branch>...HEAD
# left count > 0  => branch is behind upstream
```

If the branch is **not** behind, continue — no sync needed.

If the branch **is** behind, check for merge conflicts before doing anything:

```bash
git merge --no-commit --no-ff <upstream-remote>/<default-branch>
# inspect result, then:
git merge --abort   # if you only wanted to test for conflicts
```

### 3a. Conflicts exist
- **Ask the user** whether they want to update the branch and resolve the conflicts
  before continuing the review.
- Only proceed to merge + resolve conflicts after the user confirms.

### 3b. No conflicts
- **Notify the user** that the branch is behind but conflict-free, then sync it by
  merging upstream into the branch (a merge commit, not a rebase):
  ```bash
  git merge <upstream-remote>/<default-branch>
  ```

## 5. Push updates (no force)
Push all new work — fixes from review and any sync merge commit — as ordinary,
additive commits:

```bash
git push <fork-remote> HEAD
```

Never force-push while the PR is open. If history seems to require rewriting, stop
and ask the user instead.

## Done When
- The PR title follows Conventional Commits and the description satisfies the repo's
  template/guidelines and matches the actual code (with any edits confirmed by the
  user beforehand).
- Every open review thread has a `gh` reply stating the action taken, and resolvable
  threads are marked resolved.
- The branch is either in sync with upstream or the user has explicitly chosen to
  defer the sync.
- All updates were pushed without force-push, keeping the PR incrementally
  reviewable.
