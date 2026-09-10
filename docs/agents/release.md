# Release configuration

Per-repo facts for the `publish-release` skill. A release is three artefacts that must
agree: the **tag** on upstream, the **crates** on crates.io, and the **release notes**.

## Upstream

| | |
| --- | --- |
| Repo slug | `funkode-io/replay` |
| Remote | `upstream` (the `origin` fork `carlos-verdes/replay` is never released from) |
| Default branch | `main` |
| Tag format | `vX.Y.Z`, annotated, on the bump commit |

Every `gh` call needs `--repo funkode-io/replay`.

## Version policy

Pre-1.0 semver as Cargo reads it: `0.y` is the compatibility line.

- **Breaking** change to a public signature or semantics → bump the **minor** (`0.9.0` →
  `0.10.0`).
- **New** public API, no breakage → bump the **minor** too. Patch releases (`0.5.1`) are
  reserved for fixes with no API delta.
- Mark breaking PR titles with `!` (`feat(persistence)!: ...`) so the inventory is
  readable from the commit list — but confirm each one against the diff, not the subject.

## Version files

The workspace version lives in one place, but each crate pins its siblings by an explicit
version so a published crate resolves against the release rather than a path. **All four
must move together**, or publishing a downstream crate resolves the previous release:

| File | What to change |
| --- | --- |
| `Cargo.toml` | `[workspace.package] version` |
| `macros/Cargo.toml` | `replay` pin |
| `persistence/Cargo.toml` | `replay`, `replay-macros` pins |
| `macros-tests/Cargo.toml` | `replay`, `replay-macros` pins |

`Cargo.lock` is gitignored (this is a library workspace) — do not commit it.

## Publish set and order

Dependency order, so each crate's dependencies already exist on crates.io at the new
version:

1. `es-replay` (`es/`)
2. `es-replay-macros` (`macros/`) — depends on `es-replay`
3. `es-replay-persistence` (`persistence/`) — depends on both

`es-replay-macros-tests` (`macros-tests/`) is an internal test crate and is **not**
published; the `0.1.1` on crates.io is a historical accident, leave it there.

```sh
cargo publish -p es-replay --dry-run   # then without --dry-run
cargo publish -p es-replay-macros
cargo publish -p es-replay-persistence
```

## Pre-flight gate

The same checks CI runs (`.github/workflows/ci.yml`) and the pre-commit hook enforces
(`.githooks/pre-commit`, installed with `make install-hooks`):

```sh
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace          # needs Docker: testcontainers starts Postgres
```

The allocation-budget tests (`persistence/tests/*_allocations.rs`) are part of that run and
are the guard on the bounded-memory standard in `AGENTS.md` — a release must not go out
with them failing or skipped.

`make wasm-test` (`wasm-pack test --headless --chrome macros-tests`) covers the wasm
target; run it when the release touches `es/` or `macros/`.

## Release notes

Match the house style of the previous release page (`gh release view v<prev> --repo
funkode-io/replay`): Highlights, Breaking changes, New API, Migration, Changes since
`<prev>`, and a `**Full diff:**` compare link. Sample code uses this repo's own example
domain — `BankAccountUrn`, `BranchUrn`, `BankAccountEvent` — so the snippets read like the
README a consumer already has open. Compile-check non-trivial snippets in a scratch test
under `macros-tests/tests/`, then delete it.
