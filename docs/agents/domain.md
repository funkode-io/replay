# Domain Docs

How the engineering skills should consume this repo's domain documentation when exploring the codebase.

`replay` is a **single-context** repo: one `CONTEXT.md` and one `docs/adr/` at the root.
The Cargo workspace splits into `es`, `macros`, `persistence`, and `macros-tests`, but
these are layers of one domain, not separate bounded contexts — they share a single
ubiquitous language.

## Before exploring, read these

- **`CONTEXT.md`** at the repo root — the ubiquitous language for the `replay` event-sourcing library.
- **`docs/adr/`** — read ADRs that touch the area you're about to work in.

If any of these files don't exist, **proceed silently**. Don't flag their absence; don't suggest creating them upfront. The `/domain-modeling` skill (reached via `/grill-with-docs` and `/improve-codebase-architecture`) creates them lazily when terms or decisions actually get resolved.

## File structure

```
/
├── CONTEXT.md                ← glossary: Projection, Live/Inline/Async, Aggregate, …
├── docs/adr/
│   ├── 0001-inline-projection-architecture.md
│   ├── 0002-projection-event-routing.md
│   ├── …
│   └── 0009-compaction-skips-when-nothing-to-compact.md
├── es/                       ← core event-sourcing traits
├── macros/                   ← derive macros
├── macros-tests/             ← macro expansion tests
└── persistence/              ← store implementations, projections, policies
```

New ADRs continue the four-digit sequence (`0010-…`) in `docs/adr/`.

## Use the glossary's vocabulary

When your output names a domain concept (in an issue title, a refactor proposal, a hypothesis, a test name), use the term as defined in `CONTEXT.md`. Don't drift to synonyms the glossary explicitly avoids.

In particular, `CONTEXT.md` requires that "projection" never stand alone — always qualify it as a **Live**, **Inline**, or **Async** projection.

If the concept you need isn't in the glossary yet, that's a signal — either you're inventing language the project doesn't use (reconsider) or there's a real gap (note it for `/domain-modeling`).

## Flag ADR conflicts

If your output contradicts an existing ADR, surface it explicitly rather than silently overriding:

> _Contradicts ADR-0007 (dead-letter retry reproduces reaction from triggering event) — but worth reopening because…_
