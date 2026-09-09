# Nested scoped URNs: the left-most `@` is the outermost scope

**Status:** accepted

A [Scoped URN] embedded exactly one scope: `at` refused a scope that was itself scoped,
and `extract_scope` refused an NSS holding more than one `@` as *ambiguous*. That made an
identity which is itself scoped unusable as a scope — a wallet-scoped user
(`urn:user:0x78@wallet-type:evm`) could not own a watchlist. Scopes now nest to any depth
under the grammar `scoped := urn:<nid>:<own-nss-without-@>@<scoped>`, decomposed by
splitting on the **first** `@` rather than by forbidding the second. What makes that
unambiguous is a precondition on `at`'s **receiver**, not its argument: `at` still refuses
to scope an already-scoped URN, so an aggregate's own NSS never contains `@` and the
left-most one is always the outermost boundary. The asymmetry is the whole design — the
old symmetric rule bought unambiguity by amputating the recursion when only its left half
was load-bearing. `extract_scope` peels exactly one level, returning a scope that may
itself be peeled; `unscoped` drops the whole scope in one step, since "what was scoped?"
is depth-independent. A single-scope URN is the depth-1 case of the same grammar, so no
stored id changes meaning.

## Considered options

- **Keep one level; flatten the discriminator into the NSS** (`urn:user:evm:0x78`). Works
  today — `extract_scope` splits the scope on its first `:`, so colons survive — but it
  dissolves a typed `WalletTypeUrn` into an untyped prefix every caller re-parses by hand,
  and mis-states the model: the wallet type *scopes* the user, it is not a syllable of its id.
- **Parse from the right.** Symmetric on paper, but the invariant would then have to
  constrain the scope argument instead of the receiver — exactly the restriction being
  lifted — and makes `unscoped` O(depth) instead of one split.
- **Escape `@` inside a nested scope** (`%40`). Needs no invariant, at the cost of
  unreadable ids, breaking those already written, and an unescape step in every consumer.
- **A separate `at_nested` entry point.** Two spellings of one operation, chosen on a
  property of the argument the caller may not know. `at` had the right signature; the guard
  was wrong.
- **A `scope_chain()` / `root_scope()` accessor.** Recursion over a self-similar grammar
  needs no new vocabulary; adding one before a caller needs it freezes an interface around
  a guess.

## Consequences

- A pure relaxation: calls that returned `Err` now return `Ok`, and nothing that succeeds
  today changes meaning. Callers matching on the two removed messages ("scope URN is
  already scoped", "multiple `@` in NSS") lose an unreachable branch.
- `persistence` is untouched — stream ids are opaque `text` there, with no scope-aware
  query and no migration. The change lives entirely in `es/src/stream.rs`, the only place
  in the workspace that parses `@`. Consumers **outside** it that split on `@` assuming at
  most one must move to first-`@` parsing.
- Depth is unbounded and deliberately unenforced: `streams.id` is `text` under a btree PK,
  so a pathological chain fails loudly at write time on Postgres's index-row limit. A cap
  in `es` would be a number invented in the wrong layer — `es` knows nothing of the store,
  and a WASM caller has no such limit. Deep chains are a modelling smell first.
- The invariant is now load-bearing. Any future constructor of a scoped URN — builder,
  deserializer, macro — must preserve "own NSS contains no `@`", or decomposition silently
  returns the wrong half. `at` is currently the only one, and it enforces it.

[Scoped URN]: ../../CONTEXT.md#scoped-urn
