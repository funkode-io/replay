# Nested scoped URNs: the left-most `@` is the outermost scope

**Status:** accepted

A [Scoped URN] embedded exactly one scope: `at` refused a scope that was itself scoped,
and `extract_scope` refused an NSS holding more than one `@` as *ambiguous*. That made an
identity which is itself scoped unusable as a scope — a wallet-scoped user
(`urn:user:0x78@wallet-type:evm`) could not own a watchlist
(`urn:watchlist:main@user:0x78@wallet-type:evm`). We allow scopes to nest to any depth and
resolve the ambiguity by **splitting on the first `@`** rather than by forbidding the
second.

## Decisions

- **Scoping is a right-nested grammar, not a pair.** A scoped URN is

  ```text
  scoped := urn:<nid>:<own-nss-without-@>@<scoped>
  ```

  The scope suffix is itself a complete (possibly scoped) URN, so depth is unbounded and
  the shape is uniform at every level. Nothing in the format changed — a single-scope URN
  is the depth-1 case of the same grammar, so every existing id keeps its meaning and no
  stored id is migrated.

- **The invariant that makes first-`@` parsing unambiguous is a precondition on `at`'s
  *receiver*, not on its argument.** `at` continues to refuse a receiver whose NSS already
  contains `@`; it now accepts an argument that does. Because an aggregate's own NSS
  therefore never contains `@`, **the left-most `@` in a scoped URN is always the outermost
  scope boundary**. `extract_scope` and `unscoped` both split there. The asymmetry is the
  whole design: the earlier symmetric rule ("neither side may contain `@`") bought
  unambiguity by amputating the recursion, when only the left half of it was load-bearing.
  Re-scoping a scoped URN is still an error — the caller must `unscoped()` first, which
  states which of the two scopes was meant.

- **`extract_scope` peels exactly one level and stays total.** It returns the scope URN
  *with its own scope still attached*, so the caller peels again to go deeper:
  `watchlist → user@wallet-type → wallet-type`. Each step is the same call, validated by
  the same `TryFrom<Urn>` NID check, so a wrong expected type still fails loudly. No
  `scope_chain()`, `root_scope()` or depth-indexed accessor is introduced: recursion over a
  self-similar grammar needs no new vocabulary, and adding it before a caller needs it
  would freeze an interface around a guess.

- **`unscoped` drops the entire scope in one step.** It answers "what was scoped?", which
  is depth-independent — everything from the first `@` onward is scope. It is not the
  inverse of one `extract_scope` peel at depth > 1, and does not need to be: `at` and
  `unscoped` remain exact inverses, which is the law that matters.

- **Depth is documented, not enforced.** `streams.id` is `text` under a btree primary key,
  so a pathological chain would eventually hit Postgres's index-row limit (~2704 bytes) —
  a storage-level failure, loud and at write time. A hard depth cap in `es` would be a
  number invented in the wrong layer: `es` knows nothing of the store, and a WASM caller
  has no such limit at all. Deep chains are a modelling smell long before they are a
  storage problem.

## Rejected alternatives

- **Keep one level; flatten the discriminator into the NSS**
  (`urn:user:evm:0x78`, then `urn:watchlist:main@user:evm:0x78`). This works today —
  `extract_scope` splits the scope on its first `:`, so colons inside a scope's NSS survive
  — and it was the recommended workaround. But it dissolves a typed `WalletTypeUrn` into an
  untyped string prefix that every caller re-parses by hand, which is exactly the
  duplication `ScopedUrn` exists to prevent. It also mis-states the model: the wallet type
  *scopes* the user, it is not a syllable of the user's id.

- **Parse from the right (last `@` is the outermost scope).** Symmetric on paper, but the
  invariant would then have to constrain the *scope* argument instead of the receiver —
  i.e. an aggregate could never be scoped by anything containing `@`, which is the
  restriction we are lifting. It also makes `unscoped` O(depth) string surgery instead of
  one split.

- **Escape `@` inside a nested scope** (`%40` or a doubled `@@`). Removes the need for any
  invariant, at the cost of making every stored id unreadable, breaking the ids already
  written, and requiring an unescape step in every consumer that today reads a stream id as
  a plain string. The grammar buys the same unambiguity for free.

- **A separate `at_nested` / `at_scoped` entry point.** Two spellings of one operation,
  with callers choosing based on a property of the argument they may not know. `at` already
  had the right signature; it was the guard that was wrong.

## Consequences

- A pure relaxation of the public API: calls that returned `Err` now return `Ok`, and no
  call that succeeds today changes meaning. The two removed guards ("scope URN is already
  scoped", "multiple `@` in NSS") no longer fire, so a caller matching on those messages
  loses a branch it can no longer reach.
- `persistence` is untouched: stream ids are opaque `text` there, with no scope-aware query,
  no `LIKE`, and no migration. The whole change lives in `es/src/stream.rs`, the only place
  in the workspace that parses `@`.
- Consumers **outside** the workspace that split a stream id on `@` assuming at most one
  must move to first-`@` parsing, or they will mis-read a nested id.
- The invariant is now load-bearing in a way it was not before. Any future API that
  constructs a scoped URN — a builder, a deserializer, a macro — must preserve "own NSS
  contains no `@`", or decomposition silently returns the wrong half. `at` is currently the
  only such constructor, and it enforces it.

[Scoped URN]: ../../CONTEXT.md#scoped-urn
