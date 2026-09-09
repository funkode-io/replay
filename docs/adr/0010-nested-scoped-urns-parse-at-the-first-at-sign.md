# Nested scoped URNs: the left-most `@` is the outermost scope

**Status:** accepted

Scoping used to allow one level only — `at` refused a scope that was itself scoped, and
`extract_scope` refused a second `@` as ambiguous — so a wallet-scoped user
(`urn:user:0x78@wallet-type:evm`) could not own a watchlist. Scopes now nest, and
decomposition splits on the **first** `@` instead of forbidding the second:

```text
urn:watchlist:main@user:0x78@wallet-type:evm
    extract_scope  →  urn:user:0x78@wallet-type:evm   (peels one level)
    unscoped       →  urn:watchlist:main             (drops the scope whole)
```

That is unambiguous because `at` still refuses to scope an already-scoped **receiver**,
so a base never contains `@`. The guard stays on the receiver and comes off the argument;
that asymmetry is the whole change. A single-scope URN is the depth-1 case, so no stored
id changes meaning.

## Considered options

- **Flatten the discriminator into the NSS** (`urn:user:evm:0x78`). Works today, but turns
  a typed `WalletTypeUrn` into a prefix every caller re-parses, and says the wallet type is
  part of the user's id rather than its scope.
- **Parse from the right.** Would move the guard onto the argument — the restriction being
  lifted — and makes `unscoped` walk the whole chain.
- **Escape `@` in nested scopes** (`%40`). Unreadable ids, breaks those already written,
  needs an unescape in every consumer.
- **A separate `at_nested`.** Two spellings of one operation. `at` had the right signature.
- **`scope_chain()` / `root_scope()`.** No caller needs them; `extract_scope` recurses.

## Consequences

- Pure relaxation: `Err` becomes `Ok`, nothing that succeeds today changes. The messages
  "scope URN is already scoped" and "multiple `@` in NSS" are gone.
- Confined to `es/src/stream.rs`, the only place that parses `@`. `persistence` stores ids
  as opaque `text` — no query change, no migration. Consumers outside the workspace that
  split on `@` must take the first one.
- Depth is unenforced. `streams.id` is `text` under a btree PK, so a pathological chain
  fails at write time on Postgres's index-row limit; a cap in `es` would be a storage
  number in a crate that knows nothing of storage.
- Any future constructor of a scoped URN must keep `@` out of the base, or decomposition
  returns the wrong half. Today `at` is the only one.

[Scoped URN]: ../../CONTEXT.md#scoped-urn
