# 28. A single-target policy declares its target, and a blanket impl keeps the runner erased

**Status:** accepted

Extends [ADR-0027](0027-the-policy-rule-is-core-vocabulary.md), which moved the rule into
`es-replay` and kept `Dispatch` erased (its decision 6). Implements
[#239](https://github.com/funkode-io/replay/issues/239), a slice of the PRD in
[#244](https://github.com/funkode-io/replay/issues/244).

## Context

`Policy::react` returns `Vec<Dispatch>`, erased so one reaction can address several
aggregate types. A rule whose commands all go to one aggregate pays that erasure anyway:
its unit test can only downcast (`Dispatch::parts::<A>()`) to see what it asked for.

The inventory of the 19 policies in the reference application decided the shape: 8 are
genuinely multi-target (a business command plus a dead letter back to the import
stream), so the erased `Policy` must stay; 16 attach metadata computed once per reaction
and applied to every dispatch of it; and all 19 register through `register_policy_fn`, so
a trait with no closure twin would ship unused.

## Decisions

- **The target is an associated type, not a parameter of `Dispatch`.** `AggregatePolicy`
  declares `type Target: Aggregate` and returns
  `Vec<(<Target as WithId>::StreamId, <Target as Aggregate>::Command)>`. `Dispatch` is
  untouched, so the runner, the dead-letter identity and the multi-target reactions are
  untouched too.

- **A blanket `impl<P: AggregatePolicy> Policy for P`**, rather than a `Typed<P>` wrapper
  the consumer registers. Nothing changes at the registration or in the runner, which is
  what the 8 multi-target policies and every existing `impl Policy` need. The cost is the
  rule below.

- **A type implements one trait or the other, never both.** A second `impl Policy` for a
  type the blanket impl already covers is `E0119`. A downstream `impl Policy for X`
  coexists with the blanket impl — verified by compiling a two-crate reproduction — so
  this constrains one type, not one crate.

- **Metadata is a hook, not a return type.** `dispatch_metadata` defaults to `None` and
  runs **once per reaction**, before the pairs become dispatches, so the correlation id
  the 16 metadata-attaching policies mint is identical on every command of that reaction.
  Putting it in the return type would have made the typed return — the thing a test
  asserts with `==` — carry bookkeeping the assertion is not about.

- **`AggregatePolicy` is exported from both crates but in neither prelude.** With `Policy`
  in scope every implementor has two `react`s and a method call is ambiguous (`E0034`).
  Keeping the trait out of the preludes leaves `policy.react(&event)` available to the
  single-target test the trait exists for.

- **The closure registration gets a typed twin.** `register_aggregate_policy_fn` takes the
  target as a type parameter and a closure returning pairs;
  `register_aggregate_policy_fn_with_metadata` adds the per-reaction hook. Without them
  the trait would be unreachable from the registration all 19 policies use.

## Consequences

A single-target reaction is asserted as a value: `assert_eq!(policy.react(&event), vec![(id, command)])`,
with no `Dispatch`, no downcast and no store — `es/tests/single_target_policy.rs`.

A rule that grows a second target type converts back to a raw `Policy`: the pairs become
`Dispatch::to::<A>` calls and the metadata hook becomes `Dispatch::with_metadata`. That is
the migration the blanket impl's `E0119` announces, and it is mechanical.

`Policy` remains the contract the runner knows. Nothing about erasure, dead letters or
settlement changes, so ADR-0007, ADR-0021 and ADR-0024 are untouched by this.
