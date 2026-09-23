# 27. The policy rule is core vocabulary; the machinery stays in persistence

Date: 2026-09-29

## Status

Accepted. Records the decisions of the PRD in
[funkode-io/replay#244](https://github.com/funkode-io/replay/issues/244), implemented by
[#237](https://github.com/funkode-io/replay/issues/237). Narrows
[ADR-0003](0003-policies-as-checkpointed-background-subscribers.md), which described the
Policy trait and its runner as one thing.

## Context

`Policy`, `Dispatch` and the envelope `react` receives all lived in
`es-replay-persistence`, the crate that also holds `Cqrs`, `PostgresEventStore` and the
runner. Declaring *when this event happens, issue that command* — a business rule, and the
second half of event-storming's `actor → command → event → policy → command → event` —
therefore required the infrastructure crate. In the reference application all 19 policies
sit under `src/infrastructure/` for exactly that reason, despite being domain rules.

The trait carried the rule and its operational knobs together: `stream_filter`,
`start_at`, `read_batch_size`, `checkpoint_batch_size`, `dispatch_timeout` and
`max_causation_depth` alongside `react`. None of them is reachable through
`register_policy_fn`, so all 19 of those policies — every one of them registered as a
closure — ran on defaults with no way to narrow their feed.

`react` took `&PersistedEvent<E>`: eight fields, of which an inventory of those 19
policies reads four. `data` and `stream_id` are read by all 19, `metadata` by 16 and
`created` by 2; `id`, `type`, `version` and `aggregate_version` by none.

## Decision

**The rule is core, the machinery is persistence.** `es-replay` gains `Policy`,
`Dispatch` and `ObservedEvent`. `es-replay-persistence` keeps the feed, the cursor, the
runner, the dead letters and every tunable, and re-exports the three permanently so a
consumer of the runner has one import.

**`ObservedEvent<E>` is the policy's input**, carrying `data`, `stream_id`, `metadata`
and `created`, and nothing else. `created` keeps `DateTime<Utc>`, so `es-replay` takes
chrono — with no default features, because the type is used without the clock and the
crate must keep building for `wasm32`.

**`PersistedEvent` embeds `ObservedEvent` and derefs to it**, so `event.data`,
`event.stream_id`, `event.metadata` and `event.created` keep working at every call site;
`id`, `type`, `version` and `aggregate_version` stay on the outer struct. Struct-literal
construction breaks — `PersistedEvent::of` and its withers are the replacement.

**Dropping `id` from a rule's view is deliberate.** A policy cannot mint a causation key
into a command it emits, so at-least-once delivery is absorbed by **idempotent command
shape** keyed on an identifier the triggering event carries, never by identity dedup.
Nothing else in the envelope is a substitute — `created` is Postgres's transaction-stable
`now()`, shared by every event of one append. The identity route is closed at
both ends anyway: neither `Aggregate::handle` nor `EventStream::apply` receives metadata,
and infrastructure-side dedup on correlation ids is not a guarantee this library offers —
only the application knows whether something genuinely happened twice. This follows the
at-least-once discipline of [ADR-0003](0003-policies-as-checkpointed-background-subscribers.md).

**The trait keeps only the rule:** `type Event`, `name`, `react`. The six tunables move
into a `PolicySettings` value supplied at registration, for a trait policy and a closure
policy alike. `StartAt` and `StreamFilter` stay in the persistence crate — they are feed
vocabulary. This is a gain, not a move: those five knobs were unreachable from a closure
registration.

**`name` stays on the trait** as the single source of the cursor key. The closure
registration keeps taking a name argument, because a closure has nowhere else to carry
one.

**`Dispatch` stays erased.** One reaction must be able to address several aggregate types
— 8 of the 19 policies do, pairing a business dispatch with a dead letter back to the
import stream — so it is not parameterised by target. Its internals become public
accessors, including a consuming `into_parts`, because the runner now reads it across a
crate boundary and the executor *moves* the payload.

**Clean break at `0.11.0`.** No shim accepting the old envelope: it would reintroduce the
dependency the move exists to remove.

## Consequences

A domain crate that depends on `es-replay` alone can declare a policy and unit-test it —
`es/tests/policy_declared_in_core.rs` is that test, and it compiles in a crate with no
path to `Cqrs` or any store.

Every existing policy needs four mechanical edits: the envelope type in the `react`
signature, `PersistedEvent` → `ObservedEvent`, the tunables moved out of the impl, and
the registration call taking settings.

A policy that read `event.id` needs its trigger to carry an operation identifier, and an
event that carries none has to gain one — a real schema change, pushed onto the domain by
this decision. That is the cost of the trade: the envelope cannot be narrowed to what a
rule should read and also keep serving as a dedup key.

The runner gained a seam it did not have: `PolicySettings` is data, so nothing of the
consumer's runs while a worker prepares its drain. The supervision tests that used
`stream_filter` as a place to panic from now use `name`, the one call the worker still
makes into consumer code outside the reaction.

`es-replay` takes a dependency it did not have (chrono). It is a type-level use with no
clock call; the wasm32 build is checked in CI.
