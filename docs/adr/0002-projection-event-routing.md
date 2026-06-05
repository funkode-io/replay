# Projection event routing by deserialize-or-skip over JSON

**Status:** accepted

Inline projections declare their consumed events as a `query_events!`-merged enum
(`type Event`), exactly like the existing live `Query`, and the store routes
events to them by **attempting to deserialize and skipping on failure**. On append,
each event is serialized to `serde_json::Value` (the store serializes to JSON for
storage anyway) and the batch is offered to every registered projection; the
internal erased wrapper deserializes the batch into `Vec<PersistedEvent<P::Event>>`,
dropping events that aren't one of the projection's types, and calls the typed
`handle` once with whatever remains (skipping the projection entirely if nothing
matches). This lets the store hold a heterogeneous `Vec` of projections without
naming each one's event type, and keeps inline projections symmetric with `Query`
so the `query_events!` machinery is reused rather than reinvented.

## Considered options

- **Type-keyed dispatch** via an associated `Source: EventStream`, routing by
  `stream_type`. Rejected for now: exact and cheaper on the hot path, but it
  diverges from the `Query` authoring model and adds trait surface; the chosen
  approach reuses proven code and stays uniform with live queries.

## Consequences

- Routing is by JSON *shape*: two aggregates with structurally compatible event
  payloads could in principle mis-route. Event enums are externally tagged by
  variant name, so collisions are unlikely; revisit if it bites.
- Each registered projection pays a `from_value` attempt (with a `Value` clone)
  per append. Negligible at catalog write volumes; if it ever matters, projections
  can be pre-indexed by `event_type` or the planned in-memory `StreamFilter`
  evaluation can pre-filter before deserialization.
- This routing lives in the native-only `persistence` crate, so the erased
  wrapper and its `BoxFuture`/`Send` bounds are fine. The `query_events!` macro it
  reuses is itself WASM-compatible (it lives in `macros`); reusing it here does
  not change that — only the Postgres-side routing is native-only.
