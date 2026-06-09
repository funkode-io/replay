# Commands emit events as a stream, the store ingests it under one transaction

**Status:** accepted

Reads and writes were asymmetric. The read side is lazy —
[`EventStore::stream_events`](../../persistence/src/store.rs) returns an
`impl TryStream` and folds events with bounded memory. The write side was eager:
[`Aggregate::handle`](../../es/src/aggregate.rs) returns `Vec<Event>`, and
[`Cqrs::execute`](../../persistence/src/cqrs.rs) hands that whole `Vec` to
`store_events(&[Event])`. A command that emits a very large batch (the motivating
case: a bulk import producing ~160k row events into a single stream) must
therefore materialise every event in memory at once, even though the aggregate
itself only needs to fold tiny bounded state (e.g. counters).

The goal is **stream-first**: the canonical event path is a stream, and arrays
become a convenience layered on top — *without breaking the existing
`handle -> Vec` contract* that every current aggregate depends on.

## Decision

**Producer (the `es` crate).** `Aggregate` gains `handle_stream`, returning an
**owned** stream of raw domain events:

```text
async fn handle_stream(&self, cmd, services)
    -> Result<impl Stream<Item = Result<Self::Event, Error>>, Self::Error>
```

- `handle` gets a default returning `Ok(vec![])`; `handle_stream` gets a default
  that wraps `handle` via `stream::iter`. The defaults point in **one direction**
  (`handle_stream → handle`), so there is no mutual recursion. Existing aggregates
  implement `handle` and get streaming for free; bulk aggregates implement
  `handle_stream` and never build a `Vec`.
- The returned stream is **owned** and does not borrow the aggregate, so the
  orchestrator can hold `&mut aggregate` to fold each event as it streams past
  (apply-as-you-stream). The producer reads its external source (a file/reader),
  not evolving aggregate state — it yields **raw `Self::Event`**, before any id or
  version exists.
- Both WASM cfg arms of `Aggregate` carry the two defaulted methods: `+ Send` on
  the future *and* the inner stream in the native arm, neither under
  `target_arch = "wasm32"`.

**Consumption (the native `persistence` crate).** `Cqrs::execute` calls **only**
`handle_stream` ("stream-first internally") and still returns the folded
aggregate. The store owns the loop and the transaction; the orchestrator injects
a sink:

- `EventStore::store_events_stream` becomes the **canonical** method: it drives a
  `TryStream<Ok = S::Event>` into storage under **one transaction**, notifying an
  `EventSink<E>` per appended event. `store_events(&[Event])` becomes a default
  that wraps the slice in `stream::iter` with a `NoSink`. Both the Postgres and
  in-memory backends implement only the streaming method; in-memory honours the
  same all-or-nothing semantics.
- `EventSink<E>` is a named trait — `fn on_event(&mut self, &PersistedEvent<E>)` —
  with a blanket impl for `FnMut(&PersistedEvent<E>)` (so closures work
  everywhere) and a provided `NoSink`. The sink is an **infallible observer**:
  `apply` is sync and side-effect-free by definition, so the sink can never abort.
  Atomicity is owned **solely** by the producer yielding `Err` and by database
  errors. `Cqrs`'s sink is simply `|e| aggregate.apply(e.data.clone())`.
- Because `PersistedEvent` lives only in `persistence`, the whole sink/store
  machinery is **native-only, single-arm**; only `handle_stream` in `es` needs the
  dual-cfg treatment.

**Optimistic concurrency.** A streamed append checks `expected_version`
**once at the head** of the transaction and then appends the whole stream without
re-checking. The `SELECT ... FOR UPDATE` on the stream row already serialises
concurrent writers for the transaction's duration, so no mid-batch race is
possible. This also corrects a latent bug: the previous per-event check compared
the same `expected_version` against a stream version that increments on every
append, so any multi-event batch with `expected_version` set would have failed on
the second event.

## Consequences

- A large atomic append now holds **one long-running write transaction** for the
  duration of the import (locks held, WAL retained). This is the deliberate price
  of all-or-nothing semantics with bounded memory; chunked/resumable commits were
  rejected for this iteration.
- An aggregate that implements **neither** `handle` nor `handle_stream` compiles
  and silently swallows commands (empty stream). Mitigated by documentation
  ("implement at least one") rather than added machinery.
- `expected_version` now means "the stream head when my decision began," checked
  once — a small but real change to the append concurrency contract.
- Per-event metadata is intentionally **not** supported: one batch-level
  `Metadata` is stamped on every event in the append, as before. Per-event
  provenance belongs in the event payload and can be added later without breaking
  this design.
