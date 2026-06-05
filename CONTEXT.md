# Context: replay

The ubiquitous language for the `replay` event-sourcing library.

## Glossary

### Projection

An umbrella term for a derived read model built from events. "Projection" on its
own never names a single mechanism — always qualify it as a Live, Inline, or
Async projection. Distinct from an [Aggregate], which is the write-side state
rebuilt from a stream to make command decisions.

### Live projection

A read model computed on demand by folding events in memory, without persisting
any state or progress. This is the existing [Query] mechanism. The caller drives
it; nothing is stored between runs.

### Inline projection

A projection whose write is applied **inside the same transaction that appends
the events**, against the **same event store instance**. It is strongly
consistent with the events: the events and the projection write commit together
or not at all. The projection does not begin or commit the transaction — it only
contributes writes to a transaction the store owns.

### Async projection

A projection that updates an **external** system (for example a search index)
**eventually**, decoupled from the append transaction, driven by a background
process that compares projection progress against the event stream. Eventually
consistent rather than strongly consistent. (Planned; not yet implemented.)

### Query

The existing on-demand, in-memory fold over filtered events. It is the
realisation of a [Live projection].

### Projection version

A number declared in projection code that identifies the shape/logic of a
projection. When the version recorded in the store is older than the version in
code, the projection is rebuilt: its state is reset and all events are replayed.

### Rebuild

Discarding a projection's current state (reset) and replaying the full event
history through it to reconstruct it. Triggered when a projection's code version
is newer than the version recorded in the store.

[Aggregate]: #aggregate
[Query]: #query
[Live projection]: #live-projection
