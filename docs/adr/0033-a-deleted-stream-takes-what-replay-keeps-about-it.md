# 33. A deleted stream takes what Replay keeps about it

Date: 2026-11-02

## Status

Accepted. Decides [#292](https://github.com/funkode-io/replay/issues/292). Narrows the
permanence [Stream place] promises (ADR-0023, ADR-0026) to the life of a stream.

## Context

A consumer (dxp-data-loom) hard-deletes catalog aggregates. Their stream ids are name
slugs, so an aggregate created later under the same name gets the deleted one's id. With
no API for it, its runbook deletes rows from `events`, `streams`, `policy_stream_cursors`
and `policy_dead_letters` by hand.

Replay keeps state about a stream outside its events, and a hand delete gets it wrong in
ways only Replay can see:

- A Policy's [Stream place] outlives the stream. The recreated stream is numbered from 1
  again, so the Policy skips its events up to wherever it had got in the old one.
- A [Dead letter] whose triggering event is gone can never be retried, and it aborts the
  bulk retry of everything its Policy parked.
- A running Policy that read the stream before the delete can write a place or park a
  dead letter after it, recreating both problems with no one at the keyboard.
- An [Inline projection] is handed events, and a deletion is not one: its rows for the
  stream stay unless the consumer deletes them, and deleting them outside the deletion's
  transaction can leave one without the other.

## Decision

**Deleting a set of streams is one store operation that takes everything Replay keeps
about them, and runs the caller's own read-model deletion in the same transaction.**

- `PostgresEventStore::delete_streams(ids, read_models)` deletes the streams and all
  their events, deletes every Policy's place in them, archives the dead letters their
  events triggered with reason `stream_deleted`, then runs the caller's closure on the
  same connection, and commits only if it succeeds.
- A stream recreated under a deleted id is a **new stream**: numbered from 1, delivered
  to Policies from its first event, applied to inline projections like any other. Stream
  place permanence holds for the life of a stream; a deletion ends one.
- A Policy writes a place or parks a dead letter only while the event it is about is
  still in the log, so a deletion leaves nothing behind for a Policy that was running
  through it. A reaction already running finishes.
- What refers to the streams only through the transactions that wrote them — an inline
  projection's applied marks (ADR-0032) — is left. Transaction ids are never reused, so
  those marks match nothing a recreated stream writes, and the catch-up horizon clears
  them.
- A dead letter **addressed to** a deleted stream by an event of another stream stays:
  its trigger exists, so it is still retryable, and whether a command to a deleted
  aggregate should run is the domain's call.

## Considered options

- **A tombstone**: keep the `streams` row and its counter, so a recreated stream goes on
  numbering where the old one stopped and no place needs deleting. The stream would not
  be deleted: its row pins the old type, every Policy behind its head would count it as
  owed for ever, and the catalog delete the consumer asked for would not happen.
- **Destroy the dead letters** rather than archive them. Every other way out of the
  active set archives, and a parked failure has never left the schema without a record.
- **An `InlineProjection` hook** that each registered projection implements to forget
  streams. A default that does nothing leaves rows silently, and a consumer's read models
  are not all inline projections; one closure covers both and cannot be forgotten. A hook
  can still be added on top.
- **Delete the applied marks of the deleted streams' transactions.** They do no harm, and
  a mark covers a transaction, not a stream.
- **A method on `EventStore`.** The in-memory store keeps no places, no dead letters and
  no transaction a caller could join.
- **Require Policies to be stopped** while deleting. The consumer deletes from a running
  service, at any time.

## Consequences

- **Migration 0039** adds `stream_deleted` to the archive's reasons. Additive: a running
  release never writes it.
- **`(stream_id, stream_seq)` can name two events across a deletion**, as can
  `(stream_id, version)`. Anything outside Replay that kept a place or a version of a
  deleted stream is the caller's to clear in the closure.
- **A Policy's first place in a stream, and every park, now lock the event** they are
  about until they commit; an update of an existing place does not. A deletion waits for
  such a write in flight, and for an in-flight append, catch-up or rebuild; those waits
  are bounded by the [Stream lock wait] and end as `Unavailable`.
- **A deletion costs about twice the events it deletes**, and finds dead letters by a
  scan of the active set. It is an operator's action, not a hot path.
- **One window is left open**: a Policy started for the first time with
  `StartAt::Now` seeds its places from the stream heads in one unguarded statement, once
  in its life. A deletion committing during that statement can leave a place for the
  recreated stream; deleting that row by hand is a [Cursor move].
- The consumer's runbook SQL (dxp-data-loom PR #2036) can be replaced by this call.

[Stream place]: ../../CONTEXT.md#stream-place
[Dead letter]: ../../CONTEXT.md#dead-letter
[Inline projection]: ../../CONTEXT.md#inline-projection
[Stream lock wait]: ../../CONTEXT.md#stream-lock-wait
[Cursor move]: ../../CONTEXT.md#cursor-move
