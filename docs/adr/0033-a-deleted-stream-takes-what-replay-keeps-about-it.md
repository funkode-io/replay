# 33. A deleted stream takes what Replay keeps about it

Date: 2026-11-02

## Status

Accepted. Decides [#292](https://github.com/funkode-io/replay/issues/292). Narrows the
permanence of a [Stream place] (ADR-0023, ADR-0026) to the life of a stream, and extends
the [Stream lock wait] (ADR-0022) to a deletion.

## Context

A consumer (dxp-data-loom) hard-deletes catalog aggregates. Their stream ids are name
slugs, so an aggregate created later under the same name gets the deleted one's id. With
no API for it, its runbook deletes Replay's rows by hand.

Replay keeps state about a stream outside its events, and a hand delete gets it wrong in
ways only Replay can see:

- A Policy's [Stream place] outlives the stream. The recreated stream is numbered from 1
  again, so the Policy skips its events up to wherever it had got in the old one.
- A [Dead letter] whose triggering event is gone can never be retried, and it aborts the
  bulk retry of everything its Policy parked.
- A Policy running through the delete can write either of them again afterwards.
- An [Inline projection] is handed events, and a deletion is not one: its rows for the
  stream stay unless the consumer deletes them, and deleting them apart from the rest can
  leave one without the other.

## Decision

**Deleting streams is one store operation that takes everything Replay keeps about them,
with the caller's own read-model deletion inside the same transaction.**

- The streams and all their events go, every Policy's place in them goes, and the dead
  letters their events triggered are deleted — not archived: a deletion is a cleanup.
  Dead letters that had already left the active set stay in the archive as history. The
  caller's read-model deletion runs in the same transaction, and a failure of either half
  undoes both.
- Only streams that exist when the deletion locks them are deleted and handed to the
  caller. An id with no stream is ignored, even if a stream is created under it while the
  deletion runs: the caller never saw it, so it is left whole.
- A stream recreated under a deleted id is a **new stream**: numbered from 1, delivered
  to Policies from its first event, applied to inline projections like any other.
- A running Policy, inline-projection catch-up or rebuild never writes for a stream after
  its deletion: each either finishes first or finds the events gone.
- What refers to the streams only through the transactions that wrote them — an inline
  projection's applied marks (ADR-0032) — stays. Transaction ids are never reused, so it
  matches nothing a recreated stream writes, and ages out as before.
- A dead letter **addressed to** a deleted stream by an event of another stream stays:
  its trigger exists, and whether a command to a deleted aggregate should run is the
  domain's call.

## Considered options

- **A tombstone**: keep the stream's row and its numbering, so a recreated stream carries
  on where the old one stopped and no place needs deleting. Then the stream is not
  deleted: its row pins the old type, every Policy behind its head counts it as owed for
  ever, and the catalog delete the consumer asked for does not happen.
- **Archive the dead letters** with a reason of their own, as every other way out of the
  active set does. A deletion is a cleanup of an aggregate that is gone: a record of
  failures against events that no longer exist is state nobody can act on.
- **An `InlineProjection` hook** for each registered inline projection to forget streams.
  A default that does nothing leaves rows silently, and a consumer's read models are not
  all inline projections; one closure covers both and cannot be forgotten.
- **Delete the applied marks** of the deleted streams' transactions. They do no harm, and
  a mark is about a transaction, not a stream.
- **A method on `EventStore`.** The in-memory store keeps no places, no dead letters and
  no transaction for a caller to join.
- **Require Policies to be stopped** while deleting. The consumer deletes from a running
  service.

## Consequences

- **No migration.**
- **A stream place, like a version, can name two events across a deletion.** Anything
  outside Replay that kept one for a deleted stream is the caller's to clear.
- **A Policy's writes of a first place and of a dead letter now wait for a deletion in
  flight**, and a deletion waits for appends, catch-ups and rebuilds in flight; it gives up
  as `Unavailable` after the stream lock wait.
- **Inline projections are waited for before any stream is locked.** A read model with a
  foreign key to `streams` takes a stream's row when it is written, so the other order
  would deadlock a build or a catch-up against the deletion.
- **A deletion is an operator's action**, priced as one: it costs in proportion to the
  events it deletes.
- **A Policy's first start at `Now` fails instead of seeding a place for a stream being
  deleted** — a place that would skip the recreated stream's first events. The start is
  refused as a conflict and retried, as an initialization error already is.
- **A poll spanning a deletion *and* a recreation can deliver some of the new stream's
  events twice**, the second time in order, as at-least-once delivery already allows.
- **Deleting needs every process upgraded first.** The guarantees above are kept by the
  writers as much as by the deletion: a Policy runner, or a first registration of an
  inline projection, from an earlier release can still write for a stream after its
  deletion — a place that skips the recreated stream's events, a dead letter nothing can
  retry, a view row for nothing. The rollout rule is to call it only once every process
  that runs Policies or registers inline projections runs this release, or is stopped.
- The consumer's runbook SQL can be replaced by this call.

[Stream place]: ../../CONTEXT.md#stream-place
[Stream lock wait]: ../../CONTEXT.md#stream-lock-wait
[Dead letter]: ../../CONTEXT.md#dead-letter
[Inline projection]: ../../CONTEXT.md#inline-projection
