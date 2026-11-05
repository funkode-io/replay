# 36. A busy command is held, not dead-lettered

Date: 2026-11-04

## Status

Accepted. Decides the runner half of
[#308](https://github.com/funkode-io/replay/issues/308), with
[ADR-0035](0035-a-command-passes-through-middleware-before-it-runs-and-before-it-commits.md).
Adds one outcome to the resilience table of the [Policy runner]; the rest of it, and
[ADR-0021](0021-retry-settles-a-reaction-not-a-row.md), are unchanged.

## Context

A [Command middleware] can answer `Busy`: the command is not wrong and nothing failed, it
cannot run *now* — the motivating case is maintenance mode. The caller must be able to
tell that from a failure, and so must the runner, which today treats every retryable error
the same way: three attempts over well under a second, then a [Dead letter]. A maintenance
window lasts minutes, so every Policy that reacted during it would park its commands, and
an operator would retry them one by one afterwards.

No existing `ErrorKind` says "not now" without saying something else. `Unavailable` is what
Replay already returns for a row-lock wait or a pool timeout, which must still reach a dead
letter once their retries run out; `RateLimited` names a different cause, and a consumer's
own `RateLimited` errors would stop being parked.

## Decision

**`Busy` is its own `ErrorKind`, temporary, and the runner holds a busy command until it
runs rather than parking it.**

- `ErrorKind::Busy`, status `Temporary`, is what `Cqrs::execute` returns when a middleware
  answered `Busy`. Its message is the middleware's reason. Anyone may return it; a command
  handler that cannot proceed now may too.
- **The runner holds.** A dispatch that fails with `Busy` keeps the Policy on that event and
  tries the reaction again, with a backoff that doubles from 100 ms up to a 5 s ceiling, for
  as long as it stays busy. The [Dispatch timeout] still bounds each attempt. Busy attempts do not count against the dispatch retry budget and never write a
  dead letter. Once the dispatch returns anything else, the reaction is settled by the
  existing rules, with a fresh budget.
- **The hold is narrated.** It is an edge: one record when a Policy starts holding, one when
  it stops, never one per attempt ([Narration]). A held Policy keeps beating its
  [Heartbeat]: it is alive and waiting, which is not the same as a reaction that has hung.
- **On the bulk lane** a busy command ends the batch instead of holding inside it: the
  batch's claim rolls back, parking nothing, and its work is claimed again on the next
  poll — the same outcome, held at the queue rather than in memory.
- **Only standing down ends a hold early.** A shutdown, or the worker losing its
  leadership, calls the hold off wherever it is — in a backoff or a dispatch in flight:
  the delivery fails, nothing is parked or advanced, the hold is narrated as abandoned, and
  whoever leads next delivers the event again.
- **A [Retry] that meets `Busy`** leaves the row parked, untouched, and reports the reaction
  as busy (`DeadLetterRetry::Busy`; a bulk retry counts it in `reactions_busy`): retrying
  during maintenance does nothing and loses nothing. A retry never holds.

## Considered options

- **Reuse `Unavailable` and accept the dead letters.** The cheapest change, and the
  consumer can freeze its runners before switching maintenance on. But a runner left
  running fills the dead-letter table for the length of the window, and the runner could
  not hold on `Unavailable` without holding on every lock timeout too.
- **A crate-private path from `Cqrs` to the runner that reports `Busy` without a new
  kind.** Keeps the public enum unchanged, at the cost of a second execution path inside
  the crate. A new variant is simpler and lets application callers answer `Busy` too, for
  example with HTTP 503.
- **A `Busy` with a temporary or final flag.** A final refusal is already an error
  (`Forbidden`, `Permanent`); `Busy` only ever means "later".

## Consequences

- **Breaking for exhaustive matches on `ErrorKind`.** The release notes carry the new arm.
- **A Policy can be held indefinitely** for as long as a middleware stays busy. That is the
  intent; an operator sees it in the narration, and it ends when the middleware lets
  commands through.
- Freezing the runners before maintenance remains possible but is no longer needed to keep
  the dead-letter table clean.

[Command middleware]: ../../CONTEXT.md#command-middleware
[Policy runner]: ../../CONTEXT.md#policy-runner
[Dead letter]: ../../CONTEXT.md#dead-letter
[Narration]: ../../CONTEXT.md#narration
[Heartbeat]: ../../CONTEXT.md#heartbeat
[Retry]: ../../CONTEXT.md#retry
[Dispatch timeout]: ../../CONTEXT.md#dispatch-timeout
