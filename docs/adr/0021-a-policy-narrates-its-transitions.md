# A Policy narrates its transitions, and nothing else

**Status:** accepted

The success path was silent. A healthy idle worker and a permanently wedged one
emitted byte-identical output — nothing — which is half of why one burned
`global_position` cost three days in funkode-io/replay#164. The obvious fix, a
line per dispatch, is unaffordable: a ten-thousand-row import fanned out across
eleven Policies is over a hundred thousand records, and a log nobody can read
during an incident is no better than silence (funkode-io/replay#188).

A Policy's output is now proportional to how often it changes state, not to how
much work it does: [Narration](../../CONTEXT.md#narration) is edge-triggered.

## Decisions

- **A burst is bracketed by two records.** One when a Policy at zero lag finds
  work, one when it reaches [Caught up](../../CONTEXT.md#caught-up), naming how
  many events it took and how long. A ten-thousand-event import costs those two
  plus one progress record per `PROGRESS_EVERY` it lasts, whatever its size, and
  an idle Policy costs none, so silence keeps meaning "nothing happened" rather
  than "nothing is known".

- **A long backlog emits a bounded progress record**, spaced by wall-clock time
  (`PROGRESS_EVERY`, 30s) rather than by poll or event count, because the
  question it answers — moving slowly, or not moving? — is a question about
  time. It is the only record that repeats while a Policy is doing what it
  should. The spacing is a floor, not a schedule: the record is written by the
  first cursor advance at least that long after the previous one, so a Policy
  that stops advancing stops writing them — which is the distinction the record
  exists to draw.

- **Records are earned as the cursor moves, not when a poll returns**, and the
  bracket is opened by the window rather than by the first reaction in it. One
  poll's batch is dispatched event by event, each bounded only by the
  [Dispatch timeout](../../CONTEXT.md#dispatch-timeout) and its retries, so a
  single poll can outlast the progress cadence several times over and its first
  event alone can cost minutes; a decision taken between polls would be paced by
  the work rather than by the clock, and one taken after the first reaction would
  put that reaction's own records — including a dead letter — outside the bracket
  that is supposed to contain them. What this still cannot see is a worker held
  inside one reaction — the narration runs on the worker's own thread of control.
  That is the question [Liveness](../../CONTEXT.md#liveness) answers, from a task
  of its own
  ([ADR-0020](0020-liveness-is-published-from-memory-and-beaten-on-a-cadence.md)).

- **Only an exhausted feed closes a burst.** A poll that read nothing has either
  reached the end of the feed or stopped in front of something: a hole
  ([ADR-0013](0013-policy-feed-contiguity-on-unfiltered-positions.md)), or a
  cursor an operator moved under it. Both read as zero events, and conflating
  them would announce that a [Blocked policy](../../CONTEXT.md#blocked-policy)
  had caught up — the opposite of what happened, in the one state
  funkode-io/replay#164 was about. A stalled poll is no edge at all: the bracket
  stays open until the feed genuinely ends, and the block has a record of its own.

- **The edge is computed from positions the cursor advanced over, not from
  dispatches executed.** A window a Policy's filter excludes entirely, and one
  whose reactions all park, are both work: a Policy that read them is moving, and
  reporting it as caught up would be false. The count in the record is therefore
  a backlog measure, not an audit of what reacted — the cursor and the dead
  letters answer that.

- **A burst is timed from the read that found the work to the last position it
  advanced over.** The clock starts before the feed read, not when a record is
  decided — otherwise a backlog drained inside one poll would be timed from after
  the work and read as instantaneous. It stops at the last advance, so neither the
  checkpoint that follows the last event nor the idle interval before the empty
  poll that notices the catch-up is charged to the work.

- **The narration belongs to one election.** A worker that loses its lock
  mid-backlog abandons its bracket silently: it has not caught up, and leaving it
  open would let the next Leader close somebody else's burst with a duration
  measured across the gap. The boundary is the one
  [ADR-0008](0008-policy-runner-shared-connection-leadership.md) already draws,
  not a tighter one: a demoted worker finishes the batch it is inside before it
  sees the revocation, so for that window its records overlap the new Leader's.
  They are true — it did that work, and the checkpoint it then attempts is
  refused as superseded — and the alternative, cutting the narration off
  mid-batch, would hide work that happened rather than prevent it. Restart and
  escalation records belong to supervision
  ([ADR-0017](0017-dead-policy-worker-restarted-on-a-budget.md),
  [ADR-0019](0019-escalation-is-a-consumer-hook-that-exits-by-default.md)) and are
  not repeated here.

- **A failed poll narrates nothing.** It did not catch up and it is not evidence
  of work; the error it already logs is the record.

- **Per-dispatch detail is a `debug` record, off by default.** It is what an
  operator turns on for as long as they are looking at one Policy, and the reason
  the `info` path can stay at two lines per burst.

- **The state machine decides; the caller writes.** `policy_narration` returns a
  record and holds no `tracing` call, so what the lines say stays free to change
  and the decision is unit-testable without capturing output
  ([ADR-0014](0014-policy-daemon-tests-assert-through-operator-visible-observations.md)).

## Consequences

- An operator reading a Policy's log sees when work started, roughly how it
  progressed, and what it cost — and sees nothing at all from a Policy with
  nothing to do, whatever its poll interval.
- The catch-up record lags the actual catch-up by up to one poll interval: it is
  written by the first empty poll, which is what proves the feed is exhausted.
  The duration it reports excludes that wait.
- A Policy that stops in front of a hole never writes a catch-up record for that
  burst, which is correct and means a bracket can stay open for as long as the
  block lasts. The progress record keeps arriving once it moves again, and the
  block itself is reported by `policy_blocked`.

## Rejected

- **A line per dispatch at `info`.** The volume the edge-triggering exists to
  avoid; kept at `debug`.
- **A record on every poll that found work.** Proportional to the backlog and to
  the poll interval — a 100ms poll interval turns a minute of work into six
  hundred lines that say the same thing.
- **Publishing the working/caught-up state on `daemon.liveness()`.** It reads as
  the same axis and is not: [Liveness](../../CONTEXT.md#liveness) is what only the
  process knows, and whether a Policy is moving is
  [Progress](../../CONTEXT.md#progress), already derivable by anyone with a
  connection ([ADR-0006](0006-policy-status-read-only-operational-snapshot.md)).
  The narration's state is a log-rate decision, not a third axis — which is why
  funkode-io/replay#188 depends on
  [ADR-0020](0020-liveness-is-published-from-memory-and-beaten-on-a-cadence.md)
  for the shape of an in-memory published state and not for the state itself.
- **Making the progress spacing configurable.** Another knob on the builder for a
  value whose only job is to be smaller than an alerting window and larger than a
  poll. Revisit if a deployment's backlogs are routinely shorter than 30s and the
  progress record therefore never fires — the bracket already covers that case.
