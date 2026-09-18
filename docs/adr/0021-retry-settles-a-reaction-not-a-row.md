# The unit of retry is the reaction, not the parked row

**Status:** accepted

**Supersedes:** the retry-unit and settlement decisions of
[ADR-0007](0007-dead-letter-retry-reproduces-reaction-from-triggering-event.md).

A [Policy](../../CONTEXT.md#policy) reaction that dispatches several commands
parks one [Dead letter](../../CONTEXT.md#dead-letter) per failing command
(ADR-0018). Retry was per **row**: each row replayed the whole reaction, the
replay stopped at the first failure, and that one error was written to whichever
row had been retried. One event, two commands failing with distinct errors, one
bulk retry, against a live Postgres (ADR-0014):

```
summary          : DeadLetterRetrySummary { resolved: 0, still_failing: 2 }
reaction replays : 2 (for 2 parked rows)
row after retry  : probe refuses: first-command-failed
row after retry  : probe refuses: first-command-failed
```

The second command's error was gone, the reaction had run twice, and one broken
reaction reported as two (funkode-io/replay#211).

ADR-0007's core stands: a retry re-runs the reaction from the triggering event
against current state and current code, rather than replaying a stored command.
What changes is what a retry is *for* — one reaction, not one row.

## Decisions

- **A reaction is `(policy_name, event_id)`, and it is the unit of retry.** No
  synthetic reaction id: the event a reaction is a pure function of already
  identifies it (ADR-0003), and a synthetic id would have to be reproduced by
  every re-`react` to keep meaning anything. `retry_policy_dead_letters` groups
  the policy's parked rows by that pair and replays each group **once**.

- **`retry_dead_letter(id)` keeps its signature and settles the whole group.** It
  still replays the reaction the row belongs to, as ADR-0007 defines; it now
  settles every row of that reaction from the replay it just ran and reports the
  outcome of the row `id` names. The by-id and bulk paths cannot conclude
  different things from the same replay, and neither leaves a row carrying a
  verdict no replay reached.

- **A replay carries on past a failure**, exactly as the forward drain does.
  Stopping at the first one is what loses the later commands' errors, and it was
  the one place the two paths disagreed.

- **Settlement is per row, keyed by identity.** A row is matched to the replay's
  commands by `(aggregate type, target stream URN, command type)` — the columns
  the row already carries (funkode-io/replay#210). A row whose command
  resolves (`Ok` or `BusinessRuleViolation`) is archived with reason `retried`; a
  row whose command fails again is updated in place with **its own** error. Two
  identical dispatches to the same stream are indistinguishable — the command's
  variant and payload are not recorded, and the recorded command type is the
  aggregate's command *enum* — so they settle the two rows in production order.
  In order only while the counts line up: when the replay ran more dispatches of
  an identity than the group has rows naming it (the reaction sent two commands
  to one instance and only the later failed) or fewer (a second delivery parked a
  copy of one row, funkode-io/replay#220), every row of that identity takes one
  shared verdict — a failure among those dispatches re-parks it, all of them
  resolving archives it. Matching by position there would archive a row whose
  command had just failed again.

- **A row the replay ran no command for is settled by what the replay can say
  about it.** A row naming a command the reaction no longer emits is resolved:
  the reaction as defined now does not contain it, which is the same clean
  resolution a declined command is. A row naming **nothing** — parked before the
  identity migration, or parked for a panic in `react` itself — is settled by the
  replay as a whole: archived when nothing failed, re-parked with the replay's
  first failure otherwise, which is exactly the all-or-nothing semantics it was
  parked under. A replay that **panicked** concluded nothing about the commands
  it never reached, so every row it did not run stays parked with the panic.

- **The summary counts reactions, and says so.**
  `DeadLetterRetrySummary { reactions_resolved, reactions_still_failing }` — a
  deliberate breaking change to a public type. The old names were silent about
  their unit and the old numbers multiplied one broken reaction by its command
  count. `PolicyStatus::dead_letter_count` keeps counting **rows** (parked
  commands): it feeds `Degraded`, which is `> 0` either way, and a gauge silently
  changing units is worse than two numbers with stated units.

- **Every settlement is recorded on the row**: `retry_count` and
  `last_retried_at`, stamped by the archive move as much as by the re-park, so
  "has anyone tried this since the outage?" survives the error message being
  overwritten. `discard_dead_letter` re-runs nothing and stamps nothing.

- **A failed retry leaves the row retryable.** What makes another retry worth
  making is a change outside the library — a dependency that came back — which
  the library cannot observe, so it does not pretend to. Taking a row out of play
  permanently stays `discard_dead_letter`, an explicit operator decision.

## Rejected alternatives

- **One row per event.** It makes the reaction the unit everywhere and needs no
  matching at all — but it is the state the identity columns deliberately left
  behind (funkode-io/replay#210): a row would again fail to say which customer is
  stuck, and a reaction whose commands fail for two reasons would keep one of
  them.

- **A synthetic reaction id column.** A group key that does not have to be
  re-derived, at the cost of a column that every re-`react` would have to
  reproduce to stay meaningful, and a migration for rows that predate it. The
  triggering event already is that key.

- **Narrowing a replay to only the failed commands.** It would make a retry cost
  what is actually broken rather than the whole reaction. Rejected for now:
  re-running a command that already succeeded is the at-least-once contract
  working as intended (ADR-0003), and a reaction whose commands cannot tolerate
  that is one aggregate pretending to be two. The identity this shape matches on
  is what would make narrowing possible later, without another migration.

## Consequences

- **A retry still re-runs commands that already succeeded.** A reaction with one
  parked command out of five costs five executions, and with a dispatch timeout
  in play (ADR-0018) a reaction that hangs costs that timeout again. What is
  bounded is the number of *replays*: one per reaction, not one per parked row.

- **An operator watching `dead_letter_count` and a retry summary is reading two
  units.** Rows in the gauge, reactions in the summary. Stated in both names, and
  the alternative — moving the gauge to reactions — would silently change the
  meaning of a number services already alert on.

- **A reaction sending two commands to one instance settles its rows more
  coarsely.** The identity a row records cannot tell those dispatches apart, so
  when they do not line up one-to-one with the rows, the rows share a verdict
  instead of each carrying its own error. Recording which dispatch of the
  reaction a row was parked for — an ordinal — would settle them exactly; it is
  another column and another migration, and two commands to one instance from one
  reaction is a shape worth questioning before it is worth optimising for.

- **A row parked before the identity migration is settled more coarsely than its
  neighbours**: all-or-nothing on the whole replay, since it names no command.
  Such rows are a finite backlog that an upgrade inherits and retries clear.

- **The drain and the retry now agree on what happens after a command fails.**
  They are still separate code paths — the drain inserts rows, the retry settles
  existing ones — but "carry on past the failure, park what failed with its own
  error" is one rule stated twice rather than two rules.

- **A bulk retry holds one page of reactions, never the backlog.** The set it
  drains is the size of the outage that made it, so it is walked by keyset —
  `(global_position, event_id)` after the last reaction settled — a page at a
  time. A reaction re-parked by the run is behind the keyset and is not replayed
  twice. The group a replay settles is bounded by the commands the reaction
  returns, times the deliveries of that event: a dead letter is written before
  the batched cursor checkpoint, so a crash in between parks the reaction again
  (funkode-io/replay#220).

- **The retry's access path needed its own index.** `(policy_name, created_at)`
  answers "what failed recently", not "which rows belong to this reaction", so
  reading a group would have rescanned the policy's backlog once per reaction —
  quadratic in the thing a bulk retry exists to drain. One index,
  `(policy_name, global_position, event_id, id)`, serves both the page's keyset
  and the group's lookup.

[Dead letter]: ../../CONTEXT.md#dead-letter
[Retry]: ../../CONTEXT.md#retry
