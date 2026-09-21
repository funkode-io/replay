# A parked command is one row, and the table enforces it

**Status:** accepted

**Supersedes:** the "a crash between parking and the next checkpoint parks the
row again — accepted, not fixed" consequence of
[ADR-0016](0016-panicking-reaction-parked-as-a-permanent-failure.md).

Parking a [Dead letter](../../CONTEXT.md#dead-letter) was an unconditional
`INSERT` into a table with no key over it. The park commits immediately while the
cursor is written every `checkpoint_batch_size` events, so a process killed in
between — or an operator's [Cursor move](../../CONTEXT.md#cursor-move) — delivers
the event again, the reaction fails the same way, and the same command is parked
a second time. Against a live Postgres (ADR-0014), a panicking reaction whose
cursor is rewound one position:

```
PARKED ROWS AFTER REDELIVERY: [
    DeadLetter { global_position: 1, event_id: afa7ec8a-…, error_kind: "Panic", … },
    DeadLetter { global_position: 1, event_id: afa7ec8a-…, error_kind: "Panic", … },
]
```

ADR-0016 accepted that: a dead letter is read by hand, and showing the same one
twice costs a moment's triage. Two things changed since. A retry now settles
every generation from one replay (ADR-0021), which makes the duplicates the
library's own bookkeeping rather than only an operator's reading; and the
`retry_count` a retry stamps is meaningless spread over generations nobody can
tell apart. The cost the ADR weighed against — a uniqueness constraint over the
parking path — is now one `ON CONFLICT` in the one function that parks.

## Decisions

- **The key is the reaction and the dispatch within it**: `(policy_name,
  event_id, aggregate_name, target_stream_id, command_name, dispatch_ordinal)`,
  a unique index built `CONCURRENTLY`
  ([0029](../../persistence/tests/migrations/0029_dead_letter_unique_command.sql)).
  `global_position` is left out as redundant — one event has one position.

- **The ordinal is what keeps a reaction's own repeats apart.** A reaction may
  emit the same command type to the same instance twice; those are two parked
  commands and must stay two rows, and nothing but the dispatch's index in the
  vector `react_erased` returned tells them apart. Its stability across a replay
  is exactly that of the production-order matching ADR-0021 already relies on.

- **Only a provable duplicate is retired.** A row parked after 0024 and before
  0027 names its command but not its place; a row parked before 0024 names
  nothing at all, and n commands failing on one event parked n such rows (0024's
  own header). Either group is *either* a redelivery's duplicate or several
  distinct commands, and nothing recorded says which. The exception is a panic:
  the unwind settles the delivery, so `error_kind = 'Panic'` with no dispatch
  named is one row per delivery in every release that contained a panic at all
  (ADR-0016). So the migration collapses that shape and **numbers the rest
  apart** — a negative ordinal, unique within the group and outside the range a
  dispatch's index can take. The cost of keeping an ambiguous pair is the triage
  noise the table already has; the cost of collapsing one is an active failure
  retired on a guess, which is the one thing a dead letter must never do (0014
  refuses to clean `events` positions for the same reason).

- **The survivor is the newest *parking*, not the greatest id.** An id is taken
  when a row is inserted and its timestamps when its transaction began, so the
  two can disagree; and the survivor carries the group's latest `last_parked_at`,
  so the recency signal cannot move backwards over the collapse.

- **`NULLS NOT DISTINCT`**, which is why the crate's floor is PostgreSQL 15
  (funkode-io/replay#222). A panic in `react` fails before any dispatch exists, so
  its row names none; before 15 every such row is distinct from every other and
  the one case that parks exactly one row per delivery would be the one case that
  kept duplicating.

- **A redelivery refreshes, it does not retry.** The park updates the row with the
  error the new delivery produced, counts the delivery in `deliveries` and stamps
  `last_parked_at`. `created_at` — when the command first failed — and
  `retry_count` / `last_retried_at` are untouched: nobody invoked the control
  surface.

- **Only a delivery counts as one.** The same `ON CONFLICT` catches a retry that
  parks a command the reaction had not parked and loses the race to another retry
  of the same reaction (ADR-0021). That is not the event arriving again, so it
  refreshes the error and leaves `deliveries` and `last_parked_at` alone —
  otherwise two operators clicking retry would fabricate a redelivery that never
  happened.

- **The stamp is `clock_timestamp()`, and never moves backwards.** `now()` is
  fixed at transaction start and a retry parks inside a transaction, so a
  settlement that began earlier can commit later; `GREATEST` over the stored
  value keeps the recency signal monotonic.

- **The recency signal moves with it.** `PolicyStatus::last_dead_letter_at` reads
  `MAX(last_parked_at)`, not `MAX(created_at)`, which an in-place refresh never
  moves. A command failing on fifty deliveries must not read like one that failed
  once and stopped. `dead_letter_count` keeps counting parked commands and does
  not change units. The index the status read scans carries `last_parked_at` as a
  payload column
  ([0030](../../persistence/tests/migrations/0030_dead_letter_status_index.sql)),
  so the poll a consumer's health endpoint makes on a timer stays index-only.

- **A settlement settles the row it read.** A retry reads a reaction's group,
  replays it, and writes the settlements after — and a parked command being one
  row means a delivery arriving in that window refreshes a row the replay is
  about to settle, where it used to insert a generation of its own and leave the
  read row untouched. So the retry carries the row's `deliveries` and
  `last_parked_at` into its `WHERE`: a row that moved is neither archived
  `retried` (retiring a failure nobody retried) nor overwritten with the staler
  error the replay produced, and the caller hears `DeadLetterRetry::Superseded`.
  A [Discard] carries no version — it re-runs nothing, so what the row says now
  does not change what the operator asked to retire.

- **The rows already duplicated are collapsed by the migration**
  ([0028](../../persistence/tests/migrations/0028_dead_letter_dedupe.sql)), not by
  the operator: these duplicates are the library's own, unlike the hand-written
  `events` positions 0014 refuses to clean. The newest generation survives with
  the earliest `created_at`, the summed `deliveries` and the group's retry
  bookkeeping; the losers are archived with a reason of their own, `superseded`,
  since neither [Retry] nor [Discard] retired them.

## Consequences

- **The migrations belong with the release, before it runs.** The park needs the
  key to conflict on, and a binary that predates it parks with an unconditional
  INSERT. A replica still running the old code while 0029 exists takes a `23505`
  on the one thing the key forbids for *it* — re-parking a command it has already
  parked itself — which fails that poll rather than that row: the failure it
  could not write is the one already in the table. Against a row the *new* code
  parked it takes no error at all and leaves a sibling: it writes no ordinal, and
  a null one is distinct from a dispatch's index, which is the same residue an
  upgrade's backlog holds below. The same window can make the concurrent build
  fail on a duplicate the old writer created, which is why the build must not be
  stepped over by `IF NOT EXISTS`.

- **A crash inside the checkpoint window costs nothing in the table.** The window
  itself is untouched: the park still does not participate in the checkpoint's
  transaction, and re-running a reaction whose commands partially succeeded is
  defined behaviour (ADR-0003). What the key removes is the row, not the work.

- **A row parked before 0027 and a later delivery's row for the same command can
  still coexist**: the older row carries a synthetic negative ordinal, the newer
  one a dispatch's index, and nothing can merge them. So ADR-0021's shared-verdict
  settlement for rows a replay cannot tell apart is still load-bearing, and its
  fabricated-duplicate tests still describe a table an upgrade can hold.

- **One reaction's group is not yet bounded by a number in the code.** Keeping the
  ambiguous rows keeps their count, which is the old code's commands times the
  deliveries it saw. The tail is frozen at the migration — every later park
  refreshes a row — but `load_parked_reaction` reads the group whole, and
  `AGENTS.md` asks for a number. Tracked by funkode-io/replay#228; a `LIMIT` is
  not the fix, because a retry settles every row of a reaction from one replay.

- **A reaction that changes shape between two deliveries of one event can park
  the same command twice.** The ordinal is the dispatch's index, so inserting or
  reordering a dispatch in a deploy gives the same failing command a different
  one, and a redelivery after that deploy parks beside the row rather than
  refreshing it. What it leaves is a stale duplicate, not a lost or wrongly
  retired failure: the rows are settled by `ParkedIdentity::names`, which is
  blind to the ordinal, so the next replay settles both. The narrower key that
  would avoid it is the one that merges a reaction's legitimate repeats, and the
  window is a redelivery — a crash inside the checkpoint window, or a [Cursor
  move] — spanning a deploy that changed the reaction.

- **`deliveries` is a new triage signal**: a command that keeps being re-parked is
  one whose Policy keeps crashing or being rewound, which the error message alone
  never said.

## Rejected

- **Matching a parked row to a replayed dispatch by its ordinal**, the exactness
  ADR-0021 wanted. A group can mix rows that carry an ordinal with rows parked
  before the column existed, so the coarser matching has to stay for the latter;
  having two matching rules run side by side buys exactness for the newer rows at
  the cost of a second way for a retry to settle a row. Worth revisiting when the
  null-ordinal backlog is gone.

- **Writing the dead letter in the cursor checkpoint's transaction.** It closes
  only the crash window — an operator's rewind is a deliberate redelivery no
  transaction can refuse — and once the table enforces uniqueness it saves work
  rather than fixing a defect.

- **An advisory lock around the one insert a retry makes.** It would fix the
  concurrent-retry half (ADR-0021) and not the redelivery half, and would be
  removed again when the key landed.

- **Reporting a row a delivery re-parked mid-replay as `StillFailing`.** True of
  the row and false of the call: the operator would read an error their retry
  never produced, on a row whose `retry_count` did not move, with no way to tell
  it from a replay that failed the same way twice. `Superseded` says which, and
  says to retry again.

- **De-duplicating on read.** The question "what is a parked command" belongs in
  the schema; a read that collapses rows leaves every writer free to make more,
  and every reader free to disagree about how.

- **Collapsing every duplicate-looking sibling** — what the issue asked for,
  before the ambiguity above was noticed. It retires a row that may be the only
  record of a second failing command, and the noise it saves is finite and
  clearable by hand.

- **Dating rows against the migration history** to tell a row an old binary parked
  from one the new binary parked. `_sqlx_migrations.installed_on` is sqlx's
  bookkeeping, not this schema's, and a rolling deploy parks through the old code
  after the migration lands anyway — so it would answer the question wrongly in
  exactly the window it was added for.

[Retry]: ../../CONTEXT.md#retry
[Discard]: ../../CONTEXT.md#discard
[Cursor move]: ../../CONTEXT.md#cursor-move
