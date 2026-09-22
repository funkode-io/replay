# A retry settles the group it locked, a page at a time

**Status:** accepted

**Supersedes:** the per-row settlement guard of
[ADR-0024](0024-a-parked-command-is-one-row.md).

[ADR-0021](0021-retry-settles-a-reaction-not-a-row.md) made the reaction the unit
of retry: one replay settles every row that reaction parked. The group was read
with `fetch_all`, which is a buffer whose length comes from the data — the thing
`AGENTS.md` forbids in library code, and the shape that OOM-killed a consuming
service in funkode-io/replay#146.

After ADR-0024 a parked command is one row, so the group is one row per command
the *current* code dispatches. What is left over is the tail an upgrade inherits:
rows the dedupe could not prove duplicate, which is the old code's commands times
the deliveries it saw. Finite, frozen at the migration, and not a number in the
code (funkode-io/replay#228).

## Decisions

- **The group is read a page at a time**, `RETRY_ROW_PAGE_SIZE` rows, keyset by
  the order the settlement must run in — rows naming a command first, then by id.
  A retry still settles **every** row of the reaction from one replay, in one
  transaction: ADR-0021's unit is untouched, and what changed is only how much of
  the group is in hand at once.

- **The staleness guard moves from the row to the group.** A per-row version
  (ADR-0024) has to be read *before* the replay to mean anything, which is the
  read that cannot be paged. So a retry carries a [digest] of the whole group
  instead — its row count, its summed `deliveries` and `retry_count`, and its
  latest `last_parked_at` — four scalars, whatever the group holds. Every write a
  concurrent writer can make moves one of them: a delivery's re-park bumps
  `deliveries` and `last_parked_at`, another retry's settlement bumps
  `retry_count`, an insert or an archive moves the count.

- **The digest is re-read under the rows' locks**, `FOR UPDATE` over the group,
  before the first settlement. The lock is what makes the comparison decisive
  rather than advisory: a writer that had already moved a row is in the numbers,
  and one that has not yet cannot move it behind the settlement's back — it
  waits, and applies its fresher failure on top of what the retry concluded.

- **The settlement reads one snapshot**, `REPEATABLE READ`, as a chunked rebuild
  does ([ADR-0011](0011-inline-projections-flushed-in-bounded-chunks.md)). The locks cover the rows that exist; a command parked
  *while* the settlement walks the group is a row nobody could have locked, and
  at the default READ COMMITTED a later page would read it on a fresh snapshot
  and settle it from a replay that ran before it existed. Digest and snapshot are
  the two halves: the digest catches what moved while the replay ran, the
  snapshot what moves while it settles.

- **Postgres refusing the snapshot means the same thing as a digest mismatch.** A
  `40001` is a writer having moved a row the settlement had to read consistently,
  which is the definition of a group that moved, so it is classified as a
  conflict (`db_error`) and reported `Superseded` rather than surfacing as a
  database error that aborts a bulk retry's walk.

- **A group that moved settles nothing.** Every row of it is reported
  `DeadLetterRetry::Superseded`, and the bulk summary counts the reaction as
  still failing, which it is. The alternative — settling the rows that did not
  move — needs the per-row versions this change exists to stop reading.

- **The count that decides how a row is matched is taken over the group**, by a
  window function, before the keyset narrows it to a page. Whether a row is
  settled in production order or shares a verdict with its indistinguishable
  siblings is a question about the reaction (ADR-0021), and a count taken over a
  page would answer it for the page.

- **The outcome is folded, not listed.** `retry_reaction` returns whether any row
  resolved, whether any is still failing, and the outcome of the one row a by-id
  retry asked about. A `Vec<(id, outcome)>` over the group is the same unbounded
  buffer one call further up.

## Rejected alternatives

- **A `LIMIT` on the group read, settling only what fits.** One replay would
  conclude things about commands whose rows it never settled, and the rows past
  the limit would keep an error no replay produced while the summary called the
  reaction settled. The page is a bound on memory; it must not become a bound on
  what a retry does.

- **A timestamp taken before the replay** (`last_parked_at > started_at` means
  "moved"), which needs no lock and no digest. Rejected: `now()` is transaction
  start, so a park whose transaction began before the replay and committed during
  it reads as older than the replay and would be overwritten — losing exactly the
  freshest failure funkode-io/replay#227 exists to keep.

- **Retiring the tail with a maintenance path** that archives or numbers the
  legacy rows, after which the group is bounded by the Policy's code alone. It
  bounds the table rather than the read, and it needs an operator to judge rows
  the library cannot. The paged read is the bound; retiring the tail stays
  available and is no longer urgent.

## Consequences

- **A reaction being re-parked while it is retried cannot be retried.** Its
  digest moves under every attempt, so each one settles nothing and reports the
  reaction still failing. That is a policy whose event is being redelivered as
  fast as an operator can retry it — the retry has nothing to conclude that the
  next delivery is not about to overwrite — but it is coarser than ADR-0024's
  per-row guard, which could settle the rows that had not moved.

- **A moved group parks nothing either**, including a command the replay found
  failing that no row spoke for — the one row a retry inserts (ADR-0021). Under
  the per-row guard that insert was independent of any other row's state. What
  moved the group is a writer that ran this same reaction: a delivery parks every
  command that failed for it, and another retry parks its own unclaimed failures,
  so the failure is recorded by the writer that is current rather than by this
  stale replay. When the mover was a [Discard] instead, nothing records it and
  the reaction is reported still failing: the next retry parks it, if it still
  fails.

- **A retry holds row locks over the whole group for the length of its
  settlement**, rather than taking them one row at a time as it writes. The
  settlement is statements only — the replay is outside the transaction — so the
  window is short, and a drain parking into that group waits rather than losing
  its write.

- **The group read costs one round trip per page.** A reaction with more rows
  than a page is rare enough that the trips are not worth tuning away, and the
  replay it belongs to has already executed the reaction's commands.

- **The allocation budget is pinned by a test.**
  `tests/policy_retry_allocations.rs` settles a reaction with a thousand retired
  rows and asserts the peak live bytes stay near a page: ~0.19 MB paged against
  ~1.55 MB when the group was read whole, on a ~0.6 MB budget. The snapshot is
  pinned by `policy_runner.rs`'s `settlement_tests`, which parks a command while
  a settlement holds its group and asserts no page of that settlement sees it.

[Discard]: ../../CONTEXT.md#discard

[digest]: ../../persistence/src/policy_runner.rs
