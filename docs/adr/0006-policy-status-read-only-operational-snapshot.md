# Policy status is a read-only operational snapshot, not a projection

**Status:** accepted

Policies are checkpointed background subscribers (ADR-0003): they store a
`global_position` cursor, advance at-least-once, and on permanent failure write a
`policy_dead_letters` row and advance rather than wedge (a *recorded* skip, never
a poison pill). That machinery is correct but **opaque** to an operator: nothing
answers "is every policy keeping up, and is anything failing?" without hand-rolled
SQL against internal tables. We add **policy status** — a lightweight, read-only
health signal the host application can poll to monitor policies, surface dead
letters, and decide when human intervention is needed.

The defining constraint is that **observing is separated from controlling**.
Policy status only *reports*; it never mutates a cursor, retries a dead letter, or
otherwise acts on a policy. It is **not** domain state (an aggregate's
rebuilt-from-stream state) and **not** a [`Projection`](../../persistence/src/query.rs)
(it derives no read model from the event log and uses none of the
`Query`/`InlineProjection` machinery). It is a point-in-time snapshot read from the
operational tables the runner already maintains.

## Decisions

- **Read the operational tables, never the event log.** A status read is a
  **single** SQL query over
  [`policy_cursors`](../../persistence/tests/migrations/0008_policy_cursors.sql)
  (per-policy stored position + `updated_at`), `MAX(global_position)` on `events`
  (the global head), and a per-policy aggregate over
  [`policy_dead_letters`](../../persistence/tests/migrations/0010_policy_dead_letters.sql)
  (`COUNT(*)` + `MAX(created_at)`). The event log is never scanned — status is
  O(policies), not O(events), so it stays cheap to poll on a dashboard interval.

- **Extend the existing read model; do not fork a parallel one.** Status lives in
  [`PolicyStatusStore`](../../persistence/src/policy_status.rs) and is read via
  `PolicyStatusStore::list()`. The dead-letter dimension *extends* the existing
  `PolicyStatus` / `PolicyCondition` types (adding fields and a variant) rather
  than introducing a second status type or a competing `PolicyRunner` method.
  One read model, one set of field names (`position` / `head`), one query.

- **A status is a derived health label plus the raw numbers behind it.**
  `PolicyStatus` carries `name`, `position`, `head`, `lag` (`head - position`),
  `last_checkpoint_at`, `dead_letter_count`, `last_dead_letter_at`, and a derived
  `condition`. The raw fields are always present so a consumer can render its own
  view; `condition` is the at-a-glance summary.

- **`PolicyCondition` precedence: dead letters outrank lag.** The condition is
  derived by `PolicyCondition::from_fields(lag, dead_letter_count)` with a strict
  precedence (highest wins):

  | Condition  | When                                  |
  |------------|---------------------------------------|
  | `Degraded` | `dead_letter_count > 0`               |
  | `Working`  | `dead_letter_count == 0`, `lag > 0`   |
  | `CaughtUp` | `dead_letter_count == 0`, `lag == 0`  |

  A policy that is **both** behind and dead-lettered resolves to `Degraded`, so a
  parked failure is never hidden behind a benign "still catching up" label. Lag is
  expected and self-healing; a dead letter means an event was skipped and needs a
  human. `condition` has a stable `as_str()` / `Display` form (`"CaughtUp"`,
  `"Working"`, `"Degraded"`) so JSON/UI consumers can match on it.

- **Only policies that have run appear.** Status is keyed off `policy_cursors`
  rows. A policy that has been registered but has never started (no cursor row)
  does **not** appear in `list()`; "registered" is a runner-side fact, while status
  reports observed runtime progress. A consumer that needs the registered set joins
  it against status itself.

## Consequences

- Status is **native-only**, living in the server-only `persistence` crate
  alongside the runner and its tables. It is exported from the crate root and the
  prelude next to `PolicyRunner`.

- Because the query reads committed aggregate scalars, status is **eventually
  consistent** with an in-flight drain: a cursor mid-batch reads as slightly behind
  and a just-written dead letter appears on the next read. This is correct for a
  monitoring signal and avoids taking any lock on the runner's hot path.

- This ADR covers **observing** only. **Controlling** a policy — explicitly
  retrying or discarding a dead letter, or rewinding a cursor — is a separate
  capability deliberately deferred to its own decision, so the read path carries no
  mutation surface.

- The condition precedence and the dead-letter / lag interaction are covered by a
  unit test (the `from_fields` precedence table) and Docker-gated Postgres
  integration tests (caught-up, behind, multiple policies, never-run-absent, and a
  degraded-with-dead-letters case proving `Degraded` beats `Working`), which double
  as the executable specification of these contracts.
