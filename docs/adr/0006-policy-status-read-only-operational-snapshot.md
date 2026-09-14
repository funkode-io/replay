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
  (the global head), `MIN(global_position) > cursor` on `events` (the next
  position that actually exists), and a per-policy aggregate over
  [`policy_dead_letters`](../../persistence/tests/migrations/0010_policy_dead_letters.sql)
  (`COUNT(*)` + `MAX(created_at)`). The event log is never *scanned*: both
  `events` reads are `MIN`/`MAX` probes served by `idx_events_global_position`,
  so status stays O(policies · log events) rather than O(events) and remains
  cheap to poll on a dashboard interval.

- **Extend the existing read model; do not fork a parallel one.** Status lives in
  [`PolicyStatusStore`](../../persistence/src/policy_status.rs) and is read via
  `PolicyStatusStore::list()`. The dead-letter dimension *extends* the existing
  `PolicyStatus` / `PolicyCondition` types (adding fields and a variant) rather
  than introducing a second status type or a competing `PolicyRunner` method.
  One read model, one set of field names (`position` / `head`), one query.

- **A status is a derived health label plus the raw numbers behind it.**
  `PolicyStatus` carries `name`, `position`, `head`, `lag` (`head - position`),
  `next_position`, `missing_position`, `last_checkpoint_at`, `dead_letter_count`,
  `last_dead_letter_at`, and a derived `condition`. The raw fields are always
  present so a consumer can render its own view; `condition` is the at-a-glance
  summary.

- **Report the next position that exists, and the hole in front of the cursor.**
  `lag` counts *positions*, not events, so it cannot distinguish a policy that is
  chewing through a backlog from one parked in front of a `global_position` that
  will never exist — the failure in
  [#164](https://github.com/funkode-io/replay/issues/164), where one burned
  `BIGSERIAL` value stopped 19 policies for three days. `next_position` is
  `MIN(global_position) > position` (`None` when the tail is empty);
  `missing_position` is derived from it as `Some(position + 1)` when
  `next_position > position + 1`, and `None` otherwise. Those are exactly the
  facts the incident was diagnosed with by hand-written SQL, so they belong in
  the read model rather than in an operator's notebook. A hole spanning several
  positions reports only its first position: that is the one the feed stops at,
  and the rest follows from `next_position`. An empty tail is *not* a hole —
  a drained policy reports `next_position: None` and is `CaughtUp`, never
  `Blocked`.

- **`PolicyCondition` precedence: a hole outranks dead letters, dead letters
  outrank lag.** The condition is derived by
  `PolicyCondition::from_fields(lag, dead_letter_count, missing_position)` with a
  strict precedence (highest wins):

  | Condition  | When                                       |
  |------------|--------------------------------------------|
  | `Blocked`  | `missing_position.is_some()`               |
  | `Degraded` | `dead_letter_count > 0`                    |
  | `Working`  | `dead_letter_count == 0`, `lag > 0`        |
  | `CaughtUp` | `dead_letter_count == 0`, `lag == 0`       |

  A policy that is **both** behind and dead-lettered resolves to `Degraded`, so a
  parked failure is never hidden behind a benign "still catching up" label. Lag is
  expected and self-healing; a dead letter means an event was skipped and needs a
  human. `Blocked` sits above `Degraded` because it is a *throughput* statement,
  not a *failure count*: a blocked policy processes nothing at all and will not
  recover on its own, while a degraded one is still draining its feed. Reporting a
  blocked policy as `Degraded` (or worse, `Working`) is how the outage above stayed
  invisible. `condition` has a stable `as_str()` / `Display` form (`"CaughtUp"`,
  `"Working"`, `"Degraded"`, `"Blocked"`) so JSON/UI consumers can match on it.

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

- `Blocked` is an **observation, not a permanence proof**. `global_position` is
  assigned at INSERT and made visible at COMMIT, so an append in flight leaves a
  momentary hole and a status read taken during it can report `Blocked` for the
  duration of that transaction, then recover by itself on the next poll. Alerting
  should therefore require the condition to persist across polls. Deciding that a
  hole can *never* fill — and acting on it by advancing the cursor — needs
  transaction-snapshot evidence on the runner's side, which is the write-path fix
  tracked separately by
  [#164](https://github.com/funkode-io/replay/issues/164). This ADR still covers
  observing only: `Blocked` names the condition, it does not clear it.

- This ADR covers **observing** only. **Controlling** a policy — explicitly
  retrying or discarding a dead letter, or rewinding a cursor — is a separate
  capability deliberately deferred to its own decision, so the read path carries no
  mutation surface.

- The condition precedence and the dead-letter / lag interaction are covered by a
  unit test (the `from_fields` precedence table, and the `missing_position`
  derivation) and Docker-gated Postgres integration tests (caught-up, behind,
  multiple policies, never-run-absent, a degraded-with-dead-letters case proving
  `Degraded` beats `Working`, and a blocked case that burns a sequence value the
  way an aborted append does and proves `Blocked` beats `Degraded`), which double
  as the executable specification of these contracts.
