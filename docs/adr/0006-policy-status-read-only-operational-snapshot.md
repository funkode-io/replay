# Policy status is a read-only operational snapshot, not a projection

**Status:** accepted; its fields were replaced by [ADR-0026](0026-a-policy-tracks-its-position-per-stream.md). The decision below — a read-only operational snapshot, derived from the operational tables, reporting progress and not liveness — stands. What it reports changed with the position it reports on: `lag` is an exact count of events over the streams a Policy is behind on, `streams_behind` says how widely it is spread, and `position`, `head`, `next_position`, `missing_position` and `PolicyCondition::Blocked` are gone with the global order they described (funkode-io/replay#196).

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
  **single** SQL query. *Amended by
  [ADR-0026](0026-a-policy-tracks-its-position-per-stream.md): the tables it reads
  changed with the position it reports on.* It is now `policy_cursors`
  (`discovered_through` + `updated_at`), a per-policy `LATERAL` over `streams`
  against that policy's rows in `policy_stream_cursors`, and a per-policy aggregate
  over
  [`policy_dead_letters`](../../persistence/tests/migrations/0010_policy_dead_letters.sql).
  ~~`MAX(global_position)` on `events` and `MIN(global_position) > cursor`~~ are gone
  with the global order. The event log is still never scanned, but the frontier
  `LATERAL` reads one row per stream per policy, because no index answers a
  comparison between two tables' columns — affordable for an endpoint scraped every
  few seconds, and the reason the runner does not discover work this way.

- **Extend the existing read model; do not fork a parallel one.** Status lives in
  [`PolicyStatusStore`](../../persistence/src/policy_status.rs) and is read via
  `PolicyStatusStore::list()`. The dead-letter dimension *extends* the existing
  `PolicyStatus` / `PolicyCondition` types (adding fields and a variant) rather
  than introducing a second status type or a competing `PolicyRunner` method.
  One read model, one set of field names, one query.

- **A status is a derived health label plus the raw numbers behind it.** The label
  and the numbers behind it stand; *which* numbers changed with
  [ADR-0026](0026-a-policy-tracks-its-position-per-stream.md). `PolicyStatus` carries
  `name`, `lag`, `streams_behind`, `discovered_through`, `last_checkpoint_at`,
  `dead_letter_count`, `last_dead_letter_at`, and a derived `condition`. ~~`position`,
  `head`, `next_position`, `missing_position`~~ are gone with the global order
  (funkode-io/replay#196). `lag` is no longer a subtraction of two positions but an
  exact count of undelivered **events**, summed over the streams a Policy is behind
  on, and `streams_behind` says how widely that is spread — one stream a million
  events behind and a million streams one event behind are the same `lag` and
  different problems.

- ~~**Report the next position that exists, and the hole in front of the cursor.**~~
  *Deleted with funkode-io/replay#197.* It existed because `lag` counted positions and
  so could not tell a Policy draining a backlog from one parked in front of a
  `global_position` that would never exist —
  [#164](https://github.com/funkode-io/replay/issues/164), where one burned
  `BIGSERIAL` value stopped 19 policies for three days. A Policy now reads each stream
  over a sequence with no holes in it, so there is no number to be parked in front of
  and nothing for the field to report. `lag` counting events rather than positions
  also makes the distinction it was invented for unnecessary: a burned position is not
  in the count.

- **`PolicyCondition` precedence: dead letters outrank lag.** *Amended by
  funkode-io/replay#196, which removed the condition that used to outrank both.*

  | Condition  | When                                       |
  |------------|--------------------------------------------|
  | `Degraded` | `dead_letter_count > 0`                    |
  | `Working`  | `dead_letter_count == 0`, `lag > 0`        |
  | `CaughtUp` | `dead_letter_count == 0`, `lag == 0`       |

  A policy that is **both** behind and dead-lettered resolves to `Degraded`, so a
  parked failure is never hidden behind a benign "still catching up" label. Lag is
  expected and self-healing; a dead letter means an event was skipped and needs a
  human. ~~`Blocked`~~ has no replacement because it has no cause: a write still in
  flight leaves a Policy *not behind at all*, since its events are invisible to every
  reader, including the one computing `lag`. `condition` has a stable `as_str()` /
  `Display` form (`"CaughtUp"`, `"Working"`, `"Degraded"`) so JSON/UI consumers can
  match on it.

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

- ~~`Blocked` is an **observation, not a permanence proof**.~~ *Gone with the
  condition (funkode-io/replay#196).* It was an observation of exactly what this
  design removed: an append in flight and a burned position were identical from one
  read of the global order, and telling them apart needed transaction-snapshot
  evidence on the runner side ([#164](https://github.com/funkode-io/replay/issues/164),
  and the watermark that tried it in #215). Reading per stream means neither is
  visible to a reader at all.

- This ADR covers **observing** only. **Controlling** a policy — explicitly
  retrying or discarding a dead letter, or rewinding a cursor — is a separate
  capability deliberately deferred to its own decision, so the read path carries no
  mutation surface.

- The condition precedence and the dead-letter / lag interaction are covered by a
  unit test (the `from_fields` precedence table) and Docker-gated Postgres
  integration tests (caught-up, behind, multiple policies, never-run-absent,
  `Degraded` over `Working`), which double as the executable specification of these
  contracts.
