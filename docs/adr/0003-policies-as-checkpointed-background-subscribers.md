# Policies are a checkpointed background subscriber, distinct from projections

**Status:** accepted

We add **Policies** — event-driven reactions where an appended event triggers a
reaction that issues a command, which may raise further events. A Policy is
**not** a projection: it derives no read model, its output is a command, and its
effects are side effects on the write side. Projections and Policies happen to
share one substrate — a **checkpointed background subscriber over the event log**
(catch-up replay, a stored position, at-least-once delivery, global ordering) —
but they are siblings on that substrate, not a parent/child. The umbrella term
"Projection" stays honest (a derived *read model*); a command-issuing reaction is
named separately so the two never leak requirements into each other.

The defining asymmetry: a projection's rebuild is idempotent by construction
(reset the view, re-fold), whereas a Policy's "rebuild" **re-executes side
effects**. Therefore a **projection version bump ⇒ reset + replay**, but a
**Policy version/code change ⇒ resume from the stored checkpoint, never replay**.

## Decisions

- **Start position** is an explicit per-policy choice, `start_at: Beginning | Now`,
  defaulting to the *safe* `Now` (a newly registered policy does not retroactively
  fire commands across all history unless asked). `Beginning` is the deliberate
  backfill path. A version change never moves the position.
- **Global cursor.** Policies are the first feature needing a total order over the
  whole log. We add a `BIGSERIAL global_position` to `events` and read it with a
  **high-water-mark** reader: advance only across a contiguous, gap-free prefix
  (with a short visibility grace), so a sequence value that is assigned but not yet
  committed by a concurrent append can never be skipped. We rejected an append-time
  serializing lock (kills bulk-load write throughput) and a naive `created`/per-
  stream `version` cursor (not a total order; skips and double-counts under clock
  skew and concurrency).
- **Delivery is at-least-once.** We do not chase exactly-once delivery (high cost,
  illusory across crash boundaries). Correctness comes from **idempotent aggregate
  commands** (at-least-once + idempotent consumer = effectively-once). The dedup key
  is **causation** — the triggering event's identity — never command-value equality
  (two structurally equal deposits must both apply). Idempotency is a contract we
  *teach* (causation-guard recipe in the example aggregate, invariant-first trait
  docs, and a duplicate-delivery integration test as executable proof), not a
  compile-time marker trait — a marker would assert a property it cannot check, at
  the wrong granularity (idempotency is per-command, not per-aggregate-type), and
  would hand newcomers a confusing first error they would rubber-stamp away.
- **Loop prevention** is orthogonal to idempotency (each turn of a loop is a *new*
  event, so idempotency does not bound it). The author implicitly declares a DAG of
  event → command → event that the library cannot verify (handlers are opaque; a
  code change can introduce a back-edge). We carry a **causation depth** in metadata
  and refuse to react past a configured maximum, acting as a runtime cycle-detector.
  At the limit we **skip the event and log loudly** (with the causation chain and
  the flags that change the behavior) while the policy keeps advancing — a circuit
  breaker, not a poison pill. The depth limit resolves most-specific-first:
  per-policy override → global env var → built-in default. We rejected provenance
  filtering (policies ignoring policy-authored events) because cascading policies
  are a wanted first-class use case.
- **Runtime** is one long-lived `tokio` task per policy (the async-daemon shape) —
  the honest analog of an actor without importing an actor framework, with the
  cursor owned task-locally. Event discovery is **polling** as the correctness
  baseline (works across disconnect/crash), with Postgres `LISTEN/NOTIFY` as an
  optional latency hint layered on top (best-effort, never the source of truth).
- **Single active runner per policy** via `pg_advisory_lock` (zero new
  infrastructure; leader failover for free — a dead leader's session-scoped lock
  releases and a standby resumes from the stored cursor). This is **not** a scaling
  compromise: **global event order is a correctness property of policies**, and only
  a single sequential consumer draining `global_position` in order preserves it.
  Competing-consumers/partitioning (the Kafka model) gives only per-partition order
  — which is exactly why Kafka is wrong for ordered event-sourced reactions. The
  inherent cost is that a single ordered policy cannot be parallelized; an
  order-indifferent policy could later opt *into* partitioning, but that is an
  explicit per-policy opt-out of the guarantee, never the default.
- **Batching.** Reads and cursor writes are batched, but a batch is a
  checkpoint/commit boundary, **never** a parallelism boundary: within a batch,
  events are applied strictly sequentially in `global_position` order. Durability is
  one transaction per command execution (slow but safe); the cursor is written every
  *K* committed commands. The **skip-safety rule**: the cursor only ever advances
  across events whose commands are durably committed — a duplicate is harmless, a
  skip is silent data loss. Batch sizes resolve per-policy → global env var →
  default. (Batching multiple commands into one transaction is deferred to a
  separate issue.)
- **Failure handling** dispatches on the existing `ErrorKind`/`ErrorStatus` (which
  already categorise by "what the caller can do"): `Temporary` → bounded retry with
  backoff; `Conflict` (optimistic-concurrency) → reload and retry (the natural dedup
  outcome of redelivery); `BusinessRuleViolation` → not a failure, the aggregate
  legitimately declined, log and advance; permanent/exhausted → write a
  `policy_dead_letters` row and advance (a *recorded* skip, never silent). Dead-
  letter-and-advance is safe precisely because v1 policies are stateless per-event
  reactions; a future stateful process manager would need halt-on-poison instead.
- **Authoring API.** A `Policy` is a sibling of `Query`/`InlineProjection`: a
  `query_events!`-merged `Event`, a stable `name`, a `stream_filter`, a `start_at`,
  and a single `react(&self, event) -> Result<Vec<Dispatch>>`. `react` is a **pure
  function of the event** — it returns typed `Dispatch::to::<A>(id, command)`
  descriptors and the **runner** executes them, owning the `Cqrs`, stamping causation
  and depth, applying optimistic concurrency, and dispatching errors. This keeps a
  policy unit-testable with no database and prevents authors from smuggling reads or
  out-of-band writes into a reaction. Aggregate `Services` are registered on the
  runner (`register_services::<A>(..)`), never held by the policy. A closure
  shortcut mirrors `register_postgres_event_handler` for the low-ceremony case.

## Consequences

- Policies are **native-only**, living in the server-only `persistence` crate and
  free to use `Send`/`BoxFuture`. The `Policy` *trait* and `Dispatch` descriptor are
  kept free of Postgres/tokio types so they remain a portable contract; the *runner*
  is the swappable, currently-native-only runtime. Supporting policies on WASM
  (single-instance, no leader election, local event log, `spawn_local`, `?Send`) is a
  second runner over the same contract and is deferred to its own issue.
- Archived (compacted) events become load-bearing for policies that may still need to
  walk pre-compaction history; see ADR-0004 for how the policy feed reads the true log
  across compaction.
- The compaction-versus-lagging-policy interaction and the duplicate-delivery,
  loop-limit, and failover behaviours are covered by Docker-gated Postgres
  integration tests, which double as the executable specification of these contracts.
