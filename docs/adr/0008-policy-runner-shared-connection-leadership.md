# Policy runner shares one lock connection and one listener across all policies

**Status:** accepted

ADR-0003 chose a runtime of **one long-lived `tokio` task per policy** and
**single-active-runner-per-policy via `pg_advisory_lock`**, with optional
`LISTEN/NOTIFY` as a latency hint. That shape was correct, but its first
implementation pinned **two** dedicated Postgres connections *per policy* for the
whole leadership tenure: one connection holding the policy's session-scoped
advisory lock, and a second connection running a `PgListener` for `NOTIFY`. With
`P` policies that is `O(2P)` permanently-held connections. Once `2P` exceeds the
shared SQLx pool's `max_connections`, later policies can never acquire their
connections and silently stop processing — observed in a consuming application as
a "database connection pool timed out" error (issue #125).

This ADR keeps every correctness property of ADR-0003 (single ordered consumer per
policy, automatic leader failover, polling-as-baseline with NOTIFY-as-hint) but
makes the steady-state connection footprint **constant in the number of policies**
rather than linear.

## Decisions

- **One shared lock-manager task per runner**, holding **one** connection, acquires
  *every* policy's `pg_advisory_lock` on that single session. Postgres session-level
  advisory locks are per-session and a single session may hold many distinct lock
  keys simultaneously, so one connection can elect leadership for all `P` policies.
  Each key is tried independently with non-blocking `pg_try_advisory_lock`, so
  **different policies still lead on different instances** and **exactly one**
  instance leads each policy — the single-ordered-consumer guarantee is unchanged.
  The manager publishes per-policy leadership on a `tokio::sync::watch<bool>` that
  each worker observes. On shutdown it explicitly `pg_advisory_unlock`s the held
  keys so a standby takes over without waiting for a TCP session timeout.
- **One shared `PgListener` task per runner**, holding **one** connection, receives
  every append `NOTIFY` and fans each wakeup out to all workers in-process via a
  `tokio::sync::broadcast<()>` channel. This is consistent with ADR-0003's rule that
  NOTIFY is only a best-effort latency hint, never the source of truth: a lagged,
  dropped, or missed broadcast is harmless because each worker still wakes on the
  poll `interval`. The listener task reconnects on error; while it is down, workers
  degrade to pure polling.
- **Per-policy worker tasks hold no pinned connection.** A worker waits for its
  leadership `watch` to go true, loads its cursor, then drains — acquiring only
  *transient* pooled connections for the duration of each drain and releasing them
  between polls. The drain loop wakes on `tokio::select!` over shutdown, the
  leadership signal, the poll interval, and the broadcast wakeup.

## Consequences

- **Steady-state pinned connections are `~2` regardless of policy count** (one lock
  manager + one listener), plus transient connections borrowed per drain. A runner
  with dozens of policies no longer exhausts a modest pool. This is verified by
  `policy_bounded_connection_footprint_many_policies_postgres_test`: 12 policies on
  an 8-connection pool (the old design needed 24) all process the trigger event.
- **Failover granularity is coarser**, and this is the deliberate trade-off. Because
  all of an instance's advisory locks live on one session, when that session drops
  (process death, connection loss) **all** of that instance's policies fail over to a
  standby together, rather than each failing over independently. This is acceptable:
  each policy is still recovered **independently and exactly once** from its stored
  cursor, and instance-granularity failover matches how processes actually fail (the
  whole process, not one policy's connection). The previous per-policy failover
  granularity was an accident of the per-policy-connection implementation, not a
  designed guarantee.
- The `Policy` trait, `Dispatch` descriptor, cursor/checkpoint semantics, dead-letter
  handling, causation-depth loop limit, and `start_at` behaviour are all untouched —
  this is purely a change to how the *runner* maps policies onto connections. No
  schema change.
- The behaviour is covered by the existing Docker-gated Postgres integration tests
  (single-active-runner failover, NOTIFY-wakes-before-interval, without-notifications
  polling, dead-letter, and status) plus the new bounded-footprint test, which
  together remain the executable specification of these contracts.
