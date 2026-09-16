# Policy daemon tests assert through operator-visible observations

**Status:** accepted

Every Policy test in the crate calls `react` directly and inspects the
`Dispatch`es it returns. That seam is blind to everything the supervision work
(funkode-io/replay#180) is about: whether a worker exists, whether a cursor
moved, whether a restart happened, whether a dispatch ever came back. Building
supervision on top of it would mean testing supervision by inspecting the code
that implements it.

So the tests for the daemon assert from where an operator stands. A test starts
a real Postgres, runs a real `PolicyRunner` polling in the background, appends
an event, and reads back out of the database what the policy did. The harness
that does this lives in `persistence/tests/common/policy_harness.rs`.

## Decisions

- **The observation set is what an operator can see, and only that**: the
  commands a policy dispatched (read from `events` via the causation metadata the
  runner stamps, not recorded in-process), its persisted cursor, and its dead
  letters. Task handles, channels and internal counters are not exposed, so a
  test cannot accidentally assert on a mechanism instead of a behaviour — which
  is the failure mode that made the existing policy tests unable to see any of
  this.

- **Log output is not an observation.** Wording of a log line must stay free to
  change; a test that asserts on it makes observability part of the contract.
  Where a spec is about logging, the test asserts the state machine that decides
  *whether* to log.

- **Waiting is bounded, never timed.** Observations are re-read until they hold,
  with a deadline; no test sleeps a fixed duration and then asserts. A passing
  test therefore costs what the daemon actually takes, and a failing one reports
  the whole observation set rather than "assertion failed".

- **One container per harness.** Each test gets its own database and a
  process-unique policy name, so cursors, dead letters and the advisory locks
  that elect a leader cannot collide between tests — including tests that
  deliberately run two runners against one database.

- **Shutdown is explicit, and isolation covers the rest.** `shutdown()` awaits
  the daemon's workers, its shared listener and its lock manager, so a passing
  test leaves nothing running and releases its advisory locks rather than
  waiting out a session timeout. A panicking test cannot await anything — `Drop`
  is synchronous — but its tasks die with its own `#[tokio::test]` runtime and
  hold locks in a database no other test can reach, so the guarantee that
  matters (no task runs into the next test) comes from the per-test database,
  not from the happy path.

- **One harness, shared by both halves of the health work.** The progress-axis
  slices (#166–#171) and the liveness-axis slices (#180) assert through the same
  seam. A second harness would drift from the first, and the drift would be
  invisible until a test disagreed with production.

## Rejected

- **A shared container with a database per test.** Faster, and advisory locks are
  scoped per database so it would isolate correctly. It buys seconds at the cost
  of a global fixture whose lifetime spans the binary — and the crate already
  pays for a container per integration test elsewhere. Revisit if the daemon
  suite grows large enough for the arithmetic to change.

- **Recording dispatches in-process by wrapping the registered policy.** Simpler
  to write, but it records what a policy *intended*, not what the runner
  *executed* — precisely the distinction a panicking reaction, a timed-out
  dispatch or a parked one turns on.

- **Asserting on the aggregate's state after a reaction.** It works for one
  policy and one aggregate, and says nothing about which event caused what. The
  causation metadata already names the policy and the triggering event, so
  reading the dispatched commands answers the sharper question at the same cost.
