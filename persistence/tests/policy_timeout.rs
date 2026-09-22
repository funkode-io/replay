//! A reaction that hangs is cut loose by the dispatch timeout.
//!
//! Asserted from where an operator stands: the Policy keeps reacting, its cursor
//! keeps moving, and the reaction that hung is readable as a parked
//! [Dead letter] that says it timed out. Every observation goes through
//! `tests/common/policy_harness.rs` — a real daemon, a real database, no
//! inspection of tasks or channels.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common::policy_harness::{
    PolicyDaemonHarness, Probe, ProbeCommand, ProbeEvent, ProbeUrn, OBSERVE_TIMEOUT,
};
use replay_persistence::{
    Dispatch, PersistedEvent, Policy, PolicyRunnerBuilder, StartAt, TIMEOUT_ERROR_KIND,
};
use tracing_test::traced_test;

/// Tag whose reaction dispatches a command that does not come back. Any other
/// tag echoes.
const HANGS: &str = "hang";

/// How long the hung command sleeps: longer than any of these tests will run,
/// so a test that passes proves the dispatch was abandoned rather than awaited.
const HANG_FOR: Duration = Duration::from_secs(600);

/// The timeout a Policy that is expected to blow it sets: short enough that four
/// attempts plus their back-offs fit inside the harness's observation budget.
const TIGHT_TIMEOUT: Duration = Duration::from_millis(150);

/// The timeout a Policy that is expected to stay inside it sets. It bounds the
/// whole dispatch — the aggregate load and the append too, not just the command's
/// own sleep — so it is set where a loaded CI box cannot reach it.
const AMPLE_TIMEOUT: Duration = Duration::from_secs(5);

/// How long the command sleeps when the test wants it to finish in time.
const SLEEP_FOR: Duration = Duration::from_millis(50);

/// Dispatches a command that never returns for `HANGS`, an echo for anything
/// else, counting the reactions.
///
/// The count is how "retried under the existing backoff" is visible from
/// outside: each retry calls `react` again before anything is parked.
struct HangingPolicy {
    name: String,
    hang_for: Duration,
    timeout: Duration,
    reactions: Arc<AtomicUsize>,
}

impl Policy for HangingPolicy {
    type Event = ProbeEvent;

    fn name(&self) -> &str {
        &self.name
    }

    fn start_at(&self) -> StartAt {
        StartAt::Beginning
    }

    fn dispatch_timeout(&self) -> Option<Duration> {
        Some(self.timeout)
    }

    fn react(&self, event: &PersistedEvent<Self::Event>) -> Vec<Dispatch> {
        match &event.data {
            ProbeEvent::Pinged { tag } if tag == HANGS => {
                self.reactions.fetch_add(1, Ordering::SeqCst);
                vec![Dispatch::to::<Probe>(
                    ProbeUrn::new("wedged").unwrap(),
                    ProbeCommand::Sleep {
                        millis: self.hang_for.as_millis() as u64,
                    },
                )]
            }
            ProbeEvent::Pinged { tag } => vec![Dispatch::to::<Probe>(
                ProbeUrn::new(format!("{tag}-echo")).unwrap(),
                ProbeCommand::Echo { tag: tag.clone() },
            )],
            _ => vec![],
        }
    }
}

/// Register a [`HangingPolicy`] whose command sleeps for `hang_for` under a
/// `timeout`.
fn hanging_policy(
    hang_for: Duration,
    timeout: Duration,
    reactions: Arc<AtomicUsize>,
) -> impl Fn(PolicyRunnerBuilder, &str) -> PolicyRunnerBuilder + Send + Sync + 'static {
    move |builder, policy| {
        builder.register_policy(HangingPolicy {
            name: policy.to_string(),
            hang_for,
            timeout,
            reactions: Arc::clone(&reactions),
        })
    }
}

/// The whole of the [Dispatch timeout] in one run: the dispatch is abandoned and
/// retried, parked once the retries are exhausted, recorded as a timeout rather
/// than as a panic or a returned error, and the worker carries on.
#[tokio::test]
#[traced_test]
async fn a_hung_dispatch_is_parked_as_a_timeout_and_the_policy_keeps_reacting_postgres_test() {
    let reactions = Arc::new(AtomicUsize::new(0));
    let harness = PolicyDaemonHarness::start(
        "hangs",
        hanging_policy(HANG_FOR, TIGHT_TIMEOUT, Arc::clone(&reactions)),
    )
    .await;

    let hung = harness.ping("subject-1", HANGS).await;

    let parked = harness.await_dead_letters(1).await;
    assert_eq!(parked[0].global_position, hung.global_position);
    assert_eq!(parked[0].event_id, hung.event_id);
    assert_eq!(
        parked[0].error_kind, TIMEOUT_ERROR_KIND,
        "a timeout must be told apart from a panic and from a returned error, got {:?}",
        parked[0].error_kind
    );
    assert!(
        parked[0].error_message.contains("timeout"),
        "the parked row must say the dispatch was abandoned on its timeout, got {:?}",
        parked[0].error_message
    );

    // Retried, not parked on first occurrence: a hang may be a dependency that
    // comes back, which is what the existing back-off is for.
    assert!(
        reactions.load(Ordering::SeqCst) > 1,
        "a timed-out dispatch must be retried under the existing backoff, reacted {} time(s)",
        reactions.load(Ordering::SeqCst)
    );

    // Still leading, still reading: the event after the hang is reacted to.
    let next = harness.ping("subject-2", "hello").await;
    let dispatched = harness.await_dispatch_caused_by(next.global_position).await;
    assert_eq!(dispatched.event_type, "Echoed");

    harness.await_passed(hung.global_position).await;
    assert_eq!(
        harness.dead_letters().await.len(),
        1,
        "exhausting the retries parks one dead letter, not one per attempt"
    );

    // A worker cut loose from a hung reaction says so, with the one number that
    // is not inferable from the table: how long it waited. Keyed on the fields,
    // not the sentence — ADR-0014 keeps log wording free to change.
    logs_assert(|lines: &[&str]| {
        let position = format!("global_position={}", hung.global_position);
        let warned: Vec<&str> = lines
            .iter()
            .copied()
            .filter(|line| {
                line.contains("WARN")
                    && line.contains(harness.policy_name())
                    && line.contains(&position)
                    && line.contains("elapsed_ms=")
                    && line.contains("timeout_ms=")
            })
            .collect();
        if warned.is_empty() {
            let warnings: Vec<&str> = lines
                .iter()
                .copied()
                .filter(|line| line.contains("WARN"))
                .collect();
            return Err(format!(
                "expected a WARN carrying the policy, {position}, timeout_ms and elapsed_ms, got {warnings:#?}"
            ));
        }
        Ok(())
    });

    harness.shutdown().await;
}

/// A reaction that finishes inside its timeout is untouched by it: the command
/// commits, nothing is parked, and the Policy advances as it always did.
#[tokio::test]
async fn a_dispatch_that_finishes_inside_its_timeout_is_unaffected_postgres_test() {
    let reactions = Arc::new(AtomicUsize::new(0));
    let harness = PolicyDaemonHarness::start(
        "within_timeout",
        hanging_policy(SLEEP_FOR, AMPLE_TIMEOUT, Arc::clone(&reactions)),
    )
    .await;

    let slow = harness.ping("subject-1", HANGS).await;

    let dispatched = harness.await_dispatch_caused_by(slow.global_position).await;
    assert_eq!(
        dispatched.event_type, "Echoed",
        "the slow command must have committed its event"
    );
    assert_eq!(
        reactions.load(Ordering::SeqCst),
        1,
        "a dispatch inside its timeout must not be retried"
    );

    harness.await_passed(slow.global_position).await;
    assert!(
        harness.dead_letters().await.is_empty(),
        "a dispatch that finished in time must park nothing"
    );

    harness.shutdown().await;
}

/// The operator's own controls are bounded too: retrying a row parked for a
/// timeout replays the reaction that hung, and that retry must come back rather
/// than wedging the call — re-parking the row in place as a timeout.
#[tokio::test]
async fn retrying_a_parked_timeout_re_parks_it_rather_than_hanging_the_retry_postgres_test() {
    let reactions = Arc::new(AtomicUsize::new(0));
    let harness = PolicyDaemonHarness::start(
        "timeout_retry",
        hanging_policy(HANG_FOR, TIGHT_TIMEOUT, Arc::clone(&reactions)),
    )
    .await;

    let hung = harness.ping("subject-1", HANGS).await;
    harness.await_dead_letters(1).await;

    // The assertion is that this *returns*, so it gets a deadline of its own:
    // unbounded, a regression in the retry path would sit here for HANG_FOR and
    // be killed by CI instead of failing as a test.
    let summary = tokio::time::timeout(OBSERVE_TIMEOUT, harness.retry_parked())
        .await
        .unwrap_or_else(|_| {
            panic!(
                "the operator's retry did not return within {OBSERVE_TIMEOUT:?}: \
                 it is replaying a reaction that hangs without a dispatch timeout"
            )
        });
    assert_eq!(
        summary.reactions_resolved, 0,
        "a command that hangs cannot resolve"
    );
    assert_eq!(
        summary.reactions_still_failing, 1,
        "the row that timed out again must be reported as still failing"
    );

    let parked = harness.dead_letters().await;
    assert_eq!(parked.len(), 1, "a retry must never insert a second row");
    assert_eq!(parked[0].global_position, hung.global_position);
    assert_eq!(parked[0].error_kind, TIMEOUT_ERROR_KIND);

    harness.shutdown().await;
}
