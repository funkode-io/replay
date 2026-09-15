//! A panicking reaction is contained at the **event**, not at the worker.
//!
//! A reaction that panics used to unwind its worker task: nothing caught it,
//! nothing restarted it, and the Policy stopped reacting for the rest of the
//! process's life. These tests assert the containment from where an operator
//! stands — the Policy keeps reacting, its cursor keeps moving, and the event
//! that panicked is readable as a parked [Dead letter] that says it was a panic.
//!
//! Every observation goes through `tests/common/policy_harness.rs`: a real
//! daemon, a real database, no inspection of tasks or channels.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use common::policy_harness::{PolicyDaemonHarness, Probe, ProbeCommand, ProbeEvent, ProbeUrn};
use replay_persistence::{Dispatch, StartAt, PANIC_ERROR_KIND};
use tracing_test::traced_test;

/// Tag whose reaction panics. Any other tag echoes.
const POISON: &str = "panic";

/// The panic's message, so the parked row can be asserted to carry it.
const PANIC_MESSAGE: &str = "reaction exploded on a malformed payload";

/// A Policy that panics on `POISON` and echoes everything else, counting how
/// many times it was asked to react to a poison event.
///
/// The count is the only way to see "never retried" from outside: a retry would
/// call `react` again before parking anything.
fn panicking_policy(
    reactions: Arc<AtomicUsize>,
) -> impl Fn(replay_persistence::PolicyRunnerBuilder, &str) -> replay_persistence::PolicyRunnerBuilder
       + Send
       + Sync
       + 'static {
    move |builder, policy| {
        let reactions = Arc::clone(&reactions);
        builder.register_policy_fn::<ProbeEvent, _>(policy, StartAt::Beginning, move |event| {
            match &event.data {
                ProbeEvent::Pinged { tag } if tag == POISON => {
                    reactions.fetch_add(1, Ordering::SeqCst);
                    panic!("{PANIC_MESSAGE}");
                }
                ProbeEvent::Pinged { tag } => vec![Dispatch::to::<Probe>(
                    ProbeUrn::new(format!("{tag}-echo")).unwrap(),
                    ProbeCommand::Echo { tag: tag.clone() },
                )],
                _ => vec![],
            }
        })
    }
}

/// The whole of the per-event boundary in one run: the panicking event is
/// parked once, as a panic, without ever being re-reacted to; the cursor moves
/// past it; and the next event is still reacted to.
#[tokio::test]
#[traced_test]
async fn a_panicking_reaction_is_parked_and_the_policy_keeps_reacting_postgres_test() {
    let reactions = Arc::new(AtomicUsize::new(0));
    let harness =
        PolicyDaemonHarness::start("panics", panicking_policy(Arc::clone(&reactions))).await;

    let poison = harness.ping("subject-1", POISON).await;

    let parked = harness.await_dead_letters(1).await;
    assert_eq!(parked[0].global_position, poison.global_position);
    assert_eq!(parked[0].event_id, poison.event_id);
    assert_eq!(
        parked[0].error_kind, PANIC_ERROR_KIND,
        "a panic must be told apart from a returned error, got {:?}",
        parked[0].error_kind
    );
    assert!(
        parked[0].error_message.contains(PANIC_MESSAGE),
        "parked row must carry the panic's message, got {:?}",
        parked[0].error_message
    );

    // Never retried: a panic is permanent on first occurrence, so the reaction
    // ran exactly once before the row was parked.
    assert_eq!(
        reactions.load(Ordering::SeqCst),
        1,
        "a panicking reaction must be parked on its first occurrence, not retried"
    );

    // Still leading, still reading: the event after the panic is reacted to.
    let next = harness.ping("subject-2", "hello").await;
    let dispatched = harness.await_dispatch_caused_by(next.global_position).await;
    assert_eq!(dispatched.event_type, "Echoed");

    let cursor = harness.await_cursor_at_least(poison.global_position).await;
    assert!(
        cursor >= poison.global_position,
        "cursor {cursor} must have advanced past the event that panicked"
    );
    assert_eq!(
        harness.dead_letters().await.len(),
        1,
        "exactly one dead letter for the event that panicked"
    );
    assert_eq!(
        reactions.load(Ordering::SeqCst),
        1,
        "later events must not re-deliver the event that panicked"
    );

    // A parked panic is loud: a defect in a reaction must not be readable only
    // by querying the table for it.
    logs_assert(|lines: &[&str]| {
        let parked: Vec<&str> = lines
            .iter()
            .copied()
            .filter(|line| line.contains("policy reaction panicked"))
            .collect();
        let position = format!("global_position={}", poison.global_position);
        match parked.as_slice() {
            [line]
                if line.contains("ERROR")
                    && line.contains(harness.policy_name())
                    && line.contains(&position)
                    && line.contains(PANIC_MESSAGE) =>
            {
                Ok(())
            }
            _ => Err(format!(
                "expected one ERROR naming the policy, {position} and the panic, got {parked:#?}"
            )),
        }
    });

    harness.shutdown().await;
}

/// The cursor advance is durable, not just in-memory: a worker that stops and
/// starts again resumes *past* the panicking event rather than re-delivering it
/// and parking a second row.
#[tokio::test]
async fn a_restart_does_not_redeliver_the_panicking_event_postgres_test() {
    let reactions = Arc::new(AtomicUsize::new(0));
    let mut harness =
        PolicyDaemonHarness::start("panic_restart", panicking_policy(Arc::clone(&reactions))).await;

    let poison = harness.ping("subject-1", POISON).await;
    harness.await_dead_letters(1).await;
    harness.await_cursor_at_least(poison.global_position).await;

    harness.restart().await;

    // The restarted worker is demonstrably running: it reacts to a fresh event.
    let next = harness.ping("subject-2", "hello").await;
    harness.await_dispatch_caused_by(next.global_position).await;

    assert_eq!(
        reactions.load(Ordering::SeqCst),
        1,
        "the restarted worker must resume past the event that panicked"
    );
    assert_eq!(
        harness.dead_letters().await.len(),
        1,
        "a restart must not park a second dead letter for the same event"
    );

    harness.shutdown().await;
}
