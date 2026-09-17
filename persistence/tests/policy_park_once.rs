//! A delivery parks each failing dispatch once, however many attempts it takes.
//!
//! A reaction returning several commands retries the whole reaction when any one
//! of them fails retryably, so a sibling that fails *permanently* is executed
//! again on every attempt. What an operator must not see is that repetition in
//! `policy_dead_letters`: one delivery, one row per failing command.
//!
//! Every observation goes through `tests/common/policy_harness.rs` — a real
//! daemon, a real database, no inspection of tasks or channels.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use common::policy_harness::{
    DeadLetter, PolicyDaemonHarness, Probe, ProbeCommand, ProbeEvent, ProbeUrn,
};
use replay_persistence::{Dispatch, PersistedEvent, Policy, PolicyRunnerBuilder, StartAt};

/// Tag whose reaction returns a permanently failing command followed by a
/// retryable one: the permanent command is re-executed on every attempt the
/// retryable one forces.
const MIXED: &str = "mixed";

/// The same pair the other way round: the retryable command comes first, so on
/// every attempt but the last it breaks out before the permanent one runs.
const REVERSED: &str = "reversed";

/// Tag whose reaction returns two commands that both fail permanently on the
/// first attempt — nothing forces a second one.
const BOTH_PERMANENT: &str = "both-permanent";

/// Tag whose reaction returns a single permanently failing command.
const ONE_PERMANENT: &str = "one-permanent";

/// Tag whose reaction returns a single retryable command, which parks once its
/// retries are exhausted.
const ONE_RETRYABLE: &str = "one-retryable";

/// Reasons the probe reports, so a parked row can be attributed to the command
/// that produced it.
const PERMANENT_REASON: &str = "permanent-command";
const RETRYABLE_REASON: &str = "transient-command";

/// The reasons the two permanently failing commands of [`BOTH_PERMANENT`]
/// report, so each is distinguishable in the rows they park.
const FIRST_REASON: &str = "first-command";
const SECOND_REASON: &str = "second-command";

/// The `error_kind` a refused command is parked under.
const PERMANENT_KIND: &str = "Invalid Input";

/// The `error_kind` an exhausted retryable command is parked under.
const RETRYABLE_KIND: &str = "Unavailable";

/// Dispatches the commands a tag asks for, counting the reactions.
///
/// The count is how "the retry loop ran" is visible from outside: each attempt
/// calls `react` again.
struct ParkingPolicy {
    name: String,
    reactions: Arc<AtomicUsize>,
}

impl ParkingPolicy {
    fn refuse(reason: &str) -> Dispatch {
        Dispatch::to::<Probe>(
            ProbeUrn::new("subject").unwrap(),
            ProbeCommand::Refuse {
                reason: reason.to_string(),
            },
        )
    }

    fn flake(reason: &str) -> Dispatch {
        Dispatch::to::<Probe>(
            ProbeUrn::new("subject").unwrap(),
            ProbeCommand::Flake {
                reason: reason.to_string(),
            },
        )
    }
}

impl Policy for ParkingPolicy {
    type Event = ProbeEvent;

    fn name(&self) -> &str {
        &self.name
    }

    fn start_at(&self) -> StartAt {
        StartAt::Beginning
    }

    fn react(&self, event: &PersistedEvent<Self::Event>) -> Vec<Dispatch> {
        let ProbeEvent::Pinged { tag } = &event.data else {
            return vec![];
        };
        self.reactions.fetch_add(1, Ordering::SeqCst);
        match tag.as_str() {
            MIXED => vec![
                Self::refuse(PERMANENT_REASON),
                Self::flake(RETRYABLE_REASON),
            ],
            REVERSED => vec![
                Self::flake(RETRYABLE_REASON),
                Self::refuse(PERMANENT_REASON),
            ],
            BOTH_PERMANENT => vec![Self::refuse(FIRST_REASON), Self::refuse(SECOND_REASON)],
            ONE_PERMANENT => vec![Self::refuse(PERMANENT_REASON)],
            ONE_RETRYABLE => vec![Self::flake(RETRYABLE_REASON)],
            tag => vec![Dispatch::to::<Probe>(
                ProbeUrn::new(format!("{tag}-echo")).unwrap(),
                ProbeCommand::Echo {
                    tag: tag.to_string(),
                },
            )],
        }
    }
}

fn parking_policy(
    reactions: Arc<AtomicUsize>,
) -> impl Fn(PolicyRunnerBuilder, &str) -> PolicyRunnerBuilder + Send + Sync + 'static {
    move |builder, policy| {
        builder.register_policy(ParkingPolicy {
            name: policy.to_string(),
            reactions: Arc::clone(&reactions),
        })
    }
}

/// Every row parked for the event at `position`.
fn parked_for(parked: &[DeadLetter], position: i64) -> Vec<&DeadLetter> {
    parked
        .iter()
        .filter(|row| row.global_position == position)
        .collect()
}

/// How many of `rows` are what a command failing with `reason` parks.
fn rows_for(rows: &[&DeadLetter], kind: &str, reason: &str) -> usize {
    rows.iter()
        .filter(|row| row.error_kind == kind && row.error_message.contains(reason))
        .count()
}

/// Assert that the event at `position` parked exactly one row for each of the
/// two commands its reaction returned — the permanent one and the one whose
/// retries were exhausted — and nothing else.
fn assert_one_row_per_failing_command(parked: &[DeadLetter], position: i64) {
    let rows = parked_for(parked, position);
    assert_eq!(
        rows.len(),
        2,
        "one row per failing command, not one per command per attempt, got {rows:#?}"
    );
    assert_eq!(
        rows_for(&rows, PERMANENT_KIND, PERMANENT_REASON),
        1,
        "the permanently failing command must be parked exactly once, got {rows:#?}"
    );
    assert_eq!(
        rows_for(&rows, RETRYABLE_KIND, RETRYABLE_REASON),
        1,
        "the command whose retries were exhausted must be parked once, got {rows:#?}"
    );
}

/// The regression: a permanent failure alongside a retryable sibling is parked
/// once, not once per attempt.
#[tokio::test]
async fn a_permanent_failure_is_parked_once_however_many_attempts_a_sibling_forces_postgres_test() {
    let reactions = Arc::new(AtomicUsize::new(0));
    let harness =
        PolicyDaemonHarness::start("park_once", parking_policy(Arc::clone(&reactions))).await;

    let mixed = harness.ping("subject-1", MIXED).await;

    // The cursor moves only after the delivery has parked everything it is going
    // to park, so a count read afterwards is the final one.
    harness.await_cursor_at_least(mixed.global_position).await;

    let parked = harness.dead_letters().await;
    assert_one_row_per_failing_command(&parked, mixed.global_position);

    // The premise of the test: the retryable sibling did force further attempts,
    // each of which re-executed the permanent command.
    assert!(
        reactions.load(Ordering::SeqCst) > 1,
        "a retryable sibling must have forced more than one attempt, reacted {} time(s)",
        reactions.load(Ordering::SeqCst)
    );

    // Order must not matter: the same pair the other way round parks the same
    // two rows.
    let reversed = harness.ping("subject-2", REVERSED).await;
    harness
        .await_cursor_at_least(reversed.global_position)
        .await;
    let parked = harness.dead_letters().await;
    assert_one_row_per_failing_command(&parked, reversed.global_position);

    // Still leading, still reading.
    let next = harness.ping("subject-3", "hello").await;
    let dispatched = harness.await_dispatch_caused_by(next.global_position).await;
    assert_eq!(dispatched.event_type, "Echoed");

    harness.shutdown().await;
}

/// The deliveries no sibling forces a second attempt on are untouched: one row
/// per failing command, whether the reaction returned two commands that both
/// fail permanently or a single one, and whether that single command failed
/// permanently or ran out of retries on its own.
#[tokio::test]
async fn a_delivery_without_a_retryable_sibling_parks_one_row_per_failing_command_postgres_test() {
    let reactions = Arc::new(AtomicUsize::new(0));
    let harness =
        PolicyDaemonHarness::start("park_unchanged", parking_policy(Arc::clone(&reactions))).await;

    let both = harness.ping("subject-1", BOTH_PERMANENT).await;
    let single = harness.ping("subject-2", ONE_PERMANENT).await;
    let exhausted = harness.ping("subject-3", ONE_RETRYABLE).await;

    harness
        .await_cursor_at_least(exhausted.global_position)
        .await;
    let parked = harness.dead_letters().await;

    let both_rows = parked_for(&parked, both.global_position);
    assert_eq!(
        both_rows.len(),
        2,
        "two commands failing permanently on the first attempt park two rows, got {parked:#?}"
    );
    assert_eq!(
        rows_for(&both_rows, PERMANENT_KIND, FIRST_REASON),
        1,
        "the first command must be parked exactly once, got {both_rows:#?}"
    );
    assert_eq!(
        rows_for(&both_rows, PERMANENT_KIND, SECOND_REASON),
        1,
        "the second command must be parked exactly once, got {both_rows:#?}"
    );

    let single_rows = parked_for(&parked, single.global_position);
    assert_eq!(
        single_rows.len(),
        1,
        "a single permanently failing command parks one row, got {parked:#?}"
    );
    assert_eq!(
        rows_for(&single_rows, PERMANENT_KIND, PERMANENT_REASON),
        1,
        "the row must be the one that command parks, got {single_rows:#?}"
    );

    let retried = parked_for(&parked, exhausted.global_position);
    assert_eq!(
        retried.len(),
        1,
        "a single command that exhausts its retries parks one row, got {parked:#?}"
    );
    assert_eq!(
        rows_for(&retried, RETRYABLE_KIND, RETRYABLE_REASON),
        1,
        "the row must be the one that command parks, got {retried:#?}"
    );

    harness.shutdown().await;
}
