//! A redelivered event refreshes what it parked; it does not park it again.
//!
//! A dead letter is written before the batched cursor checkpoint, so a crash in
//! between — or an operator rewinding the cursor (ADR-0012) — delivers the event
//! again. Parking was an unconditional INSERT, so every such delivery left a
//! fresh generation of rows for one reaction: the same command listed twice,
//! each copy with its own id to triage and discard (funkode-io/replay#220).
//!
//! The redelivery here is the real one — the cursor is rewound and the daemon
//! drains forward again — not a copied row: what the key has to survive is the
//! park path running twice, which a fabricated duplicate never exercises.
//!
//! Every observation goes through `tests/common/policy_harness.rs` — a real
//! daemon, a real database, no inspection of tasks or channels.

use crate::common;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use common::policy_harness::{
    DeadLetter, PolicyDaemonHarness, Probe, ProbeCommand, ProbeEvent, ProbeUrn,
};
use replay_persistence::{
    Dispatch, ObservedEvent, Policy, PolicyRunnerBuilder, PolicySettings, StartAt, PANIC_ERROR_KIND,
};

/// Tag whose reaction returns one permanently failing command, reporting a
/// different reason on each delivery — which is how "the row carries the latest
/// delivery's error" is observable at all.
const ONE_COMMAND: &str = "one-command";

/// Tag whose reaction returns the same command type to the same instance twice,
/// both failing permanently: two parked commands, not one parked twice.
const TWICE_TO_ONE_INSTANCE: &str = "twice-to-one-instance";

/// Tag whose reaction panics before it has built a dispatch, parking a row with
/// no identity to name.
const PANIC_IN_REACT: &str = "panic-in-react";

/// The stream every failing command is addressed to.
const SUBJECT: &str = "subject";

/// What [`PANIC_IN_REACT`]'s reaction panics with.
const REACT_PANIC_MESSAGE: &str = "reaction exploded";

/// A reaction whose failure reason names the delivery that produced it.
struct RedeliveredPolicy {
    name: String,
    /// Deliveries of a matching event so far: its reason, and the proof the
    /// reaction really ran a second time.
    deliveries: Arc<AtomicUsize>,
    /// Set to make `react` panic on every event, whatever its tag — the deploy
    /// between two deliveries that a test needs to park a row with no identity
    /// for an event that already has one.
    panic_always: Arc<AtomicBool>,
}

impl RedeliveredPolicy {
    fn refuse(reason: String) -> Dispatch {
        Dispatch::to::<Probe>(
            ProbeUrn::new(SUBJECT).unwrap(),
            ProbeCommand::Refuse { reason },
        )
    }
}

impl Policy for RedeliveredPolicy {
    type Event = ProbeEvent;

    fn name(&self) -> &str {
        &self.name
    }

    fn react(&self, event: &ObservedEvent<Self::Event>) -> Vec<Dispatch> {
        let ProbeEvent::Pinged { tag } = &event.data else {
            return vec![];
        };
        if self.panic_always.load(Ordering::SeqCst) {
            self.deliveries.fetch_add(1, Ordering::SeqCst);
            panic!("{REACT_PANIC_MESSAGE}");
        }
        match tag.as_str() {
            ONE_COMMAND => {
                let delivery = self.deliveries.fetch_add(1, Ordering::SeqCst) + 1;
                vec![Self::refuse(format!("delivery {delivery}"))]
            }
            TWICE_TO_ONE_INSTANCE => {
                let delivery = self.deliveries.fetch_add(1, Ordering::SeqCst) + 1;
                vec![
                    Self::refuse(format!("first of delivery {delivery}")),
                    Self::refuse(format!("second of delivery {delivery}")),
                ]
            }
            PANIC_IN_REACT => {
                self.deliveries.fetch_add(1, Ordering::SeqCst);
                panic!("{REACT_PANIC_MESSAGE}")
            }
            tag => vec![Dispatch::to::<Probe>(
                ProbeUrn::new(format!("{tag}-echo")).unwrap(),
                ProbeCommand::Echo {
                    tag: tag.to_string(),
                },
            )],
        }
    }
}

fn redelivered_policy(
    deliveries: Arc<AtomicUsize>,
) -> impl Fn(PolicyRunnerBuilder, &str) -> PolicyRunnerBuilder + Send + Sync + 'static {
    redelivered_policy_that(deliveries, Arc::new(AtomicBool::new(false)))
}

fn redelivered_policy_that(
    deliveries: Arc<AtomicUsize>,
    panic_always: Arc<AtomicBool>,
) -> impl Fn(PolicyRunnerBuilder, &str) -> PolicyRunnerBuilder + Send + Sync + 'static {
    move |builder, policy| {
        builder.register_policy(
            RedeliveredPolicy {
                name: policy.to_string(),
                deliveries: Arc::clone(&deliveries),
                panic_always: Arc::clone(&panic_always),
            },
            PolicySettings::new().starting_at(StartAt::Beginning),
        )
    }
}

/// Wait until every row the policy has parked has been parked `deliveries`
/// times, and return them.
async fn await_deliveries(harness: &PolicyDaemonHarness, deliveries: i32) -> Vec<DeadLetter> {
    harness
        .observe(
            &format!("every parked row to count {deliveries} delivery(s)"),
            || async {
                let parked = harness.dead_letters().await;
                (!parked.is_empty() && parked.iter().all(|row| row.deliveries == deliveries))
                    .then_some(parked)
            },
        )
        .await
}

/// The regression: a second delivery of an event whose reaction fails the same
/// way leaves one row, not two.
///
/// The row is the first one — same id, same `created_at`, the retry bookkeeping
/// untouched, because nobody invoked [Retry] — carrying the error the *latest*
/// delivery produced and a count of the deliveries that parked it.
#[tokio::test]
async fn a_redelivered_command_refreshes_its_row_instead_of_parking_a_second_postgres_test() {
    let deliveries = Arc::new(AtomicUsize::new(0));
    let harness =
        PolicyDaemonHarness::start("redeliver_one", redelivered_policy(Arc::clone(&deliveries)))
            .await;

    let event = harness.ping("subject-1", ONE_COMMAND).await;
    let parked = harness.await_dead_letters(1).await;
    assert_eq!(parked.len(), 1, "one failing command, one row: {parked:#?}");
    let first = parked[0].clone();
    assert_eq!(first.deliveries, 1);
    assert!(first.error_message.contains("delivery 1"));
    assert_eq!(
        first.dispatch_ordinal,
        Some(0),
        "the only dispatch of the reaction"
    );

    let before = harness
        .status()
        .await
        .expect("a policy that has parked something has a cursor row");

    harness.redeliver(&event).await;
    let after = await_deliveries(&harness, 2).await;

    assert_eq!(
        after.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![first.id],
        "the redelivery must refresh the row the first delivery parked, not add one: {after:#?}"
    );
    assert!(
        after[0].error_message.contains("delivery 2"),
        "and it must carry the latest delivery's error, got {:?}",
        after[0].error_message
    );
    assert_eq!(
        (
            after[0].created_at,
            after[0].retry_count,
            after[0].last_retried_at
        ),
        (first.created_at, 0, None),
        "a redelivery is not a retry: when the command first failed, and what an \
         operator has tried on it, are untouched"
    );
    assert!(
        after[0].last_parked_at > first.last_parked_at,
        "but when it was last parked must move: {:?} then {:?}",
        first.last_parked_at,
        after[0].last_parked_at
    );
    assert_eq!(
        deliveries.load(Ordering::SeqCst),
        2,
        "the premise: the reaction really did run a second time"
    );

    // What an operator monitoring the policy sees: one parked command still, and
    // a recency signal that moved although nothing new was parked.
    let status = harness
        .status()
        .await
        .expect("the status row is still there");
    assert_eq!(
        status.dead_letter_count, 1,
        "one parked command, however many deliveries parked it"
    );
    assert!(
        status.last_dead_letter_at > before.last_dead_letter_at,
        "the last parking must advance on a redelivery that parks nothing new: \
         {:?} then {:?}",
        before.last_dead_letter_at,
        status.last_dead_letter_at
    );

    harness.shutdown().await;
}

/// A reaction's own repeats are two parked commands, and stay two rows.
///
/// Same aggregate, same instance, same command type: nothing but the dispatch's
/// position within the reaction tells them apart, which is why the key carries
/// that ordinal. Collapsing them would report one failure where two commands
/// failed.
#[tokio::test]
async fn a_reactions_repeated_command_keeps_a_row_of_its_own_postgres_test() {
    let deliveries = Arc::new(AtomicUsize::new(0));
    let harness = PolicyDaemonHarness::start(
        "redeliver_twice",
        redelivered_policy(Arc::clone(&deliveries)),
    )
    .await;

    let event = harness.ping("subject-1", TWICE_TO_ONE_INSTANCE).await;
    let parked = harness.await_dead_letters(2).await;
    assert_eq!(
        parked
            .iter()
            .map(|row| row.dispatch_ordinal)
            .collect::<Vec<_>>(),
        vec![Some(0), Some(1)],
        "two indistinguishable commands, told apart by their place in the reaction: \
         {parked:#?}"
    );

    harness.redeliver(&event).await;
    let after = await_deliveries(&harness, 2).await;

    assert_eq!(
        after.iter().map(|row| row.id).collect::<Vec<_>>(),
        parked.iter().map(|row| row.id).collect::<Vec<_>>(),
        "the redelivery refreshes both rows and adds neither: {after:#?}"
    );
    assert!(
        after[0].error_message.contains("first of delivery 2")
            && after[1].error_message.contains("second of delivery 2"),
        "each row takes the error of the dispatch that holds its place, got {after:#?}"
    );

    harness.shutdown().await;
}

/// A panic in `react` parks one row per reaction, however often the event is
/// delivered — and it collapses onto a row an older release parked for the same
/// event.
///
/// Such a row names no dispatch: `react` failed before building one. The key
/// covers it through `NULLS NOT DISTINCT`, so the case that parks exactly one
/// row per delivery keeps exactly one row in total. The pre-identity row
/// (funkode-io/replay#210) is null in the same columns, so it is that row —
/// ids, `created_at` and all — that the redelivery refreshes.
#[tokio::test]
async fn a_panicking_reaction_parks_one_row_however_often_the_event_arrives_postgres_test() {
    let deliveries = Arc::new(AtomicUsize::new(0));
    let harness = PolicyDaemonHarness::start(
        "redeliver_panic",
        redelivered_policy(Arc::clone(&deliveries)),
    )
    .await;

    let event = harness.ping("subject-1", PANIC_IN_REACT).await;
    let parked = harness.await_dead_letters(1).await;
    assert_eq!(parked.len(), 1, "one panic, one row: {parked:#?}");
    assert_eq!(parked[0].error_kind, PANIC_ERROR_KIND);
    assert_eq!(
        (
            parked[0].aggregate_name.as_deref(),
            parked[0].command_name.as_deref(),
            parked[0].dispatch_ordinal
        ),
        (None, None, None),
        "a panic in `react` has no dispatch to name: {parked:#?}"
    );

    harness.redeliver(&event).await;
    let after = await_deliveries(&harness, 2).await;

    assert_eq!(
        after.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![parked[0].id],
        "a second delivery of a panicking reaction must refresh its row, not add one: \
         {after:#?}"
    );

    harness.shutdown().await;
}

/// A row parked before the identity migration is refreshed by a redelivery of
/// its event, not duplicated by it.
///
/// The row an upgrade leaves with a null ordinal is a panicking reaction's: the
/// dedupe numbers every other identity-less row apart, because only a panic
/// parks exactly one row per delivery. A panic in `react` parks exactly that
/// shape again, so a redelivery has the inherited row to land on — and lands on
/// it.
#[tokio::test]
async fn a_pre_migration_row_is_refreshed_by_a_redelivery_not_duplicated_postgres_test() {
    let deliveries = Arc::new(AtomicUsize::new(0));
    let panic_always = Arc::new(AtomicBool::new(false));
    let harness = PolicyDaemonHarness::start(
        "redeliver_premigration",
        redelivered_policy_that(Arc::clone(&deliveries), Arc::clone(&panic_always)),
    )
    .await;

    // Appended with a tag the policy echoes, so the row below is the only thing
    // parked for it: the state an upgrade inherits.
    let event = harness.ping("subject-1", "hello").await;
    harness
        .await_dispatch_caused_by(event.global_position)
        .await;
    let inherited = harness
        .park_panic_without_identity(&event, "parked by an older release")
        .await;

    // The reaction now panics before it builds a dispatch, so the redelivery
    // parks a row with the same null identity the inherited one has.
    panic_always.store(true, Ordering::SeqCst);
    harness.redeliver(&event).await;
    let after = await_deliveries(&harness, 2).await;

    assert_eq!(
        after.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![inherited],
        "the redelivery must refresh the inherited row rather than park beside it: \
         {after:#?}"
    );
    assert_eq!(after[0].error_kind, PANIC_ERROR_KIND);

    harness.shutdown().await;
}
