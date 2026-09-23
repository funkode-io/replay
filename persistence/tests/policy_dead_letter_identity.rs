//! A parked dead letter says *what* failed, not only *when*.
//!
//! A row used to carry the triggering event and an error string, so n commands
//! failing on one event produced n rows an operator could not tell apart. These
//! tests assert the identity an operator now reads off the row: the aggregate
//! type, the instance the command was addressed to, and the command's type.
//!
//! Every observation goes through `tests/common/policy_harness.rs` — a real
//! daemon, a real database, no inspection of tasks or channels.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use common::policy_harness::{
    DeadLetter, PolicyDaemonHarness, Probe, ProbeCommand, ProbeEvent, ProbeUrn,
};
use replay_persistence::{
    DeadLetterDiscard, DeadLetterRetry, Dispatch, ObservedEvent, Policy, PolicyRunnerBuilder,
    PolicySettings, StartAt, PANIC_ERROR_KIND, TIMEOUT_ERROR_KIND,
};

/// Tag whose reaction dispatches a command the aggregate refuses permanently.
const REFUSED: &str = "refused";

/// Tag whose reaction dispatches a command that fails retryably every time, so
/// it is parked once its retry budget is spent.
const EXHAUSTED: &str = "exhausted";

/// Tag whose reaction dispatches a command whose *handler* panics: the panic
/// settles the delivery from inside the await, with a dispatch in flight.
const EXPLODES: &str = "explodes";

/// Tag whose reaction dispatches a command that never comes back, so it is
/// abandoned on the dispatch timeout.
const HANGS: &str = "hangs";

/// Tag whose reaction panics before it has built a dispatch at all.
const PANICS_IN_REACT: &str = "panics-in-react";

/// The instance each failing command is addressed to — what "which customer is
/// stuck" means here.
const REFUSED_SUBJECT: &str = "refused-customer";
const EXHAUSTED_SUBJECT: &str = "exhausted-customer";
const EXPLODED_SUBJECT: &str = "exploded-customer";
const HUNG_SUBJECT: &str = "hung-customer";

/// The `error_kind` a refused command is parked under.
const PERMANENT_KIND: &str = "Invalid Input";

/// The `error_kind` a command that ran out of retries is parked under.
const RETRYABLE_KIND: &str = "Unavailable";

/// Long enough that a dispatch waiting on it is abandoned rather than awaited.
const HANG_FOR: Duration = Duration::from_secs(600);

/// Short enough that four attempts and their back-offs fit in the harness's
/// observation budget.
const TIGHT_TIMEOUT: Duration = Duration::from_millis(150);

/// A Policy whose reactions fail on every path that parks a row.
struct FailingPolicy {
    name: String,
    /// Calls to the reaction, so a test can prove a retry replayed it.
    reactions: Arc<AtomicUsize>,
}

impl FailingPolicy {
    fn dispatch(stream: &str, command: ProbeCommand) -> Dispatch {
        Dispatch::to::<Probe>(ProbeUrn::new(stream).unwrap(), command)
    }
}

impl Policy for FailingPolicy {
    type Event = ProbeEvent;

    fn name(&self) -> &str {
        &self.name
    }

    fn react(&self, event: &ObservedEvent<Self::Event>) -> Vec<Dispatch> {
        let ProbeEvent::Pinged { tag } = &event.data else {
            return vec![];
        };
        self.reactions.fetch_add(1, Ordering::SeqCst);
        match tag.as_str() {
            REFUSED => vec![Self::dispatch(
                REFUSED_SUBJECT,
                ProbeCommand::Refuse {
                    reason: "refused".to_string(),
                },
            )],
            EXHAUSTED => vec![Self::dispatch(
                EXHAUSTED_SUBJECT,
                ProbeCommand::Flake {
                    reason: "flaked".to_string(),
                },
            )],
            EXPLODES => vec![Self::dispatch(
                EXPLODED_SUBJECT,
                ProbeCommand::Explode {
                    reason: "exploded".to_string(),
                },
            )],
            HANGS => vec![Self::dispatch(
                HUNG_SUBJECT,
                ProbeCommand::Sleep {
                    millis: HANG_FOR.as_millis() as u64,
                },
            )],
            PANICS_IN_REACT => panic!("reaction exploded before it built a dispatch"),
            tag => vec![Self::dispatch(
                &format!("{tag}-echo"),
                ProbeCommand::Echo {
                    tag: tag.to_string(),
                },
            )],
        }
    }
}

fn failing_policy(
    reactions: Arc<AtomicUsize>,
) -> impl Fn(PolicyRunnerBuilder, &str) -> PolicyRunnerBuilder + Send + Sync + 'static {
    move |builder, policy| {
        builder.register_policy(
            FailingPolicy {
                name: policy.to_string(),
                reactions: Arc::clone(&reactions),
            },
            PolicySettings::new()
                .starting_at(StartAt::Beginning)
                .with_dispatch_timeout(TIGHT_TIMEOUT),
        )
    }
}

/// The URN a command addressed to `stream` carries.
fn urn_of(stream: &str) -> String {
    ProbeUrn::new(stream).unwrap().to_urn().to_string()
}

/// The single row parked for the event at `position`.
fn row_at(parked: &[DeadLetter], position: i64) -> DeadLetter {
    let rows: Vec<&DeadLetter> = parked
        .iter()
        .filter(|row| row.global_position == position)
        .collect();
    assert_eq!(
        rows.len(),
        1,
        "expected exactly one row parked for position {position}, got {rows:#?}"
    );
    rows[0].clone()
}

/// Assert the row names the dispatch it was parked for.
fn assert_identifies(row: &DeadLetter, stream: &str) {
    assert_eq!(
        row.aggregate_name.as_deref(),
        Some(std::any::type_name::<Probe>()),
        "the row must name the aggregate type the command targeted, got {row:#?}"
    );
    assert_eq!(
        row.target_stream_id.as_deref(),
        Some(urn_of(stream).as_str()),
        "the row must name the instance the command was addressed to, got {row:#?}"
    );
    assert_eq!(
        row.command_name.as_deref(),
        Some(std::any::type_name::<ProbeCommand>()),
        "the row must name the command type, got {row:#?}"
    );
}

/// Every path that parks a row names the dispatch that failed: a refused
/// command, a command that ran out of retries, a handler that panicked, and a
/// dispatch abandoned on its timeout.
#[tokio::test]
async fn every_parked_row_names_the_command_and_the_instance_it_failed_on_postgres_test() {
    let reactions = Arc::new(AtomicUsize::new(0));
    let harness =
        PolicyDaemonHarness::start("identity", failing_policy(Arc::clone(&reactions))).await;

    let refused = harness.ping("subject-1", REFUSED).await;
    let exhausted = harness.ping("subject-2", EXHAUSTED).await;
    let exploded = harness.ping("subject-3", EXPLODES).await;
    let hung = harness.ping("subject-4", HANGS).await;

    let parked = harness.await_dead_letters(4).await;

    let refused_row = row_at(&parked, refused.global_position);
    assert_eq!(refused_row.error_kind, PERMANENT_KIND);
    assert_identifies(&refused_row, REFUSED_SUBJECT);

    let exhausted_row = row_at(&parked, exhausted.global_position);
    assert_eq!(exhausted_row.error_kind, RETRYABLE_KIND);
    assert_identifies(&exhausted_row, EXHAUSTED_SUBJECT);

    // A panic inside a command handler has a dispatch in flight, and the row
    // carries it even though the failure arrived by unwinding.
    let exploded_row = row_at(&parked, exploded.global_position);
    assert_eq!(exploded_row.error_kind, PANIC_ERROR_KIND);
    assert_identifies(&exploded_row, EXPLODED_SUBJECT);

    let hung_row = row_at(&parked, hung.global_position);
    assert_eq!(hung_row.error_kind, TIMEOUT_ERROR_KIND);
    assert_identifies(&hung_row, HUNG_SUBJECT);

    // The identity survives the row leaving the active set: an archived failure
    // is still readable as the command it was.
    assert_eq!(
        harness.discard_parked_row(refused_row.id).await,
        DeadLetterDiscard::Discarded
    );
    let archived = harness.archived_dead_letters().await;
    assert_eq!(archived.len(), 1, "the discard must archive the row");
    assert_eq!(archived[0].dead_letter_id, refused_row.id);
    assert_eq!(
        archived[0].target_stream_id.as_deref(),
        Some(urn_of(REFUSED_SUBJECT).as_str()),
        "the archive must keep what the row identified, got {archived:#?}"
    );
    assert_eq!(
        archived[0].command_name.as_deref(),
        Some(std::any::type_name::<ProbeCommand>())
    );

    harness.shutdown().await;
}

/// A panic in `react` itself has no dispatch to name: its identity columns are
/// null, and the row is still one an operator can list, retry and discard.
#[tokio::test]
async fn a_panic_in_react_parks_a_row_with_no_identity_that_is_still_actionable_postgres_test() {
    let reactions = Arc::new(AtomicUsize::new(0));
    let harness =
        PolicyDaemonHarness::start("react_panic", failing_policy(Arc::clone(&reactions))).await;

    let panicked = harness.ping("subject-1", PANICS_IN_REACT).await;

    let row = row_at(
        &harness.await_dead_letters(1).await,
        panicked.global_position,
    );
    assert_eq!(row.error_kind, PANIC_ERROR_KIND);
    assert_eq!(
        (
            row.aggregate_name.as_deref(),
            row.target_stream_id.as_deref(),
            row.command_name.as_deref()
        ),
        (None, None, None),
        "a reaction that panicked before building a dispatch has no identity to record, got {row:#?}"
    );

    // Retryable: the reaction panics again, so the row stays parked in place
    // rather than the retry unwinding or the row vanishing.
    assert_eq!(
        harness.retry_parked_row(row.id).await,
        DeadLetterRetry::StillFailing
    );
    let after_retry = row_at(&harness.dead_letters().await, panicked.global_position);
    assert_eq!(
        after_retry.id, row.id,
        "a retry must not insert a second row"
    );

    // Discardable: it leaves the active set and is archived, identity columns
    // still null.
    assert_eq!(
        harness.discard_parked_row(row.id).await,
        DeadLetterDiscard::Discarded
    );
    assert!(
        harness.dead_letters().await.is_empty(),
        "a discarded row must leave the active set"
    );
    let archived = harness.archived_dead_letters().await;
    assert_eq!(archived.len(), 1, "the discard must archive the row");
    assert_eq!(archived[0].dead_letter_id, row.id);
    assert_eq!(archived[0].reason, "discarded");
    assert_eq!(archived[0].aggregate_name, None);

    harness.shutdown().await;
}

/// A row parked before the migration carries no identity, and the controls an
/// operator has over it are unchanged: it lists, it retries, it discards.
#[tokio::test]
async fn a_row_parked_before_the_migration_is_still_listable_retryable_and_discardable_postgres_test(
) {
    let reactions = Arc::new(AtomicUsize::new(0));
    let harness =
        PolicyDaemonHarness::start("legacy_rows", failing_policy(Arc::clone(&reactions))).await;

    // Two events whose reaction succeeds, parked by hand the way the release
    // before this migration parked them.
    let recovered = harness.ping("subject-1", "hello").await;
    let abandoned = harness.ping("subject-2", "hello").await;
    harness
        .await_dispatch_caused_by(abandoned.global_position)
        .await;

    let recovered_id = harness
        .park_without_identity(&recovered, "parked by an older release")
        .await;
    let abandoned_id = harness
        .park_without_identity(&abandoned, "parked by an older release")
        .await;

    let parked = harness.dead_letters().await;
    assert_eq!(parked.len(), 2, "both legacy rows must be listable");
    assert!(
        parked
            .iter()
            .all(|row| row.aggregate_name.is_none() && row.command_name.is_none()),
        "a legacy row has no identity to read, got {parked:#?}"
    );

    // Retryable: the reaction runs and succeeds, so the row is archived.
    assert_eq!(
        harness.retry_parked_row(recovered_id).await,
        DeadLetterRetry::Resolved
    );

    // Discardable: the other leaves the active set without being re-run.
    assert_eq!(
        harness.discard_parked_row(abandoned_id).await,
        DeadLetterDiscard::Discarded
    );

    assert!(
        harness.dead_letters().await.is_empty(),
        "both legacy rows must have left the active set"
    );
    let archived = harness.archived_dead_letters().await;
    let reasons: Vec<(i64, &str)> = archived
        .iter()
        .map(|row| (row.dead_letter_id, row.reason.as_str()))
        .collect();
    assert_eq!(
        reasons,
        vec![(recovered_id, "retried"), (abandoned_id, "discarded")],
        "each legacy row must be archived under what settled it"
    );

    harness.shutdown().await;
}
