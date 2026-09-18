//! The unit of retry is the reaction, not the row.
//!
//! A reaction that dispatches several commands parks one row per failing
//! command. Retrying used to be a per-row act: every row replayed the whole
//! reaction, the replay stopped at the first failure, and that one error was
//! written to whichever row had been retried — so a reaction's rows converged on
//! the first command's error and one broken reaction cost n replays
//! (funkode-io/replay#211).
//!
//! These tests assert what an operator reads out of the tables after a retry:
//! how many times the reaction ran, which error each row carries, which rows
//! left the active set, and what has been tried.
//!
//! Every observation goes through `tests/common/policy_harness.rs` — a real
//! daemon, a real database, no inspection of tasks or channels (ADR-0014).

mod common;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use common::policy_harness::{
    ArchivedDeadLetter, DeadLetter, PolicyDaemonHarness, Probe, ProbeCommand, ProbeEvent, ProbeUrn,
};
use replay_persistence::{
    DeadLetterDiscard, DeadLetterRetry, DeadLetterRetrySummary, Dispatch, PersistedEvent, Policy,
    PolicyRunnerBuilder, StartAt,
};

/// Tag whose reaction dispatches two commands, each to its own instance.
const TWO_COMMANDS: &str = "two-commands";

/// The instances the two commands are addressed to. Distinct, because the
/// identity a row is matched by is the aggregate type, the target URN and the
/// command *type* — and both commands are a `ProbeCommand` on a `Probe`.
const FIRST_SUBJECT: &str = "first-customer";
const SECOND_SUBJECT: &str = "second-customer";

/// What each command fails with while its dependency is down. Distinct, because
/// keeping each row's own error is the point.
const FIRST_FAILURE: &str = "first-command-failed";
const SECOND_FAILURE: &str = "second-command-failed";

/// The `error_kind` a refused command is parked under.
const PERMANENT_KIND: &str = "Invalid Input";

/// A Policy whose reaction dispatches two commands, each refused until the test
/// says that command's dependency is back.
///
/// The flip is what a retry is for: the reaction is re-evaluated as the Policy
/// defines it *now*, against current state (ADR-0007).
struct TwoCommandPolicy {
    name: String,
    /// Reaction calls, so a test can count what a retry replayed.
    reactions: Arc<AtomicUsize>,
    first_recovered: Arc<AtomicBool>,
    second_recovered: Arc<AtomicBool>,
}

impl TwoCommandPolicy {
    fn dispatch(stream: &str, recovered: &AtomicBool, failure: &str) -> Dispatch {
        let command = if recovered.load(Ordering::SeqCst) {
            ProbeCommand::Echo {
                tag: failure.to_string(),
            }
        } else {
            ProbeCommand::Refuse {
                reason: failure.to_string(),
            }
        };
        Dispatch::to::<Probe>(ProbeUrn::new(stream).unwrap(), command)
    }
}

impl Policy for TwoCommandPolicy {
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
        if tag != TWO_COMMANDS {
            return vec![];
        }
        self.reactions.fetch_add(1, Ordering::SeqCst);
        vec![
            Self::dispatch(FIRST_SUBJECT, &self.first_recovered, FIRST_FAILURE),
            Self::dispatch(SECOND_SUBJECT, &self.second_recovered, SECOND_FAILURE),
        ]
    }
}

/// The policy under test, and the switches a test drives it with.
struct Reaction {
    calls: Arc<AtomicUsize>,
    first_recovered: Arc<AtomicBool>,
    second_recovered: Arc<AtomicBool>,
}

impl Reaction {
    fn new() -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
            first_recovered: Arc::new(AtomicBool::new(false)),
            second_recovered: Arc::new(AtomicBool::new(false)),
        }
    }

    fn policy(
        &self,
    ) -> impl Fn(PolicyRunnerBuilder, &str) -> PolicyRunnerBuilder + Send + Sync + 'static {
        let reactions = Arc::clone(&self.calls);
        let first_recovered = Arc::clone(&self.first_recovered);
        let second_recovered = Arc::clone(&self.second_recovered);
        move |builder, policy| {
            builder.register_policy(TwoCommandPolicy {
                name: policy.to_string(),
                reactions: Arc::clone(&reactions),
                first_recovered: Arc::clone(&first_recovered),
                second_recovered: Arc::clone(&second_recovered),
            })
        }
    }

    fn replays(&self) -> usize {
        self.calls.load(Ordering::SeqCst)
    }
}

/// The URN a command addressed to `stream` carries.
fn urn_of(stream: &str) -> String {
    ProbeUrn::new(stream).unwrap().to_urn().to_string()
}

/// The parked row whose command was addressed to `stream`.
fn row_for(parked: &[DeadLetter], stream: &str) -> DeadLetter {
    let urn = urn_of(stream);
    let rows: Vec<&DeadLetter> = parked
        .iter()
        .filter(|row| row.target_stream_id.as_deref() == Some(urn.as_str()))
        .collect();
    assert_eq!(
        rows.len(),
        1,
        "expected exactly one row parked for {stream}, got {rows:#?}"
    );
    rows[0].clone()
}

/// What settled each archived row, by the id it had while it was parked.
fn archived_ids(archived: &[ArchivedDeadLetter]) -> Vec<(i64, &str)> {
    archived
        .iter()
        .map(|row| (row.dead_letter_id, row.reason.as_str()))
        .collect()
}

/// A bulk retry replays a reaction **once** however many of its commands are
/// parked, leaves each row carrying its **own** command's error, records what
/// has been tried, and leaves both rows retryable — only a discard takes one out
/// of play.
#[tokio::test]
async fn a_bulk_retry_replays_a_reaction_once_and_keeps_each_rows_own_error_postgres_test() {
    let reaction = Reaction::new();
    let harness = PolicyDaemonHarness::start("retry_unit", reaction.policy()).await;

    harness.ping("subject-1", TWO_COMMANDS).await;
    let parked = harness.await_dead_letters(2).await;
    assert_eq!(
        parked.len(),
        2,
        "one row per failing command, got {parked:#?}"
    );
    let first = row_for(&parked, FIRST_SUBJECT);
    let second = row_for(&parked, SECOND_SUBJECT);
    assert!(
        first.retry_count == 0 && first.last_retried_at.is_none(),
        "a row nobody has retried says so, got {first:#?}"
    );

    let before = reaction.replays();
    let summary = harness.retry_parked().await;

    assert_eq!(
        summary,
        DeadLetterRetrySummary {
            reactions_resolved: 0,
            reactions_still_failing: 1,
        },
        "one broken reaction counts as one, whatever it parked"
    );
    assert_eq!(
        reaction.replays() - before,
        1,
        "the reaction's two parked rows must cost one replay, not two"
    );

    let after = harness.dead_letters().await;
    assert_eq!(
        after.len(),
        2,
        "a failed retry re-parks in place, never inserts"
    );
    let first_after = row_for(&after, FIRST_SUBJECT);
    let second_after = row_for(&after, SECOND_SUBJECT);
    assert_eq!((first_after.id, second_after.id), (first.id, second.id));
    assert!(
        first_after.error_message.contains(FIRST_FAILURE),
        "the first command's row must keep the first command's error, got {first_after:#?}"
    );
    assert!(
        second_after.error_message.contains(SECOND_FAILURE),
        "the second command's row must keep its own error, not the first's, got {second_after:#?}"
    );
    assert_eq!(first_after.error_kind, PERMANENT_KIND);
    assert_eq!(
        (first_after.retry_count, second_after.retry_count),
        (1, 1),
        "every row of the group records the retry that settled it"
    );
    assert!(
        first_after.last_retried_at.is_some() && second_after.last_retried_at.is_some(),
        "a settled row records when it was last retried"
    );

    // A failed retry leaves the row retryable: the dependency may come back an
    // hour later, and nothing in the library can tell that it has.
    let before = reaction.replays();
    assert_eq!(
        harness.retry_parked().await,
        DeadLetterRetrySummary {
            reactions_resolved: 0,
            reactions_still_failing: 1,
        }
    );
    assert_eq!(reaction.replays() - before, 1);
    let twice = harness.dead_letters().await;
    assert_eq!(
        (
            row_for(&twice, FIRST_SUBJECT).retry_count,
            row_for(&twice, SECOND_SUBJECT).retry_count
        ),
        (2, 2),
        "the count is what has been tried, so it accumulates"
    );

    // Taking a row out of play permanently is still the operator's explicit act.
    assert_eq!(
        harness.discard_parked_row(first.id).await,
        DeadLetterDiscard::Discarded
    );
    let left = harness.dead_letters().await;
    assert_eq!(
        left.len(),
        1,
        "only the discarded row leaves, got {left:#?}"
    );
    assert_eq!(left[0].id, second.id);
    let archived = harness.archived_dead_letters().await;
    assert_eq!(archived_ids(&archived), vec![(first.id, "discarded")]);
    assert_eq!(
        archived[0].retry_count, 2,
        "the archive keeps what had been tried before the discard"
    );

    harness.shutdown().await;
}

/// A retry where some commands now succeed and others still fail archives the
/// former's rows and re-parks only the latter's; once every command succeeds,
/// every row of the reaction is cleared.
#[tokio::test]
async fn a_retry_archives_the_commands_that_now_succeed_and_reparks_the_rest_postgres_test() {
    let reaction = Reaction::new();
    let harness = PolicyDaemonHarness::start("retry_partial", reaction.policy()).await;

    harness.ping("subject-1", TWO_COMMANDS).await;
    let parked = harness.await_dead_letters(2).await;
    let first = row_for(&parked, FIRST_SUBJECT);
    let second = row_for(&parked, SECOND_SUBJECT);

    // The first command's dependency is back; the second's is not.
    reaction.first_recovered.store(true, Ordering::SeqCst);
    let before = reaction.replays();
    assert_eq!(
        harness.retry_parked().await,
        DeadLetterRetrySummary {
            reactions_resolved: 0,
            reactions_still_failing: 1,
        },
        "a reaction with one command still failing is still failing"
    );
    assert_eq!(reaction.replays() - before, 1);

    let after = harness.dead_letters().await;
    assert_eq!(
        after.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![second.id],
        "only the command that still fails stays parked, got {after:#?}"
    );
    assert!(after[0].error_message.contains(SECOND_FAILURE));
    assert_eq!(
        archived_ids(&harness.archived_dead_letters().await),
        vec![(first.id, "retried")],
        "the command that now succeeds is archived as retried"
    );

    // And now the second dependency comes back too.
    reaction.second_recovered.store(true, Ordering::SeqCst);
    let before = reaction.replays();
    assert_eq!(
        harness.retry_parked().await,
        DeadLetterRetrySummary {
            reactions_resolved: 1,
            reactions_still_failing: 0,
        },
        "a reaction whose every command succeeds is one resolved reaction"
    );
    assert_eq!(reaction.replays() - before, 1);
    assert!(
        harness.dead_letters().await.is_empty(),
        "a reaction that fully succeeds clears every row it parked"
    );
    assert_eq!(
        archived_ids(&harness.archived_dead_letters().await),
        vec![(first.id, "retried"), (second.id, "retried")]
    );

    harness.shutdown().await;
}

/// `retry_dead_letter(id)` replays the whole reaction, as ADR-0007 defines it,
/// and settles **every** row of that reaction — so the by-id and the bulk path
/// cannot conclude different things from the same replay.
#[tokio::test]
async fn a_by_id_retry_settles_the_whole_reaction_it_replayed_postgres_test() {
    let reaction = Reaction::new();
    let harness = PolicyDaemonHarness::start("retry_by_id", reaction.policy()).await;

    harness.ping("subject-1", TWO_COMMANDS).await;
    let parked = harness.await_dead_letters(2).await;
    let first = row_for(&parked, FIRST_SUBJECT);
    let second = row_for(&parked, SECOND_SUBJECT);

    // Retrying the *first* row settles the second one too, with its own error.
    let before = reaction.replays();
    assert_eq!(
        harness.retry_parked_row(first.id).await,
        DeadLetterRetry::StillFailing
    );
    assert_eq!(reaction.replays() - before, 1);

    let after = harness.dead_letters().await;
    assert_eq!(after.len(), 2);
    let second_after = row_for(&after, SECOND_SUBJECT);
    assert!(
        second_after.error_message.contains(SECOND_FAILURE),
        "the row that was not named still gets its own command's error, got {second_after:#?}"
    );
    assert_eq!(
        (
            row_for(&after, FIRST_SUBJECT).retry_count,
            second_after.retry_count
        ),
        (1, 1),
        "one replay, one settlement on every row of the reaction"
    );

    // With both dependencies back, retrying one row clears the whole reaction.
    reaction.first_recovered.store(true, Ordering::SeqCst);
    reaction.second_recovered.store(true, Ordering::SeqCst);
    assert_eq!(
        harness.retry_parked_row(second.id).await,
        DeadLetterRetry::Resolved
    );
    assert!(
        harness.dead_letters().await.is_empty(),
        "a by-id retry archives every row its replay resolved"
    );
    assert_eq!(
        archived_ids(&harness.archived_dead_letters().await),
        vec![(first.id, "retried"), (second.id, "retried")]
    );

    harness.shutdown().await;
}

/// A row with no dispatch to name — parked before the identity migration — is
/// settled by the whole replay, because there is no command of its own to settle
/// it by. It must not be archived while the reaction it belongs to still fails.
#[tokio::test]
async fn a_row_with_no_identity_is_settled_by_the_whole_replay_postgres_test() {
    let reaction = Reaction::new();
    let harness = PolicyDaemonHarness::start("retry_legacy", reaction.policy()).await;

    let event = harness.ping("subject-1", TWO_COMMANDS).await;
    let parked = harness.await_dead_letters(2).await;
    let first = row_for(&parked, FIRST_SUBJECT);
    let second = row_for(&parked, SECOND_SUBJECT);
    let legacy = harness
        .park_without_identity(&event, "parked by an older release")
        .await;

    // One command recovers, the other does not: the legacy row cannot be matched
    // to either, so what settles it is the reaction as a whole — still failing.
    reaction.first_recovered.store(true, Ordering::SeqCst);
    assert_eq!(
        harness.retry_parked().await,
        DeadLetterRetrySummary {
            reactions_resolved: 0,
            reactions_still_failing: 1,
        }
    );

    let after = harness.dead_letters().await;
    let still_parked: Vec<i64> = after.iter().map(|row| row.id).collect();
    assert_eq!(
        still_parked,
        vec![second.id, legacy],
        "the legacy row stays parked while the reaction still fails, got {after:#?}"
    );
    let legacy_row = after
        .iter()
        .find(|row| row.id == legacy)
        .expect("the legacy row is still parked");
    assert!(
        legacy_row.error_message.contains(SECOND_FAILURE),
        "a row that names no command carries what the replay failed on, got {legacy_row:#?}"
    );
    assert_eq!(legacy_row.retry_count, 1);

    // With every command succeeding there is nothing left for it to carry, and
    // it is archived with the rest of the reaction.
    reaction.second_recovered.store(true, Ordering::SeqCst);
    assert_eq!(
        harness.retry_parked().await,
        DeadLetterRetrySummary {
            reactions_resolved: 1,
            reactions_still_failing: 0,
        }
    );
    assert!(harness.dead_letters().await.is_empty());
    assert_eq!(
        archived_ids(&harness.archived_dead_letters().await),
        vec![
            (first.id, "retried"),
            (second.id, "retried"),
            (legacy, "retried")
        ]
    );

    harness.shutdown().await;
}
