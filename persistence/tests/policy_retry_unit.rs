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
    OBSERVE_TIMEOUT,
};
use replay_persistence::{
    DeadLetterDiscard, DeadLetterRetry, DeadLetterRetrySummary, Dispatch, PersistedEvent, Policy,
    PolicyRunnerBuilder, StartAt,
};

/// Tag whose reaction dispatches two commands, each to its own instance.
const TWO_COMMANDS: &str = "two-commands";

/// Tag whose reaction dispatches those same two commands at the **same**
/// instance, so both rows name the same identity: the aggregate type, the target
/// URN and the command *type* is all a row records, and `ProbeCommand` is one
/// type whatever variant it carries.
const SAME_TARGET: &str = "same-target";

/// Tag whose reaction fails permanently and *then* panics: which of the two a
/// row with no identity carries is the order the replay concluded them in.
const FAILS_THEN_PANICS: &str = "fails-then-panics";

/// Tag whose reaction dispatches a command that concludes and then one that
/// panics inside its handler.
const PANIC_AFTER: &str = "panic-after";

/// The same, both at the **same** instance: the concluded command and the
/// panicking one are indistinguishable to a row, so the second row can only be
/// settled by the panic.
const PANIC_SAME_TARGET: &str = "panic-same-target";

/// Tag whose reaction sends two commands to the **same** instance where the
/// first always succeeds: at park time only the second has a row.
const SHIFTED: &str = "shifted";

/// The same shift, with the second command panicking instead of refusing.
const SHIFTED_PANIC: &str = "shifted-panic";

/// Tag whose reaction parks a row *and* appends an event that makes another
/// reaction of the same kind: a policy that keeps parking while a retry runs.
const FEEDS: &str = "feeds";

/// Tag whose reaction dispatches one always-failing command, at the instance the
/// policy's *current* code names: the switch stands for a deploy that changed the
/// reaction between the park and the retry.
const RETARGETED: &str = "retargeted";

/// Tag whose reaction dispatches one command, for tests that care about how many
/// reactions there are rather than what each parks.
const ONE_COMMAND: &str = "one-command";

/// Reactions parked by
/// [`a_bulk_retry_pages_a_backlog_settling_each_reaction_once_postgres_test`]:
/// one more than the page the runner reads, so the walk must fetch a second one
/// and must not re-read the first.
const MORE_THAN_A_PAGE: usize = 101;

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

/// What the panicking command panics with, and the `error_kind` its row carries.
const PANIC_REASON: &str = "handler-exploded";
const PANIC_KIND: &str = "Panic";

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
    retargeted: Arc<AtomicBool>,
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

    /// A dispatch the aggregate always refuses.
    fn refuse(stream: &str, reason: &str) -> Dispatch {
        Dispatch::to::<Probe>(
            ProbeUrn::new(stream).unwrap(),
            ProbeCommand::Refuse {
                reason: reason.to_string(),
            },
        )
    }

    /// A dispatch that panics inside the command handler.
    fn explode(stream: &str) -> Dispatch {
        Dispatch::to::<Probe>(
            ProbeUrn::new(stream).unwrap(),
            ProbeCommand::Explode {
                reason: PANIC_REASON.to_string(),
            },
        )
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
        let first = || Self::dispatch(FIRST_SUBJECT, &self.first_recovered, FIRST_FAILURE);
        let dispatches = match tag.as_str() {
            TWO_COMMANDS => vec![
                first(),
                Self::dispatch(SECOND_SUBJECT, &self.second_recovered, SECOND_FAILURE),
            ],
            SAME_TARGET => vec![
                first(),
                Self::dispatch(FIRST_SUBJECT, &self.second_recovered, SECOND_FAILURE),
            ],
            PANIC_AFTER => vec![first(), Self::explode(SECOND_SUBJECT)],
            FAILS_THEN_PANICS => vec![
                Self::refuse(FIRST_SUBJECT, FIRST_FAILURE),
                Self::explode(SECOND_SUBJECT),
            ],
            PANIC_SAME_TARGET => vec![first(), Self::explode(FIRST_SUBJECT)],
            SHIFTED => vec![
                Dispatch::to::<Probe>(
                    ProbeUrn::new(FIRST_SUBJECT).unwrap(),
                    ProbeCommand::Echo {
                        tag: "always-succeeds".to_string(),
                    },
                ),
                Self::dispatch(FIRST_SUBJECT, &self.second_recovered, SECOND_FAILURE),
            ],
            SHIFTED_PANIC => vec![
                Dispatch::to::<Probe>(
                    ProbeUrn::new(FIRST_SUBJECT).unwrap(),
                    ProbeCommand::Echo {
                        tag: "always-succeeds".to_string(),
                    },
                ),
                Self::explode(FIRST_SUBJECT),
            ],
            FEEDS => vec![
                Dispatch::to::<Probe>(
                    ProbeUrn::new(SECOND_SUBJECT).unwrap(),
                    ProbeCommand::Ping {
                        tag: FEEDS.to_string(),
                    },
                ),
                first(),
            ],
            RETARGETED => vec![if self.retargeted.load(Ordering::SeqCst) {
                Self::refuse(SECOND_SUBJECT, SECOND_FAILURE)
            } else {
                Self::refuse(FIRST_SUBJECT, FIRST_FAILURE)
            }],
            ONE_COMMAND => vec![first()],
            _ => return vec![],
        };
        self.reactions.fetch_add(1, Ordering::SeqCst);
        dispatches
    }
}

/// The policy under test, and the switches a test drives it with.
struct Reaction {
    calls: Arc<AtomicUsize>,
    first_recovered: Arc<AtomicBool>,
    second_recovered: Arc<AtomicBool>,
    retargeted: Arc<AtomicBool>,
}

impl Reaction {
    fn new() -> Self {
        Self {
            calls: Arc::new(AtomicUsize::new(0)),
            first_recovered: Arc::new(AtomicBool::new(false)),
            second_recovered: Arc::new(AtomicBool::new(false)),
            retargeted: Arc::new(AtomicBool::new(false)),
        }
    }

    fn policy(
        &self,
    ) -> impl Fn(PolicyRunnerBuilder, &str) -> PolicyRunnerBuilder + Send + Sync + 'static {
        let reactions = Arc::clone(&self.calls);
        let first_recovered = Arc::clone(&self.first_recovered);
        let second_recovered = Arc::clone(&self.second_recovered);
        let retargeted = Arc::clone(&self.retargeted);
        move |builder, policy| {
            builder.register_policy(TwoCommandPolicy {
                name: policy.to_string(),
                reactions: Arc::clone(&reactions),
                first_recovered: Arc::clone(&first_recovered),
                second_recovered: Arc::clone(&second_recovered),
                retargeted: Arc::clone(&retargeted),
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

/// Two dispatches a row cannot tell apart — same aggregate type, same target
/// URN, same command type — settle the rows they parked in **production order**.
///
/// The identity a row records is all three of those and nothing else: the
/// command's variant and payload are not stored, so the only thing that can
/// distinguish these two rows is the order the reaction produced them in.
#[tokio::test]
async fn identical_dispatches_settle_their_rows_in_production_order_postgres_test() {
    let reaction = Reaction::new();
    let harness = PolicyDaemonHarness::start("retry_same_target", reaction.policy()).await;

    harness.ping("subject-1", SAME_TARGET).await;
    let parked = harness.await_dead_letters(2).await;
    assert_eq!(
        parked
            .iter()
            .map(|row| row.target_stream_id.clone())
            .collect::<Vec<_>>(),
        vec![Some(urn_of(FIRST_SUBJECT)); 2],
        "the fixture is only a test of ordering if both rows name the same target"
    );

    // Neither row can be told from the other by what it records, so each must
    // take the outcome of the dispatch at its own position.
    harness.retry_parked().await;
    let after = harness.dead_letters().await;
    assert!(
        after[0].error_message.contains(FIRST_FAILURE)
            && after[1].error_message.contains(SECOND_FAILURE),
        "each row keeps the error of the dispatch at its own position, got {after:#?}"
    );

    // And when the *first* of the two resolves, it is the first row that leaves.
    reaction.first_recovered.store(true, Ordering::SeqCst);
    assert_eq!(
        harness.retry_parked().await,
        DeadLetterRetrySummary {
            reactions_resolved: 0,
            reactions_still_failing: 1,
        }
    );
    let left = harness.dead_letters().await;
    assert_eq!(
        left.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![parked[1].id],
        "the row of the dispatch that resolved leaves; the other stays, got {left:#?}"
    );
    assert!(left[0].error_message.contains(SECOND_FAILURE));
    assert_eq!(
        archived_ids(&harness.archived_dead_letters().await),
        vec![(parked[0].id, "retried")]
    );

    harness.shutdown().await;
}

/// A dispatch that concludes before a later one panics keeps **its own**
/// outcome; the rows the replay never reached carry the panic.
///
/// The dispatches are recorded outside the `catch_unwind` for exactly this
/// (ADR-0016): unwinding must not take what its siblings already concluded with
/// it, or a panic in the last command would re-park every row of the reaction
/// under `Panic` and lose the rest.
#[tokio::test]
async fn a_panic_mid_replay_leaves_what_concluded_before_it_intact_postgres_test() {
    let reaction = Reaction::new();
    let harness = PolicyDaemonHarness::start("retry_panic_after", reaction.policy()).await;

    harness.ping("subject-1", PANIC_AFTER).await;
    let parked = harness.await_dead_letters(2).await;
    let refused = row_for(&parked, FIRST_SUBJECT);
    let exploded = row_for(&parked, SECOND_SUBJECT);
    assert_eq!(exploded.error_kind, PANIC_KIND);

    // The refused command's dependency is back; the panicking one is a defect,
    // so it panics again.
    reaction.first_recovered.store(true, Ordering::SeqCst);
    let before = reaction.replays();
    assert_eq!(
        harness.retry_parked().await,
        DeadLetterRetrySummary {
            reactions_resolved: 0,
            reactions_still_failing: 1,
        },
        "a reaction that panics is one reaction still failing, and the panic \
         must not reach the caller"
    );
    assert_eq!(reaction.replays() - before, 1);

    let after = harness.dead_letters().await;
    assert_eq!(
        after.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![exploded.id],
        "the command that concluded before the panic settled its own row, got {after:#?}"
    );
    assert_eq!(after[0].error_kind, PANIC_KIND);
    assert!(
        after[0].error_message.contains(PANIC_REASON),
        "the panicking row carries the panic's message, got {after:#?}"
    );
    assert_eq!(after[0].retry_count, 1);

    let archived = harness.archived_dead_letters().await;
    assert_eq!(archived_ids(&archived), vec![(refused.id, "retried")]);
    assert!(
        archived[0].last_retried_at.is_some(),
        "an archived settlement records when it was retried, got {archived:#?}"
    );

    harness.shutdown().await;
}

/// A backlog larger than one page is walked a page at a time, and every
/// reaction in it is settled exactly once.
///
/// The backlog a bulk retry drains is the size of the outage that made it — one
/// parked reaction per event the downstream refused — so the enumeration is
/// keyset-paged rather than read whole. What that must not do is skip a page or
/// replay one twice, which is what the replay count and the per-row retry count
/// say here.
#[tokio::test]
async fn a_bulk_retry_pages_a_backlog_settling_each_reaction_once_postgres_test() {
    let reaction = Reaction::new();
    let harness = PolicyDaemonHarness::start("retry_paging", reaction.policy()).await;

    for _ in 0..MORE_THAN_A_PAGE {
        harness.ping("subject-1", ONE_COMMAND).await;
    }
    let parked = harness.await_dead_letters(MORE_THAN_A_PAGE).await;
    assert_eq!(parked.len(), MORE_THAN_A_PAGE);

    let before = reaction.replays();
    assert_eq!(
        harness.retry_parked().await,
        DeadLetterRetrySummary {
            reactions_resolved: 0,
            reactions_still_failing: MORE_THAN_A_PAGE,
        },
        "every reaction of the backlog is counted, not only the first page"
    );
    assert_eq!(
        reaction.replays() - before,
        MORE_THAN_A_PAGE,
        "one replay per reaction: a page must not be re-read after its rows are settled"
    );

    let after = harness.dead_letters().await;
    assert!(
        after.iter().all(|row| row.retry_count == 1),
        "every reaction is settled exactly once: no page re-read, none skipped, got {after:#?}"
    );

    harness.shutdown().await;
}

/// A second copy of a parked row — what a redelivery of its event leaves — is
/// settled by the command it names, not archived as though it had resolved.
///
/// The replay ran that command once and two rows name it. The first takes its
/// turn; the second is not a dispatch the reaction stopped emitting, it is the
/// same command parked twice (funkode-io/replay#220), so it carries the same
/// verdict. Archiving it as `retried` would report a recovery the replay just
/// watched fail, and the duplicate rows outlive their cause.
#[tokio::test]
async fn a_redeliverys_duplicate_row_is_settled_by_the_command_it_names_postgres_test() {
    let reaction = Reaction::new();
    let harness = PolicyDaemonHarness::start("retry_duplicate", reaction.policy()).await;

    harness.ping("subject-1", ONE_COMMAND).await;
    let parked = harness.await_dead_letters(1).await;
    let duplicate = harness.park_again(parked[0].id).await;

    assert_eq!(
        harness.retry_parked().await,
        DeadLetterRetrySummary {
            reactions_resolved: 0,
            reactions_still_failing: 1,
        },
        "one reaction, however many rows it has parked over its deliveries"
    );
    let after = harness.dead_letters().await;
    assert_eq!(
        after.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![parked[0].id, duplicate],
        "both rows name a command that failed again, so both stay parked, got {after:#?}"
    );
    assert!(
        after
            .iter()
            .all(|row| row.error_message.contains(FIRST_FAILURE) && row.retry_count == 1),
        "and both carry that command's fresh error, got {after:#?}"
    );
    assert!(
        harness.archived_dead_letters().await.is_empty(),
        "nothing resolved, so nothing is archived as retried"
    );

    // And when the command does resolve, the duplicate leaves with the row it
    // duplicates: one replay, one verdict, every row that names it settled.
    reaction.first_recovered.store(true, Ordering::SeqCst);
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
        vec![(parked[0].id, "retried"), (duplicate, "retried")]
    );

    harness.shutdown().await;
}

/// A row the replay never reached carries the panic, even when a *concluded*
/// dispatch shares its identity.
///
/// The two dispatches are indistinguishable to a row — same aggregate, same
/// URN, same command type — so once the first has taken its turn, the second row
/// looks exactly like a redelivery's duplicate of it. It is not: the reaction
/// panicked before saying anything about the command that row was parked for,
/// and only a replay that ran to completion can settle a left-over row from a
/// dispatch of the same identity.
#[tokio::test]
async fn a_row_the_panic_cut_short_is_not_settled_by_its_concluded_twin_postgres_test() {
    let reaction = Reaction::new();
    let harness = PolicyDaemonHarness::start("retry_panic_twin", reaction.policy()).await;

    harness.ping("subject-1", PANIC_SAME_TARGET).await;
    let parked = harness.await_dead_letters(2).await;
    assert_eq!(
        parked
            .iter()
            .map(|row| row.target_stream_id.clone())
            .collect::<Vec<_>>(),
        vec![Some(urn_of(FIRST_SUBJECT)); 2],
        "the fixture is only a test of this if both rows name the same target"
    );
    assert_eq!(parked[1].error_kind, PANIC_KIND);

    // The first command resolves this time; the second still panics.
    reaction.first_recovered.store(true, Ordering::SeqCst);
    assert_eq!(
        harness.retry_parked().await,
        DeadLetterRetrySummary {
            reactions_resolved: 0,
            reactions_still_failing: 1,
        }
    );

    let after = harness.dead_letters().await;
    assert_eq!(
        after.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![parked[1].id],
        "the row the panic cut short stays parked, got {after:#?}"
    );
    assert_eq!(after[0].error_kind, PANIC_KIND);
    assert!(after[0].error_message.contains(PANIC_REASON));
    assert_eq!(
        archived_ids(&harness.archived_dead_letters().await),
        vec![(parked[0].id, "retried")],
        "only the command that actually concluded resolves its row"
    );

    harness.shutdown().await;
}

/// A row whose command is not the *first* dispatch of its identity is settled by
/// what that identity concluded, not by whichever dispatch came first.
///
/// A reaction sending two commands to one instance produces two dispatches a row
/// cannot tell apart, and when only the later one fails, the reaction parks one
/// row. Claiming in production order would hand that row the first dispatch's
/// success and archive it while its command still fails — a parked reaction
/// leaving the table because a *different* command of the same reaction worked.
#[tokio::test]
async fn a_row_is_not_settled_by_a_dispatch_that_parked_nothing_postgres_test() {
    let reaction = Reaction::new();
    let harness = PolicyDaemonHarness::start("retry_shifted", reaction.policy()).await;

    harness.ping("subject-1", SHIFTED).await;
    let parked = harness.await_dead_letters(1).await;
    assert_eq!(
        parked.len(),
        1,
        "the first command succeeds, so only the second parks a row, got {parked:#?}"
    );

    assert_eq!(
        harness.retry_parked().await,
        DeadLetterRetrySummary {
            reactions_resolved: 0,
            reactions_still_failing: 1,
        },
        "the reaction still fails: one of its commands is still refused"
    );
    let after = harness.dead_letters().await;
    assert_eq!(
        after.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![parked[0].id],
        "the row stays parked, got {after:#?}"
    );
    assert!(
        after[0].error_message.contains(SECOND_FAILURE),
        "carrying the failure of the identity it names, got {after:#?}"
    );
    assert!(
        harness.archived_dead_letters().await.is_empty(),
        "and nothing is archived as though the command had recovered"
    );

    // When that command recovers, the row clears — the shared verdict resolves
    // as readily as it re-parks.
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
        vec![(parked[0].id, "retried")]
    );

    harness.shutdown().await;
}

/// A command that fails again always leaves a parked row — including where the
/// rows and the dispatches line up only by accident.
///
/// A redelivery's duplicate of a shifted row (funkode-io/replay#220) makes two
/// rows for one command while the replay runs two dispatches of their shared
/// identity, so the counts align and the rows are settled in order although
/// neither is the first dispatch's. Matching is a bijection, so each dispatch's
/// outcome still lands on exactly one row and the failure keeps a row of its
/// own; what the accident costs is *which* row, so the duplicate is archived
/// `retried` when it never resolved. An ordinal recorded at park time would
/// settle them exactly (ADR-0021); removing the duplicates removes the
/// ambiguity, which is what #220 is for.
#[tokio::test]
async fn a_failing_command_keeps_a_row_even_when_a_duplicate_aligns_the_counts_postgres_test() {
    let reaction = Reaction::new();
    let harness = PolicyDaemonHarness::start("retry_shift_dup", reaction.policy()).await;

    harness.ping("subject-1", SHIFTED).await;
    let parked = harness.await_dead_letters(1).await;
    let duplicate = harness.park_again(parked[0].id).await;

    assert_eq!(
        harness.retry_parked().await,
        DeadLetterRetrySummary {
            reactions_resolved: 0,
            reactions_still_failing: 1,
        },
        "the reaction still fails, so the retry says so"
    );
    let after = harness.dead_letters().await;
    assert_eq!(
        after.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![duplicate],
        "the failing command keeps a row, got {after:#?}"
    );
    assert!(after[0].error_message.contains(SECOND_FAILURE));

    // And once it recovers, the row it kept leaves too: nothing is stranded.
    reaction.second_recovered.store(true, Ordering::SeqCst);
    assert_eq!(
        harness.retry_parked().await,
        DeadLetterRetrySummary {
            reactions_resolved: 1,
            reactions_still_failing: 0,
        }
    );
    assert!(harness.dead_letters().await.is_empty());

    harness.shutdown().await;
}

/// A row parked for a dispatch that **panicked** is not settled by a sibling
/// that succeeded before it, even when the two are indistinguishable.
///
/// The reaction sends two commands to one instance: the first succeeds, the
/// second panics, so only the second parks a row. The dispatch a replay unwinds
/// out of is one of its conclusions — carrying the panic — rather than a
/// dispatch it never reached, which is what keeps that row from claiming its
/// sibling's success and leaving the table resolved.
#[tokio::test]
async fn a_panicking_dispatch_settles_its_own_row_not_its_siblings_postgres_test() {
    let reaction = Reaction::new();
    let harness = PolicyDaemonHarness::start("retry_shift_panic", reaction.policy()).await;

    harness.ping("subject-1", SHIFTED_PANIC).await;
    let parked = harness.await_dead_letters(1).await;
    assert_eq!(parked.len(), 1, "only the panicking command parks a row");
    assert_eq!(parked[0].error_kind, PANIC_KIND);

    assert_eq!(
        harness.retry_parked().await,
        DeadLetterRetrySummary {
            reactions_resolved: 0,
            reactions_still_failing: 1,
        },
        "a reaction that still panics has not recovered"
    );
    let after = harness.dead_letters().await;
    assert_eq!(
        after.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![parked[0].id],
        "the panicking row stays parked, got {after:#?}"
    );
    assert_eq!(after[0].error_kind, PANIC_KIND);
    assert!(after[0].error_message.contains(PANIC_REASON));
    assert!(
        harness.archived_dead_letters().await.is_empty(),
        "and nothing leaves on the strength of the command that did work"
    );

    harness.shutdown().await;
}

/// A bulk retry returns against a policy that is still parking.
///
/// Retries take no advisory lock, so the worker goes on reacting — and a policy
/// whose reaction appends an event parks a *new* reaction every time one of its
/// reactions is replayed. Walking to an empty page would chase that forever and
/// never return to the operator, so the walk stops at the last reaction parked
/// when the call began.
///
/// How many that is, this test cannot say: the daemon parks more between the
/// observation below and the retry's own high-water read, and more again while
/// it runs. What is not timing-dependent is that the call **returns** and that it
/// settled at least the backlog that was already there — the high-water mark
/// cannot be behind rows this test has seen.
#[tokio::test]
async fn a_bulk_retry_returns_while_the_policy_keeps_parking_postgres_test() {
    let reaction = Reaction::new();
    let harness = PolicyDaemonHarness::start("retry_feeds", reaction.policy()).await;

    // Each reaction pings another stream and then fails: the drain parks a chain
    // of them, bounded by the causation-depth limit.
    harness.ping("subject-1", FEEDS).await;
    let seen = harness.await_dead_letters(3).await.len();

    let summary = tokio::time::timeout(OBSERVE_TIMEOUT, harness.retry_parked())
        .await
        .expect("a bulk retry must return rather than chase a policy that keeps parking");
    assert!(
        summary.reactions_resolved + summary.reactions_still_failing >= seen,
        "it drains at least what was parked when it was called, got {summary:?} for \
         {seen} reaction(s) already parked"
    );

    harness.shutdown().await;
}

/// A retry that fails on a command the reaction had *not* parked leaves a dead
/// letter for it, and says the reaction is still failing.
///
/// A replay runs the policy as its code defines it **now** (ADR-0007), so a
/// deploy between the park and the retry can make the reaction emit a different
/// command — one that fails permanently and has no row of its own. The row it
/// used to park resolves, because the reaction no longer emits what that row
/// named; parking the new failure is what keeps the retry the drain's equal
/// rather than a path that executes a failing command and leaves nothing behind.
#[tokio::test]
async fn a_retry_parks_a_failure_the_reaction_had_not_parked_postgres_test() {
    let reaction = Reaction::new();
    let harness = PolicyDaemonHarness::start("retry_retarget", reaction.policy()).await;

    harness.ping("subject-1", RETARGETED).await;
    let parked = harness.await_dead_letters(1).await;
    assert_eq!(parked[0].target_stream_id, Some(urn_of(FIRST_SUBJECT)));

    // The deploy: the reaction now sends its command somewhere else, where it
    // also fails.
    reaction.retargeted.store(true, Ordering::SeqCst);
    assert_eq!(
        harness.retry_parked().await,
        DeadLetterRetrySummary {
            reactions_resolved: 0,
            reactions_still_failing: 1,
        },
        "a reaction that still fails permanently is not resolved, whichever \
         command it now fails on"
    );

    let after = harness.dead_letters().await;
    assert_eq!(after.len(), 1, "the new failure has a row, got {after:#?}");
    assert_ne!(after[0].id, parked[0].id);
    assert_eq!(after[0].target_stream_id, Some(urn_of(SECOND_SUBJECT)));
    assert!(after[0].error_message.contains(SECOND_FAILURE));
    assert_eq!(
        archived_ids(&harness.archived_dead_letters().await),
        vec![(parked[0].id, "retried")],
        "and the row for the command the reaction no longer emits leaves"
    );

    harness.shutdown().await;
}

/// A row that names no command is settled **after** the rows that do, whatever
/// the ids say.
///
/// The real upgrade shape: the identity-less row is the *older* one — parked
/// before the migration — and the rows a later delivery parked come after it.
/// Reading it first would let it speak for every dispatch of the reaction and
/// leave its neighbours nothing of their own to take, archiving rows whose
/// commands had just failed.
#[tokio::test]
async fn an_older_row_with_no_identity_does_not_speak_for_its_neighbours_postgres_test() {
    let reaction = Reaction::new();
    let harness = PolicyDaemonHarness::start("retry_legacy_first", reaction.policy()).await;

    let event = harness.ping("subject-1", TWO_COMMANDS).await;
    let parked = harness.await_dead_letters(2).await;
    let first = row_for(&parked, FIRST_SUBJECT);
    let second = row_for(&parked, SECOND_SUBJECT);
    let legacy = harness
        .park_without_identity_first(&event, "parked by an older release")
        .await;
    assert!(legacy < first.id, "the upgrade's row is the older one");

    assert_eq!(
        harness.retry_parked().await,
        DeadLetterRetrySummary {
            reactions_resolved: 0,
            reactions_still_failing: 1,
        }
    );

    let after = harness.dead_letters().await;
    assert_eq!(
        after.iter().map(|row| row.id).collect::<Vec<_>>(),
        vec![legacy, first.id, second.id],
        "every row stays parked: both commands failed again, got {after:#?}"
    );
    assert!(
        harness.archived_dead_letters().await.is_empty(),
        "and nothing is archived on the strength of a row that names no command"
    );
    let first_after = row_for(&after, FIRST_SUBJECT);
    let second_after = row_for(&after, SECOND_SUBJECT);
    assert!(
        first_after.error_message.contains(FIRST_FAILURE)
            && second_after.error_message.contains(SECOND_FAILURE),
        "each identified row still takes its own command's error, got {after:#?}"
    );

    harness.shutdown().await;
}

/// A row that names no command carries the **first** failure of the replay, not
/// a panic that came after it.
///
/// Such a row is settled all-or-nothing, as the retry it was parked under did —
/// and that retry stopped at the first failure. A panic later in the reaction
/// says nothing about the command that had already failed.
#[tokio::test]
async fn a_row_with_no_identity_carries_the_first_failure_not_a_later_panic_postgres_test() {
    let reaction = Reaction::new();
    let harness = PolicyDaemonHarness::start("retry_first_failure", reaction.policy()).await;

    let event = harness.ping("subject-1", FAILS_THEN_PANICS).await;
    harness.await_dead_letters(2).await;
    let legacy = harness
        .park_without_identity(&event, "parked by an older release")
        .await;

    harness.retry_parked().await;

    let after = harness.dead_letters().await;
    let legacy_row = after
        .iter()
        .find(|row| row.id == legacy)
        .expect("the identity-less row is still parked");
    assert!(
        legacy_row.error_message.contains(FIRST_FAILURE),
        "it carries what the replay failed on first, got {legacy_row:#?}"
    );
    assert_eq!(legacy_row.error_kind, PERMANENT_KIND);

    harness.shutdown().await;
}
