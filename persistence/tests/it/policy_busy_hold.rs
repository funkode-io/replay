//! A busy command is held, not dead-lettered (ADR-0036, funkode-io/replay#316).
//!
//! A middleware that answers `Busy` for a Policy's commands stands in for maintenance
//! mode. Everything here is asserted through what an operator can see (ADR-0014): the
//! dead-letter table, the Policy's places, the events its commands wrote, its liveness
//! and heartbeat, and the narration in the log — keyed on its fields, never its wording.

use crate::common;

use std::sync::atomic::{AtomicU8, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use tracing_test::traced_test;

use common::policy_harness::{
    DeadLetter, PolicyDaemonHarness, Probe, ProbeCommand, ProbeEvent, ProbeUrn,
};
use replay_persistence::{
    CommandContext, CommandMiddleware, DeadLetterRetry, Dispatch, Liveness, Next, ObservedEvent,
    PolicySettings, StartAt,
};

/// More attempts than the dispatch retry budget allows: three retries after the first
/// attempt are four, after which a merely transient error is parked.
const PAST_THE_RETRY_BUDGET: usize = 6;

const CONTINUE: u8 = 0;
const BUSY: u8 = 1;
const REFUSE: u8 = 2;
/// Let the command through, but only after longer than any test waits: a dispatch
/// still in flight when the test acts.
const HANG: u8 = 3;

/// Maintenance mode for a Policy's commands: what it answers is switched from the test.
///
/// Only a Policy's commands to an `-echo` stream are its business: the harness's own
/// pings — an application's commands — always go through, and so does a sibling
/// command a reaction sends anywhere else.
#[derive(Clone)]
struct Gate {
    answer: Arc<AtomicU8>,
    busy_answers: Arc<AtomicUsize>,
    hangs: Arc<AtomicUsize>,
}

impl Gate {
    fn answering(answer: u8) -> Self {
        Self {
            answer: Arc::new(AtomicU8::new(answer)),
            busy_answers: Arc::new(AtomicUsize::new(0)),
            hangs: Arc::new(AtomicUsize::new(0)),
        }
    }

    fn hangs(&self) -> usize {
        self.hangs.load(Ordering::SeqCst)
    }

    fn answer(&self, answer: u8) {
        self.answer.store(answer, Ordering::SeqCst);
    }

    fn busy_answers(&self) -> usize {
        self.busy_answers.load(Ordering::SeqCst)
    }
}

impl CommandMiddleware for Gate {
    fn before<'a>(&'a self, context: &'a mut CommandContext) -> BoxFuture<'a, Next> {
        Box::pin(async move {
            if context.causation().is_none() || !context.stream_id().as_str().ends_with("-echo") {
                return Next::Continue;
            }
            match self.answer.load(Ordering::SeqCst) {
                BUSY => {
                    self.busy_answers.fetch_add(1, Ordering::SeqCst);
                    Next::Busy("maintenance in progress".to_string())
                }
                REFUSE => Next::Error(replay::Error::invalid_input("refused by the gate")),
                HANG => {
                    self.hangs.fetch_add(1, Ordering::SeqCst);
                    tokio::time::sleep(Duration::from_secs(600)).await;
                    Next::Continue
                }
                _ => Next::Continue,
            }
        })
    }
}

/// Echo a ping into its own `-echo` stream, so a command that ran is an event.
fn echo(event: &ObservedEvent<ProbeEvent>) -> Vec<Dispatch> {
    match &event.data {
        ProbeEvent::Pinged { tag } => vec![Dispatch::to::<Probe>(
            ProbeUrn::new(format!("{tag}-echo")).unwrap(),
            ProbeCommand::Echo { tag: tag.clone() },
        )],
        _ => vec![],
    }
}

/// Flake into a `-flake` stream first, then echo: a reaction whose first command
/// always fails retryably and whose second the gate decides.
fn flake_then_echo(event: &ObservedEvent<ProbeEvent>) -> Vec<Dispatch> {
    match &event.data {
        ProbeEvent::Pinged { tag } => vec![
            Dispatch::to::<Probe>(
                ProbeUrn::new(format!("{tag}-flake")).unwrap(),
                ProbeCommand::Flake {
                    reason: "the downstream is down".to_string(),
                },
            ),
            Dispatch::to::<Probe>(
                ProbeUrn::new(format!("{tag}-echo")).unwrap(),
                ProbeCommand::Echo { tag: tag.clone() },
            ),
        ],
        _ => vec![],
    }
}

async fn start(label: &str, gate: &Gate) -> PolicyDaemonHarness {
    start_reacting(label, gate, echo).await
}

async fn start_reacting(
    label: &str,
    gate: &Gate,
    reaction: fn(&ObservedEvent<ProbeEvent>) -> Vec<Dispatch>,
) -> PolicyDaemonHarness {
    PolicyDaemonHarness::start_with_middleware(label, gate.clone(), move |builder, policy| {
        builder.register_policy_fn::<ProbeEvent, _>(
            policy,
            PolicySettings::new().starting_at(StartAt::Beginning),
            reaction,
        )
    })
    .await
}

/// Lines carrying this policy and the hold edge `edge`, whichever way the formatter
/// quotes a string field.
fn hold_records<'a>(lines: &[&'a str], policy: &str, edge: &str) -> Vec<&'a str> {
    let quoted = format!("hold=\"{edge}\"");
    let bare = format!("hold={edge}");
    lines
        .iter()
        .copied()
        .filter(|line| line.contains(policy) && (line.contains(&quoted) || line.contains(&bare)))
        .collect()
}

/// The whole hold in one run: busy for longer than the retry budget parks nothing and
/// does not advance; the held Policy is alive and beating; once the middleware lets the
/// command through it commits and the Policy catches up with no operator action; and
/// the narration shows one record on entering the hold and one on leaving it.
#[tokio::test]
#[traced_test]
async fn a_busy_command_is_held_and_runs_once_the_middleware_lets_it_through_postgres_test() {
    let gate = Gate::answering(BUSY);
    let harness = start("busy_hold", &gate).await;
    let policy = harness.policy_name().to_string();

    let pinged = harness.ping("subject-1", "hello").await;

    harness
        .observe(
            "the command busy for longer than the retry budget",
            || async { (gate.busy_answers() >= PAST_THE_RETRY_BUDGET).then_some(()) },
        )
        .await;

    assert_eq!(
        harness.dead_letters().await.len(),
        0,
        "a busy command is held, never parked"
    );
    assert!(
        !harness.has_passed(pinged.global_position).await,
        "a held Policy stays on the event"
    );
    assert_eq!(
        harness.dispatches().await.len(),
        0,
        "nothing ran while the command was busy"
    );

    // Alive and waiting, not hung: leading in process, and the beat keeps arriving.
    let worker = harness
        .liveness()
        .into_iter()
        .find(|worker| worker.policy == policy)
        .expect("the held Policy is reported");
    assert_eq!(worker.liveness, Liveness::Leading);
    let before = harness
        .await_heartbeat("a beat during the hold", |beat| beat.liveness == "Leading")
        .await;
    let during = harness
        .await_heartbeat("the beat to advance during the hold", |beat| {
            beat.beat_at > before.beat_at
        })
        .await;
    assert_eq!(during.liveness, "Leading");
    assert!(
        !harness.has_passed(pinged.global_position).await,
        "still held while it beats"
    );

    // Maintenance over: the next attempt runs, with no operator action.
    gate.answer(CONTINUE);
    let dispatched = harness
        .await_dispatch_caused_by(pinged.global_position)
        .await;
    assert_eq!(dispatched.event_type, "Echoed");
    harness.await_passed(pinged.global_position).await;
    assert_eq!(
        harness.dead_letters().await.len(),
        0,
        "a hold that ends in a commit parks nothing"
    );

    // An edge, not a stream: however many attempts the hold took, it is two records.
    logs_assert(|lines: &[&str]| {
        let entered = hold_records(lines, &policy, "entered");
        let left = hold_records(lines, &policy, "left");
        if entered.len() != 1 || left.len() != 1 {
            return Err(format!(
                "expected one record entering the hold and one leaving it, got {entered:#?} and {left:#?}"
            ));
        }
        Ok(())
    });

    harness.shutdown().await;
}

/// A hold lasts as long as the middleware stays busy, so it cannot be what keeps a
/// daemon from shutting down: the worker lets go of the event, parks nothing, and the
/// next leader delivers it again.
#[tokio::test]
async fn a_held_policy_shuts_down_without_waiting_for_the_middleware_postgres_test() {
    let gate = Gate::answering(BUSY);
    let mut harness = start("busy_shutdown", &gate).await;

    let pinged = harness.ping("subject-1", "hello").await;
    harness
        .observe("the command held", || async {
            (gate.busy_answers() >= 2).then_some(())
        })
        .await;

    tokio::time::timeout(Duration::from_secs(5), harness.stop())
        .await
        .expect("a held worker stops when asked, not when the middleware relents");
    assert_eq!(harness.dead_letters().await.len(), 0);
    assert!(!harness.has_passed(pinged.global_position).await);

    gate.answer(CONTINUE);
    harness.resume();
    harness.await_passed(pinged.global_position).await;
    let dispatched = harness
        .await_dispatch_caused_by(pinged.global_position)
        .await;
    assert_eq!(dispatched.event_type, "Echoed");

    harness.shutdown().await;
}

/// A hold lets go of a dispatch still in flight when the worker is told to stop, as
/// it lets go of a backoff: waiting for the dispatch would let a worker that is no
/// longer leading advance the event, or park it.
#[tokio::test]
async fn a_held_policy_shuts_down_without_waiting_for_a_dispatch_in_flight_postgres_test() {
    let gate = Gate::answering(BUSY);
    let mut harness = start("busy_in_flight", &gate).await;

    let pinged = harness.ping("subject-1", "hello").await;
    harness
        .observe("the command held", || async {
            (gate.busy_answers() >= 1).then_some(())
        })
        .await;
    gate.answer(HANG);
    harness
        .observe("a held attempt in flight", || async {
            (gate.hangs() >= 1).then_some(())
        })
        .await;

    tokio::time::timeout(Duration::from_secs(5), harness.stop())
        .await
        .expect("a held worker lets go of the dispatch in flight when asked to stop");
    assert_eq!(harness.dead_letters().await.len(), 0);
    assert!(!harness.has_passed(pinged.global_position).await);
    assert_eq!(harness.dispatches().await.len(), 0);

    gate.answer(CONTINUE);
    harness.resume();
    harness.await_passed(pinged.global_position).await;

    harness.shutdown().await;
}

/// A sibling that fails retryably before the held command is reached does not end
/// the hold: it is still one hold, entered once, until the held command answers — and
/// then the sibling is settled by the ordinary rules, parked once.
#[tokio::test]
#[traced_test]
async fn a_failing_sibling_does_not_end_the_hold_postgres_test() {
    let gate = Gate::answering(BUSY);
    let harness = start_reacting("busy_sibling", &gate, flake_then_echo).await;
    let policy = harness.policy_name().to_string();

    let pinged = harness.ping("subject-1", "hello").await;
    // Each busy answer is preceded by the sibling spending a whole retry budget, so
    // three of them are three times round that loop.
    harness
        .observe("the held command busy three times", || async {
            (gate.busy_answers() >= 3).then_some(())
        })
        .await;

    assert_eq!(harness.dead_letters().await.len(), 0);
    logs_assert(|lines: &[&str]| {
        let entered = hold_records(lines, &policy, "entered");
        let left = hold_records(lines, &policy, "left");
        if entered.len() != 1 || !left.is_empty() {
            return Err(format!(
                "expected one hold still open, got {entered:#?} entering and {left:#?} leaving"
            ));
        }
        Ok(())
    });

    gate.answer(CONTINUE);
    harness.await_passed(pinged.global_position).await;
    let parked = harness.await_dead_letters(1).await;
    assert_eq!(parked.len(), 1, "the sibling is parked once");
    assert_eq!(
        parked[0].target_stream_id.as_deref(),
        Some("urn:probe:hello-flake")
    );
    let dispatched = harness
        .await_dispatch_caused_by(pinged.global_position)
        .await;
    assert_eq!(dispatched.event_type, "Echoed");
    logs_assert(|lines: &[&str]| {
        let entered = hold_records(lines, &policy, "entered");
        let left = hold_records(lines, &policy, "left");
        if entered.len() != 1 || left.len() != 1 {
            return Err(format!(
                "expected one hold entered and left, got {entered:#?} and {left:#?}"
            ));
        }
        Ok(())
    });

    harness.shutdown().await;
}

/// The rows as an operator reads them, in a form two readings can be compared in.
fn as_read(rows: &[DeadLetter]) -> String {
    format!("{rows:?}")
}

/// A Retry during maintenance does nothing and loses nothing: the row stays parked
/// exactly as it was — its error, its retry count — and the result says the reaction
/// was busy, by id and in bulk. Once the middleware relents, the same row retries.
#[tokio::test]
async fn a_retry_that_meets_a_busy_command_leaves_the_row_parked_and_reports_busy_postgres_test() {
    let gate = Gate::answering(REFUSE);
    let harness = start("busy_retry", &gate).await;

    let pinged = harness.ping("subject-1", "hello").await;
    let parked = harness.await_dead_letters(1).await;
    assert_eq!(parked[0].event_id, pinged.event_id);

    gate.answer(BUSY);
    let busy_before = gate.busy_answers();

    assert_eq!(
        harness.retry_parked_row(parked[0].id).await,
        DeadLetterRetry::Busy
    );
    assert!(
        gate.busy_answers() > busy_before,
        "the retry replayed the reaction"
    );
    assert_eq!(
        as_read(&harness.dead_letters().await),
        as_read(&parked),
        "a busy retry leaves the row untouched"
    );
    assert_eq!(harness.archived_row_count("retried").await, 0);

    let summary = harness.retry_parked().await;
    assert_eq!(summary.reactions_busy, 1);
    assert_eq!(summary.reactions_resolved, 0);
    assert_eq!(summary.reactions_still_failing, 0);
    assert_eq!(as_read(&harness.dead_letters().await), as_read(&parked));

    gate.answer(CONTINUE);
    assert_eq!(
        harness.retry_parked_row(parked[0].id).await,
        DeadLetterRetry::Resolved
    );
    assert_eq!(harness.dead_letters().await.len(), 0);

    harness.shutdown().await;
}

/// Fail retryably: the transient error a held command's sibling can meet.
const FLAKE: u8 = 4;

/// One answer for each of two commands, told apart by the stream each is sent to.
#[derive(Clone)]
struct TwoGates {
    first: Arc<AtomicU8>,
    second: Arc<AtomicU8>,
    /// How many times each gate has been asked, in order `[first, second]`.
    asked: Arc<[AtomicUsize; 2]>,
}

impl TwoGates {
    fn answering(first: u8, second: u8) -> Self {
        Self {
            first: Arc::new(AtomicU8::new(first)),
            second: Arc::new(AtomicU8::new(second)),
            asked: Arc::new([AtomicUsize::new(0), AtomicUsize::new(0)]),
        }
    }

    fn answer(&self, first: u8, second: u8) {
        self.first.store(first, Ordering::SeqCst);
        self.second.store(second, Ordering::SeqCst);
    }

    fn asked(&self, gate: usize) -> usize {
        self.asked[gate].load(Ordering::SeqCst)
    }
}

impl CommandMiddleware for TwoGates {
    fn before<'a>(&'a self, context: &'a mut CommandContext) -> BoxFuture<'a, Next> {
        Box::pin(async move {
            let stream = context.stream_id().as_str().to_string();
            let gate = if stream.ends_with("-first") {
                0
            } else if stream.ends_with("-second") {
                1
            } else {
                return Next::Continue;
            };
            self.asked[gate].fetch_add(1, Ordering::SeqCst);
            let answer = [&self.first, &self.second][gate].load(Ordering::SeqCst);
            match answer {
                BUSY => Next::Busy("maintenance in progress".to_string()),
                FLAKE => Next::Error(replay::Error::unavailable("the downstream is down")),
                FLAKE_ONCE => {
                    [&self.first, &self.second][gate].store(CONTINUE, Ordering::SeqCst);
                    Next::Error(replay::Error::unavailable("the downstream blinked"))
                }
                _ => Next::Continue,
            }
        })
    }
}

/// Echo into a `-first` stream, then into a `-second` stream.
fn echo_twice(event: &ObservedEvent<ProbeEvent>) -> Vec<Dispatch> {
    match &event.data {
        ProbeEvent::Pinged { tag } => ["first", "second"]
            .into_iter()
            .map(|which| {
                Dispatch::to::<Probe>(
                    ProbeUrn::new(format!("{tag}-{which}")).unwrap(),
                    ProbeCommand::Echo { tag: tag.clone() },
                )
            })
            .collect(),
        _ => vec![],
    }
}

/// A hold passes from one busy command to the next: the first runs and the second
/// turns busy, and it is still one hold — so the first failing retryably afterwards,
/// before the second is reached, does not end it.
#[tokio::test]
#[traced_test]
async fn a_hold_follows_the_command_that_is_busy_now_postgres_test() {
    let gates = TwoGates::answering(BUSY, CONTINUE);
    let harness = PolicyDaemonHarness::start_with_middleware(
        "busy_handoff",
        gates.clone(),
        |builder, policy| {
            builder.register_policy_fn::<ProbeEvent, _>(
                policy,
                PolicySettings::new().starting_at(StartAt::Beginning),
                echo_twice,
            )
        },
    )
    .await;
    let policy = harness.policy_name().to_string();

    let pinged = harness.ping("subject-1", "hello").await;
    harness
        .observe("the first command held", || async {
            (gates.asked(0) >= 2).then_some(())
        })
        .await;

    // The first runs; the second is busy now.
    gates.answer(CONTINUE, BUSY);
    let second_asked = gates.asked(1);
    harness
        .observe("the second command held", || async {
            (gates.asked(1) >= second_asked + 2).then_some(())
        })
        .await;

    // The first fails retryably from now on, so the second is reached only once the
    // first has spent a whole retry budget — and the hold must survive that.
    gates.answer(FLAKE, BUSY);
    let first_asked = gates.asked(0);
    harness
        .observe("the first command failing retryably", || async {
            (gates.asked(0) >= first_asked + 2).then_some(())
        })
        .await;

    assert_eq!(harness.dead_letters().await.len(), 0);
    assert!(!harness.has_passed(pinged.global_position).await);
    logs_assert(|lines: &[&str]| {
        let entered = hold_records(lines, &policy, "entered");
        let left = hold_records(lines, &policy, "left");
        if entered.len() != 1 || !left.is_empty() {
            return Err(format!(
                "expected one hold still open, got {entered:#?} entering and {left:#?} leaving"
            ));
        }
        Ok(())
    });

    gates.answer(CONTINUE, CONTINUE);
    harness.await_passed(pinged.global_position).await;
    assert_eq!(harness.dead_letters().await.len(), 0);
    logs_assert(|lines: &[&str]| {
        let entered = hold_records(lines, &policy, "entered");
        let left = hold_records(lines, &policy, "left");
        if entered.len() != 1 || left.len() != 1 {
            return Err(format!(
                "expected one hold entered and left, got {entered:#?} and {left:#?}"
            ));
        }
        Ok(())
    });

    harness.shutdown().await;
}

/// Fail retryably once, then let the command through.
const FLAKE_ONCE: u8 = 5;

/// The held command's budget starts when it stops being busy. Behind a sibling that
/// always fails retryably it is reached only once its sibling has spent the reaction's
/// budget; a transient failure on that first answer out of busy is retried, not parked.
#[tokio::test]
async fn a_held_command_gets_a_fresh_budget_when_it_stops_being_busy_postgres_test() {
    let gates = TwoGates::answering(FLAKE, BUSY);
    let harness = PolicyDaemonHarness::start_with_middleware(
        "busy_fresh_budget",
        gates.clone(),
        |builder, policy| {
            builder.register_policy_fn::<ProbeEvent, _>(
                policy,
                PolicySettings::new().starting_at(StartAt::Beginning),
                echo_twice,
            )
        },
    )
    .await;

    let pinged = harness.ping("subject-1", "hello").await;
    harness
        .observe("the second command held", || async {
            (gates.asked(1) >= 2).then_some(())
        })
        .await;

    gates.answer(FLAKE, FLAKE_ONCE);
    harness.await_passed(pinged.global_position).await;

    let parked = harness.dead_letters().await;
    assert_eq!(
        parked.len(),
        1,
        "only the sibling that never recovers is parked"
    );
    assert_eq!(
        parked[0].target_stream_id.as_deref(),
        Some("urn:probe:hello-first")
    );
    assert!(
        harness
            .dispatches()
            .await
            .iter()
            .any(|dispatched| dispatched.stream_id == "urn:probe:hello-second"),
        "the held command committed once its transient failure was retried"
    );

    harness.shutdown().await;
}

/// Echo into the gated `-echo` stream, then explode in the command handler.
fn echo_then_explode(event: &ObservedEvent<ProbeEvent>) -> Vec<Dispatch> {
    match &event.data {
        ProbeEvent::Pinged { tag } => vec![
            Dispatch::to::<Probe>(
                ProbeUrn::new(format!("{tag}-echo")).unwrap(),
                ProbeCommand::Echo { tag: tag.clone() },
            ),
            Dispatch::to::<Probe>(
                ProbeUrn::new(format!("{tag}-boom")).unwrap(),
                ProbeCommand::Explode {
                    reason: "after the hold".to_string(),
                },
            ),
        ],
        _ => vec![],
    }
}

/// A panic that settles a delivery with its hold still open closes the hold in the
/// narration too: an operator never reads a hold that entered and never left.
#[tokio::test]
#[traced_test]
async fn a_panic_that_ends_a_hold_narrates_it_left_postgres_test() {
    let gate = Gate::answering(BUSY);
    let harness = start_reacting("busy_panic", &gate, echo_then_explode).await;
    let policy = harness.policy_name().to_string();

    let pinged = harness.ping("subject-1", "hello").await;
    harness
        .observe("the command held", || async {
            (gate.busy_answers() >= 2).then_some(())
        })
        .await;

    gate.answer(CONTINUE);
    let parked = harness.await_dead_letters(1).await;
    assert_eq!(parked[0].error_kind, replay_persistence::PANIC_ERROR_KIND);
    harness.await_passed(pinged.global_position).await;

    logs_assert(|lines: &[&str]| {
        let entered = hold_records(lines, &policy, "entered");
        let left = hold_records(lines, &policy, "left");
        if entered.len() != 1 || left.len() != 1 {
            return Err(format!(
                "expected one hold entered and left, got {entered:#?} and {left:#?}"
            ));
        }
        Ok(())
    });

    harness.shutdown().await;
}

/// Refuses a Policy's command until told otherwise; after that it discards the row
/// `discarding` names — an operator's concurrent discard, landing while the retry is
/// replaying — and answers busy.
#[derive(Clone, Default)]
struct DiscardsWhileBusy {
    discarding: Arc<std::sync::OnceLock<(sqlx::PgPool, i64)>>,
}

impl CommandMiddleware for DiscardsWhileBusy {
    fn before<'a>(&'a self, context: &'a mut CommandContext) -> BoxFuture<'a, Next> {
        Box::pin(async move {
            if context.causation().is_none() {
                return Next::Continue;
            }
            let Some((pool, id)) = self.discarding.get() else {
                return Next::Error(replay::Error::invalid_input("refused by the gate"));
            };
            sqlx::query("DELETE FROM policy_dead_letters WHERE id = $1")
                .bind(id)
                .execute(pool)
                .await
                .expect("the discard runs");
            Next::Busy("maintenance in progress".to_string())
        })
    }
}

/// A busy retry reports the row it was asked about as busy only while it is still
/// parked: one discarded while the replay ran is not found, as for any retry.
#[tokio::test]
async fn a_busy_retry_of_a_row_discarded_meanwhile_reports_it_not_found_postgres_test() {
    let middleware = DiscardsWhileBusy::default();
    let harness = PolicyDaemonHarness::start_with_middleware(
        "busy_discarded",
        middleware.clone(),
        |builder, policy| {
            builder.register_policy_fn::<ProbeEvent, _>(
                policy,
                PolicySettings::new().starting_at(StartAt::Beginning),
                echo,
            )
        },
    )
    .await;

    harness.ping("subject-1", "hello").await;
    let parked = harness.await_dead_letters(1).await;

    middleware
        .discarding
        .set((harness.pool().clone(), parked[0].id))
        .expect("set once");
    assert_eq!(
        harness.retry_parked_row(parked[0].id).await,
        DeadLetterRetry::NotFound
    );
    assert_eq!(harness.dead_letters().await.len(), 0);

    harness.shutdown().await;
}
