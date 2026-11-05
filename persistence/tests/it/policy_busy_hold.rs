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

/// Maintenance mode for a Policy's commands: what it answers is switched from the test.
///
/// Only commands carrying a Causation are its business, so the harness's own pings —
/// an application's commands — always go through.
#[derive(Clone)]
struct Gate {
    answer: Arc<AtomicU8>,
    busy_answers: Arc<AtomicUsize>,
}

impl Gate {
    fn answering(answer: u8) -> Self {
        Self {
            answer: Arc::new(AtomicU8::new(answer)),
            busy_answers: Arc::new(AtomicUsize::new(0)),
        }
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
            if context.causation().is_none() {
                return Next::Continue;
            }
            match self.answer.load(Ordering::SeqCst) {
                BUSY => {
                    self.busy_answers.fetch_add(1, Ordering::SeqCst);
                    Next::Busy("maintenance in progress".to_string())
                }
                REFUSE => Next::Error(replay::Error::invalid_input("refused by the gate")),
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

async fn start(label: &str, gate: &Gate) -> PolicyDaemonHarness {
    PolicyDaemonHarness::start_with_middleware(label, gate.clone(), |builder, policy| {
        builder.register_policy_fn::<ProbeEvent, _>(
            policy,
            PolicySettings::new().starting_at(StartAt::Beginning),
            echo,
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
