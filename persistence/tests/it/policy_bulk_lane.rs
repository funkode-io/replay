//! A bulk append reaches a bulk Policy through the work queue (funkode-io/replay#302,
//! ADR-0034).
//!
//! `Cqrs::execute_bulk` stamps its events bulk and, in the same transaction, queues one
//! row of work per event per Policy registered on the bulk lane. A worker in the
//! Policy's leader claims that work in batches, reacts with the Policy's unchanged
//! `react`, dispatches grouped by target stream and deletes what it claimed. What is
//! asserted is what a consumer and an operator can see: the commands each Policy
//! dispatched and in what order, the rows of `policy_bulk_work`, and — for the grouped
//! dispatch — how many times the target aggregate was loaded.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, OnceLock};

use futures::future::BoxFuture;
use sqlx::PgPool;

use serde::{Deserialize, Serialize};
use urn::Urn;

use crate::common;

use common::policy_harness::{
    reconciliation_timeout, AppendedEvent, PolicyDaemonHarness, Probe, ProbeCommand, ProbeEvent,
    ProbeUrn,
};
use replay_macros::define_aggregate;
use replay_persistence::{
    CommandContext, CommandContextMut, CommandMiddleware, Dispatch, Lanes, Next, ObservedEvent,
    PolicyRunnerBuilder, PolicySettings, StartAt, StreamFilter,
};

/// Echo every ping into the stream `<prefix>-<source>`, one per source stream, so what a
/// Policy delivered reads as that stream's events, in the order they were delivered.
fn echo_into(prefix: &'static str) -> impl Fn(&ObservedEvent<ProbeEvent>) -> Vec<Dispatch> {
    move |event| match &event.data {
        ProbeEvent::Pinged { tag } => vec![Dispatch::to::<Probe>(
            echo_stream(prefix, &event.stream_id),
            ProbeCommand::Echo { tag: tag.clone() },
        )],
        ProbeEvent::Echoed { .. } => vec![],
    }
}

fn echo_stream(prefix: &str, source: &Urn) -> ProbeUrn {
    ProbeUrn::new(format!("{prefix}-{}", source.nss())).expect("a valid NSS")
}

fn tags(prefix: &str, count: usize) -> Vec<String> {
    (0..count).map(|n| format!("{prefix}-{n}")).collect()
}

fn bulk() -> PolicySettings {
    PolicySettings::new().on_lanes(Lanes::Bulk)
}

/// The tags echoed into `stream`, in the stream's order.
async fn echoes(harness: &PolicyDaemonHarness, stream: &ProbeUrn) -> Vec<String> {
    sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT data FROM events WHERE stream_id = $1 ORDER BY stream_seq LIMIT 1000",
    )
    .bind(stream.to_urn().to_string())
    .fetch_all(harness.pool())
    .await
    .expect("the echo stream must be readable")
    .into_iter()
    .filter_map(|data| match serde_json::from_value(data) {
        Ok(ProbeEvent::Echoed { tag }) => Some(tag),
        _ => None,
    })
    .collect()
}

/// Wait until `stream` holds exactly `expected` echoes, in that order.
async fn await_echoes(harness: &PolicyDaemonHarness, stream: &ProbeUrn, expected: &[String]) {
    harness
        .observe(&format!("{stream:?} to hold {expected:?}"), || async {
            let echoed = echoes(harness, stream).await;
            assert!(
                echoed.len() <= expected.len(),
                "more echoes than events: {echoed:?}"
            );
            (echoed == expected).then_some(())
        })
        .await;
}

/// How many commands `policy` dispatched in reaction to `event`.
async fn reactions_to(harness: &PolicyDaemonHarness, policy: &str, event: &AppendedEvent) -> usize {
    harness
        .dispatches_for(policy)
        .await
        .iter()
        .filter(|dispatched| dispatched.caused_by_event_id == event.event_id)
        .count()
}

async fn start(label: &str, settings: PolicySettings) -> PolicyDaemonHarness {
    PolicyDaemonHarness::start(label, move |builder, policy| {
        builder.register_policy_fn::<ProbeEvent, _>(policy, settings.clone(), echo_into("echo"))
    })
    .await
}

/// Every event of two bulk executions reaches the bulk Policy, each stream's in that
/// stream's order, once; and when the worker has drained, the work table is empty.
#[tokio::test]
async fn a_bulk_policy_reacts_to_every_event_of_a_bulk_execution_in_order_postgres_test() {
    let harness = start("bulk_in_order", bulk()).await;
    harness.await_bulk_registration(harness.policy_name()).await;

    let (left, right) = (tags("left", 12), tags("right", 12));
    let imported_left = harness.bulk_import("import-left", &left).await;
    let imported_right = harness.bulk_import("import-right", &right).await;
    assert_eq!(imported_left.len(), left.len());

    await_echoes(
        &harness,
        &echo_stream("echo", &imported_left[0].stream_id.parse().unwrap()),
        &left,
    )
    .await;
    await_echoes(
        &harness,
        &echo_stream("echo", &imported_right[0].stream_id.parse().unwrap()),
        &right,
    )
    .await;

    for event in imported_left.iter().chain(&imported_right) {
        assert_eq!(
            reactions_to(&harness, harness.policy_name(), event).await,
            1,
            "each bulk event is reacted to once"
        );
    }
    harness
        .observe("the work table to be empty", || async {
            (harness.all_bulk_work().await == 0).then_some(())
        })
        .await;

    harness.shutdown().await;
}

/// The queue holds what is owed and nothing else: a row per event for the bulk Policy
/// tracking the stream's type, none for a bulk Policy tracking another type or for an
/// online Policy, and none once the worker has drained it.
#[tokio::test]
async fn the_work_table_is_sized_by_pending_work_only_postgres_test() {
    let mut harness = PolicyDaemonHarness::start("bulk_sizing", |builder, policy| {
        let probe = StreamFilter::ForStreamTypes(vec!["Probe".to_string()]);
        let elsewhere = StreamFilter::ForStreamTypes(vec!["Elsewhere".to_string()]);
        builder
            .register_policy_fn::<ProbeEvent, _>(
                policy,
                bulk().with_stream_filter(probe.clone()),
                echo_into("echo"),
            )
            .register_policy_fn::<ProbeEvent, _>(
                format!("{policy}_elsewhere"),
                bulk().with_stream_filter(elsewhere),
                echo_into("elsewhere"),
            )
            .register_policy_fn::<ProbeEvent, _>(
                format!("{policy}_online"),
                PolicySettings::new().with_stream_filter(probe),
                echo_into("online"),
            )
    })
    .await;
    let policy = harness.policy_name().to_string();
    let elsewhere = format!("{policy}_elsewhere");
    harness.await_bulk_registration(&policy).await;
    harness.await_bulk_registration(&elsewhere).await;

    // No worker runs while the work is counted, so nothing is drained underneath.
    harness.stop().await;
    let imported = harness.bulk_import("sized", &tags("sized", 5)).await;

    assert_eq!(harness.bulk_work(&policy).await, 5, "one row per event");
    assert_eq!(
        harness.bulk_work(&elsewhere).await,
        0,
        "nothing for a Policy that does not track the stream's type"
    );
    assert_eq!(
        harness.all_bulk_work().await,
        5,
        "and nothing for an online Policy"
    );

    harness.resume();
    harness
        .observe("the worker to drain the work table", || async {
            (harness.all_bulk_work().await == 0).then_some(())
        })
        .await;
    for event in &imported {
        assert_eq!(reactions_to(&harness, &policy, event).await, 1);
    }

    harness.shutdown().await;
}

/// One `react` serves both lanes: an online command reaches the Policy through the feed,
/// a bulk execution through the queue, and neither reaches it twice.
#[tokio::test]
async fn a_policy_on_both_lanes_reacts_to_each_through_its_own_path_postgres_test() {
    let harness = start(
        "both_lanes",
        PolicySettings::new()
            .on_lanes(Lanes::Both)
            .starting_at(StartAt::Beginning),
    )
    .await;
    let policy = harness.policy_name().to_string();
    harness.await_bulk_registration(&policy).await;

    let online = harness.ping("online-source", "online").await;
    let imported = harness.bulk_import("bulk-source", &tags("bulk", 3)).await;

    await_echoes(
        &harness,
        &echo_stream("echo", &online.stream_id.parse().unwrap()),
        &["online".to_string()],
    )
    .await;
    await_echoes(
        &harness,
        &echo_stream("echo", &imported[0].stream_id.parse().unwrap()),
        &tags("bulk", 3),
    )
    .await;

    // Through the feed: the online event is passed by the Policy's place in its stream.
    harness.await_passed(online.global_position).await;
    // The feed passes the bulk stream too, once the reconciliation finds it, and
    // delivers none of it: the queue already did.
    harness
        .observe_within(
            "the online place to pass the bulk stream",
            reconciliation_timeout(),
            || async {
                (harness.place_in(&imported[0].stream_id).await == Some(imported.len() as i64))
                    .then_some(())
            },
        )
        .await;
    assert_eq!(harness.bulk_work(&policy).await, 0, "the queue is drained");

    for event in std::iter::once(&online).chain(&imported) {
        assert_eq!(
            reactions_to(&harness, &policy, event).await,
            1,
            "each event reaches the Policy through one lane only"
        );
    }

    harness.shutdown().await;
}

/// An online Policy tracking the stream's type reacts to nothing a bulk execution
/// appended, while a bulk Policy beside it reacts to all of it.
#[tokio::test]
async fn an_online_policy_reacts_to_nothing_from_a_bulk_execution_postgres_test() {
    let harness = PolicyDaemonHarness::start("online_beside_bulk", |builder, policy| {
        let probe = StreamFilter::ForStreamTypes(vec!["Probe".to_string()]);
        builder
            .register_policy_fn::<ProbeEvent, _>(
                policy,
                PolicySettings::new()
                    .starting_at(StartAt::Beginning)
                    .with_stream_filter(probe.clone()),
                echo_into("online"),
            )
            .register_policy_fn::<ProbeEvent, _>(
                format!("{policy}_bulk"),
                bulk().with_stream_filter(probe),
                echo_into("bulk"),
            )
    })
    .await;
    let online = harness.policy_name().to_string();
    let bulk_policy = format!("{online}_bulk");
    harness.await_bulk_registration(&bulk_policy).await;

    let imported = harness.bulk_import("shared", &tags("shared", 4)).await;
    let source: Urn = imported[0].stream_id.parse().unwrap();

    await_echoes(&harness, &echo_stream("bulk", &source), &tags("shared", 4)).await;
    harness
        .observe_within(
            "the online Policy's place to pass the bulk events",
            reconciliation_timeout(),
            || async {
                (harness.place_in(&imported[0].stream_id).await == Some(imported.len() as i64))
                    .then_some(())
            },
        )
        .await;

    assert!(
        echoes(&harness, &echo_stream("online", &source))
            .await
            .is_empty(),
        "the online Policy dispatched nothing for the bulk events"
    );
    assert!(
        harness.dispatches_for(&online).await.is_empty(),
        "nor anything else"
    );

    harness.shutdown().await;
}

/// A bulk Policy registered after a bulk execution committed never receives it — as
/// `StartAt::Now`, even when it asks to start at the beginning — and receives the next.
#[tokio::test]
async fn a_bulk_policy_registered_after_a_bulk_execution_does_not_receive_it_postgres_test() {
    let harness = start("registered_late", bulk()).await;
    let first = harness.policy_name().to_string();
    harness.await_bulk_registration(&first).await;

    let before = harness.bulk_import("before", &tags("before", 2)).await;
    await_echoes(
        &harness,
        &echo_stream("echo", &before[0].stream_id.parse().unwrap()),
        &tags("before", 2),
    )
    .await;

    let late = format!("{first}_late");
    let late_runner = harness.start_runner({
        let late = late.clone();
        move |builder: PolicyRunnerBuilder, _| {
            builder.register_policy_fn::<ProbeEvent, _>(
                late.clone(),
                bulk().starting_at(StartAt::Beginning),
                echo_into("late"),
            )
        }
    });
    harness.await_bulk_registration(&late).await;

    let after = harness.bulk_import("after", &tags("after", 2)).await;
    await_echoes(
        &harness,
        &echo_stream("late", &after[0].stream_id.parse().unwrap()),
        &tags("after", 2),
    )
    .await;
    harness
        .observe("every queue to be drained", || async {
            (harness.all_bulk_work().await == 0).then_some(())
        })
        .await;

    assert!(
        echoes(
            &harness,
            &echo_stream("late", &before[0].stream_id.parse().unwrap())
        )
        .await
        .is_empty(),
        "the late Policy never receives the bulk execution that preceded it"
    );
    for event in &before {
        assert_eq!(reactions_to(&harness, &late, event).await, 0);
    }
    for event in &after {
        assert_eq!(reactions_to(&harness, &first, event).await, 1);
        assert_eq!(reactions_to(&harness, &late, event).await, 1);
    }

    late_runner.shutdown().await;
    harness.shutdown().await;
}

// ── One load per target stream ───────────────────────────────────────────────

/// Loads of a [`Tally`], counted where every load starts: `with_id` builds each field
/// from its `Default`. Every test that loads a `Tally` holds [`TALLY_TESTS`] for its
/// whole run and starts from zero, so a count is that test's.
static TALLY_LOADS: AtomicUsize = AtomicUsize::new(0);

/// Serialises the tests that count [`TALLY_LOADS`].
static TALLY_TESTS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Take the tally counter for this test, from zero.
async fn count_tally_loads() -> tokio::sync::MutexGuard<'static, ()> {
    let guard = TALLY_TESTS.lock().await;
    TALLY_LOADS.store(0, Ordering::SeqCst);
    guard
}

/// Armed with a pool, the next `Tally` handler opens its own tally through it while it
/// decides — after the worker checked the head, before the decision lands. Only tests
/// holding [`TALLY_TESTS`] arm it, so it is that test's.
static OPEN_WHILE_DECIDING: std::sync::Mutex<Option<PgPool>> = std::sync::Mutex::new(None);

async fn write_tally_event(pool: &PgPool, stream_id: &str, event: &TallyEvent) {
    sqlx::query(
        "SELECT id FROM write_event(gen_random_uuid(), $1, '{}'::jsonb, $2, $3, 'Tally', \
         NULL, FALSE)",
    )
    .bind(serde_json::to_value(event).expect("a tally event serialises"))
    .bind(replay::Event::event_type(event))
    .bind(stream_id)
    .fetch_one(pool)
    .await
    .expect("the outside write must succeed");
}

/// A field whose construction is a load of its aggregate.
#[derive(Clone, Debug, Serialize, Deserialize)]
struct LoadCount;

impl Default for LoadCount {
    fn default() -> Self {
        TALLY_LOADS.fetch_add(1, Ordering::SeqCst);
        LoadCount
    }
}

define_aggregate! {
    Tally {
        namespace: "tally",
        state: {
            loaded: LoadCount,
            counted: i64,
            open: bool,
        },
        commands: {
            Count { tag: String },
            CountIfOpen { tag: String },
            Touch {},
        },
        events: {
            Counted { tag: String },
            Opened {},
        }
    }
}

impl replay::EventStream for Tally {
    type Event = TallyEvent;

    fn stream_type() -> String {
        "Tally".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            TallyEvent::Counted { .. } => self.counted += 1,
            TallyEvent::Opened => self.open = true,
        }
    }
}

impl replay::Aggregate for Tally {
    type Command = TallyCommand;
    type Error = replay::Error;
    type Services = ();

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        // A write by somebody else while this handler decides, when a test has armed one.
        let armed = OPEN_WHILE_DECIDING
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(pool) = armed {
            write_tally_event(&pool, self.id.to_urn().as_ref(), &TallyEvent::Opened).await;
        }
        match command {
            TallyCommand::Count { tag } => Ok(vec![TallyEvent::Counted { tag }]),
            // A decision that appends nothing when it refuses: what a stale instance
            // would get wrong without ever reaching the append's version check.
            TallyCommand::CountIfOpen { tag } if self.open => Ok(vec![TallyEvent::Counted { tag }]),
            TallyCommand::CountIfOpen { .. } => Err(replay::Error::business_rule_violation(
                "the tally is not open",
            )),
            // A decision to do nothing: an append of no events, which still meets the
            // commit gate.
            TallyCommand::Touch => Ok(vec![]),
        }
    }
}

/// Count each imported tag on one of two tallies, alternating: two target streams, each
/// aimed at by half the batch's commands.
fn count_alternately(event: &ObservedEvent<ProbeEvent>) -> Vec<Dispatch> {
    match &event.data {
        ProbeEvent::Pinged { tag } => {
            let n: usize = tag
                .rsplit('-')
                .next()
                .and_then(|n| n.parse().ok())
                .expect("an imported tag ends in its index");
            let tally = if n.is_multiple_of(2) { "even" } else { "odd" };
            vec![Dispatch::to::<Tally>(
                TallyUrn::new(tally).expect("a valid NSS"),
                TallyCommand::Count { tag: tag.clone() },
            )]
        }
        ProbeEvent::Echoed { .. } => vec![],
    }
}

async fn counted(harness: &PolicyDaemonHarness, tally: &str) -> Vec<String> {
    sqlx::query_scalar::<_, serde_json::Value>(
        "SELECT data FROM events WHERE stream_id = $1 ORDER BY stream_seq LIMIT 1000",
    )
    .bind(TallyUrn::new(tally).unwrap().to_urn().to_string())
    .fetch_all(harness.pool())
    .await
    .expect("the tally must be readable")
    .into_iter()
    .map(|data| match serde_json::from_value(data) {
        Ok(TallyEvent::Counted { tag }) => tag,
        Ok(TallyEvent::Opened) => "opened".to_string(),
        Err(error) => panic!("a tally holds only tally events: {error}"),
    })
    .collect()
}

/// Every command a batch aims at one target stream is executed against one load of its
/// aggregate: ten commands at two tallies load each tally once, and each tally receives
/// its commands in the order of the events that caused them.
#[tokio::test]
async fn commands_aimed_at_one_target_load_it_once_per_batch_postgres_test() {
    let _counting = count_tally_loads().await;
    let harness = PolicyDaemonHarness::start("grouped_by_target", |builder, policy| {
        builder
            .register_services::<Tally>(())
            .register_policy_fn::<ProbeEvent, _>(
                policy,
                bulk().with_read_batch_size(100),
                count_alternately,
            )
    })
    .await;
    harness.await_bulk_registration(harness.policy_name()).await;

    let imported = tags("t", 10);
    harness.bulk_import("tallied", &imported).await;

    let even: Vec<String> = imported.iter().step_by(2).cloned().collect();
    let odd: Vec<String> = imported.iter().skip(1).step_by(2).cloned().collect();
    harness
        .observe("both tallies to hold their counts", || async {
            (counted(&harness, "even").await == even && counted(&harness, "odd").await == odd)
                .then_some(())
        })
        .await;

    assert_eq!(
        TALLY_LOADS.load(Ordering::SeqCst),
        2,
        "one load per target stream for the whole batch"
    );

    harness.shutdown().await;
}

/// Writes `outside` into the tally just before the second Tally command a Policy
/// dispatches runs — between two commands of one target group, so the instance the group
/// loaded is stale when the second one runs.
#[derive(Clone)]
struct WriteBetweenCommands {
    pool: Arc<OnceLock<PgPool>>,
    tally_commands: Arc<AtomicUsize>,
    outside: TallyEvent,
    /// Delete the stream first, so `outside` recreates it.
    recreate: bool,
}

impl WriteBetweenCommands {
    fn writing(outside: TallyEvent) -> Self {
        Self {
            pool: Arc::default(),
            tally_commands: Arc::default(),
            outside,
            recreate: false,
        }
    }

    fn recreating_with(outside: TallyEvent) -> Self {
        Self {
            recreate: true,
            ..Self::writing(outside)
        }
    }
}

impl CommandMiddleware for WriteBetweenCommands {
    fn before<'a>(&'a self, context: CommandContextMut<'a>) -> BoxFuture<'a, Next> {
        Box::pin(async move {
            if context.stream_type() != "Tally" || context.causation().is_none() {
                return Next::Continue;
            }
            if self.tally_commands.fetch_add(1, Ordering::SeqCst) + 1 == 2 {
                let pool = self.pool.get().expect("the pool is set before any import");
                let event = &self.outside;
                if self.recreate {
                    for statement in [
                        "DELETE FROM events WHERE stream_id = $1",
                        "DELETE FROM streams WHERE id = $1",
                    ] {
                        sqlx::query(statement)
                            .bind(context.stream_id().to_string())
                            .execute(pool)
                            .await
                            .expect("deleting the stream must succeed");
                    }
                }
                sqlx::query(
                    "SELECT id FROM write_event(gen_random_uuid(), $1, '{}'::jsonb, $2, $3, \
                     'Tally', NULL, FALSE)",
                )
                .bind(serde_json::to_value(event).expect("a tally event serialises"))
                .bind(replay::Event::event_type(event))
                .bind(context.stream_id().to_string())
                .fetch_one(pool)
                .await
                .expect("the outside write must succeed");
            }
            Next::Continue
        })
    }
}

/// A write by anybody else between two grouped commands of one target is not decided
/// over: the second command's append is checked against the version the group loaded,
/// fails as a conflict, and is retried on a fresh load — so the target holds the outside
/// write between the two, and is loaded twice.
#[tokio::test]
async fn a_write_between_grouped_commands_reloads_the_target_postgres_test() {
    let _counting = count_tally_loads().await;
    let middleware = WriteBetweenCommands::writing(TallyEvent::Counted {
        tag: "outside".to_string(),
    });
    let pool = Arc::clone(&middleware.pool);
    let harness = PolicyDaemonHarness::start_with_middleware(
        "grouped_stale",
        middleware,
        |builder, policy| {
            builder
                .register_services::<Tally>(())
                .register_policy_fn::<ProbeEvent, _>(policy, bulk(), count_alternately)
        },
    )
    .await;
    pool.set(harness.pool().clone())
        .expect("the pool is set once");
    harness.await_bulk_registration(harness.policy_name()).await;

    harness.bulk_import("stale", &tags("t", 4)).await;

    let even = vec!["t-0".to_string(), "outside".to_string(), "t-2".to_string()];
    let odd = vec!["t-1".to_string(), "t-3".to_string()];
    harness
        .observe("both tallies to hold their counts", || async {
            (counted(&harness, "even").await == even && counted(&harness, "odd").await == odd)
                .then_some(())
        })
        .await;

    assert_eq!(
        TALLY_LOADS.load(Ordering::SeqCst),
        3,
        "the stale target is loaded again; the other target once"
    );

    harness.shutdown().await;
}

/// One claim at a time per Policy: while another transaction holds the Policy's claim,
/// its worker waits rather than claiming the rows after it, and drains once it ends.
#[tokio::test]
async fn a_claim_waits_for_the_policys_claim_in_flight_postgres_test() {
    let mut harness = start("claim_in_flight", bulk()).await;
    let policy = harness.policy_name().to_string();
    harness.await_bulk_registration(&policy).await;
    harness.stop().await;
    let imported = harness.bulk_import("waits", &tags("waits", 3)).await;

    // Another claimer — a manual drain, or the previous leader finishing its batch.
    let mut in_flight = harness.pool().begin().await.expect("a transaction");
    sqlx::query("SELECT pg_advisory_xact_lock(hashtext('replay.bulk_claim'), hashtext($1))")
        .bind(&policy)
        .execute(&mut *in_flight)
        .await
        .expect("taking the policy's claim lock");

    harness.resume();
    harness
        .observe("the worker to wait for the claim in flight", || async {
            sqlx::query_scalar::<_, bool>(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity \
                  WHERE pid <> pg_backend_pid() AND state = 'idle in transaction' \
                    AND query LIKE '%pg_try_advisory_xact_lock%replay.bulk_claim%')",
            )
            .fetch_one(harness.pool())
            .await
            .expect("pg_stat_activity must be readable")
            .then_some(())
        })
        .await;
    assert_eq!(
        harness.bulk_work(&policy).await,
        3,
        "nothing is claimed past the claim in flight"
    );

    // A worker waiting for the claim stops when asked, without waiting the claim out.
    tokio::time::timeout(std::time::Duration::from_secs(5), harness.stop())
        .await
        .expect("shutdown ends a worker waiting for another claim");
    assert_eq!(harness.bulk_work(&policy).await, 3);

    in_flight
        .commit()
        .await
        .expect("ending the claim in flight");
    harness.resume();
    harness
        .observe("the worker to drain once the claim ends", || async {
            (harness.bulk_work(&policy).await == 0).then_some(())
        })
        .await;
    for event in &imported {
        assert_eq!(reactions_to(&harness, &policy, event).await, 1);
    }

    harness.shutdown().await;
}

/// [`count_alternately`], counting only on an open tally — but `t-0` unconditionally, so
/// the first command of the `even` group commits and its instance is kept for the next.
fn count_alternately_if_open(event: &ObservedEvent<ProbeEvent>) -> Vec<Dispatch> {
    count_alternately(event)
        .into_iter()
        .map(|dispatch| {
            let tally = dispatch.target_stream_id().clone();
            let (_, command) = dispatch
                .into_parts::<Tally>()
                .unwrap_or_else(|_| panic!("count_alternately dispatches to a Tally"));
            let TallyCommand::Count { tag } = command else {
                unreachable!("count_alternately counts")
            };
            let command = if tag == "t-0" {
                TallyCommand::Count { tag }
            } else {
                TallyCommand::CountIfOpen { tag }
            };
            Dispatch::to::<Tally>(tally.try_into().expect("a tally URN"), command)
        })
        .collect()
}

/// A decision that appends nothing is not made on stale state either: the second
/// command's handler would refuse on the instance loaded before the tally was opened,
/// and so append nothing for any version check to catch. It sees the stream moved before
/// it decides, reloads, finds the tally open and counts.
#[tokio::test]
async fn a_refusal_is_not_decided_on_a_stale_instance_postgres_test() {
    let _counting = count_tally_loads().await;
    let middleware = WriteBetweenCommands::writing(TallyEvent::Opened {});
    let pool = Arc::clone(&middleware.pool);
    let harness = PolicyDaemonHarness::start_with_middleware(
        "grouped_refusal",
        middleware,
        |builder, policy| {
            builder
                .register_services::<Tally>(())
                .register_policy_fn::<ProbeEvent, _>(policy, bulk(), count_alternately_if_open)
        },
    )
    .await;
    pool.set(harness.pool().clone())
        .expect("the pool is set once");
    harness.await_bulk_registration(harness.policy_name()).await;

    harness.bulk_import("refusal", &tags("t", 4)).await;

    // t-0 counts and keeps the instance; the tally is opened before t-2, which would be
    // refused on that instance and counts on a fresh one. The odd tally is never opened,
    // so both of its commands are refused.
    harness
        .observe("the even tally to count once it is open", || async {
            (counted(&harness, "even").await == ["t-0", "opened", "t-2"]).then_some(())
        })
        .await;
    harness
        .observe("every queue to be drained", || async {
            (harness.all_bulk_work().await == 0).then_some(())
        })
        .await;
    assert!(counted(&harness, "odd").await.is_empty());

    harness.shutdown().await;
}

/// Answers `Busy` for the Policy's commands to the `gated-echo` stream until it is opened,
/// counting how often it did.
#[derive(Clone, Default)]
struct BusyGate {
    open: Arc<std::sync::atomic::AtomicBool>,
    busy_answers: Arc<AtomicUsize>,
}

impl CommandMiddleware for BusyGate {
    fn before<'a>(&'a self, context: CommandContextMut<'a>) -> BoxFuture<'a, Next> {
        Box::pin(async move {
            if context.causation().is_none()
                || !context.stream_id().as_str().ends_with("gated-echo")
                || self.open.load(Ordering::SeqCst)
            {
                return Next::Continue;
            }
            self.busy_answers.fetch_add(1, Ordering::SeqCst);
            Next::Busy("the gate is closed".to_string())
        })
    }
}

/// `refuse` is refused for good, and `gated` is echoed into the `gated-echo` stream.
fn refuse_then_echo(event: &ObservedEvent<ProbeEvent>) -> Vec<Dispatch> {
    match &event.data {
        ProbeEvent::Pinged { tag } if tag == "refuse" => vec![Dispatch::to::<Probe>(
            ProbeUrn::new("refused").expect("a valid NSS"),
            ProbeCommand::Refuse {
                reason: "for good".to_string(),
            },
        )],
        ProbeEvent::Pinged { tag } => vec![Dispatch::to::<Probe>(
            ProbeUrn::new(format!("{tag}-echo")).expect("a valid NSS"),
            ProbeCommand::Echo { tag: tag.clone() },
        )],
        ProbeEvent::Echoed { .. } => vec![],
    }
}

/// A batch abandoned on a busy command takes the dead letters it parked with it: while the
/// gate is closed the refused command's park rolls back with every claim, and once the
/// batch settles it is parked once.
#[tokio::test]
async fn an_abandoned_batch_parks_nothing_postgres_test() {
    let gate = BusyGate::default();
    let (open, busy_answers) = (Arc::clone(&gate.open), Arc::clone(&gate.busy_answers));
    let harness =
        PolicyDaemonHarness::start_with_middleware("abandoned_parks", gate, |builder, policy| {
            builder.register_policy_fn::<ProbeEvent, _>(policy, bulk(), refuse_then_echo)
        })
        .await;
    harness.await_bulk_registration(harness.policy_name()).await;

    harness
        .bulk_import("abandoned", &["refuse".to_string(), "gated".to_string()])
        .await;

    // The second busy answer comes from a second claim of the batch: the first was
    // abandoned after its refused command had been parked inside it.
    harness
        .observe("the batch to be abandoned and claimed again", || async {
            (busy_answers.load(Ordering::SeqCst) >= 2).then_some(())
        })
        .await;
    assert_eq!(
        harness.parked_row_count().await,
        0,
        "an abandoned batch leaves no dead letter behind"
    );

    open.store(true, Ordering::SeqCst);
    harness
        .observe("the batch to settle", || async {
            (harness.all_bulk_work().await == 0).then_some(())
        })
        .await;
    let parked = harness.dead_letters().await;
    assert_eq!(parked.len(), 1, "the refused command is parked: {parked:?}");
    assert_eq!(
        parked[0].deliveries, 1,
        "by the delivery that settled the batch, and by no abandoned one"
    );

    harness.shutdown().await;
}

/// A stream deleted and recreated between two grouped commands, to the very counts the
/// cached instance holds, is not mistaken for it: its head is another event. The second
/// command decides on the recreated tally, which is open, and counts.
#[tokio::test]
async fn a_recreated_stream_is_not_mistaken_for_the_cached_one_postgres_test() {
    // Loads a Tally, so it holds the counter's guard like every test that does: an
    // unguarded load would land in another test's exact count.
    let _counting = count_tally_loads().await;
    let middleware = WriteBetweenCommands::recreating_with(TallyEvent::Opened);
    let pool = Arc::clone(&middleware.pool);
    let harness = PolicyDaemonHarness::start_with_middleware(
        "grouped_recreated",
        middleware,
        |builder, policy| {
            builder
                .register_services::<Tally>(())
                .register_policy_fn::<ProbeEvent, _>(policy, bulk(), count_alternately_if_open)
        },
    )
    .await;
    pool.set(harness.pool().clone())
        .expect("the pool is set once");
    harness.await_bulk_registration(harness.policy_name()).await;

    harness.bulk_import("recreated", &tags("t", 4)).await;

    // t-0 counts: version 1, place 1. The tally is deleted and recreated with one event,
    // version 1, place 1 again — and t-2 counts on it because it is open.
    harness
        .observe("the recreated tally to count", || async {
            (counted(&harness, "even").await == ["opened", "t-2"]).then_some(())
        })
        .await;

    harness.shutdown().await;
}

/// Arms [`OPEN_WHILE_DECIDING`] for the second command the `even` tally receives.
#[derive(Clone, Default)]
struct ArmOnSecondCommand {
    pool: Arc<OnceLock<PgPool>>,
    tally_commands: Arc<AtomicUsize>,
}

impl CommandMiddleware for ArmOnSecondCommand {
    fn before<'a>(&'a self, context: CommandContextMut<'a>) -> BoxFuture<'a, Next> {
        Box::pin(async move {
            if context.stream_type() == "Tally"
                && context.causation().is_some()
                && self.tally_commands.fetch_add(1, Ordering::SeqCst) + 1 == 2
            {
                *OPEN_WHILE_DECIDING
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                    Some(self.pool.get().expect("the pool is set").clone());
            }
            Next::Continue
        })
    }
}

/// A write landing while the handler decides — after the head was checked — does not
/// let a refusal decided on the stale instance stand: the head is read again after a
/// decision that appended nothing, the refusal becomes a conflict, and the retry counts
/// on the opened tally.
#[tokio::test]
async fn a_refusal_raced_by_a_write_is_retried_postgres_test() {
    let _counting = count_tally_loads().await;
    let middleware = ArmOnSecondCommand::default();
    let pool = Arc::clone(&middleware.pool);
    let harness = PolicyDaemonHarness::start_with_middleware(
        "grouped_raced",
        middleware,
        |builder, policy| {
            builder
                .register_services::<Tally>(())
                .register_policy_fn::<ProbeEvent, _>(policy, bulk(), count_alternately_if_open)
        },
    )
    .await;
    pool.set(harness.pool().clone())
        .expect("the pool is set once");
    harness.await_bulk_registration(harness.policy_name()).await;

    harness.bulk_import("raced", &tags("t", 4)).await;

    harness
        .observe("the raced refusal to be retried and count", || async {
            (counted(&harness, "even").await == ["t-0", "opened", "t-2"]).then_some(())
        })
        .await;
    harness
        .observe("every queue to be drained", || async {
            (harness.all_bulk_work().await == 0).then_some(())
        })
        .await;
    assert_eq!(harness.parked_row_count().await, 0, "nothing was parked");

    harness.shutdown().await;
}

/// Arms [`OPEN_WHILE_DECIDING`] for the second Tally command and answers `Busy` at that
/// command's commit gate, once.
#[derive(Clone, Default)]
struct BusyAtGateWhileMoved {
    pool: Arc<OnceLock<PgPool>>,
    tally_commands: Arc<AtomicUsize>,
    busy_at_next_gate: Arc<std::sync::atomic::AtomicBool>,
}

impl CommandMiddleware for BusyAtGateWhileMoved {
    fn before<'a>(&'a self, context: CommandContextMut<'a>) -> BoxFuture<'a, Next> {
        Box::pin(async move {
            if context.stream_type() == "Tally"
                && context.causation().is_some()
                && self.tally_commands.fetch_add(1, Ordering::SeqCst) + 1 == 2
            {
                *OPEN_WHILE_DECIDING
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) =
                    Some(self.pool.get().expect("the pool is set").clone());
                self.busy_at_next_gate.store(true, Ordering::SeqCst);
            }
            Next::Continue
        })
    }

    fn before_commit<'a>(&'a self, _context: &'a CommandContext) -> BoxFuture<'a, Next> {
        Box::pin(async move {
            if self.busy_at_next_gate.swap(false, Ordering::SeqCst) {
                Next::Busy("held at the gate".to_string())
            } else {
                Next::Continue
            }
        })
    }
}

/// t-0 counts, t-2 does nothing; the rest count.
fn count_but_touch_t2(event: &ObservedEvent<ProbeEvent>) -> Vec<Dispatch> {
    count_alternately(event)
        .into_iter()
        .map(|dispatch| {
            let tally = dispatch.target_stream_id().clone();
            let (_, command) = dispatch
                .into_parts::<Tally>()
                .unwrap_or_else(|_| panic!("count_alternately dispatches to a Tally"));
            let command = match command {
                TallyCommand::Count { tag } if tag == "t-2" => TallyCommand::Touch,
                other => other,
            };
            Dispatch::to::<Tally>(tally.try_into().expect("a tally URN"), command)
        })
        .collect()
}

/// A `Busy` at the commit gate stays `Busy` when the stream also moved while the command
/// was decided: the batch is abandoned and claimed again — so `t-0`, which committed
/// before it, is delivered again (at-least-once) — rather than the command being settled
/// in the batch as a conflict.
#[tokio::test]
async fn a_busy_gate_abandons_the_batch_even_when_the_stream_moved_postgres_test() {
    let _counting = count_tally_loads().await;
    let middleware = BusyAtGateWhileMoved::default();
    let pool = Arc::clone(&middleware.pool);
    let harness = PolicyDaemonHarness::start_with_middleware(
        "busy_gate_moved",
        middleware,
        |builder, policy| {
            builder
                .register_services::<Tally>(())
                .register_policy_fn::<ProbeEvent, _>(policy, bulk(), count_but_touch_t2)
        },
    )
    .await;
    pool.set(harness.pool().clone())
        .expect("the pool is set once");
    harness.await_bulk_registration(harness.policy_name()).await;

    harness.bulk_import("busy-moved", &tags("t", 4)).await;

    harness
        .observe("every queue to be drained", || async {
            (harness.all_bulk_work().await == 0).then_some(())
        })
        .await;
    assert_eq!(
        counted(&harness, "even").await,
        ["t-0", "opened", "t-0"],
        "the abandoned batch is claimed again from its first event"
    );
    assert_eq!(counted(&harness, "odd").await, ["t-1", "t-3"]);
    assert_eq!(harness.parked_row_count().await, 0, "nothing was parked");

    harness.shutdown().await;
}

/// The stream types `policy`'s bulk registration says it tracks, sorted; `None` for every
/// stream.
async fn registered_types(harness: &PolicyDaemonHarness, policy: &str) -> Option<Vec<String>> {
    sqlx::query_scalar(
        "SELECT tracked_stream_types FROM policy_bulk_registrations WHERE policy = $1",
    )
    .bind(policy)
    .fetch_one(harness.pool())
    .await
    .expect("the registration must exist")
}

/// Two replicas of one bulk Policy that disagree on what it tracks — a rolling deploy —
/// leave a registration covering both, so neither drops the other's bulk work; and a
/// replica that tracks every stream widens it to every stream.
#[tokio::test]
async fn a_bulk_registration_only_widens_postgres_test() {
    let types = |types: &[&str]| {
        StreamFilter::ForStreamTypes(types.iter().map(|t| t.to_string()).collect())
    };
    let mut harness = start(
        "registration_widens",
        bulk().with_stream_filter(types(&["Probe"])),
    )
    .await;
    let policy = harness.policy_name().to_string();
    harness.await_bulk_registration(&policy).await;
    assert_eq!(
        registered_types(&harness, &policy).await,
        Some(vec!["Probe".to_string()])
    );

    // The other replica, on another release, tracks another type under the same name.
    let narrower = harness.start_runner({
        let policy = policy.clone();
        move |builder: PolicyRunnerBuilder, _| {
            builder.register_policy_fn::<ProbeEvent, _>(
                policy.clone(),
                bulk().with_stream_filter(StreamFilter::ForStreamTypes(vec![
                    "Elsewhere".to_string()
                ])),
                echo_into("echo"),
            )
        }
    });
    harness
        .observe("the registration to cover both replicas", || async {
            (registered_types(&harness, &policy).await
                == Some(vec!["Elsewhere".to_string(), "Probe".to_string()]))
            .then_some(())
        })
        .await;

    // A Probe import is still queued for the Policy, whichever replica registered last.
    narrower.shutdown().await;
    harness.stop().await;
    harness.bulk_import("still-queued", &tags("q", 3)).await;
    assert_eq!(harness.bulk_work(&policy).await, 3);

    // A replica tracking every stream widens the registration to every stream.
    let everything = harness.start_runner({
        let policy = policy.clone();
        move |builder: PolicyRunnerBuilder, _| {
            builder.register_policy_fn::<ProbeEvent, _>(policy.clone(), bulk(), echo_into("echo"))
        }
    });
    harness
        .observe("the registration to cover every stream", || async {
            registered_types(&harness, &policy)
                .await
                .is_none()
                .then_some(())
        })
        .await;
    everything.shutdown().await;
}
