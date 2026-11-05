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
    CommandContextMut, CommandMiddleware, Dispatch, Lanes, Next, ObservedEvent,
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
/// from its `Default`. A test that counts holds [`TALLY_TESTS`] for its whole run and
/// starts from zero, so the count is that test's.
static TALLY_LOADS: AtomicUsize = AtomicUsize::new(0);

/// Serialises the tests that count [`TALLY_LOADS`].
static TALLY_TESTS: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

/// Take the tally counter for this test, from zero.
async fn count_tally_loads() -> tokio::sync::MutexGuard<'static, ()> {
    let guard = TALLY_TESTS.lock().await;
    TALLY_LOADS.store(0, Ordering::SeqCst);
    guard
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
        },
        commands: {
            Count { tag: String },
        },
        events: {
            Counted { tag: String },
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
        match command {
            TallyCommand::Count { tag } => Ok(vec![TallyEvent::Counted { tag }]),
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
        Err(error) => panic!("a tally holds only counts: {error}"),
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

/// Writes a `Counted { tag: "outside" }` into the `even` tally just before the second
/// Tally command a Policy dispatches runs — between two commands of one target group, so
/// the instance the group loaded is stale when the second one runs.
#[derive(Clone, Default)]
struct WriteBetweenCommands {
    pool: Arc<OnceLock<PgPool>>,
    tally_commands: Arc<AtomicUsize>,
}

impl CommandMiddleware for WriteBetweenCommands {
    fn before<'a>(&'a self, context: CommandContextMut<'a>) -> BoxFuture<'a, Next> {
        Box::pin(async move {
            if context.stream_type() != "Tally" || context.causation().is_none() {
                return Next::Continue;
            }
            if self.tally_commands.fetch_add(1, Ordering::SeqCst) + 1 == 2 {
                let pool = self.pool.get().expect("the pool is set before any import");
                let event = TallyEvent::Counted {
                    tag: "outside".to_string(),
                };
                sqlx::query(
                    "SELECT id FROM write_event(gen_random_uuid(), $1, '{}'::jsonb, $2, $3, \
                     'Tally', NULL, FALSE)",
                )
                .bind(serde_json::to_value(&event).expect("a tally event serialises"))
                .bind(replay::Event::event_type(&event))
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
    let middleware = WriteBetweenCommands::default();
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
                  WHERE wait_event_type = 'Lock' AND wait_event = 'advisory' \
                    AND query LIKE '%replay.bulk_claim%')",
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

    in_flight
        .commit()
        .await
        .expect("ending the claim in flight");
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
