//! A Policy whose filter is `StreamFilter::ForStreamTypes` tracks only those stream types
//! (funkode-io/replay#267, ADR-0030).
//!
//! Every test pits a Policy that tracks `Probe` streams against streams of another type,
//! `Ledger`, that it has no business with. Under ADR-0026 alone the Policy compared, read
//! and kept a place for every one of those as well; what is asserted here is what an
//! operator sees of that — places in `policy_stream_cursors`, the sweep position, the
//! status read model — and that delivery from the streams it does track is unharmed.

use std::time::Duration;

use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

use crate::common;

use common::policy_harness::{
    PolicyDaemonHarness, Probe, ProbeCommand, ProbeEvent, ProbeUrn, OBSERVE_TIMEOUT,
};
use common::postgres_image::{start_postgres_server, POSTGRES_PORT};
use replay_persistence::{
    Cqrs, Dispatch, ObservedEvent, PolicyRunner, PolicySettings, PostgresEventStore, StartAt,
    StreamFilter,
};

/// The stream type nothing in these tests tracks.
const UNTRACKED: &str = "Ledger";

/// Echo every ping into a probe stream of its own, as `policy_per_stream_feed` does, so
/// what the Policy delivered reads as the commands it dispatched.
fn echo_back(event: &ObservedEvent<ProbeEvent>) -> Vec<Dispatch> {
    match &event.data {
        ProbeEvent::Pinged { tag } => vec![Dispatch::to::<Probe>(
            ProbeUrn::new(format!("{tag}-echo")).expect("a valid NSS"),
            ProbeCommand::Echo { tag: tag.clone() },
        )],
        ProbeEvent::Echoed { .. } => vec![],
    }
}

/// The settings every test registers its Policy with: probe streams only.
fn probes_only() -> PolicySettings {
    PolicySettings::new()
        .starting_at(StartAt::Beginning)
        .with_stream_filter(StreamFilter::for_stream_type::<Probe>())
}

/// A stream id of the untracked type.
fn ledger(name: &str) -> String {
    format!("urn:ledger:{name}")
}

/// The streams of the untracked type this Policy holds a place in. Empty is the
/// property; anything else names the streams it should never have read.
async fn untracked_places(harness: &PolicyDaemonHarness) -> Vec<(String, i64)> {
    harness
        .places()
        .await
        .into_iter()
        .filter(|(stream, _)| stream.starts_with("urn:ledger:"))
        .collect()
}

/// A write the sweep passed is still delivered when the Policy tracks only some stream
/// types — the reconciliation still compares the streams it tracks — and the streams of
/// any other type are never given a place, however many of them the sweep and the
/// reconciliation run into.
#[tokio::test]
async fn a_policy_tracking_one_stream_type_delivers_a_late_write_and_keeps_no_other_place_postgres_test(
) {
    let harness = PolicyDaemonHarness::start("tracks_types_late", |builder, policy| {
        builder.register_policy_fn::<ProbeEvent, _>(policy, probes_only(), echo_back)
    })
    .await;

    // Untracked streams on either side of everything else, so the sweep runs into them
    // before, during and after the held write, and the reconciliation finds them behind.
    let mut untracked = Vec::new();
    for n in 0..3 {
        untracked.push(
            harness
                .ping_of_type(UNTRACKED, &ledger(&format!("before-{n}")), "ignored")
                .await,
        );
    }

    let held = harness.hold_a_ping_open("slow", "held").await;
    for n in 0..3 {
        untracked.push(
            harness
                .ping_of_type(UNTRACKED, &ledger(&format!("during-{n}")), "ignored")
                .await,
        );
    }

    // Committed and delivered while the held write is open: the sweep passes the held
    // position, so only the reconciliation can deliver it.
    let overtaking = harness.ping("quick", "overtook").await;
    harness
        .await_dispatch_caused_by(overtaking.global_position)
        .await;
    assert!(
        !harness.has_passed(held.global_position).await,
        "an uncommitted write has been passed by nobody"
    );

    let position = held.global_position;
    held.commit().await;
    harness.await_dispatch_caused_by(position).await;
    harness.await_passed(position).await;

    // The sweep has read past every untracked event — it did not stop at them — and the
    // reconciliation that delivered the held write ran with every one of them behind.
    let last = untracked
        .iter()
        .map(|event| event.global_position)
        .max()
        .expect("there are untracked events");
    harness
        .observe("the sweep to pass the untracked events", || async {
            (harness.swept_through().await? >= last).then_some(())
        })
        .await;

    assert_eq!(
        untracked_places(&harness).await,
        Vec::new(),
        "a Policy tracking probe streams keeps no place in a ledger stream"
    );
    for event in &untracked {
        assert!(
            harness
                .dispatches()
                .await
                .iter()
                .all(|dispatched| dispatched.caused_by_position != event.global_position),
            "nothing of a stream the Policy does not track is delivered"
        );
    }

    harness.shutdown().await;
}

/// The status read model counts lag over the streams the Policy tracks. Every untracked
/// stream has no place, so counted against every stream it would be behind for ever, and
/// the Policy would never read as caught up.
#[tokio::test]
async fn a_policy_tracking_one_stream_type_reads_as_caught_up_whatever_else_is_written_postgres_test(
) {
    let harness = PolicyDaemonHarness::start("tracks_types_status", |builder, policy| {
        builder.register_policy_fn::<ProbeEvent, _>(policy, probes_only(), echo_back)
    })
    .await;

    for n in 0..5 {
        harness
            .ping_of_type(UNTRACKED, &ledger(&format!("busy-{}", n % 2)), "ignored")
            .await;
    }
    let tracked = harness.ping("tracked", "one").await;
    harness
        .await_dispatch_caused_by(tracked.global_position)
        .await;
    harness.await_passed(tracked.global_position).await;

    let status = harness
        .observe("the Policy to read as caught up", || async {
            harness
                .status()
                .await
                .filter(|status| status.lag == 0 && status.streams_behind == 0)
        })
        .await;
    assert_eq!(
        (status.lag, status.streams_behind),
        (0, 0),
        "five untracked events in two untracked streams are no lag of this Policy's"
    );

    harness.shutdown().await;
}

// ── The sweep, one poll at a time ────────────────────────────────────────────

/// The read budget the sweep test registers: small, so a handful of events fills it.
const READ_BATCH: u32 = 3;

async fn migrated_pool(port: u16, host: &str) -> PgPool {
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(OBSERVE_TIMEOUT)
        .idle_timeout(Duration::from_secs(5))
        .connect(&format!(
            "postgres://postgres:postgres@{host}:{port}/postgres"
        ))
        .await
        .expect("failed to create the postgres pool");
    common::migrations::MIGRATOR
        .run(&pool)
        .await
        .expect("migrations must succeed");
    pool
}

/// One event appended to `stream_id`, a stream of `stream_type`, through the store's own
/// writer; its global position.
async fn append(pool: &PgPool, stream_type: &str, stream_id: &str) -> i64 {
    let id = uuid::Uuid::new_v4();
    sqlx::query(
        "SELECT append_event($1, '{\"Pinged\":{\"tag\":\"ignored\"}}'::jsonb, '{}'::jsonb, \
         'Pinged', $2, $3, NULL)",
    )
    .bind(id)
    .bind(stream_id)
    .bind(stream_type)
    .execute(pool)
    .await
    .expect("appending must succeed");

    // A statement of its own: the row `append_event` inserts is not visible to the
    // statement that called it.
    sqlx::query_scalar("SELECT global_position FROM events WHERE id = $1")
        .bind(id)
        .fetch_one(pool)
        .await
        .expect("the appended event must be readable")
}

/// How far the Policy's sweep has got, as `policy_cursors` records it.
async fn swept_through(pool: &PgPool, policy: &str) -> i64 {
    sqlx::query_scalar("SELECT discovered_through FROM policy_cursors WHERE name = $1")
        .bind(policy)
        .fetch_one(pool)
        .await
        .expect("the policy has a cursor row")
}

/// The sweep reads its `LIMIT` of positions per poll whatever streams they belong to, and
/// moves past a batch that holds nothing the Policy tracks — it neither stops in front of
/// one nor searches ahead for a match, which would make the rows it examines grow with the
/// rows it skips (funkode-io/replay#258).
///
/// Driven by [`PolicyRunner::drain`], one poll per call, so "per poll" is something the
/// test can count rather than infer from a daemon's timing.
#[tokio::test]
async fn the_sweep_passes_a_batch_of_untracked_events_one_limit_at_a_time_postgres_test() {
    let container = start_postgres_server().await;
    let host = container
        .get_host()
        .await
        .expect("failed to read the container host")
        .to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("failed to read the container port");
    let pool = migrated_pool(port, &host).await;

    let mut untracked = Vec::new();
    for n in 0..(READ_BATCH * 3) {
        untracked.push(append(&pool, UNTRACKED, &ledger(&format!("s{n}"))).await);
    }

    let policy = "tracks_types_sweep";
    let runner = PolicyRunner::builder(Cqrs::new(PostgresEventStore::new(pool.clone())))
        .register_services::<Probe>(())
        .register_policy_fn::<ProbeEvent, _>(
            policy,
            probes_only()
                .with_checkpoint_batch_size(1)
                .with_read_batch_size(READ_BATCH),
            echo_back,
        )
        .build();

    for poll in 1..=3 {
        assert_eq!(
            runner.drain().await.expect("a drain must succeed"),
            0,
            "nothing untracked is delivered"
        );
        assert_eq!(
            swept_through(&pool, policy).await,
            untracked[(READ_BATCH as usize) * poll - 1],
            "poll {poll} sweeps exactly its budget of {READ_BATCH} untracked positions: \
             past them, and not past the next"
        );
    }

    let places: i64 =
        sqlx::query_scalar("SELECT count(*) FROM policy_stream_cursors WHERE policy = $1")
            .bind(policy)
            .fetch_one(&pool)
            .await
            .expect("the places must be readable");
    assert_eq!(places, 0, "and no untracked stream was given a place");
}
