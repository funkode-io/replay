//! A Policy's cursor records the transaction it stopped in (funkode-io/replay#194).
//!
//! These assert through a live daemon and through the schema a deployment upgrades:
//! what the runner leaves in `policy_cursors`, and what the migration leaves there for
//! a Policy that was already running. The cursor's own semantics — the compare-and-set,
//! and the transaction derived for an operator's position — are pinned next to the type
//! in `policy_runner::cursor_tests`, which can reach it without a worker in the way.

mod common;

use sqlx::postgres::PgPoolOptions;
use sqlx::{PgPool, Row};
use testcontainers_modules::testcontainers::runners::AsyncRunner;

use common::migrations::{through as migrations_through, MIGRATOR};
use common::policy_harness::{
    PolicyDaemonHarness, Probe, ProbeCommand, ProbeEvent, ProbeUrn, StoredCursor,
};
use common::postgres_image::{postgres_container, POSTGRES_PORT};
use replay_persistence::{Dispatch, StartAt};

/// The migration before the one that gives the cursor its transaction half: a database
/// staged here is one a live deployment is on when 0022 reaches it.
const BEFORE_CURSOR_STAMP: i64 = 21;

/// The sentinel a cursor that predates the stamp carries: it orders before every real
/// transaction, which is where every event that cursor has processed sits too.
const SENTINEL: &str = "0";

/// A checkpoint records both halves: the position the policy stopped at, and the
/// transaction that wrote the event there.
#[tokio::test]
async fn a_checkpoint_records_the_transaction_the_policy_stopped_in_postgres_test() {
    // A policy that dispatches nothing, so the log ends where the test put it and the
    // cursor's last point is the event this test can name.
    let harness = PolicyDaemonHarness::start("records_txid", |builder, policy| {
        builder.register_policy_fn::<ProbeEvent, _>(policy, StartAt::Beginning, |_| vec![])
    })
    .await;

    let ping = harness.ping("subject-1", "hello").await;
    harness.await_cursor_at_least(ping.global_position).await;

    assert_eq!(
        harness.stored_cursor().await,
        Some(StoredCursor {
            commit_txid: ping.commit_txid.clone(),
            position: ping.global_position,
        }),
        "the cursor names the transaction that wrote the event it stopped at"
    );
    assert_ne!(
        ping.commit_txid, SENTINEL,
        "an event appended after the stamp carries a real transaction id"
    );

    harness.shutdown().await;
}

/// ADR-0012 unchanged: the operator writes a position against a running deployment and
/// the leader adopts it, with no second column to supply. Rewinding re-delivers, which
/// is how the move is visible from outside.
#[tokio::test]
async fn an_operator_moving_the_position_alone_is_honoured_by_a_running_leader_postgres_test() {
    let harness =
        PolicyDaemonHarness::start("operator_move", |builder, policy| {
            builder.register_policy_fn::<ProbeEvent, _>(policy, StartAt::Beginning, |event| {
                match &event.data {
                    ProbeEvent::Pinged { tag } => vec![Dispatch::to::<Probe>(
                        ProbeUrn::new(format!("{tag}-echo")).unwrap(),
                        ProbeCommand::Echo { tag: tag.clone() },
                    )],
                    _ => vec![],
                }
            })
        })
        .await;

    let ping = harness.ping("subject-1", "hello").await;
    harness.await_dispatch_caused_by(ping.global_position).await;
    harness.await_cursor_at_least(ping.global_position).await;

    harness.move_cursor_to(0).await;

    let redelivered = harness
        .observe("the event to be delivered a second time", || async {
            let caused = harness
                .dispatches()
                .await
                .into_iter()
                .filter(|dispatch| dispatch.caused_by_position == ping.global_position)
                .count();
            (caused >= 2).then_some(caused)
        })
        .await;
    assert!(redelivered >= 2, "the rewind re-delivered the event");

    let resumed = harness
        .observe("the cursor to be back on a real transaction", || async {
            harness
                .stored_cursor()
                .await
                .filter(|cursor| cursor.commit_txid != SENTINEL)
        })
        .await;
    assert!(
        resumed.position >= ping.global_position,
        "the policy caught up again after the rewind: {resumed:?}"
    );

    harness.shutdown().await;
}

/// A cursor that exists when the migration arrives keeps its position and takes the
/// sentinel — exactly where it already is, because every event it has processed carries
/// the sentinel too (0018).
#[tokio::test]
async fn a_cursor_that_predates_the_migration_keeps_its_position_postgres_test() {
    let (pool, _container) = start_postgres().await;
    migrations_through(BEFORE_CURSOR_STAMP)
        .run(&pool)
        .await
        .expect("migrations up to the cursor stamp must succeed");

    sqlx::query("INSERT INTO policy_cursors (name, position) VALUES ($1, $2)")
        .bind("already_running")
        .bind(264_786_i64)
        .execute(&pool)
        .await
        .expect("the running policy's cursor must be writable");

    MIGRATOR
        .run(&pool)
        .await
        .expect("the rest of the migrations must succeed");

    let row = sqlx::query(
        "SELECT position, commit_txid::text AS commit_txid FROM policy_cursors WHERE name = $1",
    )
    .bind("already_running")
    .fetch_one(&pool)
    .await
    .expect("reading the migrated cursor must succeed");

    assert_eq!(row.get::<i64, _>("position"), 264_786);
    assert_eq!(row.get::<String, _>("commit_txid"), SENTINEL);
}

async fn start_postgres() -> (
    PgPool,
    testcontainers_modules::testcontainers::ContainerAsync<
        testcontainers_modules::postgres::Postgres,
    >,
) {
    let container = postgres_container().start().await.unwrap();
    let host = container.get_host().await.unwrap().to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("Error getting docker port");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&format!(
            "postgres://postgres:postgres@{host}:{port}/postgres"
        ))
        .await
        .expect("Failed to connect to Postgres");

    (pool, container)
}
