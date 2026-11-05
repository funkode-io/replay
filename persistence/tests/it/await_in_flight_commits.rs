//! Turning a rule on waits for the commits already past it (funkode-io/replay#315,
//! ADR-0035's *No gap between the check and the commit*).
//!
//! An append whose commit gate asks anything holds the commit gate's advisory lock shared
//! from just before it asks until its transaction ends; `await_in_flight_commits` takes it
//! exclusively and lets it go. What only Postgres can show is that the two meet: an
//! operator who commits a flag and then awaits leaves no append that read the old flag
//! still to commit, and appends that ask no gate never wait.

use std::sync::Arc;
use std::time::Duration;

use futures::future::BoxFuture;
use replay::{ErrorKind, EventStream, Metadata};
use sqlx::{postgres::PgPoolOptions, PgPool};
use tokio::sync::Notify;
use tokio::time::timeout;

use crate::common;

use common::policy_harness::{Probe, ProbeCommand, ProbeEvent, ProbeUrn, OBSERVE_TIMEOUT};
use common::postgres_image::{start_postgres_server, PostgresServer, POSTGRES_PORT};
use replay_persistence::{
    CommandContext, CommandContextMut, CommandMiddleware, Cqrs, EventStore, Next,
    PostgresEventStore,
};

/// The advisory-lock class of the commit gate — `RCGT`, as the store spells it.
const COMMIT_GATE_LOCK_CLASS: i32 = 0x5243_4754;

/// A fresh server, migrated, with the one-row `maintenance` table an operator flips.
async fn flag_pool() -> (PgPool, PostgresServer) {
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
    // Each append in flight holds a connection for its transaction and borrows another
    // for the middleware's read of the flag.
    let pool = PgPoolOptions::new()
        .max_connections(30)
        .connect(&format!(
            "postgres://postgres:postgres@{host}:{port}/postgres"
        ))
        .await
        .expect("failed to create the postgres pool");
    common::migrations::MIGRATOR
        .run(&pool)
        .await
        .expect("migrations must succeed");
    sqlx::query("CREATE TABLE maintenance (switched_on boolean NOT NULL)")
        .execute(&pool)
        .await
        .expect("creating the flag must succeed");
    sqlx::query("INSERT INTO maintenance VALUES (false)")
        .execute(&pool)
        .await
        .expect("the flag must start off");
    (pool, container)
}

/// The operator's switch: commit the flag.
async fn switch_maintenance_on(pool: &PgPool) {
    sqlx::query("UPDATE maintenance SET switched_on = true")
        .execute(pool)
        .await
        .expect("switching maintenance on must succeed");
}

async fn events(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM events")
        .fetch_one(pool)
        .await
        .expect("counting events must succeed")
}

/// Wait until some session queues for a lock another one holds.
async fn await_a_lock_wait(pool: &PgPool) {
    common::wait::wait_for_count(
        pool,
        "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock'",
        1,
        OBSERVE_TIMEOUT,
        "await_in_flight_commits to queue behind the append in flight",
    )
    .await;
}

fn probe(name: &str) -> ProbeUrn {
    ProbeUrn::new(name).expect("a valid URN NSS")
}

fn ping(tag: &str) -> ProbeCommand {
    ProbeCommand::Ping {
        tag: tag.to_string(),
    }
}

/// Stops an append inside its commit gate: says it has `entered`, then waits for
/// `release` before continuing.
#[derive(Clone, Default)]
struct Hold {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}

impl Hold {
    async fn stop_here(&self) {
        self.entered.notify_one();
        self.release.notified().await;
    }

    async fn reached(&self) {
        timeout(OBSERVE_TIMEOUT, self.entered.notified())
            .await
            .expect("the append must reach its commit gate");
    }
}

/// Maintenance mode as a consumer writes it: `Busy` in both hooks while the flag is on,
/// reading the flag as it is in the database every time.
///
/// With a `hold`, `before_commit` stops after reading the flag and before answering, so
/// a test can switch the flag on between an append's check and its commit. With a
/// `dawdle`, it sleeps there instead, so many appends sit in that gap at once.
#[derive(Clone)]
struct Maintenance {
    pool: PgPool,
    hold: Option<Hold>,
    dawdle: Duration,
}

impl Maintenance {
    fn new(pool: &PgPool) -> Self {
        Self {
            pool: pool.clone(),
            hold: None,
            dawdle: Duration::ZERO,
        }
    }

    async fn check(&self) -> Next {
        match sqlx::query_scalar::<_, bool>("SELECT switched_on FROM maintenance")
            .fetch_one(&self.pool)
            .await
        {
            Ok(true) => Next::Busy("maintenance in progress".to_string()),
            Ok(false) => Next::Continue,
            Err(error) => Next::Error(replay_persistence::db_error(error)),
        }
    }
}

impl CommandMiddleware for Maintenance {
    fn before<'a>(&'a self, _: CommandContextMut<'a>) -> BoxFuture<'a, Next> {
        Box::pin(self.check())
    }

    fn before_commit<'a>(&'a self, _: &'a CommandContext) -> BoxFuture<'a, Next> {
        Box::pin(async move {
            let answer = self.check().await;
            if let Some(hold) = &self.hold {
                hold.stop_here().await;
            }
            if !self.dawdle.is_zero() {
                tokio::time::sleep(self.dawdle).await;
            }
            answer
        })
    }
}

/// Asks nothing of anyone: a chain that is present, so the gate is not the open one.
struct Passes;

impl CommandMiddleware for Passes {}

/// An append held inside its commit gate makes `await_in_flight_commits` wait, and the
/// call returns only once that append has committed.
#[tokio::test]
async fn an_append_held_at_its_commit_gate_is_awaited_until_it_commits_postgres_test() {
    let (pool, _container) = flag_pool().await;
    let store = PostgresEventStore::new(pool.clone());
    let hold = Hold::default();
    let cqrs = Cqrs::new(store.clone()).with_middleware(Maintenance {
        hold: Some(hold.clone()),
        ..Maintenance::new(&pool)
    });

    let appending = tokio::spawn(async move {
        cqrs.execute::<Probe>(&probe("held"), Metadata::default(), ping("held"), &(), None)
            .await
    });
    hold.reached().await;

    let awaiting = tokio::spawn({
        let store = store.clone();
        async move { store.await_in_flight_commits().await }
    });
    await_a_lock_wait(&pool).await;
    assert!(
        !awaiting.is_finished(),
        "await_in_flight_commits must wait for the append past its check"
    );
    assert_eq!(events(&pool).await, 0, "the held append has not committed");

    hold.release.notify_one();
    timeout(OBSERVE_TIMEOUT, awaiting)
        .await
        .expect("await_in_flight_commits must return once the append is done")
        .expect("await_in_flight_commits must not panic")
        .expect("await_in_flight_commits must succeed");
    assert_eq!(
        events(&pool).await,
        1,
        "by the time the call returns, the append it waited for has committed"
    );

    appending
        .await
        .expect("the append must not panic")
        .expect("the append checked before the flag and commits");
}

/// The operator's procedure: an append that read the flag off is caught between its
/// check and its commit, the flag is committed on, and `await_in_flight_commits` waits
/// for it. Once the call returns, an append through the same middleware is refused.
#[tokio::test]
async fn switching_a_rule_on_then_awaiting_leaves_no_old_check_to_commit_postgres_test() {
    let (pool, _container) = flag_pool().await;
    let store = PostgresEventStore::new(pool.clone());
    let hold = Hold::default();
    let checked_early = Cqrs::new(store.clone()).with_middleware(Maintenance {
        hold: Some(hold.clone()),
        ..Maintenance::new(&pool)
    });
    let cqrs = Cqrs::new(store.clone()).with_middleware(Maintenance::new(&pool));

    // Read the flag while it is off, then stop before answering.
    let appending = tokio::spawn(async move {
        checked_early
            .execute::<Probe>(
                &probe("early"),
                Metadata::default(),
                ping("early"),
                &(),
                None,
            )
            .await
    });
    hold.reached().await;

    switch_maintenance_on(&pool).await;
    let awaiting = tokio::spawn({
        let store = store.clone();
        async move { store.await_in_flight_commits().await }
    });
    await_a_lock_wait(&pool).await;
    hold.release.notify_one();
    timeout(OBSERVE_TIMEOUT, awaiting)
        .await
        .expect("await_in_flight_commits must return once the append is done")
        .expect("await_in_flight_commits must not panic")
        .expect("await_in_flight_commits must succeed");
    let settled = events(&pool).await;
    assert_eq!(
        settled, 1,
        "the append that checked the old flag committed before the call returned"
    );

    let refused = cqrs
        .execute::<Probe>(&probe("late"), Metadata::default(), ping("late"), &(), None)
        .await
        .expect_err("an append after the switch reads the flag on");
    assert_eq!(refused.kind(), ErrorKind::Busy, "{refused}");
    assert_eq!(
        events(&pool).await,
        settled,
        "the refused append wrote nothing"
    );
    appending
        .await
        .expect("the append must not panic")
        .expect("the append that checked the old flag commits");
}

/// The race itself, many times at once: appends stream through the middleware, each
/// dawdling between its check and its commit, while the operator switches the flag on
/// and awaits. Nothing commits after the call returns.
#[tokio::test]
async fn no_append_commits_after_the_switch_and_the_await_return_postgres_test() {
    const WRITERS: usize = 8;
    let (pool, _container) = flag_pool().await;
    let store = PostgresEventStore::new(pool.clone());
    let cqrs = Cqrs::new(store.clone()).with_middleware(Maintenance {
        dawdle: Duration::from_millis(20),
        ..Maintenance::new(&pool)
    });

    let writers: Vec<_> = (0..WRITERS)
        .map(|writer| {
            let cqrs = cqrs.clone();
            tokio::spawn(async move {
                let id = probe(&format!("writer-{writer}"));
                loop {
                    match cqrs
                        .execute::<Probe>(&id, Metadata::default(), ping("w"), &(), None)
                        .await
                    {
                        Ok(_) => {}
                        Err(error) if error.kind() == ErrorKind::Busy => return,
                        Err(error) => panic!("an append failed for another reason: {error}"),
                    }
                }
            })
        })
        .collect();

    // Let the writers get going, so some are always between check and commit.
    common::wait::wait_for_count(
        &pool,
        "SELECT COUNT(*) FROM events",
        (WRITERS * 3) as i64,
        OBSERVE_TIMEOUT,
        "the writers to commit a few appends each",
    )
    .await;

    switch_maintenance_on(&pool).await;
    timeout(OBSERVE_TIMEOUT, store.await_in_flight_commits())
        .await
        .expect("await_in_flight_commits must return once the appends in flight are done")
        .expect("await_in_flight_commits must succeed");
    let settled = events(&pool).await;

    for writer in writers {
        timeout(OBSERVE_TIMEOUT, writer)
            .await
            .expect("every writer must be refused once maintenance is on")
            .expect("a writer must not panic");
    }
    assert_eq!(
        events(&pool).await,
        settled,
        "an append committed after the switch and the await returned"
    );
}

/// Appends that ask no gate — a direct append, a `Cqrs` without middleware — take no
/// lock, so they go ahead while an `await_in_flight_commits` holds it. An append through
/// middleware takes it and waits, here for no longer than the stream lock wait.
#[tokio::test]
async fn appends_that_ask_no_gate_take_no_lock_postgres_test() {
    let (pool, _container) = flag_pool().await;
    let store = PostgresEventStore::builder(pool.clone())
        .stream_lock_wait(Duration::from_millis(200))
        .build()
        .await
        .expect("building the store must succeed");

    // An `await_in_flight_commits` in another process, holding the lock as one does.
    let mut awaiting = pool.begin().await.expect("the holder must begin");
    sqlx::query("SELECT pg_advisory_xact_lock($1, 0)")
        .bind(COMMIT_GATE_LOCK_CLASS)
        .execute(&mut *awaiting)
        .await
        .expect("the holder must take the commit gate's lock");

    timeout(
        OBSERVE_TIMEOUT,
        store.store_events::<Probe>(
            &probe("direct"),
            Probe::stream_type(),
            Metadata::default(),
            &[ProbeEvent::Pinged {
                tag: "direct".to_string(),
            }],
            None,
        ),
    )
    .await
    .expect("a direct append must not wait for the commit gate's lock")
    .expect("a direct append commits");

    timeout(
        OBSERVE_TIMEOUT,
        Cqrs::new(store.clone()).execute::<Probe>(
            &probe("plain"),
            Metadata::default(),
            ping("plain"),
            &(),
            None,
        ),
    )
    .await
    .expect("a Cqrs without middleware must not wait for the commit gate's lock")
    .expect("a command without middleware commits");
    assert_eq!(events(&pool).await, 2);

    // The lock held is the one a gated append takes.
    let refused = Cqrs::new(store.clone())
        .with_middleware(Passes)
        .execute::<Probe>(
            &probe("gated"),
            Metadata::default(),
            ping("gated"),
            &(),
            None,
        )
        .await
        .expect_err("an append through middleware waits for the commit gate's lock");
    assert_eq!(refused.kind(), ErrorKind::Unavailable, "{refused}");
    assert_eq!(events(&pool).await, 2, "the gated append rolled back");

    awaiting.rollback().await.expect("the holder must let go");
    Cqrs::new(store)
        .with_middleware(Passes)
        .execute::<Probe>(
            &probe("gated"),
            Metadata::default(),
            ping("gated"),
            &(),
            None,
        )
        .await
        .expect("with the lock free, the gated append commits");
    assert_eq!(events(&pool).await, 3);
}

/// The commit gate's key collides with none of Replay's other advisory locks: with every
/// one of them held elsewhere — a bulk claim's, a projection's, the projection registry's,
/// and a one-key lock carrying the very same bits — `await_in_flight_commits` returns at once.
#[tokio::test]
async fn the_commit_gate_lock_is_apart_from_every_other_advisory_lock_postgres_test() {
    let (pool, _container) = flag_pool().await;
    let store = PostgresEventStore::new(pool.clone());

    // The bulk lane's claim class is a hash, so the value the key inventory lists for it
    // is the one the server computes.
    let bulk_claim: i32 = sqlx::query_scalar("SELECT hashtext('replay.bulk_claim')")
        .fetch_one(&pool)
        .await
        .expect("hashing the bulk claim's class must succeed");
    assert_eq!(bulk_claim, 0x59C2_65A4_u32 as i32);

    let mut holder = pool.begin().await.expect("the holder must begin");
    sqlx::query(
        "SELECT pg_advisory_xact_lock(hashtext('replay.bulk_claim'), hashtext('a_policy'))",
    )
    .execute(&mut *holder)
    .await
    .expect("the holder must take a bulk claim's lock");
    // `RPJC` over a projection name, `RPJR`, and the one-key form a Policy's leadership
    // takes, set to the bits the commit gate's two-key lock would read as.
    sqlx::query("SELECT pg_advisory_xact_lock($1, hashtext('a_view'))")
        .bind(0x5250_4A43_i32)
        .execute(&mut *holder)
        .await
        .expect("the holder must take a projection's lock");
    sqlx::query("SELECT pg_advisory_xact_lock($1, 0)")
        .bind(0x5250_4A52_i32)
        .execute(&mut *holder)
        .await
        .expect("the holder must take the registry's lock");
    sqlx::query("SELECT pg_advisory_xact_lock($1)")
        .bind(i64::from(COMMIT_GATE_LOCK_CLASS) << 32)
        .execute(&mut *holder)
        .await
        .expect("the holder must take a one-key lock");

    timeout(Duration::from_secs(5), store.await_in_flight_commits())
        .await
        .expect("no other advisory lock holds the commit gate's")
        .expect("await_in_flight_commits must succeed");
    holder.rollback().await.expect("the holder must let go");
}

/// A caller that gives up on `await_in_flight_commits` while an append is held leaves no
/// waiter behind for longer than the stream lock wait: once it is gone, another gated
/// append commits even though the first is still held.
#[tokio::test]
async fn an_abandoned_await_stops_queueing_gated_appends_postgres_test() {
    let (pool, _container) = flag_pool().await;
    let store = PostgresEventStore::builder(pool.clone())
        .stream_lock_wait(Duration::from_millis(500))
        .build()
        .await
        .expect("building the store must succeed");
    let hold = Hold::default();
    let held = Cqrs::new(store.clone()).with_middleware(Maintenance {
        hold: Some(hold.clone()),
        ..Maintenance::new(&pool)
    });
    let appending = tokio::spawn(async move {
        held.execute::<Probe>(&probe("held"), Metadata::default(), ping("held"), &(), None)
            .await
    });
    hold.reached().await;

    // The operator gives up while the held append still has the lock shared.
    timeout(Duration::from_millis(200), store.await_in_flight_commits())
        .await
        .expect_err("await_in_flight_commits waits while the append is held");

    // The abandoned wait ends on the server, within the stream lock wait.
    let deadline = std::time::Instant::now() + OBSERVE_TIMEOUT;
    loop {
        let waiting: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock'",
        )
        .fetch_one(&pool)
        .await
        .expect("counting lock waits must succeed");
        if waiting == 0 {
            break;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the abandoned wait for the commit gate's lock never ended"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    Cqrs::new(store.clone())
        .with_middleware(Passes)
        .execute::<Probe>(
            &probe("other"),
            Metadata::default(),
            ping("other"),
            &(),
            None,
        )
        .await
        .expect("no abandoned waiter queues a gated append");
    assert!(!appending.is_finished(), "the first append is still held");

    hold.release.notify_one();
    appending
        .await
        .expect("the append must not panic")
        .expect("the held append commits");
    assert_eq!(events(&pool).await, 2);
}

/// A bulk append commits each segment through the same gate: through middleware, a
/// segment waits for the commit gate's lock like any gated append; without middleware it
/// takes none.
#[tokio::test]
async fn a_bulk_segment_takes_the_lock_only_through_middleware_postgres_test() {
    let (pool, _container) = flag_pool().await;
    let store = PostgresEventStore::builder(pool.clone())
        .stream_lock_wait(Duration::from_millis(200))
        .build()
        .await
        .expect("building the store must succeed");

    let mut awaiting = pool.begin().await.expect("the holder must begin");
    sqlx::query("SELECT pg_advisory_xact_lock($1, 0)")
        .bind(COMMIT_GATE_LOCK_CLASS)
        .execute(&mut *awaiting)
        .await
        .expect("the holder must take the commit gate's lock");

    timeout(
        OBSERVE_TIMEOUT,
        Cqrs::new(store.clone()).execute_bulk::<Probe>(
            &probe("bulk-plain"),
            Metadata::default(),
            ping("plain"),
            &(),
            None,
        ),
    )
    .await
    .expect("a bulk append without middleware must not wait for the commit gate's lock")
    .expect("a bulk append without middleware commits");
    assert_eq!(events(&pool).await, 1);

    let refused = Cqrs::new(store.clone())
        .with_middleware(Passes)
        .execute_bulk::<Probe>(
            &probe("bulk-gated"),
            Metadata::default(),
            ping("gated"),
            &(),
            None,
        )
        .await
        .expect_err("a bulk segment through middleware waits for the commit gate's lock");
    assert_eq!(refused.kind(), ErrorKind::Unavailable, "{refused}");
    assert_eq!(events(&pool).await, 1, "the gated segment rolled back");

    awaiting.rollback().await.expect("the holder must let go");
}
