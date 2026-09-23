//! Two appends racing to create one stream (funkode-io/replay#232).
//!
//! `write_event` numbers an event under the stream row's lock, and a row that does not
//! exist yet cannot be locked. Both racers therefore used to insert it, and the loser
//! died on `streams_pkey` with a raw database error — a failure a caller cannot tell from
//! a broken database, on the one append where it is least expected.
//!
//! The race is staged rather than run: the winner's transaction is held open, so the
//! loser is *made* to arrive second on every run instead of on most of them. A spawned
//! pair of appends reproduces the collision roughly five times in six, which is a flaky
//! pin, not a pin.
//!
//! The appends go straight to `append_event` rather than through `Cqrs`: the store gives
//! no way to hold its transaction open from outside, which is the whole staging device.

use std::time::{Duration, Instant};

use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};

mod common;
use common::migrations::MIGRATOR;
use common::postgres_image::{postgres_container, POSTGRES_PORT};

/// The stream both racers append to. It does not exist when they start.
const STREAM_ID: &str = "urn:ledger:contested";

/// How long a test waits for the second append to become visibly blocked behind the
/// first. Generous: a loaded CI box is slow, and the test only fails if nothing ever
/// blocks at all.
const BLOCKED_TIMEOUT: Duration = Duration::from_secs(10);

/// Upper bound on the rows an observation reads. These tests write two events; a query
/// that returned more is not observing what it thinks it is.
const OBSERVATION_LIMIT: i64 = 100;

/// The bound the wait test sets. Short: it is waiting for a transaction that never
/// commits, so the whole test costs this.
const LOCK_WAIT: Duration = Duration::from_millis(500);

/// Postgres `lock_not_available`: the `lock_timeout` above fired.
const LOCK_NOT_AVAILABLE: &str = "55P03";

async fn start_postgres() -> (
    PgPool,
    testcontainers_modules::testcontainers::ContainerAsync<postgres::Postgres>,
) {
    let container = postgres_container().start().await.unwrap();
    let host = container.get_host().await.unwrap().to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("Error getting docker port");

    // Three at once: the two racers hold one each for as long as their transactions
    // last, and the observer that watches them block needs one of its own.
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&format!(
            "postgres://postgres:postgres@{host}:{port}/postgres"
        ))
        .await
        .expect("Failed to connect to Postgres");

    (pool, container)
}

/// One append through the function the store calls, on a connection the caller owns.
///
/// `fetch_optional`: no row is `write_event`'s way of reporting an optimistic-concurrency
/// mismatch, which is an outcome two of these tests are about.
async fn append(
    conn: &mut sqlx::PgConnection,
    expected_version: Option<i64>,
) -> Result<Option<i64>, sqlx::Error> {
    sqlx::query(
        "SELECT version FROM append_event(gen_random_uuid(), '{}'::jsonb, '{}'::jsonb, \
         'Added', $1, 'Ledger', $2)",
    )
    .bind(STREAM_ID)
    .bind(expected_version)
    .fetch_optional(&mut *conn)
    .await
    .map(|row| row.map(|row| row.get("version")))
}

/// The `(version, stream_seq)` of every event in the contested stream, in write order.
async fn numbering(pool: &PgPool) -> Vec<(i64, i64)> {
    sqlx::query(
        "SELECT version, stream_seq FROM events WHERE stream_id = $1 \
          ORDER BY global_position LIMIT $2",
    )
    .bind(STREAM_ID)
    .bind(OBSERVATION_LIMIT)
    .fetch_all(pool)
    .await
    .expect("reading the numbering must succeed")
    .into_iter()
    .map(|row| (row.get("version"), row.get("stream_seq")))
    .collect()
}

/// Wait until some transaction is queued behind a lock another one holds.
///
/// The loser waits on the winner's transaction id (that is how an insert defers to an
/// uncommitted conflicting row), not on a relation, so this asks the ungranted locks
/// rather than any one table.
async fn await_blocked(pool: &PgPool) {
    let deadline = Instant::now() + BLOCKED_TIMEOUT;
    loop {
        let blocked: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_locks WHERE NOT granted")
            .fetch_one(pool)
            .await
            .expect("reading pg_locks must succeed");

        if blocked > 0 {
            return;
        }
        assert!(
            Instant::now() < deadline,
            "the second append never blocked behind the first"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// Both first appends land, and the loser takes the place after the winner's rather than
/// the one the winner already holds.
#[tokio::test]
async fn concurrent_first_appends_to_one_new_stream_both_land_postgres_test() {
    let (pool, _container) = start_postgres().await;
    MIGRATOR.run(&pool).await.expect("migrations must succeed");

    let mut winner = pool.begin().await.expect("beginning the winner");
    append(&mut winner, None)
        .await
        .expect("the first append must succeed");

    let loser = tokio::spawn({
        let pool = pool.clone();
        async move {
            let mut loser = pool.begin().await.expect("beginning the loser");
            let version = append(&mut loser, None).await;
            loser.commit().await.expect("committing the loser");
            version
        }
    });
    await_blocked(&pool).await;

    winner.commit().await.expect("committing the winner");
    let version = loser
        .await
        .expect("the blocked append must finish")
        .expect("the loser of the creation race must append, not collide");

    assert_eq!(
        version,
        Some(2),
        "the loser is numbered after the winner, not alongside it"
    );
    assert_eq!(
        numbering(&pool).await,
        vec![(1, 1), (2, 2)],
        "and the stream counts both events once each, on both of its axes"
    );
}

/// The wait the loser now does is bounded like every other wait for a stream row
/// (ADR-0022): a creator that stalls costs the loser its `lock_timeout`, not its
/// connection.
#[tokio::test]
async fn the_wait_for_a_stream_being_created_is_bounded_postgres_test() {
    let (pool, _container) = start_postgres().await;
    MIGRATOR.run(&pool).await.expect("migrations must succeed");

    let mut winner = pool.begin().await.expect("beginning the winner");
    append(&mut winner, None)
        .await
        .expect("the first append must succeed");

    let mut loser = pool.begin().await.expect("beginning the loser");
    sqlx::query("SELECT set_config('lock_timeout', $1, true)")
        .bind(format!("{}ms", LOCK_WAIT.as_millis()))
        .execute(&mut *loser)
        .await
        .expect("bounding the wait must succeed");

    let refused = append(&mut loser, None)
        .await
        .expect_err("the winner never commits, so the loser cannot append");

    assert_eq!(
        refused
            .as_database_error()
            .and_then(|e| e.code())
            .as_deref(),
        Some(LOCK_NOT_AVAILABLE),
        "the server ended the wait, so the connection is free again: {refused:?}"
    );
}

/// An append that names an expected version keeps the contract it had: losing the
/// creation race is a concurrency mismatch — no row — and never a database error.
#[tokio::test]
async fn the_loser_that_expected_a_new_stream_conflicts_postgres_test() {
    let (pool, _container) = start_postgres().await;
    MIGRATOR.run(&pool).await.expect("migrations must succeed");

    let mut winner = pool.begin().await.expect("beginning the winner");
    append(&mut winner, Some(0))
        .await
        .expect("the first append must succeed");

    let loser = tokio::spawn({
        let pool = pool.clone();
        async move {
            let mut loser = pool.begin().await.expect("beginning the loser");
            // Version 0 is "this stream does not exist yet": by the time this append is
            // let through, it does.
            let version = append(&mut loser, Some(0)).await;
            loser.commit().await.expect("committing the loser");
            version
        }
    });
    await_blocked(&pool).await;

    winner.commit().await.expect("committing the winner");
    let version = loser
        .await
        .expect("the blocked append must finish")
        .expect("a mismatch is reported as no row, not as a failed statement");

    assert_eq!(
        version, None,
        "the expectation the loser was given no longer holds"
    );
    assert_eq!(
        numbering(&pool).await,
        vec![(1, 1)],
        "so only the winner's event is in the log"
    );
}
