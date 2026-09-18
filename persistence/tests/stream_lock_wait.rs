//! A stream lock this library waits for is bounded on the server
//! (funkode-io/replay#205).
//!
//! The hazard is not the wait, it is the connection: cancelling a future sends
//! Postgres nothing, so a dispatch abandoned inside `append_event`'s
//! `SELECT ... FOR UPDATE` used to hold a pool connection for as long as the
//! blocker lasted — four of them per hung event, once the retries are counted.
//!
//! Every test here runs the store on a pool of **one** connection, with the
//! blocker on a connection of its own. A bounded append that strands its
//! connection therefore fails the test by starving everything after it, which is
//! the property the fix is about.

mod common;

use std::time::Duration;

use futures::TryStreamExt;
use replay::ErrorKind;
use replay_macros::define_aggregate;
use replay_persistence::{Cqrs, PostgresEventStore};
use sqlx::{postgres::PgPoolOptions, Connection, PgConnection, PgPool};
use testcontainers_modules::postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::ContainerAsync;

use common::migrations::MIGRATOR;
use common::postgres_image::{postgres_container, POSTGRES_PORT};

/// The bound the tests set. Long enough that a loaded CI box does not report it
/// before the blocker is even in place, short enough that a test waiting it out
/// costs a moment.
const LOCK_WAIT: Duration = Duration::from_millis(500);

/// How long "it is still waiting" is given before the opt-out test concludes the
/// bound really is off. Several times `LOCK_WAIT`, so a bound that fired would
/// have fired well inside it.
const WAITED_ENOUGH: Duration = Duration::from_secs(3);

define_aggregate! {
    Ledger {
        namespace: "ledger",
        state: {
            balance: f64,
        },
        commands: {
            Add { amount: f64 },
        },
        events: {
            Added { amount: f64 },
        }
    }
}

impl replay::EventStream for Ledger {
    type Event = LedgerEvent;

    fn stream_type() -> String {
        "Ledger".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        let LedgerEvent::Added { amount } = event;
        self.balance += amount;
    }
}

impl replay::Aggregate for Ledger {
    type Command = LedgerCommand;
    type Error = replay::Error;
    type Services = ();

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        let LedgerCommand::Add { amount } = command;
        Ok(vec![LedgerEvent::Added { amount }])
    }
}

impl replay::Compactable for Ledger {
    async fn compacted_events(
        &self,
        events: impl futures::TryStream<Ok = LedgerEvent, Error = replay::Error> + Send,
    ) -> replay::Result<replay::Compaction<LedgerEvent>> {
        let balance = events
            .try_fold(0.0, |total, LedgerEvent::Added { amount }| async move {
                Ok(total + amount)
            })
            .await?;
        Ok(vec![LedgerEvent::Added { amount: balance }].into())
    }
}

// ── Fixtures ─────────────────────────────────────────────────────────────────

/// A migrated database, and a pool of exactly one connection for the store under
/// test: the whole point is that a stranded connection is a starved pool.
async fn start_postgres() -> (PgPool, String, ContainerAsync<postgres::Postgres>) {
    let container = postgres_container()
        .start()
        .await
        .expect("container starts");
    let host = container.get_host().await.expect("host").to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("port");
    let url = format!("postgres://postgres:postgres@{host}:{port}/postgres");

    let pool = PgPoolOptions::new()
        .max_connections(1)
        // Without this, a starved pool reports "pool timed out" only after 30s, so a
        // regression would look like a hang rather than a failure.
        .acquire_timeout(Duration::from_secs(5))
        .connect(&url)
        .await
        .expect("connects");
    MIGRATOR.run(&pool).await.expect("migrations must succeed");

    (pool, url, container)
}

/// A session of its own holding `stream`'s row exactly as a stalled append, an
/// open `psql` or a long compaction on another replica holds it.
struct Blocker(PgConnection);

impl Blocker {
    async fn holding(url: &str, stream: &str) -> Self {
        let mut connection = PgConnection::connect(url).await.expect("blocker connects");
        sqlx::query("BEGIN")
            .execute(&mut connection)
            .await
            .expect("begins");
        sqlx::query("SELECT id FROM streams WHERE id = $1 FOR UPDATE")
            .bind(stream)
            .execute(&mut connection)
            .await
            .expect("blocker takes the row");
        Self(connection)
    }

    async fn release(mut self) {
        sqlx::query("ROLLBACK")
            .execute(&mut self.0)
            .await
            .expect("releases");
    }
}

/// A store whose transactions give up on a held stream row after `wait`.
async fn store(pool: &PgPool, wait: Duration) -> PostgresEventStore {
    PostgresEventStore::builder(pool.clone())
        .stream_lock_wait(wait)
        .build()
        .await
        .expect("the store builds")
}

async fn append(
    cqrs: &Cqrs<PostgresEventStore>,
    urn: &LedgerUrn,
    amount: f64,
) -> replay::Result<()> {
    cqrs.execute::<Ledger>(
        urn,
        replay::Metadata::default(),
        LedgerCommand::Add { amount },
        &(),
        None,
    )
    .await
    .map(|_| ())
}

/// A stream with one event in it, so there is a row to lock, and the id that row
/// carries — read back from the table rather than derived, so the blocker holds
/// exactly what the store writes.
async fn seeded_stream(pool: &PgPool, name: &str) -> (LedgerUrn, String) {
    let urn = LedgerUrn::new(name).expect("a valid urn");
    let cqrs = Cqrs::new(store(pool, LOCK_WAIT).await);
    append(&cqrs, &urn, 1.0)
        .await
        .expect("the first append succeeds");
    let stream_id = sqlx::query_scalar::<_, String>("SELECT id FROM streams")
        .fetch_one(pool)
        .await
        .expect("the append created exactly one stream");
    (urn, stream_id)
}

/// Whether the pool still serves work while the blocker holds the row — the
/// question the whole ticket is about.
async fn pool_still_serves(pool: &PgPool) -> bool {
    sqlx::query_scalar::<_, i32>("SELECT 1")
        .fetch_one(pool)
        .await
        .is_ok()
}

// ── Tests ────────────────────────────────────────────────────────────────────

/// The bound fires, the error is one the runner already retries, and — the point
/// — the connection is back in the pool while the blocker is still holding.
#[tokio::test]
async fn an_append_blocked_on_the_stream_row_fails_and_frees_its_connection_postgres_test() {
    let (pool, url, _container) = start_postgres().await;
    let (urn, stream_id) = seeded_stream(&pool, "blocked-append").await;
    let blocker = Blocker::holding(&url, &stream_id).await;

    let cqrs = Cqrs::new(store(&pool, LOCK_WAIT).await);
    // Bounded here too: without the fix the append waits on the server for as long
    // as the blocker holds, and a regression must fail this test rather than hang it.
    let failure = tokio::time::timeout(WAITED_ENOUGH, append(&cqrs, &urn, 5.0))
        .await
        .expect("the server abandoned the wait rather than serving it")
        .expect_err("an append onto a held row cannot succeed");

    assert_eq!(
        failure.kind(),
        ErrorKind::Unavailable,
        "a lock wait that ran out says nothing about versions, so it is not a conflict: {failure}"
    );
    assert!(
        failure.is_temporary(),
        "the runner's existing retry path only applies to a temporary failure: {failure}"
    );
    let context: Vec<_> = failure
        .context()
        .iter()
        .map(|(k, v)| (*k, v.clone()))
        .collect();
    assert!(
        context
            .iter()
            .any(|(key, value)| *key == "stream_id" && *value == stream_id),
        "the failure names the stream somebody else is holding: {context:?}"
    );
    assert!(
        context.iter().any(|(key, _)| *key == "stream_lock_wait_ms"),
        "the failure names the limit it exceeded: {context:?}"
    );
    assert!(
        pool_still_serves(&pool).await,
        "the failed append's connection is back in the pool while the blocker still holds the row"
    );

    blocker.release().await;
    append(&cqrs, &urn, 5.0)
        .await
        .expect("the append succeeds once the row is free");
}

/// The same row, the same bound, the other transaction that takes it.
#[tokio::test]
async fn a_compaction_blocked_on_the_stream_row_fails_and_frees_its_connection_postgres_test() {
    let (pool, url, _container) = start_postgres().await;
    let (urn, stream_id) = seeded_stream(&pool, "blocked-compaction").await;
    let blocker = Blocker::holding(&url, &stream_id).await;

    let cqrs = Cqrs::new(store(&pool, LOCK_WAIT).await);
    let aggregate = cqrs
        .fetch_aggregate::<Ledger>(&urn)
        .await
        .expect("the aggregate loads before the row is held");

    let failure = tokio::time::timeout(
        WAITED_ENOUGH,
        cqrs.compact(&aggregate, replay::Metadata::default()),
    )
    .await
    .expect("the server abandoned the wait rather than serving it")
    .expect_err("a compaction of a held row cannot proceed");

    assert_eq!(failure.kind(), ErrorKind::Unavailable, "{failure}");
    assert_eq!(failure.operation(), "compact", "{failure}");
    assert!(
        pool_still_serves(&pool).await,
        "the failed compaction's connection is back in the pool"
    );

    blocker.release().await;
}

/// `SET LOCAL`, so the bound dies with its transaction: a pooled connection must
/// not carry one store's `lock_timeout` into whatever uses it next.
#[tokio::test]
async fn a_bounded_append_leaves_no_lock_timeout_behind_postgres_test() {
    let (pool, _url, _container) = start_postgres().await;
    let (urn, _stream_id) = seeded_stream(&pool, "no-residue").await;

    let cqrs = Cqrs::new(store(&pool, LOCK_WAIT).await);
    append(&cqrs, &urn, 2.0)
        .await
        .expect("an unblocked append succeeds");

    // One connection in the pool, so this is necessarily the connection the append
    // ran on.
    let residue: String = sqlx::query_scalar("SHOW lock_timeout")
        .fetch_one(&pool)
        .await
        .expect("reading the setting must succeed");
    assert_eq!(
        residue, "0",
        "the connection went back to the pool carrying a lock_timeout"
    );
}

/// The documented opt-out: zero is passed to `lock_timeout`, where it already
/// means "wait forever", and the append waits for the blocker exactly as it did
/// before this bound existed.
#[tokio::test]
async fn a_zero_wait_restores_the_unbounded_behaviour_postgres_test() {
    let (pool, url, _container) = start_postgres().await;
    let (urn, stream_id) = seeded_stream(&pool, "opted-out").await;
    let blocker = Blocker::holding(&url, &stream_id).await;

    let cqrs = Cqrs::new(store(&pool, Duration::ZERO).await);
    let appending = tokio::spawn({
        let urn = urn.clone();
        async move { append(&cqrs, &urn, 7.0).await }
    });

    tokio::time::sleep(WAITED_ENOUGH).await;
    assert!(
        !appending.is_finished(),
        "a zero wait must not bound anything: the append gave up while the row was held"
    );

    blocker.release().await;
    appending
        .await
        .expect("the appending task must not panic")
        .expect("the append completes once the row is free");
}
