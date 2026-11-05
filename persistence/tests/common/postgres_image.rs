//! The PostgreSQL server every Docker-gated integration test runs against.
//!
//! One place decides the server version, so "which Postgres is this suite
//! verified against?" has a single answer and moving it is a one-line change. That
//! place is `src/infrastructure/postgres_tag.rs`, beside the adapter it pins and
//! included below: `cursor_tests` in `src/policy_runner.rs` pins the same tag and
//! cannot import this module.
//!
//! Most tests take a [`postgres_database`]: a database of their own on one server the
//! whole test binary shares (#287). Starting a container per test was most of the
//! suite's wall time — 235 tests waited on Docker to start ~67 servers, four at a time.
//! A test that needs the *server* to itself takes [`start_postgres_server`] instead.
//!
//! `allow(dead_code)` module-wide: every test binary that says `mod common;` compiles
//! this module, including the ones that never start a container.
#![allow(dead_code)]

use std::sync::mpsc;
use std::sync::{Arc, LazyLock, Mutex};
use std::time::Duration;

use sqlx::{Connection, PgConnection};
use testcontainers_modules::postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ContainerRequest, ImageExt};
use tokio::sync::{OnceCell, OwnedSemaphorePermit, Semaphore};
use uuid::Uuid;

include!("../../src/infrastructure/postgres_tag.rs");

/// The port the server listens on inside the container.
pub const POSTGRES_PORT: u16 = 5432;

/// Connections the shared server accepts.
///
/// The harness runs one test per host core, and the widest pool a test opens is 50
/// (`connect_to_postgres` in `integration_tests.rs`): ten cores of those is 500. The
/// stock 100 left nothing for a second wide test.
const SHARED_MAX_CONNECTIONS: &str = "max_connections=600";

/// How long the exit hook waits for the shared server's container to be removed.
const SHARED_REMOVAL_TIMEOUT: Duration = Duration::from_secs(30);

/// A container request for the suite's PostgreSQL server.
pub fn postgres_container() -> ContainerRequest<postgres::Postgres> {
    postgres::Postgres::default().with_tag(POSTGRES_TAG)
}

/// Bounds how many *dedicated* servers are alive at once, at the Docker VM's CPU count.
///
/// The suite is one binary since #249, so the harness runs one test per host core (10).
/// Ungated, three tests that each started a server failed with `PoolTimedOut` against a
/// server that was up but starved on a 4-CPU colima VM. A permit is held for the
/// container's whole life, not just its start: a server already up still costs the CPU
/// the next one needs. Tests on the shared server take no permit.
static SERVERS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(4)));

/// A database a test owns, and how to reach it.
///
/// On the shared server it is a fresh database, dropped when this is; on a dedicated
/// server it is that server's `postgres` database, and dropping this stops the server.
pub struct TestDatabase {
    host: String,
    port: u16,
    database: String,
    owner: Owner,
}

enum Owner {
    /// The binary's shared server: dropping drops the database.
    Shared,
    /// A server of the test's own, and the permit that admitted it.
    Dedicated {
        _container: Box<ContainerAsync<postgres::Postgres>>,
        _permit: OwnedSemaphorePermit,
    },
}

impl TestDatabase {
    /// The host the server is reachable on.
    pub fn host(&self) -> &str {
        &self.host
    }

    /// The port the server is reachable on, from the host.
    pub fn port(&self) -> u16 {
        self.port
    }

    /// The name of the test's database.
    pub fn database(&self) -> &str {
        &self.database
    }

    /// The connection URL of the test's database.
    pub fn url(&self) -> String {
        url(&self.host, self.port, &self.database)
    }
}

impl Drop for TestDatabase {
    /// Drops a shared-server database, best effort: the container goes with the binary.
    ///
    /// `WITH (FORCE)` because a test's pool usually outlives this value — a tuple binding
    /// `(pool, _db)` drops `_db` first. On its own thread with its own runtime, as `Drop`
    /// cannot await and the test's runtime may be the one being torn down.
    fn drop(&mut self) {
        if !matches!(self.owner, Owner::Shared) {
            return;
        }
        let admin = url(&self.host, self.port, "postgres");
        let statement = format!("DROP DATABASE IF EXISTS \"{}\" WITH (FORCE)", self.database);
        let dropped = std::thread::spawn(move || {
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("a runtime to drop the test database on");
            runtime.block_on(async {
                let mut connection = PgConnection::connect(&admin).await?;
                sqlx::query(sqlx::AssertSqlSafe(statement))
                    .execute(&mut connection)
                    .await?;
                connection.close().await
            })
        })
        .join();
        if let Ok(Err(error)) = dropped {
            eprintln!("dropping test database {}: {error}", self.database);
        }
    }
}

fn url(host: &str, port: u16, database: &str) -> String {
    format!("postgres://postgres:postgres@{host}:{port}/{database}")
}

/// A fresh database on the binary's shared server, which starts on first use.
///
/// Isolates what lives in a database: tables, sequences, statistics, advisory locks,
/// `LISTEN`/`NOTIFY`. It does not isolate what is server-wide — transaction ids and
/// snapshots, `pg_stat_activity`, connection slots, server settings. A test that
/// depends on one of those takes [`start_postgres_server`].
pub async fn postgres_database() -> TestDatabase {
    let (host, port) = shared_server().await;
    let database = format!("test_{}", Uuid::new_v4().simple());
    let mut admin = PgConnection::connect(&url(host, *port, "postgres"))
        .await
        .expect("failed to connect to the shared postgres server");
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "CREATE DATABASE \"{database}\""
    )))
    .execute(&mut admin)
    .await
    .expect("failed to create the test database");
    admin.close().await.ok();
    TestDatabase {
        host: host.clone(),
        port: *port,
        database,
        owner: Owner::Shared,
    }
}

/// Starts a PostgreSQL server of the test's own, waiting its turn to do so.
///
/// For tests that depend on server-wide state; see [`postgres_database`]. The database
/// is the server's `postgres`.
pub async fn start_postgres_server() -> TestDatabase {
    let permit = Arc::clone(&SERVERS)
        .acquire_owned()
        .await
        .expect("the server gate is never closed");
    let container = postgres_container()
        .start()
        .await
        .expect("failed to start the postgres container");
    let host = container
        .get_host()
        .await
        .expect("failed to read the container host")
        .to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("failed to read the container port");
    TestDatabase {
        host,
        port,
        database: "postgres".to_owned(),
        owner: Owner::Dedicated {
            _container: Box::new(container),
            _permit: permit,
        },
    }
}

/// The shared server's address, starting it on first call.
static SHARED: OnceCell<(String, u16)> = OnceCell::const_new();

/// Tells the shared server's owner thread to remove the container; set once it is up.
static SHARED_STOP: Mutex<Option<(tokio::sync::oneshot::Sender<()>, mpsc::Receiver<()>)>> =
    Mutex::new(None);

async fn shared_server() -> &'static (String, u16) {
    SHARED.get_or_init(start_shared_server).await
}

/// Starts the shared server on a thread that owns it for the life of the binary.
///
/// Each `#[tokio::test]` has a runtime of its own that ends with the test, so the
/// container — and the Docker client it holds — cannot live on the runtime of whichever
/// test happened to start it. The owner thread's runtime outlives every test.
///
/// Nothing drops a static, and testcontainers-rs has no reaper, so the container is
/// removed by an `atexit` hook. A binary that is killed or aborts leaks the container;
/// `docker ps --filter ancestor=postgres:15-alpine` finds it.
async fn start_shared_server() -> (String, u16) {
    let (ready_tx, ready_rx) = tokio::sync::oneshot::channel();
    let (stop_tx, stop_rx) = tokio::sync::oneshot::channel::<()>();
    let (removed_tx, removed_rx) = mpsc::channel();
    std::thread::Builder::new()
        .name("shared-postgres".into())
        .spawn(move || {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(1)
                .enable_all()
                .build()
                .expect("a runtime for the shared postgres server");
            runtime.block_on(async move {
                let container = postgres_container()
                    .with_cmd(["-c", "fsync=off", "-c", SHARED_MAX_CONNECTIONS])
                    .start()
                    .await
                    .expect("failed to start the shared postgres container");
                let host = container
                    .get_host()
                    .await
                    .expect("failed to read the container host")
                    .to_string();
                let port = container
                    .get_host_port_ipv4(POSTGRES_PORT)
                    .await
                    .expect("failed to read the container port");
                let _ = ready_tx.send((host, port));
                // Resolves on the exit hook's signal, or never if the hook is not reached.
                let _ = stop_rx.await;
                let _ = container.rm().await;
            });
            let _ = removed_tx.send(());
        })
        .expect("failed to spawn the shared postgres owner thread");

    let address = ready_rx
        .await
        .expect("the shared postgres server failed to start");
    *SHARED_STOP.lock().unwrap_or_else(|e| e.into_inner()) = Some((stop_tx, removed_rx));
    register_exit_hook();
    address
}

extern "C" {
    fn atexit(callback: extern "C" fn()) -> std::ffi::c_int;
}

fn register_exit_hook() {
    // SAFETY: `atexit` takes a plain function pointer with no captured state; the
    // callback below only touches statics.
    let registered = unsafe { atexit(remove_shared_server) };
    if registered != 0 {
        eprintln!("could not register the shared postgres cleanup; the container will leak");
    }
}

/// Runs at process exit: libtest returns from `main`, or calls `process::exit` on a
/// failure; both reach the C runtime's exit handlers.
extern "C" fn remove_shared_server() {
    let stop = SHARED_STOP.lock().unwrap_or_else(|e| e.into_inner()).take();
    if let Some((stop_tx, removed_rx)) = stop {
        if stop_tx.send(()).is_ok() {
            let _ = removed_rx.recv_timeout(SHARED_REMOVAL_TIMEOUT);
        }
    }
}
