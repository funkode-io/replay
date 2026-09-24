//! The PostgreSQL server every Docker-gated integration test runs against.
//!
//! One place decides the server version, so "which Postgres is this suite
//! verified against?" has a single answer and moving it is a one-line change. That
//! place is `src/infrastructure/postgres_tag.rs`, beside the adapter it pins and
//! included below: `cursor_tests` in `src/policy_runner.rs` pins the same tag and
//! cannot import this module.
//!
//! `allow(dead_code)` module-wide: every test binary that says `mod common;` compiles
//! this module, including the ones that never start a container.
#![allow(dead_code)]

use std::ops::Deref;
use std::sync::{Arc, LazyLock};
use testcontainers_modules::postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ContainerRequest, ImageExt};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

include!("../../src/infrastructure/postgres_tag.rs");

/// The port the server listens on inside the container.
pub const POSTGRES_PORT: u16 = 5432;

/// A container request for the suite's PostgreSQL server.
pub fn postgres_container() -> ContainerRequest<postgres::Postgres> {
    postgres::Postgres::default().with_tag(POSTGRES_TAG)
}

/// Bounds how many servers are alive at once, at the Docker VM's CPU count.
///
/// The suite is one binary since #249, so the harness runs one test per host core (10)
/// and each test keeps its own server for the length of the test. Ungated, three tests
/// failed with `PoolTimedOut` against a server that was up but starved on a 4-CPU colima
/// VM. A permit is held for the container's whole life, not just its start: a server
/// already up still costs the CPU the next one needs.
static SERVERS: LazyLock<Arc<Semaphore>> = LazyLock::new(|| Arc::new(Semaphore::new(4)));

/// The suite's PostgreSQL server, and the permit that admitted it.
///
/// Derefs to the container, so it reads as one at the call sites; dropping it stops the
/// server and lets the next test start one.
pub struct PostgresServer {
    container: ContainerAsync<postgres::Postgres>,
    _permit: OwnedSemaphorePermit,
}

impl Deref for PostgresServer {
    type Target = ContainerAsync<postgres::Postgres>;

    fn deref(&self) -> &Self::Target {
        &self.container
    }
}

/// Starts the suite's PostgreSQL server, waiting its turn to do so.
pub async fn start_postgres_server() -> PostgresServer {
    let permit = Arc::clone(&SERVERS)
        .acquire_owned()
        .await
        .expect("the server gate is never closed");
    let container = postgres_container()
        .start()
        .await
        .expect("failed to start the postgres container");
    PostgresServer {
        container,
        _permit: permit,
    }
}
