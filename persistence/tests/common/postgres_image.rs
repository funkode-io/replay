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

use testcontainers_modules::postgres;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, ContainerRequest, ImageExt};
use tokio::sync::Semaphore;

include!("../../src/infrastructure/postgres_tag.rs");

/// The port the server listens on inside the container.
pub const POSTGRES_PORT: u16 = 5432;

/// A container request for the suite's PostgreSQL server.
pub fn postgres_container() -> ContainerRequest<postgres::Postgres> {
    postgres::Postgres::default().with_tag(POSTGRES_TAG)
}

/// Bounds how many servers start at once, at the Docker VM's CPU count.
///
/// The suite is one binary since #249, so the harness runs one test per host core (10)
/// and each test starts its own server on a 4-CPU colima VM. Ungated, three tests failed
/// with `PoolTimedOut` waiting on a server that was up but starved.
static STARTS: Semaphore = Semaphore::const_new(4);

/// Starts the suite's PostgreSQL server, waiting its turn to do so.
pub async fn start_postgres_server() -> ContainerAsync<postgres::Postgres> {
    let _permit = STARTS
        .acquire()
        .await
        .expect("the start gate is never closed");
    postgres_container()
        .start()
        .await
        .expect("failed to start the postgres container")
}
