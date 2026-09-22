//! The PostgreSQL server every Docker-gated integration test runs against.
//!
//! One place decides the server version, so "which Postgres is this suite
//! verified against?" has a single answer and moving it is a one-line change. That
//! place is `persistence/postgres_tag.rs`, included below: `cursor_tests` in
//! `src/policy_runner.rs` pins the same tag and cannot import this module.
//!
//! `allow(dead_code)` module-wide: every test binary that says `mod common;` compiles
//! this module, including the ones that never start a container.
#![allow(dead_code)]

use testcontainers_modules::postgres;
use testcontainers_modules::testcontainers::{ContainerRequest, ImageExt};

include!("../../postgres_tag.rs");

/// The port the server listens on inside the container.
pub const POSTGRES_PORT: u16 = 5432;

/// A container request for the suite's PostgreSQL server.
pub fn postgres_container() -> ContainerRequest<postgres::Postgres> {
    postgres::Postgres::default().with_tag(POSTGRES_TAG)
}
