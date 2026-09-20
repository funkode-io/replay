//! The PostgreSQL server every Docker-gated integration test runs against.
//!
//! One place decides the server version, so "which Postgres is this suite
//! verified against?" has a single answer and moving it is a one-line change.
//!
//! The pinned tag is the crate's documented floor (README "Requirements"), not the
//! newest release: the floor is what the crate promises, so the floor is what the suite
//! verifies. A run on a newer server cannot fail on a feature the floor lacks — which is
//! exactly how five tests came to run on `postgres:11-alpine` unnoticed until
//! funkode-io/replay#193 needed a type it does not have.
//!
//! PostgreSQL 13 and 14 are both out of upstream support. The pin states what the crate
//! promises, not what a deployment should be running; raising the pin is raising the
//! floor, and belongs in the README in the same change.
//!
//! `allow(dead_code)` module-wide: every test binary that says `mod common;` compiles
//! this module, including the ones that never start a container.
#![allow(dead_code)]

use testcontainers_modules::postgres;
use testcontainers_modules::testcontainers::{ContainerRequest, ImageExt};

/// Image tag pinning the PostgreSQL release the suite runs against.
pub const POSTGRES_TAG: &str = "15-alpine";

/// The port the server listens on inside the container.
pub const POSTGRES_PORT: u16 = 5432;

/// A container request for the suite's PostgreSQL server.
pub fn postgres_container() -> ContainerRequest<postgres::Postgres> {
    postgres::Postgres::default().with_tag(POSTGRES_TAG)
}
