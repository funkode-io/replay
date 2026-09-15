//! The PostgreSQL server every Docker-gated integration test runs against.
//!
//! One place decides the server version, so "which Postgres is this suite
//! verified against?" has a single answer and moving it is a one-line change.
//!
//! The pinned tag is not a free choice: the crate's documented floor is
//! PostgreSQL 13 (see the README), and the suite is verified on a release that
//! is still receiving upstream fixes. Testing on an end-of-life server would
//! mean the suite passes on a version no deployment should be running while
//! saying nothing about the versions they are.
//!
//! `allow(dead_code)` module-wide: every test binary that says `mod common;` compiles
//! this module, including the ones that never start a container.
#![allow(dead_code)]

use testcontainers_modules::postgres;
use testcontainers_modules::testcontainers::{ContainerRequest, ImageExt};

/// Image tag pinning the PostgreSQL release the suite runs against.
pub const POSTGRES_TAG: &str = "17-alpine";

/// The port the server listens on inside the container.
pub const POSTGRES_PORT: u16 = 5432;

/// A container request for the suite's PostgreSQL server.
pub fn postgres_container() -> ContainerRequest<postgres::Postgres> {
    postgres::Postgres::default().with_tag(POSTGRES_TAG)
}
