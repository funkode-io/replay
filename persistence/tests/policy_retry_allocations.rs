//! What a retry holds is a page of a reaction's rows, not the reaction's rows.
//!
//! A retry replays one reaction and settles every row that reaction parked
//! (ADR-0021). After funkode-io/replay#220 a parked command is one row, so that
//! group is one row per command the *current* code dispatches — plus the rows an
//! older version of the policy parked, which nothing bounds by a number
//! (funkode-io/replay#228). Reading the group with `fetch_all` therefore put a
//! table's worth of strings on the heap for a reaction an upgrade had inherited.
//!
//! This test pins the fixed behaviour through the counting allocator: the bytes
//! held at once while a reaction is settled must scale with the page the
//! settlement reads, not with the number of rows the reaction has.

mod common;

use common::alloc::{peak_live_bytes, reset_peak, CountingAllocator};
use common::policy_harness::{Probe, ProbeCommand, ProbeUrn};
use common::postgres_image::{postgres_container, POSTGRES_PORT};
use common::report::report;

use replay_persistence::{
    Cqrs, Dispatch, ObservedEvent, PolicyRunner, PolicySettings, PostgresEventStore, StartAt,
};
use sqlx::{postgres::PgPoolOptions, PgPool, Row};
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};

/// Every test binary registers its own global allocator; the counting itself is
/// shared (`tests/common/alloc.rs`).
#[global_allocator]
static ALLOCATOR: CountingAllocator = CountingAllocator;

/// Rows the reaction has parked, all for commands the registered policy no
/// longer emits. Well past the settlement's page, so holding the group and
/// holding a page differ by an order of magnitude rather than by slack.
const RETIRED_ROWS: usize = 1_000;

/// How long an instance URN each row names. The strings are what a group costs,
/// so they are what the measurement is made of.
const TARGET_BYTES: usize = 1_024;

/// A Policy that dispatches nothing: the rows in the table are all an upgrade's
/// residue, which is the shape funkode-io/replay#228 is about. The replay itself
/// is then free, so what the measurement sees is the settlement's own memory.
struct QuietPolicy {
    name: String,
}

impl replay_persistence::Policy for QuietPolicy {
    type Event = common::policy_harness::ProbeEvent;

    fn name(&self) -> &str {
        &self.name
    }

    fn react(&self, _event: &ObservedEvent<Self::Event>) -> Vec<Dispatch> {
        vec![]
    }
}

/// Peak live bytes while a reaction is settled must scale with the page the
/// settlement reads, not with the rows the reaction has parked.
#[tokio::test]
async fn peak_live_bytes_scale_with_the_page_not_the_reaction_postgres_test() {
    let (pool, _container) = start_postgres().await;

    let policy_name = "retry_allocations";
    let cqrs = Cqrs::new(PostgresEventStore::new(pool.clone()));
    let stream = ProbeUrn::new("subject-1").expect("stream name must be a valid URN NSS");
    cqrs.execute::<Probe>(
        &stream,
        replay::Metadata::default(),
        ProbeCommand::Ping {
            tag: "retired".to_string(),
        },
        &(),
        None,
    )
    .await
    .expect("the triggering event must be appended");

    let event = sqlx::query("SELECT id, global_position FROM events ORDER BY global_position DESC")
        .fetch_one(&pool)
        .await
        .expect("the triggering event must be readable");
    let event_id: uuid::Uuid = event.get("id");
    let global_position: i64 = event.get("global_position");

    park_retired_commands(&pool, policy_name, global_position, event_id).await;

    let runner = PolicyRunner::builder(cqrs)
        .register_services::<Probe>(())
        .register_policy(
            QuietPolicy {
                name: policy_name.to_string(),
            },
            PolicySettings::new().starting_at(StartAt::Beginning),
        )
        .build();

    reset_peak();

    let summary = runner
        .retry_policy_dead_letters(policy_name)
        .await
        .expect("a bulk retry must return a summary rather than fail");

    let peak = peak_live_bytes();

    assert_eq!(
        summary.reactions_resolved, 1,
        "the reaction's every row names a command the policy no longer emits, \
         so the reaction resolves"
    );

    // Each row costs its URN twice over while it is in hand — once in the
    // connection's row buffer, once in the `ParkedRow` read out of it — plus the
    // error strings and sqlx's per-row work. Budget six URNs per row of a page,
    // which still leaves the whole group an order of magnitude away. Measured on
    // this fixture: 0.19 MB paged, 1.55 MB with `fetch_all`, against a 0.6 MB
    // budget.
    let bounded = (RETRY_SETTLE_PAGE * TARGET_BYTES * 6) as isize;
    let whole_group = (RETIRED_ROWS * TARGET_BYTES) as isize;

    report(
        "policy_retry",
        "peak_live_bytes",
        peak as i128,
        bounded as i128,
        "bytes",
    );

    assert!(
        peak < bounded,
        "peak live bytes {peak} must stay near one page of the group (budget \
         {bounded}); holding the group would cost at least {whole_group}"
    );
}

/// The page the settlement reads, as `policy_runner.rs` sets it. Mirrored rather
/// than exported: what this test asserts is that *a* page bounds the read, and a
/// constant that has to be made public to be tested is a constant the crate
/// would then have to keep.
const RETRY_SETTLE_PAGE: usize = 100;

/// Park rows for commands the registered policy cannot dispatch, all for one
/// reaction: the tail an upgrade inherits. One statement, because a thousand
/// round trips is a slow way to write a fixture.
async fn park_retired_commands(
    pool: &PgPool,
    policy_name: &str,
    global_position: i64,
    event_id: uuid::Uuid,
) {
    sqlx::query(
        "INSERT INTO policy_dead_letters \
         (policy_name, global_position, event_id, error_kind, error_message, \
          aggregate_name, target_stream_id, command_name, dispatch_ordinal, \
          last_parked_at) \
         SELECT $1, $2, $3, 'Unavailable', 'parked by a version that is gone', \
                'Probe', 'urn:probe:' || lpad(n::text, $5, 'x'), \
                'RetiredCommand', n, now() \
         FROM generate_series(1, $4) AS n",
    )
    .bind(policy_name)
    .bind(global_position)
    .bind(event_id)
    .bind(RETIRED_ROWS as i32)
    .bind(TARGET_BYTES as i32)
    .execute(pool)
    .await
    .expect("the retired commands must be parked");
}

/// A database with the crate's schema, and the container that owns it.
async fn start_postgres() -> (
    PgPool,
    testcontainers_modules::testcontainers::ContainerAsync<postgres::Postgres>,
) {
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

    let pool = PgPoolOptions::new()
        .max_connections(4)
        .connect(&format!(
            "postgres://postgres:postgres@{host}:{port}/postgres"
        ))
        .await
        .expect("failed to create the postgres pool");

    sqlx::migrate!("./tests/migrations")
        .run(&pool)
        .await
        .expect("failed to run migrations");

    (pool, container)
}
