//! What migration 0034 does to a Policy that was already running
//! (funkode-io/replay#195).
//!
//! Every other test in this suite starts from the migrated schema, where the only way to
//! be behind is to have a place that says so. This one stages the schema as it stood
//! before — one `policy_cursors.position` per Policy, over a global order — and runs the
//! migration against it, because the backfill is a one-shot statement whose mistakes are
//! not recoverable: a place set too high skips events for good, and one set too low
//! redelivers what an operator was told had been processed.
//!
//! Why per stream at all: docs/adr/0026-a-policy-tracks-its-position-per-stream.md.

use std::collections::BTreeMap;

use sqlx::{postgres::PgPoolOptions, PgPool, Row};

use replay_macros::define_aggregate;
use replay_persistence::{Cqrs, PostgresEventStore};

use crate::common;
use common::migrations::{through as migrations_through, MIGRATOR};
use common::postgres_image::start_postgres_server;

const POSTGRES_PORT: u16 = 5432;

/// The migration that moves a Policy's position per stream, and the one before it.
const BEFORE_PER_STREAM: i64 = 33;

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
        Ok(match command {
            LedgerCommand::Add { amount } => vec![LedgerEvent::Added { amount }],
        })
    }
}

async fn start_pool() -> (PgPool, common::postgres_image::PostgresServer) {
    let container = start_postgres_server().await;
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("the container must publish its port");
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&format!(
            "postgres://postgres:postgres@127.0.0.1:{port}/postgres"
        ))
        .await
        .expect("the pool must connect");

    (pool, container)
}

async fn append(cqrs: &Cqrs<PostgresEventStore>, stream: &LedgerUrn, amount: f64) {
    cqrs.execute::<Ledger>(
        stream,
        replay::Metadata::default(),
        LedgerCommand::Add { amount },
        &(),
        None,
    )
    .await
    .expect("the append must succeed");
}

/// The places the migration left, by stream, for one policy.
async fn places(pool: &PgPool, policy: &str) -> BTreeMap<String, i64> {
    sqlx::query(
        "SELECT stream_id, stream_seq FROM policy_stream_cursors WHERE policy = $1 \
         ORDER BY stream_id",
    )
    .bind(policy)
    .fetch_all(pool)
    .await
    .expect("the places must be readable")
    .into_iter()
    .map(|row| (row.get("stream_id"), row.get("stream_seq")))
    .collect()
}

/// A Policy stopped at a global position carries over to the place each stream had
/// reached **by that position** — not to each stream's head, and not to nothing.
///
/// The interleaving is the point: with two streams written alternately, a cursor in the
/// middle of the log is a different place in each of them, and only the join the
/// migration does can tell what those places were.
#[tokio::test]
async fn a_running_policy_carries_over_to_the_place_each_stream_had_reached_postgres_test() {
    let (pool, _container) = start_pool().await;
    migrations_through(BEFORE_PER_STREAM)
        .run(&pool)
        .await
        .expect("migrations up to the per-stream cursor must succeed");

    let cqrs = Cqrs::new(PostgresEventStore::new(pool.clone()));
    let left = LedgerUrn::new("left").unwrap();
    let right = LedgerUrn::new("right").unwrap();

    // Positions 1..6, alternating: left 1,3,5 and right 2,4,6.
    for _ in 0..3 {
        append(&cqrs, &left, 1.0).await;
        append(&cqrs, &right, 1.0).await;
    }

    // Three policies as the old schema recorded them: one part-way, one that has
    // processed the whole log, one that has processed nothing.
    for (policy, position) in [("midway", 4_i64), ("caught_up", 6), ("untouched", 0)] {
        sqlx::query(
            "INSERT INTO policy_cursors (name, position, updated_at) VALUES ($1, $2, now())",
        )
        .bind(policy)
        .bind(position)
        .execute(&pool)
        .await
        .expect("the old cursor row must insert");
    }

    MIGRATOR
        .run(&pool)
        .await
        .expect("the rest of the migrations must succeed");

    assert_eq!(
        places(&pool, "midway").await,
        BTreeMap::from([
            ("urn:ledger:left".to_string(), 2),
            ("urn:ledger:right".to_string(), 2),
        ]),
        "position 4 is the second event of each stream, which is where each place lands"
    );
    assert_eq!(
        places(&pool, "caught_up").await,
        BTreeMap::from([
            ("urn:ledger:left".to_string(), 3),
            ("urn:ledger:right".to_string(), 3),
        ]),
        "a policy that had read the whole log is at every stream's head, owed nothing"
    );
    assert!(
        places(&pool, "untouched").await.is_empty(),
        "a policy at 0 had processed nothing, and a stream with no place starts at its \
         first event — so seeding nothing is what carries it over unchanged"
    );

    // The renamed column keeps the number: discovery resumes where the old cursor was
    // rather than re-reading the log from the start.
    let swept: BTreeMap<String, i64> =
        sqlx::query("SELECT name, discovered_through FROM policy_cursors ORDER BY name")
            .fetch_all(&pool)
            .await
            .expect("the cursors must be readable")
            .into_iter()
            .map(|row| (row.get("name"), row.get("discovered_through")))
            .collect();

    assert_eq!(
        swept,
        BTreeMap::from([
            ("caught_up".to_string(), 6),
            ("midway".to_string(), 4),
            ("untouched".to_string(), 0),
        ])
    );
}

/// A stream written entirely past a Policy's cursor gets no place, and a stream written
/// entirely before it gets its head.
///
/// The backfill's join is `global_position <= position`, so a stream the cursor never
/// reached contributes no row at all — which reads as "the beginning", which is where
/// that Policy was.
#[tokio::test]
async fn a_stream_the_cursor_never_reached_is_owed_whole_postgres_test() {
    let (pool, _container) = start_pool().await;
    migrations_through(BEFORE_PER_STREAM)
        .run(&pool)
        .await
        .expect("migrations up to the per-stream cursor must succeed");

    let cqrs = Cqrs::new(PostgresEventStore::new(pool.clone()));
    let early = LedgerUrn::new("early").unwrap();
    let late = LedgerUrn::new("late").unwrap();

    append(&cqrs, &early, 1.0).await;
    append(&cqrs, &early, 1.0).await;
    append(&cqrs, &late, 1.0).await;

    sqlx::query("INSERT INTO policy_cursors (name, position, updated_at) VALUES ($1, 2, now())")
        .bind("partway")
        .execute(&pool)
        .await
        .expect("the old cursor row must insert");

    MIGRATOR
        .run(&pool)
        .await
        .expect("the rest of the migrations must succeed");

    assert_eq!(
        places(&pool, "partway").await,
        BTreeMap::from([("urn:ledger:early".to_string(), 2)]),
        "the stream it had read is at its head; the one it had not is absent, and \
         absent is the beginning"
    );
}
