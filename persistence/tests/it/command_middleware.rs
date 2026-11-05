//! Command middleware against Postgres and a real Policy runner (ADR-0035).
//!
//! The in-memory unit tests in `src/command_middleware.rs` cover the chain itself. What
//! only the runner can show is that a command it dispatches reaches the middleware with
//! the Causation it stamped, while an application's command reaches it with none. What
//! only Postgres can show is that the commit gate rolls back a real transaction, Inline
//! projection writes included, and that middleware leaves nothing in the projection
//! registry.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::BoxFuture;
use replay::{ErrorKind, EventStream};
use replay_macros::define_aggregate;
use sqlx::{postgres::PgPoolOptions, PgPool};

use crate::common;

use common::policy_harness::{Probe, ProbeCommand, ProbeEvent, ProbeUrn, OBSERVE_TIMEOUT};
use common::postgres_image::{start_postgres_server, PostgresServer, POSTGRES_PORT};
use replay::Metadata;
use replay_persistence::{
    Causation, CommandContext, CommandContextMut, CommandMiddleware, Cqrs, Dispatch, EventStore,
    InlineProjection, Next, PersistedEvent, PolicyRunner, PolicySettings, PostgresEventStore,
    StartAt,
};

/// The stream and Causation of every command it sees, in the order it saw them.
#[derive(Clone, Default)]
struct RecordsCausation(Arc<Mutex<Vec<Seen>>>);

/// A command's stream id and the Causation it arrived with.
type Seen = (String, Option<Causation>);

impl CommandMiddleware for RecordsCausation {
    fn before<'a>(&'a self, context: CommandContextMut<'a>) -> BoxFuture<'a, Next> {
        Box::pin(async move {
            self.0.lock().unwrap().push((
                context.stream_id().to_string(),
                context.causation().cloned(),
            ));
            Next::Continue
        })
    }
}

/// Tries to erase and to overwrite the causation stamp through both metadata APIs,
/// ignores the refusals, and lets the command go on.
struct TampersWithCausation;

impl CommandMiddleware for TampersWithCausation {
    fn before<'a>(&'a self, mut context: CommandContextMut<'a>) -> BoxFuture<'a, Next> {
        Box::pin(async move {
            if context.causation().is_some() {
                assert!(context.set_metadata(Metadata::default()).is_err());
                assert!(context
                    .insert_metadata("causation", serde_json::json!({ "depth": 0 }))
                    .is_err());
            }
            Next::Continue
        })
    }
}

#[tokio::test]
async fn a_policy_command_shows_its_causation_and_an_application_command_none_postgres_test() {
    let container = start_postgres_server().await;
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
        .max_connections(5)
        .acquire_timeout(OBSERVE_TIMEOUT)
        .idle_timeout(Duration::from_secs(5))
        .connect(&format!(
            "postgres://postgres:postgres@{host}:{port}/postgres"
        ))
        .await
        .expect("failed to create the postgres pool");
    common::migrations::MIGRATOR
        .run(&pool)
        .await
        .expect("migrations must succeed");

    let seen = RecordsCausation::default();
    let cqrs = Cqrs::new(PostgresEventStore::new(pool.clone()))
        .with_middleware(seen.clone())
        .with_middleware(TampersWithCausation);
    let policy = "middleware_sees_causation";
    let runner = PolicyRunner::builder(cqrs.clone())
        .register_services::<Probe>(())
        .register_policy_fn::<ProbeEvent, _>(
            policy,
            PolicySettings::new().starting_at(StartAt::Beginning),
            |event| match &event.data {
                ProbeEvent::Pinged { tag } => vec![Dispatch::to::<Probe>(
                    ProbeUrn::new(format!("{tag}-echo")).expect("a valid NSS"),
                    ProbeCommand::Echo { tag: tag.clone() },
                )],
                ProbeEvent::Echoed { .. } => vec![],
            },
        )
        .build();

    let pinged = ProbeUrn::new("hello").expect("a valid NSS");
    cqrs.execute::<Probe>(
        &pinged,
        Metadata::default(),
        ProbeCommand::Ping {
            tag: "hello".to_string(),
        },
        &(),
        None,
    )
    .await
    .expect("the application's command commits");
    let ping_position: i64 =
        sqlx::query_scalar("SELECT global_position FROM events WHERE stream_id = $1")
            .bind("urn:probe:hello")
            .fetch_one(&pool)
            .await
            .expect("the ping is in the log");

    assert_eq!(runner.drain().await.expect("a drain must succeed"), 1);

    let seen = seen.0.lock().unwrap().clone();
    assert_eq!(seen.len(), 2, "one application command, one Policy command");
    assert_eq!(seen[0], ("urn:probe:hello".to_string(), None));
    let (stream, causation) = &seen[1];
    assert_eq!(stream, "urn:probe:hello-echo");
    let causation = causation
        .as_ref()
        .expect("a Policy command carries causation");
    assert_eq!(causation.policy(), policy);
    assert_eq!(causation.stream_id(), "urn:probe:hello");
    assert_eq!(causation.global_position(), ping_position);
    assert_eq!(causation.depth(), 1);

    // The stamp the runner reads to bound a cascade survived the middleware that tried
    // to erase it.
    let stored: serde_json::Value =
        sqlx::query_scalar("SELECT metadata FROM events WHERE stream_id = $1")
            .bind("urn:probe:hello-echo")
            .fetch_one(&pool)
            .await
            .expect("the Policy's command appended an event");
    assert_eq!(stored["causation"]["policy"], serde_json::json!(policy));
    assert_eq!(stored["causation"]["depth"], serde_json::json!(1));
}

define_aggregate! {
    Tally {
        namespace: "tally",
        state: {
            count: u32,
        },
        commands: {
            Count { times: u32 },
        },
        events: {
            Counted { n: u32 },
        }
    }
}

impl replay::EventStream for Tally {
    type Event = TallyEvent;

    fn stream_type() -> String {
        "Tally".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        let TallyEvent::Counted { .. } = event;
        self.count += 1;
    }
}

impl replay::Aggregate for Tally {
    type Command = TallyCommand;
    type Error = replay::Error;
    type Services = ();

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        let TallyCommand::Count { times } = command;
        Ok((1..=times).map(|n| TallyEvent::Counted { n }).collect())
    }
}

/// What happened inside one append, in the order it happened: a projection's `handle`
/// call or a middleware's `before_commit`.
type Trace = Arc<Mutex<Vec<String>>>;

/// An inline projection writing one row per event to `tally_view`, tracing each call.
struct TallyView {
    name: &'static str,
    trace: Trace,
}

impl InlineProjection for TallyView {
    type Exec = sqlx::PgConnection;
    type Event = TallyEvent;

    fn name(&self) -> &str {
        self.name
    }

    fn version(&self) -> i32 {
        1
    }

    async fn init(&mut self, _conn: &mut Self::Exec) -> Result<(), replay::Error> {
        Ok(())
    }

    async fn handle(
        &mut self,
        conn: &mut Self::Exec,
        events: &[PersistedEvent<Self::Event>],
    ) -> Result<(), replay::Error> {
        for event in events {
            sqlx::query("INSERT INTO tally_view (projection, event_id) VALUES ($1, $2)")
                .bind(self.name)
                .bind(event.id)
                .execute(&mut *conn)
                .await
                .map_err(replay_persistence::db_error)?;
        }
        self.trace
            .lock()
            .unwrap()
            .push(format!("{} handled {}", self.name, events.len()));
        Ok(())
    }
}

/// Traces its `before_commit`, then gives the answer it was built with.
struct Gate {
    trace: Trace,
    answer: fn() -> Next,
}

impl CommandMiddleware for Gate {
    fn before_commit<'a>(&'a self, _: &'a CommandContext) -> BoxFuture<'a, Next> {
        Box::pin(async move {
            self.trace.lock().unwrap().push("before_commit".to_string());
            (self.answer)()
        })
    }
}

/// A fresh server, migrated, with the `tally_view` table the projections write to.
async fn tally_pool() -> (PgPool, PostgresServer) {
    let container = start_postgres_server().await;
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
        .max_connections(5)
        .connect(&format!(
            "postgres://postgres:postgres@{host}:{port}/postgres"
        ))
        .await
        .expect("failed to create the postgres pool");
    common::migrations::MIGRATOR
        .run(&pool)
        .await
        .expect("migrations must succeed");
    sqlx::query("CREATE TABLE tally_view (projection text NOT NULL, event_id uuid NOT NULL)")
        .execute(&pool)
        .await
        .expect("creating the view must succeed");
    (pool, container)
}

/// A store with two tally views, flushing every two events so one append reaches each
/// projection in several `handle` calls.
async fn store_with_views(pool: &PgPool, trace: &Trace) -> PostgresEventStore {
    PostgresEventStore::builder(pool.clone())
        .projection_flush_size(2)
        .register(TallyView {
            name: "tally_a",
            trace: trace.clone(),
        })
        .register(TallyView {
            name: "tally_b",
            trace: trace.clone(),
        })
        .build()
        .await
        .expect("building the store must succeed")
}

async fn count(pool: &PgPool, sql: &'static str) -> i64 {
    sqlx::query_scalar(sql)
        .fetch_one(pool)
        .await
        .expect("counting must succeed")
}

fn tally() -> TallyUrn {
    TallyUrn::new("one").expect("a valid NSS")
}

#[tokio::test]
async fn a_busy_before_commit_rolls_back_the_events_and_the_inline_projection_writes_postgres_test()
{
    let (pool, _container) = tally_pool().await;
    let trace = Trace::default();
    let cqrs = Cqrs::new(store_with_views(&pool, &trace).await).with_middleware(Gate {
        trace: trace.clone(),
        answer: || Next::Busy("maintenance in progress".to_string()),
    });

    let error = cqrs
        .execute::<Tally>(
            &tally(),
            Metadata::default(),
            TallyCommand::Count { times: 5 },
            &(),
            None,
        )
        .await
        .expect_err("a busy gate rolls the append back");

    assert_eq!(error.kind(), ErrorKind::Busy);
    assert_eq!(error.to_string(), "maintenance in progress");
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM events").await, 0);
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM tally_view").await, 0);
    assert_eq!(
        count(
            &pool,
            "SELECT COUNT(*) FROM projection_applied_transactions"
        )
        .await,
        0
    );
    // The projections did write, inside the transaction the gate rolled back.
    assert_eq!(trace.lock().unwrap().last().unwrap(), "before_commit");
    assert!(trace.lock().unwrap().len() > 1);
}

#[tokio::test]
async fn before_commit_runs_after_every_inline_projection_write_postgres_test() {
    let (pool, _container) = tally_pool().await;
    let trace = Trace::default();
    let cqrs = Cqrs::new(store_with_views(&pool, &trace).await).with_middleware(Gate {
        trace: trace.clone(),
        answer: || Next::Continue,
    });

    cqrs.execute::<Tally>(
        &tally(),
        Metadata::default(),
        TallyCommand::Count { times: 5 },
        &(),
        None,
    )
    .await
    .expect("the command commits");

    // Five events, flushed two at a time, to each of two projections; then the gate.
    let expected = [
        "tally_a handled 2",
        "tally_b handled 2",
        "tally_a handled 2",
        "tally_b handled 2",
        "tally_a handled 1",
        "tally_b handled 1",
        "before_commit",
    ];
    assert_eq!(*trace.lock().unwrap(), expected);
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM events").await, 5);
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM tally_view").await, 10);
}

/// Continues in both hooks; it only has to be present.
struct Passes;

impl CommandMiddleware for Passes {}

#[tokio::test]
async fn a_command_through_middleware_leaves_no_registry_row_and_no_applied_mark_for_it_postgres_test(
) {
    let (pool, _container) = tally_pool().await;
    let trace = Trace::default();
    let cqrs = Cqrs::new(store_with_views(&pool, &trace).await).with_middleware(Passes);

    cqrs.execute::<Tally>(
        &tally(),
        Metadata::default(),
        TallyCommand::Count { times: 1 },
        &(),
        None,
    )
    .await
    .expect("the command commits");

    let registry: Vec<String> = sqlx::query_scalar("SELECT name FROM projections ORDER BY name")
        .fetch_all(&pool)
        .await
        .expect("reading the registry must succeed");
    assert_eq!(registry, vec!["tally_a", "tally_b"]);
    let marked: Vec<String> = sqlx::query_scalar(
        "SELECT DISTINCT projection FROM projection_applied_transactions ORDER BY projection",
    )
    .fetch_all(&pool)
    .await
    .expect("reading the applied marks must succeed");
    assert_eq!(marked, vec!["tally_a", "tally_b"]);
}

/// Refuses every command in both hooks, tracing each call.
struct Closed(Trace);

impl CommandMiddleware for Closed {
    fn before<'a>(&'a self, _: CommandContextMut<'a>) -> BoxFuture<'a, Next> {
        Box::pin(async move {
            self.0.lock().unwrap().push("before".to_string());
            Next::Busy("closed".to_string())
        })
    }

    fn before_commit<'a>(&'a self, _: &'a CommandContext) -> BoxFuture<'a, Next> {
        Box::pin(async move {
            self.0.lock().unwrap().push("before_commit".to_string());
            Next::Busy("closed".to_string())
        })
    }
}

#[tokio::test]
async fn a_direct_append_through_the_store_runs_no_middleware_postgres_test() {
    let (pool, _container) = tally_pool().await;
    let trace = Trace::default();
    let calls = Trace::default();
    // The application's `Cqrs` refuses everything. A store over the same database is
    // still reachable directly, and a direct append is not a command.
    let cqrs =
        Cqrs::new(store_with_views(&pool, &trace).await).with_middleware(Closed(calls.clone()));
    let store = store_with_views(&pool, &trace).await;

    store
        .store_events::<Tally>(
            &tally(),
            Tally::stream_type(),
            Metadata::default(),
            &[TallyEvent::Counted { n: 1 }],
            None,
        )
        .await
        .expect("a direct append is not a command");

    assert!(calls.lock().unwrap().is_empty());
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM events").await, 1);
    assert_eq!(count(&pool, "SELECT COUNT(*) FROM tally_view").await, 2);

    // The middleware is live: a command is refused by it.
    let refused = cqrs
        .execute::<Tally>(
            &tally(),
            Metadata::default(),
            TallyCommand::Count { times: 1 },
            &(),
            None,
        )
        .await
        .expect_err("the middleware refuses every command");
    assert_eq!(refused.kind(), ErrorKind::Busy);
    assert_eq!(*calls.lock().unwrap(), vec!["before".to_string()]);
}
