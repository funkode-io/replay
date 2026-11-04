//! Command middleware against Postgres and a real Policy runner (ADR-0035).
//!
//! The in-memory unit tests in `src/command_middleware.rs` cover the chain itself. What
//! only the runner can show is that a command it dispatches reaches the middleware with
//! the Causation it stamped, while an application's command reaches it with none.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use futures::future::BoxFuture;
use sqlx::postgres::PgPoolOptions;

use crate::common;

use common::policy_harness::{Probe, ProbeCommand, ProbeEvent, ProbeUrn, OBSERVE_TIMEOUT};
use common::postgres_image::{start_postgres_server, POSTGRES_PORT};
use replay::Metadata;
use replay_persistence::{
    Causation, CommandContext, CommandMiddleware, Cqrs, Dispatch, Next, PolicyRunner,
    PolicySettings, PostgresEventStore, StartAt,
};

/// The stream and Causation of every command it sees, in the order it saw them.
#[derive(Clone, Default)]
struct RecordsCausation(Arc<Mutex<Vec<Seen>>>);

/// A command's stream id and the Causation it arrived with.
type Seen = (String, Option<Causation>);

impl CommandMiddleware for RecordsCausation {
    fn before<'a>(&'a self, context: &'a mut CommandContext) -> BoxFuture<'a, Next> {
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
    fn before<'a>(&'a self, context: &'a mut CommandContext) -> BoxFuture<'a, Next> {
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
