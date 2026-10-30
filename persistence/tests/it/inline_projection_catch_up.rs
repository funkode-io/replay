//! Catch-up: an inline projection converges on events appended by a process that did not
//! apply it (funkode-io/replay#289, ADR-0032).
//!
//! The rolling-deploy shape is two stores over one database: one has the projection
//! registered, the other — an "old pod" — does not, or runs an older version of it. Every
//! test asserts through the projection's own view, and the view's handler is a plain
//! `INSERT` per event, so an event applied twice shows up as a duplicate row rather than
//! being absorbed by an upsert.

use sqlx::{postgres::PgPoolOptions, PgPool};

use replay_macros::define_aggregate;
use replay_persistence::{EventStore, InlineProjection, PersistedEvent, PostgresEventStore};

use crate::common;
use common::postgres_image::{start_postgres_server, PostgresServer, POSTGRES_PORT};

define_aggregate! {
    Till {
        namespace: "till",
        state: {
            balance: f64,
        },
        commands: {
            Ring { amount: f64 },
        },
        events: {
            Rang { amount: f64 },
            Voided { amount: f64 },
        }
    }
}

impl replay::EventStream for Till {
    type Event = TillEvent;

    fn stream_type() -> String {
        "Till".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            TillEvent::Rang { amount } => self.balance += amount,
            TillEvent::Voided { amount } => self.balance -= amount,
        }
    }
}

impl replay::Aggregate for Till {
    type Command = TillCommand;
    type Error = replay::Error;
    type Services = ();

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        let TillCommand::Ring { amount } = command;
        Ok(vec![TillEvent::Rang { amount }])
    }
}

async fn start_pool() -> (PgPool, PostgresServer) {
    let container = start_postgres_server().await;
    let host = container.get_host().await.unwrap().to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("Error getting docker port");

    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&format!(
            "postgres://postgres:postgres@{host}:{port}/postgres"
        ))
        .await
        .expect("Failed to connect to Postgres");

    common::migrations::MIGRATOR
        .run(&pool)
        .await
        .expect("Failed to run migrations");

    // The view is owned by a migration in a real deployment, not by `init`.
    sqlx::query(
        "CREATE TABLE till_view ( \
             event_id   uuid    NOT NULL, \
             event_type text    NOT NULL, \
             projected  integer NOT NULL \
         )",
    )
    .execute(&pool)
    .await
    .expect("creating the view must succeed");

    (pool, container)
}

/// The view under test: one row per event handed to `handle`, stamped with the
/// projection version that wrote it. Deliberately not idempotent.
struct TillView {
    version: i32,
}

impl InlineProjection for TillView {
    type Exec = sqlx::PgConnection;
    type Event = TillEvent;

    fn name(&self) -> &str {
        "till_view"
    }

    fn version(&self) -> i32 {
        self.version
    }

    async fn init(&mut self, _conn: &mut Self::Exec) -> Result<(), replay::Error> {
        Ok(())
    }

    async fn reset(&mut self, conn: &mut Self::Exec) -> Result<(), replay::Error> {
        sqlx::query("DELETE FROM till_view")
            .execute(conn)
            .await
            .map_err(replay_persistence::db_error)?;
        Ok(())
    }

    async fn handle(
        &mut self,
        conn: &mut Self::Exec,
        events: &[PersistedEvent<Self::Event>],
    ) -> Result<(), replay::Error> {
        for event in events {
            sqlx::query(
                "INSERT INTO till_view (event_id, event_type, projected) VALUES ($1, $2, $3)",
            )
            .bind(event.id)
            .bind(&event.r#type)
            .bind(self.version)
            .execute(&mut *conn)
            .await
            .map_err(replay_persistence::db_error)?;
        }
        Ok(())
    }
}

/// A store with the view registered at `version`.
async fn store_with_view(pool: &PgPool, version: i32) -> PostgresEventStore {
    PostgresEventStore::builder(pool.clone())
        .projection_flush_size(4)
        .register(TillView { version })
        .build()
        .await
        .expect("build store with the view")
}

/// Append `amounts` to one till as a single append.
async fn ring(store: &PostgresEventStore, till: &str, amounts: &[f64]) {
    let events: Vec<Result<TillEvent, replay::Error>> = amounts
        .iter()
        .map(|amount| Ok(TillEvent::Rang { amount: *amount }))
        .collect();
    store
        .store_events_stream::<Till, _, _>(
            &TillUrn::new(till).unwrap(),
            "Till".to_string(),
            replay::Metadata::default(),
            futures::stream::iter(events),
            None,
            |_: &PersistedEvent<TillEvent>| {},
        )
        .await
        .expect("append must succeed");
}

/// `(rows, distinct events)` in the view: equal when nothing was applied twice.
async fn view_rows(pool: &PgPool) -> (i64, i64) {
    sqlx::query_as("SELECT COUNT(*), COUNT(DISTINCT event_id) FROM till_view")
        .fetch_one(pool)
        .await
        .expect("reading the view must succeed")
}

async fn event_count(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM events")
        .fetch_one(pool)
        .await
        .expect("counting events must succeed")
}

/// The rolling-deploy gap: an old pod without the projection keeps appending after the
/// new pod built it. A restart at the same version must apply what the old pod wrote —
/// once — and leave what the new pod applied live alone.
#[tokio::test]
async fn restart_at_the_same_version_applies_what_a_process_without_the_projection_appended_postgres_test(
) {
    let (pool, _container) = start_pool().await;

    let new_pod = store_with_view(&pool, 1).await;
    ring(&new_pod, "t-1", &[1.0, 2.0]).await;

    let old_pod = PostgresEventStore::new(pool.clone());
    ring(&old_pod, "t-1", &[3.0, 4.0, 5.0]).await;
    ring(&old_pod, "t-2", &[6.0]).await;

    assert_eq!(view_rows(&pool).await, (2, 2), "only the live appends reached it");

    let _restarted = store_with_view(&pool, 1).await;

    assert_eq!(
        view_rows(&pool).await,
        (6, 6),
        "every event reaches the view exactly once"
    );
    assert_eq!(event_count(&pool).await, 6);
}
