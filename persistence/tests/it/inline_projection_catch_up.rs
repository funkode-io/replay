//! Catch-up: an inline projection converges on events appended by a process that did not
//! apply it (funkode-io/replay#289, ADR-0032).
//!
//! The rolling-deploy shape is two stores over one database: one has the projection
//! registered, the other — an "old pod" — does not, or runs an older version of it. Every
//! test asserts through the projection's own view, and the view's handler is a plain
//! `INSERT` per event, so an event applied twice shows up as a duplicate row rather than
//! being absorbed by an upsert.

use std::time::Duration;

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

    assert_eq!(
        view_rows(&pool).await,
        (2, 2),
        "only the live appends reached it"
    );

    let _restarted = store_with_view(&pool, 1).await;

    assert_eq!(
        view_rows(&pool).await,
        (6, 6),
        "every event reaches the view exactly once"
    );
    assert_eq!(event_count(&pool).await, 6);
}

/// A store with the view registered at `version` and no background task, so a test
/// decides when a catch-up runs.
async fn store_without_task(pool: &PgPool, version: i32) -> PostgresEventStore {
    PostgresEventStore::builder(pool.clone())
        .projection_flush_size(4)
        .projection_catch_up_interval(Duration::ZERO)
        .register(TillView { version })
        .build()
        .await
        .expect("build store with the view")
}

/// The same gap, repaired without a restart: a catch-up applies what the old pod wrote
/// and nothing the live path already applied, and a second one finds nothing to do.
#[tokio::test]
async fn catch_up_applies_only_what_no_process_applied_postgres_test() {
    let (pool, _container) = start_pool().await;

    let new_pod = store_without_task(&pool, 1).await;
    ring(&new_pod, "t-1", &[1.0, 2.0]).await;

    let old_pod = PostgresEventStore::new(pool.clone());
    ring(&old_pod, "t-1", &[3.0, 4.0, 5.0, 6.0, 7.0]).await;
    ring(&new_pod, "t-2", &[8.0]).await;

    assert_eq!(
        new_pod.catch_up_projections().await.unwrap(),
        5,
        "exactly the old pod's five events are owed"
    );
    assert_eq!(view_rows(&pool).await, (8, 8));

    assert_eq!(
        new_pod.catch_up_projections().await.unwrap(),
        0,
        "a second catch-up owes nothing"
    );
    assert_eq!(view_rows(&pool).await, (8, 8));
}

/// The hole a position checkpoint falls into: a transaction holding a lower position
/// commits after one holding a higher position has been caught up. It must still be
/// applied, once.
#[tokio::test]
async fn a_transaction_committing_after_a_later_one_was_caught_up_is_still_applied_postgres_test() {
    let (pool, _container) = start_pool().await;
    let new_pod = store_without_task(&pool, 1).await;

    // An old pod's append, held open: it has drawn its position and not committed.
    let mut slow = pool.begin().await.unwrap();
    let slow_stream: urn::Urn = TillUrn::new("slow").unwrap().into();
    sqlx::query("SELECT id FROM append_event($1, $2, '{}', 'Rang', $3, 'Till', NULL)")
        .bind(uuid::Uuid::new_v4())
        .bind(serde_json::json!({ "Rang": { "amount": 1.0 } }))
        .bind(slow_stream.to_string())
        .execute(&mut *slow)
        .await
        .unwrap();

    // A later old-pod append on another till commits first, and is caught up.
    let old_pod = PostgresEventStore::new(pool.clone());
    ring(&old_pod, "fast", &[2.0]).await;
    assert_eq!(new_pod.catch_up_projections().await.unwrap(), 1);
    assert_eq!(view_rows(&pool).await, (1, 1));

    slow.commit().await.unwrap();

    assert_eq!(
        new_pod.catch_up_projections().await.unwrap(),
        1,
        "the late commit is owed even though a higher position was caught up first"
    );
    assert_eq!(view_rows(&pool).await, (2, 2));
}

/// The other side of the same hole: while a transaction is still open, `build` replays a
/// later one that already committed. Its horizon stays below the open transaction, so the
/// next catch-up reads above it — and must apply only the open one once it commits, not
/// the one the replay already applied.
#[tokio::test]
async fn a_replay_is_not_applied_again_by_the_catch_up_that_follows_it_postgres_test() {
    let (pool, _container) = start_pool().await;

    let mut slow = pool.begin().await.unwrap();
    let slow_stream: urn::Urn = TillUrn::new("slow").unwrap().into();
    sqlx::query("SELECT id FROM append_event($1, $2, '{}', 'Rang', $3, 'Till', NULL)")
        .bind(uuid::Uuid::new_v4())
        .bind(serde_json::json!({ "Rang": { "amount": 1.0 } }))
        .bind(slow_stream.to_string())
        .execute(&mut *slow)
        .await
        .unwrap();

    let old_pod = PostgresEventStore::new(pool.clone());
    ring(&old_pod, "fast", &[2.0, 3.0]).await;

    let new_pod = store_without_task(&pool, 1).await;
    assert_eq!(
        view_rows(&pool).await,
        (2, 2),
        "the first registration replays"
    );

    slow.commit().await.unwrap();

    assert_eq!(new_pod.catch_up_projections().await.unwrap(), 1);
    assert_eq!(view_rows(&pool).await, (3, 3));
}

/// An append larger than a page, caught up while an unrelated transaction is open — the
/// normal state of a busy database. The open transaction keeps the append's id above the
/// horizon, so whatever records it as applied must not hide its later pages from the same
/// catch-up.
#[tokio::test]
async fn an_append_spanning_pages_is_caught_up_whole_while_another_transaction_is_open_postgres_test(
) {
    let (pool, _container) = start_pool().await;
    let new_pod = store_without_task(&pool, 1).await;

    let mut open = pool.begin().await.unwrap();
    sqlx::query("SELECT pg_current_xact_id()")
        .execute(&mut *open)
        .await
        .unwrap();

    // Ten events at a flush size of four: three pages.
    let old_pod = PostgresEventStore::new(pool.clone());
    let amounts: Vec<f64> = (0..10).map(f64::from).collect();
    ring(&old_pod, "t-1", &amounts).await;

    assert_eq!(new_pod.catch_up_projections().await.unwrap(), 10);
    assert_eq!(view_rows(&pool).await, (10, 10));

    // A restart while the transaction is still open owes nothing more.
    let _restarted = store_without_task(&pool, 1).await;
    assert_eq!(view_rows(&pool).await, (10, 10));

    open.rollback().await.unwrap();
    assert_eq!(new_pod.catch_up_projections().await.unwrap(), 0);
    assert_eq!(view_rows(&pool).await, (10, 10));
}

/// Without anybody calling it, the background task catches the view up.
#[tokio::test]
async fn the_background_task_catches_up_on_its_interval_postgres_test() {
    let (pool, _container) = start_pool().await;

    let _new_pod = PostgresEventStore::builder(pool.clone())
        .projection_catch_up_interval(Duration::from_millis(100))
        .register(TillView { version: 1 })
        .build()
        .await
        .unwrap();

    let old_pod = PostgresEventStore::new(pool.clone());
    ring(&old_pod, "t-1", &[1.0, 2.0, 3.0]).await;

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while view_rows(&pool).await != (3, 3) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the task must catch the view up, got {:?}",
            view_rows(&pool).await
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // And keeps applying each event once, however many rounds run.
    tokio::time::sleep(Duration::from_millis(350)).await;
    assert_eq!(view_rows(&pool).await, (3, 3));
}

/// A rolling deploy that bumps the version: the old pod still runs version 1 and applies
/// its own logic to its appends. The new version must apply them again under its logic,
/// and the old pod must never catch up into a view that version 2 owns.
#[tokio::test]
async fn a_newer_version_reapplies_what_an_older_version_applied_live_postgres_test() {
    let (pool, _container) = start_pool().await;

    let old_pod = store_without_task(&pool, 1).await;
    ring(&old_pod, "t-1", &[1.0]).await;

    let new_pod = store_without_task(&pool, 2).await;
    ring(&old_pod, "t-1", &[2.0, 3.0]).await;

    assert_eq!(
        old_pod.catch_up_projections().await.unwrap(),
        0,
        "version 1 must not catch up into version 2's view"
    );
    assert_eq!(new_pod.catch_up_projections().await.unwrap(), 2);

    let by_version: Vec<(i32, i64)> = sqlx::query_as(
        "SELECT projected, COUNT(DISTINCT event_id) FROM till_view GROUP BY projected \
         ORDER BY projected",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(
        by_version,
        vec![(1, 2), (2, 3)],
        "version 2 holds every event once; version 1's live rows are what it overwrites"
    );
    let v2_rows: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM till_view WHERE projected = 2")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(v2_rows, 3);
}

/// A registry row written before the horizon existed: what was applied to it is unknown,
/// so the first catch-up adopts the log as it stands instead of replaying it — and from
/// then on catches up as usual.
#[tokio::test]
async fn a_row_without_a_horizon_adopts_the_log_and_catches_up_from_there_postgres_test() {
    let (pool, _container) = start_pool().await;

    let old_pod = PostgresEventStore::new(pool.clone());
    ring(&old_pod, "t-1", &[1.0, 2.0]).await;
    // What an earlier release left behind: a name and a version.
    sqlx::query("INSERT INTO projections (name, version) VALUES ('till_view', 1)")
        .execute(&pool)
        .await
        .unwrap();

    let new_pod = store_without_task(&pool, 1).await;
    assert_eq!(view_rows(&pool).await, (0, 0), "history is not replayed");

    ring(&old_pod, "t-1", &[3.0]).await;
    assert_eq!(new_pod.catch_up_projections().await.unwrap(), 1);
    assert_eq!(view_rows(&pool).await, (1, 1));
}

/// An earlier release that bumps the version rebuilds the view and changes `version`, but
/// knows nothing of the horizon and leaves it at the old version's. That horizon says
/// nothing about the new version, so it is read as none and adopted — not used to apply
/// again everything the old release's rebuild already applied.
#[tokio::test]
async fn a_horizon_left_by_another_version_is_adopted_not_used_postgres_test() {
    let (pool, _container) = start_pool().await;

    let v1 = store_without_task(&pool, 1).await;
    ring(&v1, "t-1", &[1.0, 2.0, 3.0]).await;
    drop(v1);

    // What an earlier release's rebuild to version 2 leaves: the view rebuilt, `version`
    // moved, and the horizon untouched.
    sqlx::query("UPDATE projections SET version = 2 WHERE name = 'till_view'")
        .execute(&pool)
        .await
        .unwrap();

    let v2 = store_without_task(&pool, 2).await;
    assert_eq!(view_rows(&pool).await, (3, 3), "nothing is applied again");

    let old_pod = PostgresEventStore::new(pool.clone());
    ring(&old_pod, "t-1", &[4.0]).await;
    assert_eq!(v2.catch_up_projections().await.unwrap(), 1);
    assert_eq!(view_rows(&pool).await, (4, 4));
}

/// The view under test, appending an event from another connection the first time it is
/// handed one: an append committed while `build` replays, past its snapshot.
struct AppendsDuringRebuild {
    pool: PgPool,
    appended: bool,
}

impl InlineProjection for AppendsDuringRebuild {
    type Exec = sqlx::PgConnection;
    type Event = TillEvent;

    fn name(&self) -> &str {
        "till_view"
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
        TillView { version: 1 }.handle(conn, events).await?;
        if !self.appended {
            self.appended = true;
            ring(
                &PostgresEventStore::new(self.pool.clone()),
                "interloper",
                &[9.0],
            )
            .await;
        }
        Ok(())
    }
}

/// funkode-io/replay#162: an append committed while a rebuild replays is outside its
/// snapshot. It used to be applied by nobody; the next catch-up applies it.
#[tokio::test]
async fn an_append_committed_during_a_rebuild_is_caught_up_postgres_test() {
    let (pool, _container) = start_pool().await;

    let old_pod = PostgresEventStore::new(pool.clone());
    ring(&old_pod, "t-1", &[1.0, 2.0]).await;

    let new_pod = PostgresEventStore::builder(pool.clone())
        .projection_catch_up_interval(Duration::ZERO)
        .register(AppendsDuringRebuild {
            pool: pool.clone(),
            appended: false,
        })
        .build()
        .await
        .unwrap();
    assert_eq!(
        view_rows(&pool).await,
        (2, 2),
        "the rebuild reads its snapshot"
    );

    assert_eq!(new_pod.catch_up_projections().await.unwrap(), 1);
    assert_eq!(view_rows(&pool).await, (3, 3));
}

/// The view under test, declaring that it handles `Rang` only.
struct RangOnly;

impl InlineProjection for RangOnly {
    type Exec = sqlx::PgConnection;
    type Event = TillEvent;

    fn name(&self) -> &str {
        "till_view"
    }

    fn version(&self) -> i32 {
        1
    }

    fn event_types(&self) -> Option<Vec<String>> {
        Some(vec!["Rang".to_string()])
    }

    async fn init(&mut self, _conn: &mut Self::Exec) -> Result<(), replay::Error> {
        Ok(())
    }

    async fn handle(
        &mut self,
        conn: &mut Self::Exec,
        events: &[PersistedEvent<Self::Event>],
    ) -> Result<(), replay::Error> {
        TillView { version: 1 }.handle(conn, events).await
    }
}

/// Append one `Voided` event to a till.
async fn void(store: &PostgresEventStore, till: &str) {
    store
        .store_events_stream::<Till, _, _>(
            &TillUrn::new(till).unwrap(),
            "Till".to_string(),
            replay::Metadata::default(),
            futures::stream::iter(vec![Ok(TillEvent::Voided { amount: 1.0 })]),
            None,
            |_: &PersistedEvent<TillEvent>| {},
        )
        .await
        .expect("append must succeed");
}

/// Declared event types narrow every path into `handle` alike: the replay at first
/// registration, the live append and the catch-up all deliver `Rang` and never `Voided`,
/// although `Voided` is a variant of the projection's event and would deserialize.
#[tokio::test]
async fn declared_event_types_narrow_the_replay_the_live_path_and_the_catch_up_postgres_test() {
    let (pool, _container) = start_pool().await;

    let old_pod = PostgresEventStore::new(pool.clone());
    ring(&old_pod, "t-1", &[1.0]).await;
    void(&old_pod, "t-1").await;

    let new_pod = PostgresEventStore::builder(pool.clone())
        .projection_catch_up_interval(Duration::ZERO)
        .register(RangOnly)
        .build()
        .await
        .unwrap();

    ring(&new_pod, "t-1", &[2.0]).await;
    void(&new_pod, "t-1").await;
    ring(&old_pod, "t-1", &[3.0]).await;
    void(&old_pod, "t-1").await;

    assert_eq!(
        new_pod.catch_up_projections().await.unwrap(),
        1,
        "the catch-up reads only the declared type"
    );

    let types: Vec<(String, i64)> = sqlx::query_as(
        "SELECT event_type, COUNT(*) FROM till_view GROUP BY event_type ORDER BY event_type",
    )
    .fetch_all(&pool)
    .await
    .unwrap();
    assert_eq!(types, vec![("Rang".to_string(), 3)]);
}

/// The view under test, with a replay filter that admits no till event.
struct ReplaysNothing;

impl InlineProjection for ReplaysNothing {
    type Exec = sqlx::PgConnection;
    type Event = TillEvent;

    fn name(&self) -> &str {
        "till_view"
    }

    fn version(&self) -> i32 {
        1
    }

    fn stream_filter(&self) -> replay_persistence::StreamFilter {
        replay_persistence::StreamFilter::ForStreamTypes(vec!["Drawer".to_string()])
    }

    async fn init(&mut self, _conn: &mut Self::Exec) -> Result<(), replay::Error> {
        Ok(())
    }

    async fn handle(
        &mut self,
        conn: &mut Self::Exec,
        events: &[PersistedEvent<Self::Event>],
    ) -> Result<(), replay::Error> {
        TillView { version: 1 }.handle(conn, events).await
    }
}

/// A catch-up stands in for the live append, and the live append never applies
/// `stream_filter` — that filter narrows what a replay reads. So an event the live path
/// would apply is caught up even when the replay filter would not admit it: an
/// unregistered writer's append must not end up differently from a registered one's.
#[tokio::test]
async fn a_catch_up_delivers_what_the_live_path_would_whatever_the_replay_filter_postgres_test() {
    let (pool, _container) = start_pool().await;

    let new_pod = PostgresEventStore::builder(pool.clone())
        .projection_catch_up_interval(Duration::ZERO)
        .register(ReplaysNothing)
        .build()
        .await
        .unwrap();
    ring(&new_pod, "t-1", &[1.0]).await;
    assert_eq!(
        view_rows(&pool).await,
        (1, 1),
        "the live path ignores the filter"
    );

    let old_pod = PostgresEventStore::new(pool.clone());
    ring(&old_pod, "t-1", &[2.0]).await;

    assert_eq!(new_pod.catch_up_projections().await.unwrap(), 1);
    assert_eq!(view_rows(&pool).await, (2, 2));
}
