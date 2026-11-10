//! The build plan: a read-only dry run of `build()` that reports, per registered inline
//! projection, what `build()` would do (funkode-io/replay#341).
//!
//! Each test sets the registry up through real builds, asks for the plan, then runs the
//! `build()` the plan describes and checks it did exactly that — through the projection's
//! own `init`/`reset` calls and the registry it leaves behind.

use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;

use sqlx::{postgres::PgPoolOptions, PgPool};

use replay_macros::define_aggregate;
use replay_persistence::{InlineProjection, PersistedEvent, PlannedAction, PostgresEventStore};

use crate::common;
use common::postgres_image::{postgres_database, TestDatabase};

define_aggregate! {
    Lamp {
        namespace: "lamp",
        state: {
            lit: bool,
        },
        commands: {
            Light {},
        },
        events: {
            Lit {},
        }
    }
}

impl replay::EventStream for Lamp {
    type Event = LampEvent;

    fn stream_type() -> String {
        "Lamp".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            LampEvent::Lit => self.lit = true,
        }
    }
}

/// How often `build()` drove each lifecycle method of one projection.
#[derive(Default)]
struct Calls {
    init: AtomicUsize,
    reset: AtomicUsize,
}

/// A projection that only counts what `build()` asks of it.
struct Counted {
    name: &'static str,
    version: i32,
    calls: Arc<Calls>,
}

impl Counted {
    fn new(name: &'static str, version: i32) -> Self {
        Self {
            name,
            version,
            calls: Arc::default(),
        }
    }
}

impl InlineProjection for Counted {
    type Exec = sqlx::PgConnection;
    type Event = LampEvent;

    fn name(&self) -> &str {
        self.name
    }

    fn version(&self) -> i32 {
        self.version
    }

    async fn init(&mut self, _conn: &mut Self::Exec) -> Result<(), replay::Error> {
        self.calls.init.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn reset(&mut self, _conn: &mut Self::Exec) -> Result<(), replay::Error> {
        self.calls.reset.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }

    async fn handle(
        &mut self,
        _conn: &mut Self::Exec,
        _events: &[PersistedEvent<Self::Event>],
    ) -> Result<(), replay::Error> {
        Ok(())
    }
}

async fn start_pool() -> (PgPool, TestDatabase) {
    let container = postgres_database().await;
    let pool = PgPoolOptions::new()
        .max_connections(8)
        .connect(&container.url())
        .await
        .expect("Failed to connect to Postgres");
    common::migrations::MIGRATOR
        .run(&pool)
        .await
        .expect("Failed to run migrations");
    (pool, container)
}

/// Record `projections` in the registry at their versions, through a real build.
async fn build_at(pool: &PgPool, projections: &[(&'static str, i32)]) {
    let mut builder = PostgresEventStore::builder(pool.clone()).projection_catch_up_interval(
        Duration::ZERO, // no background task outliving the setup
    );
    for &(name, version) in projections {
        builder = builder.register(Counted::new(name, version));
    }
    builder.build().await.expect("setup build must succeed");
}

/// The registry, by name.
async fn registry(pool: &PgPool) -> Vec<(String, i32)> {
    sqlx::query_as("SELECT name, version FROM projections ORDER BY name")
        .fetch_all(pool)
        .await
        .expect("reading the registry must succeed")
}

/// The plan reports each of `build()`'s four branches, with the versions it decided them
/// by, and the `build()` that follows takes exactly those branches.
#[tokio::test]
async fn plan_reports_the_action_build_then_takes_for_each_projection() {
    let (pool, _container) = start_pool().await;

    // Stored: rebuilt@1, current@2, refused@3. `fresh` has never been built.
    build_at(&pool, &[("rebuilt", 1), ("current", 2), ("refused", 3)]).await;

    let fresh = Counted::new("fresh", 1);
    let rebuilt = Counted::new("rebuilt", 2);
    let current = Counted::new("current", 2);
    let refused = Counted::new("refused", 2);

    let builder = PostgresEventStore::builder(pool.clone())
        .projection_catch_up_interval(Duration::ZERO)
        .register(fresh)
        .register(rebuilt)
        .register(current)
        .register(refused);

    let plan = builder.plan().await.expect("plan must succeed");
    let reported: Vec<_> = plan
        .projections()
        .iter()
        .map(|p| (p.name(), p.stored_version(), p.code_version(), p.action()))
        .collect();
    assert_eq!(
        reported,
        vec![
            ("fresh", None, 1, PlannedAction::Init),
            ("rebuilt", Some(1), 2, PlannedAction::Rebuild),
            ("current", Some(2), 2, PlannedAction::Current),
            ("refused", Some(3), 2, PlannedAction::Refuse),
        ]
    );
    assert!(plan.needs_build());
    assert!(plan.refuses());

    // Refuse: build() refuses to start, and nothing it planned is applied.
    let error = builder
        .build()
        .await
        .err()
        .expect("build must refuse a stored version newer than the code");
    assert_eq!(error.kind(), replay::ErrorKind::InvalidInput);
    assert_eq!(
        registry(&pool).await,
        vec![
            ("current".to_string(), 2),
            ("rebuilt".to_string(), 1),
            ("refused".to_string(), 3),
        ]
    );

    // Without the refused projection, build() takes the other three branches as planned.
    let fresh = Counted::new("fresh", 1);
    let rebuilt = Counted::new("rebuilt", 2);
    let current = Counted::new("current", 2);
    let (fresh_calls, rebuilt_calls, current_calls) = (
        fresh.calls.clone(),
        rebuilt.calls.clone(),
        current.calls.clone(),
    );
    let builder = PostgresEventStore::builder(pool.clone())
        .projection_catch_up_interval(Duration::ZERO)
        .register(fresh)
        .register(rebuilt)
        .register(current);

    let actions: Vec<_> = builder
        .plan()
        .await
        .expect("plan must succeed")
        .projections()
        .iter()
        .map(|p| p.action())
        .collect();
    assert_eq!(
        actions,
        vec![
            PlannedAction::Init,
            PlannedAction::Rebuild,
            PlannedAction::Current
        ]
    );

    let _store = builder.build().await.expect("build must succeed");

    // Init ran init and no reset; Rebuild ran reset and no init; Current ran neither.
    let calls = |c: &Calls| {
        (
            c.init.load(Ordering::SeqCst),
            c.reset.load(Ordering::SeqCst),
        )
    };
    assert_eq!(calls(&fresh_calls), (1, 0), "Init runs init");
    assert_eq!(calls(&rebuilt_calls), (0, 1), "Rebuild runs reset");
    assert_eq!(
        calls(&current_calls),
        (0, 0),
        "Current neither inits nor resets"
    );
    assert_eq!(
        registry(&pool).await,
        vec![
            ("current".to_string(), 2),
            ("fresh".to_string(), 1),
            ("rebuilt".to_string(), 2),
            ("refused".to_string(), 3),
        ]
    );

    // Once built, the same registrations plan as current: nothing left to build.
    let plan = PostgresEventStore::builder(pool.clone())
        .register(Counted::new("fresh", 1))
        .register(Counted::new("rebuilt", 2))
        .register(Counted::new("current", 2))
        .plan()
        .await
        .expect("plan must succeed");
    assert!(plan
        .projections()
        .iter()
        .all(|p| p.action() == PlannedAction::Current));
    assert!(!plan.needs_build());
    assert!(!plan.refuses());
}

/// The plan takes no advisory lock and writes nothing: it answers while another session
/// holds every lock `build()` would wait for, and leaves the registry and the projection
/// untouched.
#[tokio::test]
async fn plan_takes_no_lock_and_writes_nothing() {
    let (pool, _container) = start_pool().await;
    build_at(&pool, &[("rebuilt", 1)]).await;

    // Hold the registry lock exclusively and the projections' own locks, as a running
    // build or a deletion of streams would: `build()` would wait on these.
    let mut holder = pool.acquire().await.expect("acquire the lock holder");
    sqlx::query("SELECT pg_advisory_lock(x'52504A52'::int, 0)")
        .execute(&mut *holder)
        .await
        .expect("hold the registry lock");
    for name in ["fresh", "rebuilt"] {
        sqlx::query("SELECT pg_advisory_lock(x'52504A43'::int, hashtext($1))")
            .bind(name)
            .execute(&mut *holder)
            .await
            .expect("hold the projection lock");
    }

    let fresh = Counted::new("fresh", 1);
    let rebuilt = Counted::new("rebuilt", 2);
    let (fresh_calls, rebuilt_calls) = (fresh.calls.clone(), rebuilt.calls.clone());
    let builder = PostgresEventStore::builder(pool.clone())
        .register(fresh)
        .register(rebuilt);

    let plan = tokio::time::timeout(Duration::from_secs(10), builder.plan())
        .await
        .expect("plan must not wait on an advisory lock")
        .expect("plan must succeed");
    assert!(plan.needs_build());

    // Nothing written, nothing driven.
    assert_eq!(registry(&pool).await, vec![("rebuilt".to_string(), 1)]);
    for calls in [&fresh_calls, &rebuilt_calls] {
        assert_eq!(calls.init.load(Ordering::SeqCst), 0);
        assert_eq!(calls.reset.load(Ordering::SeqCst), 0);
    }

    sqlx::query("SELECT pg_advisory_unlock_all()")
        .execute(&mut *holder)
        .await
        .expect("release the locks");
}

/// A builder with nothing registered plans nothing, and needs no build.
#[tokio::test]
async fn plan_with_nothing_registered_is_empty() {
    let (pool, _container) = start_pool().await;

    let plan = PostgresEventStore::builder(pool)
        .plan()
        .await
        .expect("plan must succeed");

    assert!(plan.projections().is_empty());
    assert!(!plan.needs_build());
}

/// A name registered twice is planned as `build()` runs it: the second registration sees
/// the version the first one records.
#[tokio::test]
async fn plan_follows_builds_order_for_a_name_registered_twice() {
    let (pool, _container) = start_pool().await;

    let first = Counted::new("twice", 1);
    let again = Counted::new("twice", 1);
    let newer = Counted::new("twice", 2);
    let calls = [
        first.calls.clone(),
        again.calls.clone(),
        newer.calls.clone(),
    ];
    let builder = PostgresEventStore::builder(pool.clone())
        .projection_catch_up_interval(Duration::ZERO)
        .register(first)
        .register(again)
        .register(newer);

    let plan = builder.plan().await.expect("plan must succeed");
    let reported: Vec<_> = plan
        .projections()
        .iter()
        .map(|p| (p.stored_version(), p.code_version(), p.action()))
        .collect();
    assert_eq!(
        reported,
        vec![
            (None, 1, PlannedAction::Init),
            (Some(1), 1, PlannedAction::Current),
            (Some(1), 2, PlannedAction::Rebuild),
        ]
    );

    let _store = builder.build().await.expect("build must succeed");

    let done: Vec<_> = calls
        .iter()
        .map(|c| {
            (
                c.init.load(Ordering::SeqCst),
                c.reset.load(Ordering::SeqCst),
            )
        })
        .collect();
    assert_eq!(done, vec![(1, 0), (0, 0), (0, 1)]);
    assert_eq!(registry(&pool).await, vec![("twice".to_string(), 2)]);
}
