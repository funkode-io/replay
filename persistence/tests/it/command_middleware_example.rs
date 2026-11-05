//! The README's command middleware examples, against Postgres and a real Policy runner
//! (funkode-io/replay#317).
//!
//! `persistence/examples/command_middleware.rs` is the source of truth for both
//! middlewares; the README quotes it. Its `main` runs them on the in-memory store. What
//! only Postgres and the runner can show is that the maintenance switch's
//! `await_in_flight_commits` returns and leaves the store refusing commands, and that a
//! Policy's command is stamped with the Policy as its acting user.

use std::time::Duration;

use replay::{ErrorKind, Metadata};
use replay_persistence::{
    Cqrs, Dispatch, PolicyRunner, PolicySettings, PostgresEventStore, StartAt,
};
use sqlx::{postgres::PgPoolOptions, PgPool};

use crate::common;

use common::policy_harness::OBSERVE_TIMEOUT;
use common::postgres_image::{postgres_database, TestDatabase};

#[path = "../../examples/command_middleware.rs"]
mod example;

use example::{
    ActingUser, BankAccount, BankAccountCommand, BankAccountEvent, BankAccountUrn, MaintenanceMode,
    ACTING_USER, ACTING_USER_KEY,
};

async fn migrated_pool() -> (PgPool, TestDatabase) {
    let container = postgres_database().await;
    let pool = PgPoolOptions::new()
        .max_connections(5)
        .acquire_timeout(OBSERVE_TIMEOUT)
        .idle_timeout(Duration::from_secs(5))
        .connect(&container.url())
        .await
        .expect("failed to create the postgres pool");
    common::migrations::MIGRATOR
        .run(&pool)
        .await
        .expect("migrations must succeed");
    (pool, container)
}

fn account(name: &str) -> BankAccountUrn {
    BankAccountUrn::new(name).expect("a valid URN NSS")
}

fn deposit(amount: f64) -> BankAccountCommand {
    BankAccountCommand::Deposit { amount }
}

/// The acting user stored with each event of `stream_id`, in append order.
async fn acting_users(pool: &PgPool, stream_id: &str) -> Vec<serde_json::Value> {
    sqlx::query_scalar(
        "SELECT metadata -> $2 FROM events WHERE stream_id = $1 ORDER BY global_position",
    )
    .bind(stream_id)
    .bind(ACTING_USER_KEY)
    .fetch_all(pool)
    .await
    .expect("reading the stored metadata must succeed")
}

async fn events(pool: &PgPool) -> i64 {
    sqlx::query_scalar("SELECT COUNT(*) FROM events")
        .fetch_one(pool)
        .await
        .expect("counting events must succeed")
}

/// The maintenance switch: once `switch_on` returns, every command is `Busy` and appends
/// nothing; once switched off, the same command commits.
#[tokio::test]
async fn command_middleware_example_maintenance_switch_holds_commands_postgres_test() {
    let (pool, _container) = migrated_pool().await;
    let store = PostgresEventStore::new(pool.clone());
    let maintenance = MaintenanceMode::default();
    let cqrs = Cqrs::new(store.clone())
        .with_middleware(maintenance.clone())
        .with_middleware(ActingUser);
    let checking = account("alice-checking");
    let as_alice = |amount| {
        ACTING_USER.scope(
            "alice".to_string(),
            cqrs.execute::<BankAccount>(&checking, Metadata::default(), deposit(amount), &(), None),
        )
    };

    as_alice(100.0).await.expect("the deposit commits");

    maintenance
        .switch_on(&store)
        .await
        .expect("switching maintenance on must succeed");
    let held = as_alice(50.0)
        .await
        .expect_err("a command during maintenance is held");
    assert_eq!(held.kind(), ErrorKind::Busy);
    assert_eq!(events(&pool).await, 1, "a held command appends nothing");

    maintenance.switch_off();
    let account_state = as_alice(50.0)
        .await
        .expect("the same command commits after maintenance");
    assert_eq!(account_state.balance, 150.0);
}

/// An application's command records the request's user, a Policy's command records the
/// Policy, and a command with neither is refused before it appends anything.
#[tokio::test]
async fn command_middleware_example_acting_user_lands_on_the_events_postgres_test() {
    let (pool, _container) = migrated_pool().await;
    let cqrs = Cqrs::new(PostgresEventStore::new(pool.clone()))
        .with_middleware(MaintenanceMode::default())
        .with_middleware(ActingUser);
    let checking = account("alice-checking");
    let savings = account("alice-savings");

    let policy = "sweep_to_savings";
    let runner = PolicyRunner::builder(cqrs.clone())
        .register_services::<BankAccount>(())
        .register_policy_fn::<BankAccountEvent, _>(
            policy,
            PolicySettings::new().starting_at(StartAt::Beginning),
            {
                let checking: urn::Urn = checking.clone().into();
                let savings = savings.clone();
                move |event| match &event.data {
                    BankAccountEvent::Deposited { amount } if event.stream_id == checking => {
                        vec![Dispatch::to::<BankAccount>(
                            savings.clone(),
                            deposit(amount / 10.0),
                        )]
                    }
                    BankAccountEvent::Deposited { .. } => vec![],
                }
            },
        )
        .build();

    let refused = cqrs
        .execute::<BankAccount>(&checking, Metadata::default(), deposit(1.0), &(), None)
        .await
        .expect_err("a command without an acting user is refused");
    assert_eq!(refused.kind(), ErrorKind::Unauthorized);
    assert_eq!(events(&pool).await, 0, "a refused command appends nothing");

    ACTING_USER
        .scope(
            "alice".to_string(),
            cqrs.execute::<BankAccount>(&checking, Metadata::default(), deposit(100.0), &(), None),
        )
        .await
        .expect("alice's deposit commits");
    assert_eq!(runner.drain().await.expect("a drain must succeed"), 1);

    assert_eq!(
        acting_users(&pool, &checking.to_string()).await,
        vec![serde_json::json!("alice")]
    );
    assert_eq!(
        acting_users(&pool, &savings.to_string()).await,
        vec![serde_json::json!(format!("policy:{policy}"))]
    );
}
