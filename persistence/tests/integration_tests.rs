use std::str::FromStr;

use futures::TryStreamExt;
use serde::{Deserialize, Serialize};

use sqlx::{postgres::PgPoolOptions, PgPool};
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};
use tokio_test::assert_err;
use urn::Urn;

use replay::{prelude::*, Compactable};
use replay_macros::{define_aggregate, Urn};
use replay_persistence::{AggregateVersion, EventStore, PersistedEvent, StreamFilter};

// Re-use the README walkthrough verbatim as the source of truth. The example's
// `main` is gated `#[cfg(not(test))]`, so including it here pulls in only the
// domain types, the live `GlobalPositionQuery`, and the inline
// `GlobalPositionProjection`.
#[path = "../examples/global_position.rs"]
mod global_position;

const POSTGRES_PORT: u16 = 5432;

// ── Aggregate definition via macro ───────────────────────────────────────────

define_aggregate! {
    BankAccount {
        namespace: "bank-account",
        state: {
            balance: f64,
        },
        commands: {
            Deposit {
                effective_on: chrono::NaiveDate,
                amount: f64,
            },
            Withdraw {
                effective_on: chrono::NaiveDate,
                amount: f64,
            },
            CloseMonth { month: chrono::NaiveDate },
        },
        events: {
            Deposited {
                operation_date: chrono::NaiveDate,
                amount: f64,
            },
            Withdrawn {
                operation_date: chrono::NaiveDate,
                amount: f64,
            },
            MonthlyClosed {
                month: chrono::NaiveDate,
                closing_balance: f64,
            },
        }
    }
}

impl replay::EventStream for BankAccount {
    type Event = BankAccountEvent;

    fn stream_type() -> String {
        "BankAccount".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            BankAccountEvent::Deposited { amount, .. } => self.balance += amount,
            BankAccountEvent::Withdrawn { amount, .. } => self.balance -= amount,
            BankAccountEvent::MonthlyClosed {
                closing_balance, ..
            } => {
                self.balance = closing_balance;
            }
        }
    }
}

impl replay::Aggregate for BankAccount {
    type Command = BankAccountCommand;
    type Error = replay::Error;
    type Services = ();

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            BankAccountCommand::Deposit {
                effective_on,
                amount,
            } => Ok(vec![BankAccountEvent::Deposited {
                operation_date: effective_on,
                amount,
            }]),
            BankAccountCommand::Withdraw {
                effective_on,
                amount,
            } => {
                if self.balance < amount {
                    return Err(replay::Error::business_rule_violation("Insufficient funds")
                        .with_operation("Withdraw")
                        .with_context("amount_tried", amount));
                }
                Ok(vec![BankAccountEvent::Withdrawn {
                    operation_date: effective_on,
                    amount,
                }])
            }
            BankAccountCommand::CloseMonth { month } => Ok(vec![BankAccountEvent::MonthlyClosed {
                month,
                closing_balance: self.balance,
            }]),
        }
    }
}

impl Compactable for BankAccount {
    async fn compacted_events(
        &self,
        events: impl futures::TryStream<Ok = BankAccountEvent, Error = replay::Error> + Send,
    ) -> replay::Result<Vec<BankAccountEvent>> {
        use futures::TryStreamExt;
        events
            .try_fold(Vec::new(), |mut tail, event| async move {
                if matches!(event, BankAccountEvent::MonthlyClosed { .. }) {
                    tail.clear();
                }
                tail.push(event);
                Ok(tail)
            })
            .await
    }
}

define_aggregate! {
    IdempotentFeeAccount {
        namespace: "idempotent-fee-account",
        state: {
            balance: f64,
            applied_causation_ids: std::collections::HashSet<uuid::Uuid>,
        },
        commands: {
            Open { balance: f64 },
            ChargeFee {
                amount: f64,
                causation_event_id: uuid::Uuid,
            },
        },
        events: {
            Opened { balance: f64 },
            FeeCharged {
                amount: f64,
                causation_event_id: uuid::Uuid,
            },
        }
    }
}

impl replay::EventStream for IdempotentFeeAccount {
    type Event = IdempotentFeeAccountEvent;

    fn stream_type() -> String {
        "IdempotentFeeAccount".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            IdempotentFeeAccountEvent::Opened { balance } => {
                self.balance = balance;
            }
            IdempotentFeeAccountEvent::FeeCharged {
                amount,
                causation_event_id,
            } => {
                self.balance -= amount;
                self.applied_causation_ids.insert(causation_event_id);
            }
        }
    }
}

impl replay::Aggregate for IdempotentFeeAccount {
    type Command = IdempotentFeeAccountCommand;
    type Error = replay::Error;
    type Services = ();

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        match command {
            IdempotentFeeAccountCommand::Open { balance } => {
                Ok(vec![IdempotentFeeAccountEvent::Opened { balance }])
            }
            IdempotentFeeAccountCommand::ChargeFee {
                amount,
                causation_event_id,
            } => {
                // Causation guard recipe: duplicate causation identity is a no-op.
                if self.applied_causation_ids.contains(&causation_event_id) {
                    return Ok(Vec::new());
                }

                Ok(vec![IdempotentFeeAccountEvent::FeeCharged {
                    amount,
                    causation_event_id,
                }])
            }
        }
    }
}

struct BankAccountStatement {
    bank_account: BankAccountUrn,
    from: chrono::NaiveDate,
    to: chrono::NaiveDate,
    transactions: Vec<BankAccountEvent>,
    balance_change: f64,
    total_transactions: u64,
}

impl std::fmt::Display for BankAccountStatement {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(
            f,
            "Bank account: {}\nFrom: {}\nTo: {}\nTransactions: {:?}\nBalance change: {}\nTotal transactions: {}",
            self.bank_account,
            self.from,
            self.to,
            self.transactions,
            self.balance_change,
            self.total_transactions
        )
    }
}

impl replay_persistence::Query for BankAccountStatement {
    type Event = BankAccountEvent;

    fn stream_filter(&self) -> StreamFilter {
        replay_persistence::StreamFilter::with_stream_id::<BankAccount>(&self.bank_account)
    }

    fn update(&mut self, event: PersistedEvent<Self::Event>) {
        match event.data {
            BankAccountEvent::Deposited {
                operation_date,
                amount,
            } => {
                if operation_date < self.from || operation_date > self.to {
                    return;
                }
                self.transactions.push(BankAccountEvent::Deposited {
                    operation_date,
                    amount,
                });
                self.balance_change += amount;
                self.total_transactions += 1;
            }
            BankAccountEvent::Withdrawn {
                operation_date,
                amount,
            } => {
                if operation_date < self.from || operation_date > self.to {
                    return;
                }
                self.transactions.push(BankAccountEvent::Withdrawn {
                    operation_date,
                    amount,
                });
                self.balance_change -= amount;
                self.total_transactions += 1;
            }
            BankAccountEvent::MonthlyClosed { .. } => {}
        }
    }
}

#[tokio::test]
async fn bank_account_postgres_test() {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .with_target(false)
        .init();

    let container = postgres::Postgres::default().start().await.unwrap();

    let host = container.get_host().await.unwrap().to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("Error getting docker port");

    println!("Postgres container started on host:port {}:{}", host, port);

    // connect to Postgres
    let pg_pool = connect_to_postgres(host, port).await;

    // run migrations on Postgres
    sqlx::migrate!("./tests/migrations")
        .run(&pg_pool)
        .await
        .expect("Failed to run migrations");

    println!("Connected to postgres and ran migrations");

    let store = replay_persistence::PostgresEventStore::new(pg_pool);
    let cqrs = replay_persistence::Cqrs::new(store);

    let stream_id = BankAccountUrn::new("1").unwrap();

    let commands = vec![
        // create deposit for 1st Jan 2025
        BankAccountCommand::Deposit {
            effective_on: chrono::NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            amount: 100.0,
        },
        // create withdraw for 2nd Jan 2025
        BankAccountCommand::Withdraw {
            effective_on: chrono::NaiveDate::from_ymd_opt(2025, 1, 2).unwrap(),
            amount: 40.0,
        },
    ];

    let id = BankAccountUrn::new_random();
    let mut bank_account: BankAccount = BankAccount::with_id(id);

    let services = &();
    let expected_version = None;

    for command in commands {
        bank_account = cqrs
            .execute(
                &stream_id,
                replay::Metadata::default(),
                command,
                services,
                expected_version,
            )
            .await
            .unwrap();
    }

    assert_eq!(bank_account.balance, 60.0);

    let result = cqrs
        .execute::<BankAccount>(
            &stream_id,
            replay::Metadata::default(),
            // create withdraw for 3rd Jan 2025
            BankAccountCommand::Withdraw {
                effective_on: chrono::NaiveDate::from_ymd_opt(2025, 1, 3).unwrap(),
                amount: 100f64,
            },
            services,
            expected_version,
        )
        .await;

    assert_err!(result, "Insufficient funds");

    let commands = vec![
        // create deposit for 5st Jan 2025
        BankAccountCommand::Deposit {
            effective_on: chrono::NaiveDate::from_ymd_opt(2025, 1, 5).unwrap(),
            amount: 20.0,
        },
        // create withdraw for 6th Jan 2025
        BankAccountCommand::Withdraw {
            effective_on: chrono::NaiveDate::from_ymd_opt(2025, 1, 6).unwrap(),
            amount: 40.0,
        },
    ];

    for command in commands {
        bank_account = cqrs
            .execute(
                &stream_id,
                replay::Metadata::default(),
                command,
                services,
                expected_version,
            )
            .await
            .unwrap();
    }

    assert_eq!(bank_account.balance, 40.0);

    // let's create a query for this stream using the from timestamp
    let mut statement = BankAccountStatement {
        bank_account: stream_id.clone(),
        // from 5th Jan 2025 to 6th Jan 2025
        from: chrono::NaiveDate::from_ymd_opt(2025, 1, 5).unwrap(),
        to: chrono::NaiveDate::from_ymd_opt(2025, 1, 6).unwrap(),
        transactions: Vec::new(),
        balance_change: 0.0,
        total_transactions: 0,
    };

    cqrs.run_query(&mut statement).await.unwrap();

    tracing::info!("Statement: {}", statement);

    assert_eq!(statement.transactions.len(), 2);
    assert_eq!(statement.balance_change, -20.0);
    assert_eq!(statement.total_transactions, 2);
}

/// A stale `expected_version` must surface as a `concurrency_error` (parity with the
/// in-memory store), and the conflicting append must not persist.
#[tokio::test]
async fn bank_account_store_events_concurrency_conflict_postgres_test() {
    let container = postgres::Postgres::default().start().await.unwrap();

    let host = container.get_host().await.unwrap().to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("Error getting docker port");

    let pg_pool = connect_to_postgres(host, port).await;

    sqlx::migrate!("./tests/migrations")
        .run(&pg_pool)
        .await
        .expect("Failed to run migrations");

    let store = replay_persistence::PostgresEventStore::new(pg_pool.clone());

    let stream_id = BankAccountUrn::new("concurrency-1").unwrap();
    let stream_id_str = Into::<Urn>::into(stream_id.clone()).to_string();

    // First append against a fresh stream (version 0) succeeds and bumps it to version 1.
    store
        .store_events::<BankAccount>(
            &stream_id,
            "bank-account".to_string(),
            replay::Metadata::default(),
            &[BankAccountEvent::Deposited {
                operation_date: chrono::NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
                amount: 100.0,
            }],
            Some(0),
        )
        .await
        .expect("first append must succeed");

    // Second append still expects version 0, but the stream is now at version 1.
    let result = store
        .store_events::<BankAccount>(
            &stream_id,
            "bank-account".to_string(),
            replay::Metadata::default(),
            &[BankAccountEvent::Deposited {
                operation_date: chrono::NaiveDate::from_ymd_opt(2025, 1, 2).unwrap(),
                amount: 50.0,
            }],
            Some(0),
        )
        .await;

    assert_err!(result, "Stream version mismatch");

    // The conflicting append must not have persisted: only the first event remains.
    let event_count: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE stream_id = $1")
        .bind(&stream_id_str)
        .fetch_one(&pg_pool)
        .await
        .expect("counting events must succeed");

    assert_eq!(event_count, 1);
}

async fn connect_to_postgres(host: String, port: u16) -> PgPool {
    // connect to Postgres
    PgPoolOptions::new()
        .max_connections(50)
        .idle_timeout(std::time::Duration::from_secs(5))
        .connect(&format!(
            "postgres://postgres:postgres@{}:{}/postgres",
            host, port
        ))
        .await
        .expect("Failed to create postgres pool")
}

/// Tests the full compaction lifecycle for a bank account aggregate.
///
/// Scenario:
///  - Three transactions are recorded in January (deposit, withdraw, deposit → balance 1 300).
///  - January is formally closed via `CloseMonth` which emits a `MonthlyClosed` event.
///  - Two transactions are recorded in February (deposit, withdraw → balance 1 600).
///  - `Cqrs::compact` is called; it archives the 6-event full history as Version(1) and
///    replaces the live stream with 3 compacted events:
///      MonthlyClosed(Jan, 1300) + Deposited(Feb) + Withdrawn(Feb).
///  - The aggregate balance after replaying the compacted stream must equal 1 600.
///  - The archived Version(1) must still contain all 6 original events.
#[tokio::test]
async fn bank_account_compaction_postgres_test() {
    let container = postgres::Postgres::default().start().await.unwrap();

    let host = container.get_host().await.unwrap().to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("Error getting docker port");

    let pg_pool = connect_to_postgres(host, port).await;

    sqlx::migrate!("./tests/migrations")
        .run(&pg_pool)
        .await
        .expect("Failed to run migrations");

    // Keep a clone of the store for direct streaming after compaction.
    let store = replay_persistence::PostgresEventStore::new(pg_pool);
    let cqrs = replay_persistence::Cqrs::new(store.clone());

    let stream_id = BankAccountUrn::new("compact-1").unwrap();
    let services = &();
    let meta = replay::Metadata::default();

    // Convenient date helpers.
    let jan_1 = chrono::NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let jan_15 = chrono::NaiveDate::from_ymd_opt(2026, 1, 15).unwrap();
    let jan_31 = chrono::NaiveDate::from_ymd_opt(2026, 1, 31).unwrap();
    let feb_15 = chrono::NaiveDate::from_ymd_opt(2026, 2, 15).unwrap();
    let feb_28 = chrono::NaiveDate::from_ymd_opt(2026, 2, 28).unwrap();

    // ── January: three transactions ──────────────────────────────────────────
    // After these: balance = 1000 - 200 + 500 = 1300.
    for cmd in [
        BankAccountCommand::Deposit {
            effective_on: jan_1,
            amount: 1000.0,
        },
        BankAccountCommand::Withdraw {
            effective_on: jan_15,
            amount: 200.0,
        },
        BankAccountCommand::Deposit {
            effective_on: jan_31,
            amount: 500.0,
        },
    ] {
        cqrs.execute::<BankAccount>(&stream_id, meta.clone(), cmd, services, None)
            .await
            .unwrap();
    }

    // ── Close January ─────────────────────────────────────────────────────────
    // CloseMonth derives closing_balance from current aggregate state (1300).
    cqrs.execute::<BankAccount>(
        &stream_id,
        meta.clone(),
        BankAccountCommand::CloseMonth { month: jan_1 },
        services,
        None,
    )
    .await
    .unwrap();

    // ── February: two transactions ───────────────────────────────────────────
    // After these: balance = 1300 + 400 - 100 = 1600.
    for cmd in [
        BankAccountCommand::Deposit {
            effective_on: feb_15,
            amount: 400.0,
        },
        BankAccountCommand::Withdraw {
            effective_on: feb_28,
            amount: 100.0,
        },
    ] {
        cqrs.execute::<BankAccount>(&stream_id, meta.clone(), cmd, services, None)
            .await
            .unwrap();
    }

    // Full history at this point: 6 events (3 Jan + MonthlyClosed + 2 Feb).
    // fetch_aggregate is the ergonomic shorthand for fetching the latest state.
    let aggregate = cqrs
        .fetch_aggregate::<BankAccount>(&stream_id)
        .await
        .unwrap();

    assert_eq!(aggregate.balance, 1600.0);

    // ── Compact ──────────────────────────────────────────────────────────────
    let archive_version = cqrs.compact(&aggregate, meta.clone()).await.unwrap();
    assert_eq!(
        archive_version, 1,
        "First compaction must produce archive version 1"
    );

    // ── Verify: balance unchanged after replaying compacted stream ────────────
    let compacted_aggregate = cqrs
        .fetch_aggregate_at::<BankAccount>(&stream_id, AggregateVersion::Latest, None, None)
        .await
        .unwrap();

    assert_eq!(
        compacted_aggregate.balance, 1600.0,
        "Replaying the compacted stream must yield the same balance"
    );

    // ── Verify: live stream now has exactly 3 events ──────────────────────────
    // Expected: MonthlyClosed(Jan,1300) · Deposited(Feb) · Withdrawn(Feb)
    let live_events: Vec<_> = store
        .stream_events_by_stream_id::<BankAccount>(&stream_id, AggregateVersion::Latest, None, None)
        .try_collect()
        .await
        .unwrap();

    assert_eq!(
        live_events.len(),
        3,
        "Compacted live stream must have 3 events"
    );
    assert!(
        matches!(live_events[0].data, BankAccountEvent::MonthlyClosed { .. }),
        "First compacted event must be MonthlyClosed"
    );
    assert!(
        matches!(live_events[1].data, BankAccountEvent::Deposited { .. }),
        "Second compacted event must be the February Deposited"
    );
    assert!(
        matches!(live_events[2].data, BankAccountEvent::Withdrawn { .. }),
        "Third compacted event must be the February Withdrawn"
    );

    // ── Verify: full history still accessible as Version(1) ───────────────────
    let archived_events: Vec<_> = store
        .stream_events_by_stream_id::<BankAccount>(
            &stream_id,
            AggregateVersion::Version(1),
            None,
            None,
        )
        .try_collect()
        .await
        .unwrap();

    assert_eq!(
        archived_events.len(),
        6,
        "Archived Version(1) must retain all 6 original events"
    );
}

// ── Inline projection (issue #58) ─────────────────────────────────────────────

/// End-to-end test for the inline-projection walking skeleton.
///
/// Registers a `BalanceProjection` via the builder, executes commands through `Cqrs`,
/// and asserts the projection's view table reflects the events and that its version is
/// recorded in the `projections` registry.
#[tokio::test]
async fn bank_account_inline_projection_postgres_test() {
    let container = postgres::Postgres::default().start().await.unwrap();

    let host = container.get_host().await.unwrap().to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("Error getting docker port");

    let pg_pool = connect_to_postgres(host, port).await;

    sqlx::migrate!("./tests/migrations")
        .run(&pg_pool)
        .await
        .expect("Failed to run migrations");

    // In normal usage, the projection table is created by SQL migrations rather than
    // by the projection runtime itself.
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS account_balances (
            stream_id text PRIMARY KEY,
            balance   double precision NOT NULL
        )",
    )
    .execute(&pg_pool)
    .await
    .expect("Failed to create account_balances test table");

    // Build the store with a low-ceremony Postgres event handler. The schema already
    // exists, so the helper only needs the SQL to run for each event batch.
    let store = replay_persistence::PostgresEventStore::builder(pg_pool.clone())
        .register_postgres_event_handler::<BankAccountEvent, _>(
            "account_balance_view",
            1,
            |conn, events| {
                Box::pin(async move {
                    for event in events {
                        let delta = match &event.data {
                            BankAccountEvent::Deposited { amount, .. } => *amount,
                            BankAccountEvent::Withdrawn { amount, .. } => -*amount,
                            BankAccountEvent::MonthlyClosed { .. } => continue,
                        };

                        sqlx::query(
                            "INSERT INTO account_balances (stream_id, balance)
                             VALUES ($1, $2)
                             ON CONFLICT (stream_id)
                             DO UPDATE SET balance = account_balances.balance + EXCLUDED.balance",
                        )
                        .bind(event.stream_id.to_string())
                        .bind(delta)
                        .execute(&mut *conn)
                        .await
                        .map_err(replay_persistence::db_error)?;
                    }

                    Ok(())
                })
            },
        )
        .build()
        .await
        .expect("Failed to build store with inline projection");

    let cqrs = replay_persistence::Cqrs::new(store);

    let stream_id = BankAccountUrn::new("inline-proj-1").unwrap();
    let services = &();

    for command in [
        BankAccountCommand::Deposit {
            effective_on: chrono::NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            amount: 100.0,
        },
        BankAccountCommand::Withdraw {
            effective_on: chrono::NaiveDate::from_ymd_opt(2025, 1, 2).unwrap(),
            amount: 40.0,
        },
    ] {
        cqrs.execute::<BankAccount>(
            &stream_id,
            replay::Metadata::default(),
            command,
            services,
            None,
        )
        .await
        .unwrap();
    }

    // The inline projection's view table must reflect the events (100 - 40 = 60).
    let stream_id_str = Into::<Urn>::into(stream_id.clone()).to_string();
    let balance: f64 =
        sqlx::query_scalar("SELECT balance FROM account_balances WHERE stream_id = $1")
            .bind(&stream_id_str)
            .fetch_one(&pg_pool)
            .await
            .expect("balance view row must exist");

    assert_eq!(balance, 60.0);

    // The projection version must be recorded in the registry.
    let version: i32 = sqlx::query_scalar("SELECT version FROM projections WHERE name = $1")
        .bind("account_balance_view")
        .fetch_one(&pg_pool)
        .await
        .expect("projection registry row must exist");

    assert_eq!(version, 1);
}

// ── Inline projection rollback (issue #59) ────────────────────────────────────

/// An inline projection that writes to its own table and then returns an error.
///
/// Used to assert that a failure inside `handle` rolls back the entire append
/// transaction — both the event rows and the projection-side writes.
struct FailingProjection;

impl replay_persistence::InlineProjection for FailingProjection {
    type Exec = sqlx::PgConnection;
    type Event = BankAccountEvent;

    fn name(&self) -> &str {
        "failing_projection"
    }

    fn version(&self) -> i32 {
        1
    }

    async fn init(&mut self, conn: &mut Self::Exec) -> Result<(), replay::Error> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS failing_projection_writes (
                stream_id text NOT NULL,
                amount    double precision NOT NULL
            )",
        )
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
            let amount = match &event.data {
                BankAccountEvent::Deposited { amount, .. } => *amount,
                BankAccountEvent::Withdrawn { amount, .. } => -*amount,
                BankAccountEvent::MonthlyClosed { .. } => 0.0,
            };

            sqlx::query(
                "INSERT INTO failing_projection_writes (stream_id, amount)
                 VALUES ($1, $2)",
            )
            .bind(event.stream_id.to_string())
            .bind(amount)
            .execute(&mut *conn)
            .await
            .map_err(replay_persistence::db_error)?;
        }

        Err(replay::Error::internal(
            "projection failure after writing (rollback test)",
        ))
    }
}

/// A projection handle error must roll back the entire append transaction.
///
/// The projection deliberately writes to its own table and then errors; this test
/// verifies neither event rows nor projection-side writes persist.
#[tokio::test]
async fn bank_account_inline_projection_failure_rolls_back_postgres_test() {
    let container = postgres::Postgres::default().start().await.unwrap();

    let host = container.get_host().await.unwrap().to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("Error getting docker port");

    let pg_pool = connect_to_postgres(host, port).await;

    sqlx::migrate!("./tests/migrations")
        .run(&pg_pool)
        .await
        .expect("Failed to run migrations");

    let store = replay_persistence::PostgresEventStore::builder(pg_pool.clone())
        .register(FailingProjection)
        .build()
        .await
        .expect("Failed to build store with failing projection");

    let cqrs = replay_persistence::Cqrs::new(store);

    let stream_id = BankAccountUrn::new("inline-proj-fail-1").unwrap();
    let stream_id_str = Into::<Urn>::into(stream_id.clone()).to_string();

    let result = cqrs
        .execute::<BankAccount>(
            &stream_id,
            replay::Metadata::default(),
            BankAccountCommand::Deposit {
                effective_on: chrono::NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
                amount: 100.0,
            },
            &(),
            None,
        )
        .await;

    assert_err!(result, "projection failure after writing (rollback test)");

    // Event append must be rolled back.
    let event_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM events WHERE stream_id = $1 AND aggregate_version IS NULL",
    )
    .bind(&stream_id_str)
    .fetch_one(&pg_pool)
    .await
    .expect("counting events must succeed");

    assert_eq!(event_count, 0);

    // Projection-side writes from handle must also be rolled back.
    let projection_write_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM failing_projection_writes WHERE stream_id = $1")
            .bind(&stream_id_str)
            .fetch_one(&pg_pool)
            .await
            .expect("counting projection writes must succeed");

    assert_eq!(projection_write_count, 0);
}

// ── Inline projection version drift rebuild (issue #60) ───────────────────────

/// A balance view that resets and rebuilds from history on a version bump.
///
/// `version` is a field so the test can register the "same" projection at two
/// different code versions to trigger a drift rebuild.
struct RebuildBalanceProjection {
    version: i32,
}

impl replay_persistence::InlineProjection for RebuildBalanceProjection {
    type Exec = sqlx::PgConnection;
    type Event = BankAccountEvent;

    fn name(&self) -> &str {
        "rebuild_balance_view"
    }

    fn version(&self) -> i32 {
        self.version
    }

    async fn init(&mut self, conn: &mut Self::Exec) -> Result<(), replay::Error> {
        sqlx::query(
            "CREATE TABLE IF NOT EXISTS rebuild_balances (
                stream_id text PRIMARY KEY,
                balance   double precision NOT NULL
            )",
        )
        .execute(conn)
        .await
        .map_err(replay_persistence::db_error)?;
        Ok(())
    }

    async fn reset(&mut self, conn: &mut Self::Exec) -> Result<(), replay::Error> {
        sqlx::query("DELETE FROM rebuild_balances")
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
            let delta = match &event.data {
                BankAccountEvent::Deposited { amount, .. } => *amount,
                BankAccountEvent::Withdrawn { amount, .. } => -*amount,
                BankAccountEvent::MonthlyClosed { .. } => continue,
            };

            sqlx::query(
                "INSERT INTO rebuild_balances (stream_id, balance)
                 VALUES ($1, $2)
                 ON CONFLICT (stream_id)
                 DO UPDATE SET balance = rebuild_balances.balance + EXCLUDED.balance",
            )
            .bind(event.stream_id.to_string())
            .bind(delta)
            .execute(&mut *conn)
            .await
            .map_err(replay_persistence::db_error)?;
        }

        Ok(())
    }
}

/// Re-registering a projection at a higher code version resets its view and rebuilds
/// it by replaying history, recording the new version.
#[tokio::test]
async fn bank_account_inline_projection_version_drift_rebuild_postgres_test() {
    let container = postgres::Postgres::default().start().await.unwrap();

    let host = container.get_host().await.unwrap().to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("Error getting docker port");

    let pg_pool = connect_to_postgres(host, port).await;

    sqlx::migrate!("./tests/migrations")
        .run(&pg_pool)
        .await
        .expect("Failed to run migrations");

    // Build at version 1 and append history (deposit 100, withdraw 40 → balance 60).
    let store = replay_persistence::PostgresEventStore::builder(pg_pool.clone())
        .register(RebuildBalanceProjection { version: 1 })
        .build()
        .await
        .expect("Failed to build store at projection version 1");

    let cqrs = replay_persistence::Cqrs::new(store);
    let stream_id = BankAccountUrn::new("rebuild-1").unwrap();
    let stream_id_str = Into::<Urn>::into(stream_id.clone()).to_string();

    for command in [
        BankAccountCommand::Deposit {
            effective_on: chrono::NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            amount: 100.0,
        },
        BankAccountCommand::Withdraw {
            effective_on: chrono::NaiveDate::from_ymd_opt(2025, 1, 2).unwrap(),
            amount: 40.0,
        },
    ] {
        cqrs.execute::<BankAccount>(&stream_id, replay::Metadata::default(), command, &(), None)
            .await
            .unwrap();
    }

    // Corrupt the view with a sentinel value. If the rebuild fails to reset, the replay
    // would accumulate on top of this (999 + 60 = 1059) instead of producing 60.
    sqlx::query(
        "INSERT INTO rebuild_balances (stream_id, balance) VALUES ($1, 999)
         ON CONFLICT (stream_id) DO UPDATE SET balance = 999",
    )
    .bind(&stream_id_str)
    .execute(&pg_pool)
    .await
    .expect("seeding sentinel balance must succeed");

    // Re-register the same projection at version 2: triggers reset + replay rebuild.
    let _store = replay_persistence::PostgresEventStore::builder(pg_pool.clone())
        .register(RebuildBalanceProjection { version: 2 })
        .build()
        .await
        .expect("Failed to rebuild store at projection version 2");

    // The view was reset (sentinel 999 gone) and rebuilt from history (100 - 40 = 60).
    let balance: f64 =
        sqlx::query_scalar("SELECT balance FROM rebuild_balances WHERE stream_id = $1")
            .bind(&stream_id_str)
            .fetch_one(&pg_pool)
            .await
            .expect("rebuilt balance row must exist");

    assert_eq!(balance, 60.0);

    // The registry records the new code version.
    let version: i32 = sqlx::query_scalar("SELECT version FROM projections WHERE name = $1")
        .bind("rebuild_balance_view")
        .fetch_one(&pg_pool)
        .await
        .expect("projection registry row must exist");

    assert_eq!(version, 2);
}

// ── Inline projection version guard + no-op fast path (issue #62) ─────────────

/// Building with a stored version NEWER than the code version is a hard error: a
/// rolled-back or older deploy must not run against a view built by newer code.
#[tokio::test]
async fn bank_account_inline_projection_stored_newer_than_code_errors_postgres_test() {
    let container = postgres::Postgres::default().start().await.unwrap();

    let host = container.get_host().await.unwrap().to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("Error getting docker port");

    let pg_pool = connect_to_postgres(host, port).await;

    sqlx::migrate!("./tests/migrations")
        .run(&pg_pool)
        .await
        .expect("Failed to run migrations");

    // First registration at version 2 records stored version = 2.
    let _store = replay_persistence::PostgresEventStore::builder(pg_pool.clone())
        .register(RebuildBalanceProjection { version: 2 })
        .build()
        .await
        .expect("Failed to build store at projection version 2");

    // Building at version 1 (older code than the stored view) must refuse to start.
    let result = replay_persistence::PostgresEventStore::builder(pg_pool.clone())
        .register(RebuildBalanceProjection { version: 1 })
        .build()
        .await;

    assert!(
        result.is_err(),
        "build must fail when stored version is newer than code version"
    );

    // The registry version is untouched: the guard does not downgrade the stored view.
    let version: i32 = sqlx::query_scalar("SELECT version FROM projections WHERE name = $1")
        .bind("rebuild_balance_view")
        .fetch_one(&pg_pool)
        .await
        .expect("projection registry row must exist");

    assert_eq!(version, 2);
}

/// Re-building at the SAME code version is a no-op: no reset, no replay, no side
/// effects. A sentinel seeded into the view survives the rebuild.
#[tokio::test]
async fn bank_account_inline_projection_same_version_is_noop_postgres_test() {
    let container = postgres::Postgres::default().start().await.unwrap();

    let host = container.get_host().await.unwrap().to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("Error getting docker port");

    let pg_pool = connect_to_postgres(host, port).await;

    sqlx::migrate!("./tests/migrations")
        .run(&pg_pool)
        .await
        .expect("Failed to run migrations");

    // Build at version 1 and append history (deposit 100, withdraw 40 → balance 60).
    let store = replay_persistence::PostgresEventStore::builder(pg_pool.clone())
        .register(RebuildBalanceProjection { version: 1 })
        .build()
        .await
        .expect("Failed to build store at projection version 1");

    let cqrs = replay_persistence::Cqrs::new(store);
    let stream_id = BankAccountUrn::new("noop-1").unwrap();
    let stream_id_str = Into::<Urn>::into(stream_id.clone()).to_string();

    for command in [
        BankAccountCommand::Deposit {
            effective_on: chrono::NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            amount: 100.0,
        },
        BankAccountCommand::Withdraw {
            effective_on: chrono::NaiveDate::from_ymd_opt(2025, 1, 2).unwrap(),
            amount: 40.0,
        },
    ] {
        cqrs.execute::<BankAccount>(&stream_id, replay::Metadata::default(), command, &(), None)
            .await
            .unwrap();
    }

    // Seed a sentinel value. A no-op rebuild leaves it untouched; a reset+replay would
    // wipe it and recompute 60.
    sqlx::query(
        "INSERT INTO rebuild_balances (stream_id, balance) VALUES ($1, 999)
         ON CONFLICT (stream_id) DO UPDATE SET balance = 999",
    )
    .bind(&stream_id_str)
    .execute(&pg_pool)
    .await
    .expect("seeding sentinel balance must succeed");

    // Re-register the same projection at the SAME version 1: must be a no-op.
    let _store = replay_persistence::PostgresEventStore::builder(pg_pool.clone())
        .register(RebuildBalanceProjection { version: 1 })
        .build()
        .await
        .expect("Failed to rebuild store at the same projection version");

    // The sentinel survives: no reset, no replay ran.
    let balance: f64 =
        sqlx::query_scalar("SELECT balance FROM rebuild_balances WHERE stream_id = $1")
            .bind(&stream_id_str)
            .fetch_one(&pg_pool)
            .await
            .expect("sentinel balance row must exist");

    assert_eq!(balance, 999.0);
}

// ── Inline projection first-registration backlog replay (issue #71) ───────────

/// Registering a brand-new projection against a store that already contains events
/// replays the existing backlog so the view catches up — not just future events.
#[tokio::test]
async fn bank_account_inline_projection_first_registration_replays_backlog_postgres_test() {
    let container = postgres::Postgres::default().start().await.unwrap();

    let host = container.get_host().await.unwrap().to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("Error getting docker port");

    let pg_pool = connect_to_postgres(host, port).await;

    sqlx::migrate!("./tests/migrations")
        .run(&pg_pool)
        .await
        .expect("Failed to run migrations");

    // Build a store with NO projections registered and append history
    // (deposit 100, withdraw 40 → balance 60). The projection does not exist yet.
    let store = replay_persistence::PostgresEventStore::builder(pg_pool.clone())
        .build()
        .await
        .expect("Failed to build store without projections");

    let cqrs = replay_persistence::Cqrs::new(store);
    let stream_id = BankAccountUrn::new("backlog-1").unwrap();
    let stream_id_str = Into::<Urn>::into(stream_id.clone()).to_string();

    for command in [
        BankAccountCommand::Deposit {
            effective_on: chrono::NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            amount: 100.0,
        },
        BankAccountCommand::Withdraw {
            effective_on: chrono::NaiveDate::from_ymd_opt(2025, 1, 2).unwrap(),
            amount: 40.0,
        },
    ] {
        cqrs.execute::<BankAccount>(&stream_id, replay::Metadata::default(), command, &(), None)
            .await
            .unwrap();
    }

    // Register the projection for the FIRST time against the store that already has
    // events. First registration must replay the backlog through `handle`.
    let _store = replay_persistence::PostgresEventStore::builder(pg_pool.clone())
        .register(RebuildBalanceProjection { version: 1 })
        .build()
        .await
        .expect("Failed to build store with first-time projection registration");

    // The view reflects the full backlog (100 - 40 = 60), not an empty/future-only state.
    let balance: f64 =
        sqlx::query_scalar("SELECT balance FROM rebuild_balances WHERE stream_id = $1")
            .bind(&stream_id_str)
            .fetch_one(&pg_pool)
            .await
            .expect("backlog-replayed balance row must exist");

    assert_eq!(balance, 60.0);

    // The registry records the projection version.
    let version: i32 = sqlx::query_scalar("SELECT version FROM projections WHERE name = $1")
        .bind("rebuild_balance_view")
        .fetch_one(&pg_pool)
        .await
        .expect("projection registry row must exist");

    assert_eq!(version, 1);
}

// ── Scoped-URN types used by the tests below ─────────────────────────────────

/// A branch that owns one or more bank accounts.
/// Namespace auto-derives to "branch" (strips Urn suffix → Branch → kebab → "branch").
#[derive(Clone, Serialize, Deserialize, Debug, Urn)]
struct BranchUrn(Urn);

// ── Scoped-URN tests (no Postgres required) ───────────────────────────────────

/// A bank-account URN can be scoped to a branch:
///   urn:bank-account:acct-1  +  urn:branch:london  →  urn:bank-account:acct-1@branch:london
#[test]
fn test_bank_account_scoped_to_branch() {
    let account = BankAccountUrn::new("acct-1").unwrap();
    let branch = BranchUrn::new("london").unwrap();

    let scoped: BankAccountUrn = account.at(&branch).unwrap();
    let scoped_urn: Urn = scoped.into();

    assert_eq!(
        scoped_urn.to_string(),
        "urn:bank-account:acct-1@branch:london"
    );
    assert_eq!(scoped_urn.nid(), "bank-account");
    assert_eq!(scoped_urn.nss(), "acct-1@branch:london");
}

/// Extracting the scope as `BranchUrn` validates the NID is "branch".
#[test]
fn test_extract_branch_from_scoped_account() {
    let scoped = BankAccountUrn::new("acct-1@branch:london").unwrap();

    let branch: BranchUrn = scoped.extract_scope::<BranchUrn>().unwrap();

    assert_eq!(branch.0.nid(), "branch");
    assert_eq!(branch.0.nss(), "london");
}

/// Extracting with the wrong type fails because the NID doesn't match.
#[test]
fn test_extract_scope_wrong_nid_fails() {
    // scope is urn:branch:london, but we ask for BankAccountUrn (nid="bank-account")
    let scoped = BankAccountUrn::new("acct-1@branch:london").unwrap();
    let err = scoped.extract_scope::<BankAccountUrn>().unwrap_err();
    assert!(err.to_string().contains("NID mismatch"));
}

/// `at` followed by `extract_scope` must round-trip to the original scope URN.
#[test]
fn test_scope_round_trip() {
    let account = BankAccountUrn::new("acct-42").unwrap();
    let branch = BranchUrn::new("paris").unwrap();

    let scoped: BankAccountUrn = account.at(&branch).unwrap();
    let extracted: BranchUrn = scoped.extract_scope::<BranchUrn>().unwrap();

    assert_eq!(extracted, branch);
}

/// Attempting to scope an already-scoped stream must fail.
#[test]
fn test_cannot_double_scope_account() {
    let scoped = BankAccountUrn::new("acct-1@branch:london").unwrap();
    let another_branch = BranchUrn::new("berlin").unwrap();

    let err = scoped.at(&another_branch).unwrap_err();
    assert!(err.to_string().contains("already scoped"));
}

/// A scoped URN survives serialisation to a string and back.
#[test]
fn test_scoped_urn_string_round_trip() {
    let account = BankAccountUrn::new("acct-7").unwrap();
    let branch = BranchUrn::new("tokyo").unwrap();
    let scoped_str: String = {
        let u: Urn = account.at(&branch).unwrap().into();
        u.to_string()
    };

    assert_eq!(scoped_str, "urn:bank-account:acct-7@branch:tokyo");

    // Reconstruct from the string and extract scope as the typed BranchUrn.
    let reparsed = BankAccountUrn::new("acct-7@branch:tokyo").unwrap();
    let branch: BranchUrn = reparsed.extract_scope::<BranchUrn>().unwrap();
    assert_eq!(branch.0.nid(), "branch");
    assert_eq!(branch.0.nss(), "tokyo");
}

/// Scoping with an already-scoped branch URN must fail.
#[test]
fn test_at_rejects_scoped_scope_urn() {
    let account = BankAccountUrn::new("acct-1").unwrap();
    // Construct a BranchUrn whose NSS itself contains '@' — simulating a scoped scope
    let already_scoped_branch = BranchUrn(Urn::from_str("urn:branch:london@region:uk").unwrap());
    let err = account.at(&already_scoped_branch).unwrap_err();
    assert!(err.to_string().contains("scope URN is already scoped"));
}

// ── README global-position example: live query vs inline projection ──────────

/// The two read-model strategies from the README walkthrough must agree.
///
/// The same `User` + `BankAccount` history is driven against Postgres while the
/// inline [`GlobalPositionProjection`](global_position::GlobalPositionProjection)
/// materialises a user's global position into SQL tables. Reading those tables
/// must yield exactly the same name and total balance as folding the history live
/// with [`GlobalPositionQuery`](global_position::GlobalPositionQuery).
#[tokio::test]
async fn global_position_live_query_and_inline_projection_agree_postgres_test() {
    use global_position::{
        BankAccount, BankAccountCommand, BankAccountUrn as AccountUrn, GlobalPositionEvent,
        GlobalPositionProjection, GlobalPositionQuery, User, UserCommand, UserUrn,
    };

    let container = postgres::Postgres::default().start().await.unwrap();

    let host = container.get_host().await.unwrap().to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("Error getting docker port");

    let pg_pool = connect_to_postgres(host, port).await;

    sqlx::migrate!("./tests/migrations")
        .run(&pg_pool)
        .await
        .expect("Failed to run migrations");

    // The projection's `init` creates the `gp_account_owner` / `gp_user_position` tables.
    let store = replay_persistence::PostgresEventStore::builder(pg_pool.clone())
        .register(GlobalPositionProjection)
        .build()
        .await
        .expect("Failed to build store with the global-position projection");

    let cqrs = replay_persistence::Cqrs::new(store);

    let alice = UserUrn::new("alice").unwrap();
    cqrs.execute::<User>(
        &alice,
        replay::Metadata::default(),
        UserCommand::Register {
            name: "Alice".to_string(),
        },
        &(),
        None,
    )
    .await
    .unwrap();

    let checking = AccountUrn::new("alice-checking").unwrap();
    let savings = AccountUrn::new("alice-savings").unwrap();

    for account in [&checking, &savings] {
        cqrs.execute::<BankAccount>(
            account,
            replay::Metadata::default(),
            BankAccountCommand::OpenAccount {
                owner: alice.clone(),
            },
            &(),
            None,
        )
        .await
        .unwrap();
    }

    for (account, command) in [
        (&checking, BankAccountCommand::Deposit { amount: 1_000.0 }),
        (&checking, BankAccountCommand::Withdraw { amount: 250.0 }),
        (&savings, BankAccountCommand::Deposit { amount: 500.0 }),
    ] {
        cqrs.execute::<BankAccount>(account, replay::Metadata::default(), command, &(), None)
            .await
            .unwrap();
    }

    // Read the materialised inline projection.
    let alice_urn = Into::<Urn>::into(alice.clone()).to_string();
    let (inline_name, inline_balance): (String, f64) =
        sqlx::query_as("SELECT name, total_balance FROM gp_user_position WHERE user_urn = $1")
            .bind(&alice_urn)
            .fetch_one(&pg_pool)
            .await
            .expect("inline projection must have a row for alice");

    // Fold the same history live.
    let mut live = GlobalPositionQuery::for_user(alice.clone());
    cqrs.run_query::<_, GlobalPositionEvent>(&mut live)
        .await
        .unwrap();

    // Both strategies agree, and agree with the hand-computed total (1000 - 250 + 500).
    assert_eq!(inline_name, "Alice");
    assert_eq!(inline_balance, 1_250.0);
    assert_eq!(live.position().name, inline_name);
    assert_eq!(live.position().total_balance, inline_balance);
}

// ── Policies: checkpointed background reactions (issue #77) ───────────────────

/// A toy policy: whenever a deposit lands, charge a flat fee by issuing a
/// `Withdraw` command back to the same account. Because `Withdrawn` is not a
/// `Deposited`, the reaction does not feed itself — no loop.
struct WithdrawFeePolicy {
    fee: f64,
}

struct WithdrawFeePolicyStartAtNow {
    fee: f64,
}

struct WithdrawFeePolicyStartAtBeginning {
    fee: f64,
}

struct ChargeFeeWithCausationPolicy {
    source: BankAccountUrn,
    target: IdempotentFeeAccountUrn,
    fee: f64,
}

impl replay_persistence::Policy for WithdrawFeePolicy {
    type Event = BankAccountEvent;

    fn name(&self) -> &str {
        "withdraw_fee_policy"
    }

    fn start_at(&self) -> replay_persistence::StartAt {
        replay_persistence::StartAt::Beginning
    }

    fn react(&self, event: &PersistedEvent<Self::Event>) -> Vec<replay_persistence::Dispatch> {
        match &event.data {
            BankAccountEvent::Deposited { operation_date, .. } => {
                let account = BankAccountUrn::try_from(event.stream_id.clone())
                    .expect("deposit events live on bank-account streams");
                vec![replay_persistence::Dispatch::to::<BankAccount>(
                    account,
                    BankAccountCommand::Withdraw {
                        effective_on: *operation_date,
                        amount: self.fee,
                    },
                )
                .with_metadata(replay::Metadata::new(serde_json::json!({
                    "user_id": "policy-runner",
                    "related_aggregate_id": event.stream_id.to_string(),
                })))]
            }
            _ => Vec::new(),
        }
    }
}

impl replay_persistence::Policy for WithdrawFeePolicyStartAtNow {
    type Event = BankAccountEvent;

    fn name(&self) -> &str {
        "withdraw_fee_policy_start_at_now"
    }

    fn start_at(&self) -> replay_persistence::StartAt {
        replay_persistence::StartAt::Now
    }

    fn react(&self, event: &PersistedEvent<Self::Event>) -> Vec<replay_persistence::Dispatch> {
        match &event.data {
            BankAccountEvent::Deposited { operation_date, .. } => {
                let account = BankAccountUrn::try_from(event.stream_id.clone())
                    .expect("deposit events live on bank-account streams");
                vec![replay_persistence::Dispatch::to::<BankAccount>(
                    account,
                    BankAccountCommand::Withdraw {
                        effective_on: *operation_date,
                        amount: self.fee,
                    },
                )]
            }
            _ => Vec::new(),
        }
    }
}

impl replay_persistence::Policy for WithdrawFeePolicyStartAtBeginning {
    type Event = BankAccountEvent;

    fn name(&self) -> &str {
        "withdraw_fee_policy_start_at_beginning"
    }

    fn start_at(&self) -> replay_persistence::StartAt {
        replay_persistence::StartAt::Beginning
    }

    fn react(&self, event: &PersistedEvent<Self::Event>) -> Vec<replay_persistence::Dispatch> {
        match &event.data {
            BankAccountEvent::Deposited { operation_date, .. } => {
                let account = BankAccountUrn::try_from(event.stream_id.clone())
                    .expect("deposit events live on bank-account streams");
                vec![replay_persistence::Dispatch::to::<BankAccount>(
                    account,
                    BankAccountCommand::Withdraw {
                        effective_on: *operation_date,
                        amount: self.fee,
                    },
                )]
            }
            _ => Vec::new(),
        }
    }
}

impl replay_persistence::Policy for ChargeFeeWithCausationPolicy {
    type Event = BankAccountEvent;

    fn name(&self) -> &str {
        "charge_fee_with_causation_policy"
    }

    fn stream_filter(&self) -> replay_persistence::StreamFilter {
        replay_persistence::StreamFilter::with_stream_id::<BankAccount>(&self.source)
    }

    fn start_at(&self) -> replay_persistence::StartAt {
        replay_persistence::StartAt::Now
    }

    fn react(&self, event: &PersistedEvent<Self::Event>) -> Vec<replay_persistence::Dispatch> {
        match &event.data {
            BankAccountEvent::Deposited { .. } => {
                vec![replay_persistence::Dispatch::to::<IdempotentFeeAccount>(
                    self.target.clone(),
                    IdempotentFeeAccountCommand::ChargeFee {
                        amount: self.fee,
                        causation_event_id: event.id,
                    },
                )]
            }
            _ => Vec::new(),
        }
    }
}

/// Pure `react` unit test — no database. Asserts the policy returns exactly one
/// dispatch, targeting the `BankAccount` aggregate, on a deposit, and nothing on
/// other events.
#[test]
fn withdraw_fee_policy_react_is_pure() {
    use std::any::TypeId;

    let policy = WithdrawFeePolicy { fee: 5.0 };
    let stream_id: Urn = BankAccountUrn::new("pure-react-1").unwrap().into();

    let deposit = PersistedEvent {
        id: uuid::Uuid::new_v4(),
        data: BankAccountEvent::Deposited {
            operation_date: chrono::NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            amount: 100.0,
        },
        stream_id: stream_id.clone(),
        r#type: "Deposited".to_string(),
        version: 1,
        created: chrono::Utc::now(),
        metadata: replay::Metadata::default(),
        aggregate_version: None,
    };

    let dispatches = replay_persistence::Policy::react(&policy, &deposit);
    assert_eq!(dispatches.len(), 1);
    assert_eq!(dispatches[0].target(), TypeId::of::<BankAccount>());

    let withdrawal = PersistedEvent {
        data: BankAccountEvent::Withdrawn {
            operation_date: chrono::NaiveDate::from_ymd_opt(2025, 1, 2).unwrap(),
            amount: 5.0,
        },
        r#type: "Withdrawn".to_string(),
        ..deposit
    };

    assert!(replay_persistence::Policy::react(&policy, &withdrawal).is_empty());
}

/// End-to-end walking skeleton: append a deposit, drain the policy once, and
/// assert the reaction's `Withdrawn` event landed (balance dropped by the fee)
/// and the policy cursor advanced past the triggering event.
#[tokio::test]
async fn withdraw_fee_policy_drain_postgres_test() {
    let container = postgres::Postgres::default().start().await.unwrap();

    let host = container.get_host().await.unwrap().to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("Error getting docker port");

    let pg_pool = connect_to_postgres(host, port).await;

    sqlx::migrate!("./tests/migrations")
        .run(&pg_pool)
        .await
        .expect("Failed to run migrations");

    let store = replay_persistence::PostgresEventStore::new(pg_pool.clone());
    let cqrs = replay_persistence::Cqrs::new(store);

    let account = BankAccountUrn::new("policy-drain-1").unwrap();

    // Append the triggering event.
    cqrs.execute::<BankAccount>(
        &account,
        replay::Metadata::default(),
        BankAccountCommand::Deposit {
            effective_on: chrono::NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            amount: 100.0,
        },
        &(),
        None,
    )
    .await
    .unwrap();

    let runner = replay_persistence::PolicyRunner::builder(cqrs.clone())
        .register_services::<BankAccount>(())
        .register_policy(WithdrawFeePolicy { fee: 5.0 })
        .build();

    let executed = runner.drain().await.expect("drain must succeed");
    assert_eq!(
        executed, 1,
        "the deposit must trigger exactly one withdrawal"
    );

    // The reaction's command landed: balance is 100 - 5 = 95.
    let account_state = cqrs.fetch_aggregate::<BankAccount>(&account).await.unwrap();
    assert_eq!(account_state.balance, 95.0);

    // The resulting command metadata includes both causation and policy-provided
    // context fields.
    let withdrawal_meta: serde_json::Value = sqlx::query_scalar(
        "SELECT metadata FROM events WHERE stream_id = $1 AND type = $2 ORDER BY version DESC LIMIT 1",
    )
    .bind(Into::<Urn>::into(account.clone()).to_string())
    .bind("Withdrawn")
    .fetch_one(&pg_pool)
    .await
    .expect("withdrawal event must exist");

    assert_eq!(withdrawal_meta["user_id"], "policy-runner");
    assert_eq!(
        withdrawal_meta["related_aggregate_id"],
        Into::<Urn>::into(account.clone()).to_string()
    );
    assert_eq!(
        withdrawal_meta["causation"]["policy"],
        "withdraw_fee_policy"
    );

    // The cursor advanced past the triggering event (global_position 1).
    let cursor: i64 = sqlx::query_scalar("SELECT position FROM policy_cursors WHERE name = $1")
        .bind("withdraw_fee_policy")
        .fetch_one(&pg_pool)
        .await
        .expect("cursor row must exist after drain");
    assert_eq!(cursor, 1);

    // A second drain re-scans only the new `Withdrawn` event, which the policy
    // ignores, so no further commands are issued and the cursor moves to 2.
    let executed_again = runner.drain().await.expect("second drain must succeed");
    assert_eq!(executed_again, 0);

    let cursor_after: i64 =
        sqlx::query_scalar("SELECT position FROM policy_cursors WHERE name = $1")
            .bind("withdraw_fee_policy")
            .fetch_one(&pg_pool)
            .await
            .expect("cursor row must exist after second drain");
    assert_eq!(cursor_after, 2);
}

#[tokio::test]
async fn policy_start_at_now_ignores_prior_history_postgres_test() {
    let container = postgres::Postgres::default().start().await.unwrap();
    let host = container.get_host().await.unwrap().to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("Error getting docker port");
    let pg_pool = connect_to_postgres(host, port).await;

    sqlx::migrate!("./tests/migrations")
        .run(&pg_pool)
        .await
        .expect("Failed to run migrations");

    let store = replay_persistence::PostgresEventStore::new(pg_pool.clone());
    let cqrs = replay_persistence::Cqrs::new(store);
    let account = BankAccountUrn::new("policy-start-now-1").unwrap();

    // Pre-existing history before policy registration.
    cqrs.execute::<BankAccount>(
        &account,
        replay::Metadata::default(),
        BankAccountCommand::Deposit {
            effective_on: chrono::NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            amount: 100.0,
        },
        &(),
        None,
    )
    .await
    .unwrap();

    let runner = replay_persistence::PolicyRunner::builder(cqrs.clone())
        .register_services::<BankAccount>(())
        .register_policy(WithdrawFeePolicyStartAtNow { fee: 5.0 })
        .build();

    // First drain should not backfill pre-existing event.
    let executed = runner.drain().await.expect("drain must succeed");
    assert_eq!(executed, 0);

    // New events appended after registration should be processed.
    cqrs.execute::<BankAccount>(
        &account,
        replay::Metadata::default(),
        BankAccountCommand::Deposit {
            effective_on: chrono::NaiveDate::from_ymd_opt(2025, 1, 2).unwrap(),
            amount: 100.0,
        },
        &(),
        None,
    )
    .await
    .unwrap();

    let executed_after = runner.drain().await.expect("second drain must succeed");
    assert_eq!(executed_after, 1);
}

#[tokio::test]
async fn policy_start_at_beginning_backfills_history_postgres_test() {
    let container = postgres::Postgres::default().start().await.unwrap();
    let host = container.get_host().await.unwrap().to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("Error getting docker port");
    let pg_pool = connect_to_postgres(host, port).await;

    sqlx::migrate!("./tests/migrations")
        .run(&pg_pool)
        .await
        .expect("Failed to run migrations");

    let store = replay_persistence::PostgresEventStore::new(pg_pool.clone());
    let cqrs = replay_persistence::Cqrs::new(store);
    let account = BankAccountUrn::new("policy-start-beginning-1").unwrap();

    cqrs.execute::<BankAccount>(
        &account,
        replay::Metadata::default(),
        BankAccountCommand::Deposit {
            effective_on: chrono::NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            amount: 100.0,
        },
        &(),
        None,
    )
    .await
    .unwrap();
    cqrs.execute::<BankAccount>(
        &account,
        replay::Metadata::default(),
        BankAccountCommand::Deposit {
            effective_on: chrono::NaiveDate::from_ymd_opt(2025, 1, 2).unwrap(),
            amount: 100.0,
        },
        &(),
        None,
    )
    .await
    .unwrap();

    let runner = replay_persistence::PolicyRunner::builder(cqrs.clone())
        .register_services::<BankAccount>(())
        .register_policy(WithdrawFeePolicyStartAtBeginning { fee: 5.0 })
        .build();

    let executed = runner.drain().await.expect("drain must succeed");
    assert_eq!(executed, 2);
}

#[tokio::test]
async fn policy_code_change_never_rewinds_cursor_postgres_test() {
    struct WithdrawFeePolicyNoRewindV1;
    struct WithdrawFeePolicyNoRewindV2;

    impl replay_persistence::Policy for WithdrawFeePolicyNoRewindV1 {
        type Event = BankAccountEvent;

        fn name(&self) -> &str {
            "withdraw_fee_policy_no_rewind"
        }

        fn start_at(&self) -> replay_persistence::StartAt {
            replay_persistence::StartAt::Now
        }

        fn react(&self, event: &PersistedEvent<Self::Event>) -> Vec<replay_persistence::Dispatch> {
            match &event.data {
                BankAccountEvent::Deposited { operation_date, .. } => {
                    let account = BankAccountUrn::try_from(event.stream_id.clone())
                        .expect("deposit events live on bank-account streams");
                    vec![replay_persistence::Dispatch::to::<BankAccount>(
                        account,
                        BankAccountCommand::Withdraw {
                            effective_on: *operation_date,
                            amount: 5.0,
                        },
                    )]
                }
                _ => Vec::new(),
            }
        }
    }

    impl replay_persistence::Policy for WithdrawFeePolicyNoRewindV2 {
        type Event = BankAccountEvent;

        fn name(&self) -> &str {
            "withdraw_fee_policy_no_rewind"
        }

        // Simulate a code change that attempts to switch bootstrap mode.
        fn start_at(&self) -> replay_persistence::StartAt {
            replay_persistence::StartAt::Beginning
        }

        fn react(&self, event: &PersistedEvent<Self::Event>) -> Vec<replay_persistence::Dispatch> {
            match &event.data {
                BankAccountEvent::Deposited { operation_date, .. } => {
                    let account = BankAccountUrn::try_from(event.stream_id.clone())
                        .expect("deposit events live on bank-account streams");
                    vec![replay_persistence::Dispatch::to::<BankAccount>(
                        account,
                        BankAccountCommand::Withdraw {
                            effective_on: *operation_date,
                            amount: 7.0,
                        },
                    )]
                }
                _ => Vec::new(),
            }
        }
    }

    let container = postgres::Postgres::default().start().await.unwrap();
    let host = container.get_host().await.unwrap().to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("Error getting docker port");
    let pg_pool = connect_to_postgres(host, port).await;

    sqlx::migrate!("./tests/migrations")
        .run(&pg_pool)
        .await
        .expect("Failed to run migrations");

    let store = replay_persistence::PostgresEventStore::new(pg_pool.clone());
    let cqrs = replay_persistence::Cqrs::new(store);
    let account = BankAccountUrn::new("policy-no-rewind-1").unwrap();

    // Pre-existing event before first policy registration.
    cqrs.execute::<BankAccount>(
        &account,
        replay::Metadata::default(),
        BankAccountCommand::Deposit {
            effective_on: chrono::NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            amount: 100.0,
        },
        &(),
        None,
    )
    .await
    .unwrap();

    let runner_v1 = replay_persistence::PolicyRunner::builder(cqrs.clone())
        .register_services::<BankAccount>(())
        .register_policy(WithdrawFeePolicyNoRewindV1)
        .build();

    // First run bootstraps at Now, so no backfill.
    assert_eq!(runner_v1.drain().await.unwrap(), 0);

    // Append a new event that should be processed once.
    cqrs.execute::<BankAccount>(
        &account,
        replay::Metadata::default(),
        BankAccountCommand::Deposit {
            effective_on: chrono::NaiveDate::from_ymd_opt(2025, 1, 2).unwrap(),
            amount: 100.0,
        },
        &(),
        None,
    )
    .await
    .unwrap();

    // Simulate redeploy/code change with same policy name but different start_at.
    let runner_v2 = replay_persistence::PolicyRunner::builder(cqrs.clone())
        .register_services::<BankAccount>(())
        .register_policy(WithdrawFeePolicyNoRewindV2)
        .build();

    // Must process only the new event; the original pre-registration event
    // must not be replayed.
    assert_eq!(runner_v2.drain().await.unwrap(), 1);
}

#[tokio::test]
async fn policy_daemon_polls_and_reacts_without_manual_drain_postgres_test() {
    let container = postgres::Postgres::default().start().await.unwrap();
    let host = container.get_host().await.unwrap().to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("Error getting docker port");
    let pg_pool = connect_to_postgres(host, port).await;

    sqlx::migrate!("./tests/migrations")
        .run(&pg_pool)
        .await
        .expect("Failed to run migrations");

    let store = replay_persistence::PostgresEventStore::new(pg_pool.clone());
    let cqrs = replay_persistence::Cqrs::new(store);
    let account = BankAccountUrn::new("policy-daemon-1").unwrap();

    let runner = std::sync::Arc::new(
        replay_persistence::PolicyRunner::builder(cqrs.clone())
            .register_services::<BankAccount>(())
            .register_policy(WithdrawFeePolicyStartAtBeginning { fee: 5.0 })
            .build(),
    );

    // Start background polling before appending the triggering event.
    let daemon = runner
        .clone()
        .start_polling(std::time::Duration::from_millis(50));

    cqrs.execute::<BankAccount>(
        &account,
        replay::Metadata::default(),
        BankAccountCommand::Deposit {
            effective_on: chrono::NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            amount: 100.0,
        },
        &(),
        None,
    )
    .await
    .unwrap();

    // Wait until the policy-generated withdrawal appears, without manual drain.
    let mut reacted = false;
    for _ in 0..20 {
        let account_state = cqrs.fetch_aggregate::<BankAccount>(&account).await.unwrap();
        if (account_state.balance - 95.0).abs() < f64::EPSILON {
            reacted = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    assert!(
        reacted,
        "expected policy daemon to react and apply withdrawal without manual drain"
    );

    daemon.shutdown().await;
}

#[tokio::test]
async fn policy_duplicate_delivery_is_absorbed_by_causation_guard_postgres_test() {
    let container = postgres::Postgres::default().start().await.unwrap();
    let host = container.get_host().await.unwrap().to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("Error getting docker port");
    let pg_pool = connect_to_postgres(host, port).await;

    sqlx::migrate!("./tests/migrations")
        .run(&pg_pool)
        .await
        .expect("Failed to run migrations");

    let store = replay_persistence::PostgresEventStore::new(pg_pool.clone());
    let cqrs = replay_persistence::Cqrs::new(store);

    let source = BankAccountUrn::new("dup-source-1").unwrap();
    let target = IdempotentFeeAccountUrn::new("dup-target-1").unwrap();

    // Open fee account with a known baseline balance.
    cqrs.execute::<IdempotentFeeAccount>(
        &target,
        replay::Metadata::default(),
        IdempotentFeeAccountCommand::Open { balance: 100.0 },
        &(),
        None,
    )
    .await
    .unwrap();

    let runner = replay_persistence::PolicyRunner::builder(cqrs.clone())
        .register_services::<IdempotentFeeAccount>(())
        .register_policy(ChargeFeeWithCausationPolicy {
            source: source.clone(),
            target: target.clone(),
            fee: 5.0,
        })
        .build();

    // Bootstrap StartAt::Now cursor at current head (after target account open,
    // before source deposit exists).
    assert_eq!(runner.drain().await.unwrap(), 0);

    // Trigger policy reaction with one source deposit (after policy
    // registration so StartAt::Now can pick it up).
    cqrs.execute::<BankAccount>(
        &source,
        replay::Metadata::default(),
        BankAccountCommand::Deposit {
            effective_on: chrono::NaiveDate::from_ymd_opt(2025, 1, 1).unwrap(),
            amount: 100.0,
        },
        &(),
        None,
    )
    .await
    .unwrap();

    // First delivery applies one fee charge.
    assert_eq!(runner.drain().await.unwrap(), 1);
    let after_first = cqrs
        .fetch_aggregate::<IdempotentFeeAccount>(&target)
        .await
        .unwrap();
    assert_eq!(after_first.balance, 95.0);

    let source_event_id: uuid::Uuid = sqlx::query_scalar(
        "SELECT id FROM events WHERE stream_id = $1 AND type = $2 ORDER BY version ASC LIMIT 1",
    )
    .bind(Into::<Urn>::into(source.clone()).to_string())
    .bind("Deposited")
    .fetch_one(&pg_pool)
    .await
    .expect("source deposited event must exist");

    let first_fee_meta: serde_json::Value = sqlx::query_scalar(
        "SELECT metadata FROM events WHERE stream_id = $1 AND type = $2 ORDER BY version ASC LIMIT 1",
    )
    .bind(Into::<Urn>::into(target.clone()).to_string())
    .bind("FeeCharged")
    .fetch_one(&pg_pool)
    .await
    .expect("first fee event must exist");

    // Contract: policy-emitted event metadata carries causation identity.
    assert_eq!(
        first_fee_meta["causation"]["event_id"],
        source_event_id.to_string()
    );

    // Simulate redelivery by rewinding cursor below the source event.
    sqlx::query("UPDATE policy_cursors SET position = 1 WHERE name = $1")
        .bind("charge_fee_with_causation_policy")
        .execute(&pg_pool)
        .await
        .expect("cursor rewind update must succeed");

    // Second delivery issues the same causation id, and the aggregate absorbs
    // it as a no-op.
    assert_eq!(runner.drain().await.unwrap(), 1);

    let after_second = cqrs
        .fetch_aggregate::<IdempotentFeeAccount>(&target)
        .await
        .unwrap();
    assert_eq!(after_second.balance, 95.0);

    let fee_event_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE stream_id = $1 AND type = $2")
            .bind(Into::<Urn>::into(target).to_string())
            .bind("FeeCharged")
            .fetch_one(&pg_pool)
            .await
            .expect("fee event count query must succeed");
    assert_eq!(fee_event_count, 1);
}

// ── Compaction feed (issue #81) ───────────────────────────────────────────────

/// Policy correctly walks the true log across a compaction boundary.
///
/// Scenario (ADR 0004):
///   1. Two real events are appended: Deposited(100) at gp=1, MonthlyClosed at gp=2.
///   2. Compact is called: originals archived (compacted_snapshot=false), one synthetic
///      MonthlyClosed snapshot inserted at gp=3 (compacted_snapshot=true).
///   3. One more real event is appended after compaction: Deposited(200) at gp=4.
///   4. A policy with `StartAt::Beginning` is registered and drained.
///
/// Acceptance criteria (from the issue):
///   - Policy processes gp=1 (Deposited) — pre-compaction original       → 1 reaction
///   - Policy skips   gp=2 (MonthlyClosed) — no matching reaction         → 0 reactions
///   - Policy skips   gp=3 (synthetic snapshot) without cursor deadlock   → 0 reactions
///   - Policy processes gp=4 (Deposited) — post-compaction real event     → 1 reaction
///   - Total dispatches = 2 (not 3, which would indicate the synthetic was
///     mis-delivered, or 1, which would indicate the cursor stalled at gp=2).
#[tokio::test]
async fn policy_lagging_behind_compaction_skips_synthetic_snapshot_postgres_test() {
    let container = postgres::Postgres::default().start().await.unwrap();
    let host = container.get_host().await.unwrap().to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("Error getting docker port");
    let pg_pool = connect_to_postgres(host, port).await;

    sqlx::migrate!("./tests/migrations")
        .run(&pg_pool)
        .await
        .expect("Failed to run migrations");

    let store = replay_persistence::PostgresEventStore::new(pg_pool.clone());
    let cqrs = replay_persistence::Cqrs::new(store);
    let account = BankAccountUrn::new("compaction-feed-1").unwrap();
    let meta = replay::Metadata::default();
    let jan_1 = chrono::NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let feb_1 = chrono::NaiveDate::from_ymd_opt(2026, 2, 1).unwrap();

    // ── Pre-compaction: Deposited(100) + MonthlyClosed ────────────────────────
    cqrs.execute::<BankAccount>(
        &account,
        meta.clone(),
        BankAccountCommand::Deposit {
            effective_on: jan_1,
            amount: 100.0,
        },
        &(),
        None,
    )
    .await
    .unwrap();

    cqrs.execute::<BankAccount>(
        &account,
        meta.clone(),
        BankAccountCommand::CloseMonth { month: jan_1 },
        &(),
        None,
    )
    .await
    .unwrap();

    // ── Compact: archives originals; inserts synthetic snapshot ───────────────
    let aggregate = cqrs.fetch_aggregate::<BankAccount>(&account).await.unwrap();
    cqrs.compact(&aggregate, meta.clone()).await.unwrap();

    // Confirm the synthetic row was written with compacted_snapshot = true.
    let synthetic_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE compacted_snapshot = TRUE")
            .fetch_one(&pg_pool)
            .await
            .expect("synthetic row count query must succeed");
    assert_eq!(
        synthetic_count, 1,
        "compact must produce exactly one synthetic row (the MonthlyClosed snapshot)"
    );

    // ── Post-compaction: append one more real event ───────────────────────────
    cqrs.execute::<BankAccount>(
        &account,
        meta.clone(),
        BankAccountCommand::Deposit {
            effective_on: feb_1,
            amount: 200.0,
        },
        &(),
        None,
    )
    .await
    .unwrap();

    // At this point the global_position sequence is:
    //   gp=1  Deposited(100)     real, archived (compacted_snapshot=false)
    //   gp=2  MonthlyClosed      real, archived (compacted_snapshot=false)
    //   gp=3  MonthlyClosed snap synthetic      (compacted_snapshot=true)
    //   gp=4  Deposited(200)     real, live      (compacted_snapshot=false)

    // ── Register a lagging policy that starts from the beginning ─────────────
    // `WithdrawFeePolicyStartAtBeginning` starts at gp=0, reacts to Deposited.
    let runner = replay_persistence::PolicyRunner::builder(cqrs.clone())
        .register_services::<BankAccount>(())
        .register_policy(WithdrawFeePolicyStartAtBeginning { fee: 5.0 })
        .build();

    let dispatches = runner.drain().await.expect("drain must succeed");

    // Two dispatches: gp=1 (Deposited pre-compaction) + gp=4 (Deposited post-compaction).
    // If dispatches == 3 the synthetic was mis-delivered.
    // If dispatches < 2 the cursor stalled before gp=4 (gap-detection broken).
    assert_eq!(
        dispatches, 2,
        "policy must react to both real Deposited events and skip the synthetic snapshot"
    );

    // The cursor must have advanced past the synthetic (gp=3) and reached gp=4.
    let final_cursor: i64 = sqlx::query_scalar(
        "SELECT position FROM policy_cursors WHERE name = 'withdraw_fee_policy_start_at_beginning'",
    )
    .fetch_one(&pg_pool)
    .await
    .expect("cursor must exist after drain");

    assert_eq!(
        final_cursor, 4,
        "cursor must have advanced past the synthetic snapshot to the final real event"
    );
}

// ── Single active runner via pg_advisory_lock (issue #82) ────────────────────

/// A minimal policy used only by the advisory-lock tests.
struct SingleRunnerPolicy;

impl replay_persistence::Policy for SingleRunnerPolicy {
    type Event = BankAccountEvent;

    fn name(&self) -> &str {
        "single_runner_policy"
    }

    fn start_at(&self) -> replay_persistence::StartAt {
        replay_persistence::StartAt::Now
    }

    /// React to every `Deposited` event by issuing a `Withdraw` of $1.
    /// `Withdrawn` is not a `Deposited`, so the reaction does not feed itself.
    fn react(&self, event: &PersistedEvent<Self::Event>) -> Vec<replay_persistence::Dispatch> {
        match &event.data {
            BankAccountEvent::Deposited { operation_date, .. } => {
                let account = BankAccountUrn::try_from(event.stream_id.clone())
                    .expect("deposit events live on bank-account streams");
                vec![replay_persistence::Dispatch::to::<BankAccount>(
                    account,
                    BankAccountCommand::Withdraw {
                        effective_on: *operation_date,
                        amount: 1.0,
                    },
                )]
            }
            _ => Vec::new(),
        }
    }
}

/// Two runners competing for the same policy do not double-process events, and
/// the surviving runner resumes from the stored cursor after the leader stops.
///
/// Scenario:
///   1. Two `PolicyRunner` instances are created with identical `SingleRunnerPolicy`.
///   2. Both start polling. Exactly one acquires the advisory lock and becomes
///      the leader; the other stands by.
///   3. Two deposits are appended. Only the leader reacts → exactly 2 Withdrawn
///      events. If both ran, we would see 4.
///   4. The first daemon is shut down (explicit `pg_advisory_unlock`). The standby
///      acquires the lock and resumes from the stored cursor.
///   5. Two more deposits are appended. The new leader reacts → 2 additional
///      Withdrawn events. Total = 4.
#[tokio::test]
async fn policy_single_active_runner_via_advisory_lock_postgres_test() {
    use std::time::Duration;

    let container = postgres::Postgres::default().start().await.unwrap();
    let host = container.get_host().await.unwrap().to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("Error getting docker port");
    let pg_pool = connect_to_postgres(host, port).await;

    sqlx::migrate!("./tests/migrations")
        .run(&pg_pool)
        .await
        .expect("Failed to run migrations");

    let store = replay_persistence::PostgresEventStore::new(pg_pool.clone());
    let cqrs = replay_persistence::Cqrs::new(store);
    let account = BankAccountUrn::new("advisory-lock-1").unwrap();
    let meta = replay::Metadata::default();
    let date = chrono::NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();

    // ── Helper: count Withdrawn events in the database ────────────────────────
    let count_withdrawn = || {
        let pool = pg_pool.clone();
        async move {
            sqlx::query_scalar::<_, i64>("SELECT COUNT(*) FROM events WHERE type = 'Withdrawn'")
                .fetch_one(&pool)
                .await
                .expect("count query must succeed")
        }
    };

    // Convenience builder so both runners are configured identically.
    let build_runner = || {
        replay_persistence::PolicyRunner::builder(cqrs.clone())
            .register_services::<BankAccount>(())
            .register_policy(SingleRunnerPolicy)
            .build()
    };

    let interval = Duration::from_millis(50);

    // ── Phase 1: two daemons, only the leader reacts ──────────────────────────
    let daemon_a = build_runner().start_polling(interval);
    let daemon_b = build_runner().start_polling(interval);

    // Give both tasks time to start and one to acquire the lock.
    tokio::time::sleep(Duration::from_millis(150)).await;

    cqrs.execute::<BankAccount>(
        &account,
        meta.clone(),
        BankAccountCommand::Deposit {
            effective_on: date,
            amount: 100.0,
        },
        &(),
        None,
    )
    .await
    .unwrap();

    cqrs.execute::<BankAccount>(
        &account,
        meta.clone(),
        BankAccountCommand::Deposit {
            effective_on: date,
            amount: 100.0,
        },
        &(),
        None,
    )
    .await
    .unwrap();

    // Allow enough polling cycles for the leader to react.
    tokio::time::sleep(Duration::from_millis(400)).await;

    let withdrawn_after_phase1 = count_withdrawn().await;
    assert_eq!(
        withdrawn_after_phase1, 2,
        "only the leader must react; if both ran the count would be 4"
    );

    // ── Phase 2: shut down daemon_a; standby acquires lock and resumes ────────
    daemon_a.shutdown().await;

    // Give the surviving daemon time to notice the lock is free and become leader.
    tokio::time::sleep(Duration::from_millis(200)).await;

    cqrs.execute::<BankAccount>(
        &account,
        meta.clone(),
        BankAccountCommand::Deposit {
            effective_on: date,
            amount: 100.0,
        },
        &(),
        None,
    )
    .await
    .unwrap();

    cqrs.execute::<BankAccount>(
        &account,
        meta.clone(),
        BankAccountCommand::Deposit {
            effective_on: date,
            amount: 100.0,
        },
        &(),
        None,
    )
    .await
    .unwrap();

    // Allow the new leader to react to the two new deposits.
    tokio::time::sleep(Duration::from_millis(400)).await;

    let withdrawn_after_phase2 = count_withdrawn().await;
    assert_eq!(
        withdrawn_after_phase2, 4,
        "the standby must have resumed from the stored cursor and reacted to the two new deposits"
    );

    daemon_b.shutdown().await;
}

// ── Loop prevention via causation-depth limit (issue #83) ────────────────────

/// A policy that deliberately creates a loop: every `Deposited` event triggers
/// another `Deposit` command on the same account. Without the depth limit this
/// would run forever.
struct LoopPolicy {
    max_depth: u32,
}

impl replay_persistence::Policy for LoopPolicy {
    type Event = BankAccountEvent;

    fn name(&self) -> &str {
        "loop_policy"
    }

    fn start_at(&self) -> replay_persistence::StartAt {
        replay_persistence::StartAt::Beginning
    }

    fn max_causation_depth(&self) -> Option<u32> {
        Some(self.max_depth)
    }

    fn react(&self, event: &PersistedEvent<Self::Event>) -> Vec<replay_persistence::Dispatch> {
        match &event.data {
            BankAccountEvent::Deposited { operation_date, .. } => {
                let account = BankAccountUrn::try_from(event.stream_id.clone())
                    .expect("deposit events live on bank-account streams");
                // Self-trigger: deposit $1 back, which creates another Deposited.
                vec![replay_persistence::Dispatch::to::<BankAccount>(
                    account,
                    BankAccountCommand::Deposit {
                        effective_on: *operation_date,
                        amount: 1.0,
                    },
                )]
            }
            _ => Vec::new(),
        }
    }
}

/// A self-triggering policy stops at the configured causation-depth limit.
///
/// Scenario (max_depth = 3):
///   - gp=1  Deposited (depth=0) → reacts → gp=2 Deposited (depth=1)
///   - gp=2  Deposited (depth=1) → reacts → gp=3 Deposited (depth=2)
///   - gp=3  Deposited (depth=2) → reacts → gp=4 Deposited (depth=3)
///   - gp=4  Deposited (depth=3 ≥ max_depth=3) → circuit-breaker fires, skipped
///
/// Exactly 3 reactions, 4 total Deposited events, cursor at gp=4.
#[tokio::test]
async fn policy_causation_depth_limit_stops_loop_postgres_test() {
    let container = postgres::Postgres::default().start().await.unwrap();
    let host = container.get_host().await.unwrap().to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("Error getting docker port");
    let pg_pool = connect_to_postgres(host, port).await;

    sqlx::migrate!("./tests/migrations")
        .run(&pg_pool)
        .await
        .expect("Failed to run migrations");

    let store = replay_persistence::PostgresEventStore::new(pg_pool.clone());
    let cqrs = replay_persistence::Cqrs::new(store);
    let account = BankAccountUrn::new("loop-1").unwrap();
    let date = chrono::NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();

    // Seed: one initial deposit at depth 0 (no causation metadata).
    cqrs.execute::<BankAccount>(
        &account,
        replay::Metadata::default(),
        BankAccountCommand::Deposit {
            effective_on: date,
            amount: 100.0,
        },
        &(),
        None,
    )
    .await
    .unwrap();

    let runner = replay_persistence::PolicyRunner::builder(cqrs.clone())
        .register_services::<BankAccount>(())
        .register_policy(LoopPolicy { max_depth: 3 })
        .build();

    // Drain until the chain stabilises (circuit breaker fires and no more reactions).
    // The upper bound of 10 ensures the test terminates even if the limit is broken.
    let mut rounds: Vec<usize> = Vec::new();
    for _ in 0..10 {
        let n = runner.drain().await.expect("drain must not error");
        rounds.push(n);
        if n == 0 {
            break;
        }
    }

    // The chain must have stopped cleanly: 1 reaction per drain, then 0.
    assert_eq!(
        rounds,
        vec![1, 1, 1, 0],
        "expected 3 reactions (depths 0→1, 1→2, 2→3) then the circuit breaker to fire"
    );

    // Exactly 4 Deposited events in the DB (depths 0, 1, 2, 3).
    let deposited_count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM events WHERE type = 'Deposited'")
            .fetch_one(&pg_pool)
            .await
            .expect("count query must succeed");
    assert_eq!(
        deposited_count, 4,
        "chain must have produced exactly 4 Deposited events"
    );

    // The last event must carry causation.depth = 3 in its metadata.
    let last_meta: serde_json::Value = sqlx::query_scalar(
        "SELECT metadata FROM events WHERE type = 'Deposited' ORDER BY global_position DESC LIMIT 1",
    )
    .fetch_one(&pg_pool)
    .await
    .expect("last deposited event must exist");

    assert_eq!(
        last_meta["causation"]["depth"],
        serde_json::json!(3),
        "last event in the chain must be at causation depth 3"
    );
}

// ── Failure handling and policy_dead_letters (issue #84) ─────────────────────

/// Policy that dispatches to an aggregate type that is NOT registered in the
/// runner, producing a permanent `InvalidInput` error every time.
struct PoisonDispatchPolicy;

impl replay_persistence::Policy for PoisonDispatchPolicy {
    type Event = BankAccountEvent;

    fn name(&self) -> &str {
        "poison_dispatch_policy"
    }

    fn start_at(&self) -> replay_persistence::StartAt {
        replay_persistence::StartAt::Beginning
    }

    fn react(&self, event: &PersistedEvent<Self::Event>) -> Vec<replay_persistence::Dispatch> {
        if matches!(event.data, BankAccountEvent::Deposited { .. }) {
            // Dispatch to IdempotentFeeAccount — which will NOT be registered in
            // the runner, triggering a permanent InvalidInput error.
            let target = IdempotentFeeAccountUrn::new("unregistered-target").unwrap();
            vec![replay_persistence::Dispatch::to::<IdempotentFeeAccount>(
                target,
                IdempotentFeeAccountCommand::Open { balance: 0.0 },
            )]
        } else {
            Vec::new()
        }
    }
}

/// Policy that dispatches a Withdraw command that the aggregate will reject
/// with a BusinessRuleViolation (insufficient funds).
struct InsufficientFundsPolicy;

impl replay_persistence::Policy for InsufficientFundsPolicy {
    type Event = BankAccountEvent;

    fn name(&self) -> &str {
        "insufficient_funds_policy"
    }

    fn start_at(&self) -> replay_persistence::StartAt {
        replay_persistence::StartAt::Beginning
    }

    fn react(&self, event: &PersistedEvent<Self::Event>) -> Vec<replay_persistence::Dispatch> {
        match &event.data {
            BankAccountEvent::Deposited { operation_date, .. } => {
                let account = BankAccountUrn::try_from(event.stream_id.clone())
                    .expect("deposit events live on bank-account streams");
                // Withdraw more than any realistic balance → BusinessRuleViolation.
                vec![replay_persistence::Dispatch::to::<BankAccount>(
                    account,
                    BankAccountCommand::Withdraw {
                        effective_on: *operation_date,
                        amount: 999_999.0,
                    },
                )]
            }
            _ => Vec::new(),
        }
    }
}

/// A permanently-failing policy dispatch is dead-lettered and the policy advances.
///
/// Scenario:
///   - Append 2 deposits.
///   - `PoisonDispatchPolicy` reacts to each but dispatches to an unregistered
///     aggregate type → permanent `InvalidInput` error each time.
///   - Both dispatches must be dead-lettered; cursor advances past both events.
///   - A third, clean event (another deposit) is then appended and processed by
///     a second, healthy policy to prove the runner is not wedged.
#[tokio::test]
async fn policy_permanent_failure_is_dead_lettered_and_advances_postgres_test() {
    let container = postgres::Postgres::default().start().await.unwrap();
    let host = container.get_host().await.unwrap().to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("Error getting docker port");
    let pg_pool = connect_to_postgres(host, port).await;

    sqlx::migrate!("./tests/migrations")
        .run(&pg_pool)
        .await
        .expect("Failed to run migrations");

    let store = replay_persistence::PostgresEventStore::new(pg_pool.clone());
    let cqrs = replay_persistence::Cqrs::new(store);
    let account = BankAccountUrn::new("dead-letter-1").unwrap();
    let date = chrono::NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let meta = replay::Metadata::default();

    // Append two deposits that will each produce a permanent dispatch failure.
    for _ in 0..2 {
        cqrs.execute::<BankAccount>(
            &account,
            meta.clone(),
            BankAccountCommand::Deposit {
                effective_on: date,
                amount: 100.0,
            },
            &(),
            None,
        )
        .await
        .unwrap();
    }

    // PoisonDispatchPolicy dispatches to IdempotentFeeAccount which is NOT
    // registered in the runner.  No services are added for it intentionally.
    let runner = replay_persistence::PolicyRunner::builder(cqrs.clone())
        .register_services::<BankAccount>(())
        .register_policy(PoisonDispatchPolicy)
        .build();

    // Drain: both events are processed, both dispatches dead-lettered, 0 commands executed.
    let dispatches = runner.drain().await.expect("drain must not error");
    assert_eq!(
        dispatches, 0,
        "permanently-failing dispatches must not be counted as executed"
    );

    // Two dead-letter rows must exist.
    let dead_letter_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM policy_dead_letters WHERE policy_name = 'poison_dispatch_policy'",
    )
    .fetch_one(&pg_pool)
    .await
    .expect("dead-letter count query must succeed");

    assert_eq!(
        dead_letter_count, 2,
        "one dead-letter row per failing dispatch"
    );

    // Cursor must have advanced past both events (policy is not wedged).
    let cursor: i64 = sqlx::query_scalar(
        "SELECT position FROM policy_cursors WHERE name = 'poison_dispatch_policy'",
    )
    .fetch_one(&pg_pool)
    .await
    .expect("cursor must exist after drain");

    assert_eq!(
        cursor, 2,
        "cursor must have advanced past both dead-lettered events"
    );
}

/// A BusinessRuleViolation from a dispatch is treated as handled: the cursor
/// advances without writing a dead-letter record.
///
/// Scenario:
///   - Append a deposit of $10; account balance = 10.
///   - `InsufficientFundsPolicy` reacts by attempting to withdraw $999 999
///     → the aggregate returns BusinessRuleViolation (insufficient funds).
///   - Drain must return 0 dispatches executed (the command was declined).
///   - No dead-letter row must be written.
///   - Cursor advances.
#[tokio::test]
async fn policy_business_rule_violation_advances_without_dead_letter_postgres_test() {
    let container = postgres::Postgres::default().start().await.unwrap();
    let host = container.get_host().await.unwrap().to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("Error getting docker port");
    let pg_pool = connect_to_postgres(host, port).await;

    sqlx::migrate!("./tests/migrations")
        .run(&pg_pool)
        .await
        .expect("Failed to run migrations");

    let store = replay_persistence::PostgresEventStore::new(pg_pool.clone());
    let cqrs = replay_persistence::Cqrs::new(store);
    let account = BankAccountUrn::new("brv-1").unwrap();
    let date = chrono::NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();

    cqrs.execute::<BankAccount>(
        &account,
        replay::Metadata::default(),
        BankAccountCommand::Deposit {
            effective_on: date,
            amount: 10.0,
        },
        &(),
        None,
    )
    .await
    .unwrap();

    let runner = replay_persistence::PolicyRunner::builder(cqrs.clone())
        .register_services::<BankAccount>(())
        .register_policy(InsufficientFundsPolicy)
        .build();

    // Drain: the Withdraw is rejected with BusinessRuleViolation; drain does not error.
    let dispatches = runner.drain().await.expect("drain must not error on BRV");
    assert_eq!(
        dispatches, 0,
        "a BRV-declined command must not be counted as executed"
    );

    // No dead-letter rows: BRV is not a failure, just the aggregate saying no.
    let dead_letter_count: i64 = sqlx::query_scalar(
        "SELECT COUNT(*) FROM policy_dead_letters WHERE policy_name = 'insufficient_funds_policy'",
    )
    .fetch_one(&pg_pool)
    .await
    .expect("dead-letter count query must succeed");

    assert_eq!(
        dead_letter_count, 0,
        "BRV must not produce a dead-letter record"
    );

    // Cursor advances: the policy is not wedged by the rejection.
    let cursor: i64 = sqlx::query_scalar(
        "SELECT position FROM policy_cursors WHERE name = 'insufficient_funds_policy'",
    )
    .fetch_one(&pg_pool)
    .await
    .expect("cursor must exist after drain");

    assert_eq!(cursor, 1, "cursor must have advanced past the BRV event");
}

// ── Batching: read-batch size and checkpoint-batch size (issue #85) ───────────

/// Policy with a custom read-batch size for testing.
struct SmallBatchPolicy {
    batch: u32,
}

impl replay_persistence::Policy for SmallBatchPolicy {
    type Event = BankAccountEvent;

    fn name(&self) -> &str {
        "small_batch_policy"
    }

    fn start_at(&self) -> replay_persistence::StartAt {
        replay_persistence::StartAt::Beginning
    }

    fn read_batch_size(&self) -> Option<u32> {
        Some(self.batch)
    }

    fn checkpoint_batch_size(&self) -> Option<u32> {
        // Match read_batch so the read_batch ≥ checkpoint_batch invariant
        // does not silently raise the read batch to the default (100).
        Some(self.batch)
    }

    fn react(&self, _: &PersistedEvent<Self::Event>) -> Vec<replay_persistence::Dispatch> {
        Vec::new() // observe-only; we care about cursor position, not commands
    }
}

/// Policy with explicit batch sizes for checkpoint testing.
struct CheckpointBatchPolicy {
    read_batch: u32,
    checkpoint_batch: u32,
}

impl replay_persistence::Policy for CheckpointBatchPolicy {
    type Event = BankAccountEvent;

    fn name(&self) -> &str {
        "checkpoint_batch_policy"
    }

    fn start_at(&self) -> replay_persistence::StartAt {
        replay_persistence::StartAt::Beginning
    }

    fn read_batch_size(&self) -> Option<u32> {
        Some(self.read_batch)
    }

    fn checkpoint_batch_size(&self) -> Option<u32> {
        Some(self.checkpoint_batch)
    }

    fn react(&self, _: &PersistedEvent<Self::Event>) -> Vec<replay_persistence::Dispatch> {
        Vec::new()
    }
}

/// `read_batch_size` caps how many events a single drain call fetches.
///
/// With 5 events appended and `read_batch_size = 2`, each drain call
/// processes exactly 2 events (until the last, which gets the remainder).
/// Three drain calls are required to exhaust all 5 events.
#[tokio::test]
async fn policy_read_batch_limits_events_per_drain_postgres_test() {
    let container = postgres::Postgres::default().start().await.unwrap();
    let host = container.get_host().await.unwrap().to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("Error getting docker port");
    let pg_pool = connect_to_postgres(host, port).await;

    sqlx::migrate!("./tests/migrations")
        .run(&pg_pool)
        .await
        .expect("Failed to run migrations");

    let store = replay_persistence::PostgresEventStore::new(pg_pool.clone());
    let cqrs = replay_persistence::Cqrs::new(store);
    let account = BankAccountUrn::new("batch-read-1").unwrap();
    let date = chrono::NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let meta = replay::Metadata::default();

    // Append 5 events.
    for _ in 0..5 {
        cqrs.execute::<BankAccount>(
            &account,
            meta.clone(),
            BankAccountCommand::Deposit {
                effective_on: date,
                amount: 10.0,
            },
            &(),
            None,
        )
        .await
        .unwrap();
    }

    let runner = replay_persistence::PolicyRunner::builder(cqrs.clone())
        .register_services::<BankAccount>(())
        .register_policy(SmallBatchPolicy { batch: 2 })
        .build();

    // First drain: processes events at positions 1 and 2.
    runner.drain().await.expect("drain 1 must succeed");
    let cursor_1: i64 =
        sqlx::query_scalar("SELECT position FROM policy_cursors WHERE name = 'small_batch_policy'")
            .fetch_one(&pg_pool)
            .await
            .expect("cursor must exist after drain 1");
    assert_eq!(
        cursor_1, 2,
        "first drain (batch=2) must stop at global_position 2"
    );

    // Second drain: events 3 and 4.
    runner.drain().await.expect("drain 2 must succeed");
    let cursor_2: i64 =
        sqlx::query_scalar("SELECT position FROM policy_cursors WHERE name = 'small_batch_policy'")
            .fetch_one(&pg_pool)
            .await
            .expect("cursor must exist after drain 2");
    assert_eq!(cursor_2, 4, "second drain must stop at global_position 4");

    // Third drain: event 5.
    runner.drain().await.expect("drain 3 must succeed");
    let cursor_3: i64 =
        sqlx::query_scalar("SELECT position FROM policy_cursors WHERE name = 'small_batch_policy'")
            .fetch_one(&pg_pool)
            .await
            .expect("cursor must exist after drain 3");
    assert_eq!(cursor_3, 5, "third drain must reach global_position 5");
}

/// Crash-recovery / skip-safety: resetting the cursor to an earlier checkpoint
/// causes only the uncommitted tail to be reprocessed — never a skip.
///
/// Scenario:
///   - 4 events appended.
///   - `checkpoint_batch_size = 2`: the cursor is written after events 2 and 4
///     (two checkpoint intervals).
///   - After a full drain the cursor is at 4.
///   - Simulate a crash by rolling the cursor back to 2 (the first checkpoint).
///   - Re-drain: events 3 and 4 are reprocessed (at-least-once; absorbed by
///     idempotency for dispatching policies).
///   - Cursor ends at 4 again.
#[tokio::test]
async fn policy_checkpoint_batch_crash_recovery_reprocesses_tail_postgres_test() {
    let container = postgres::Postgres::default().start().await.unwrap();
    let host = container.get_host().await.unwrap().to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("Error getting docker port");
    let pg_pool = connect_to_postgres(host, port).await;

    sqlx::migrate!("./tests/migrations")
        .run(&pg_pool)
        .await
        .expect("Failed to run migrations");

    let store = replay_persistence::PostgresEventStore::new(pg_pool.clone());
    let cqrs = replay_persistence::Cqrs::new(store);
    let account = BankAccountUrn::new("checkpoint-batch-1").unwrap();
    let date = chrono::NaiveDate::from_ymd_opt(2026, 1, 1).unwrap();
    let meta = replay::Metadata::default();

    for _ in 0..4 {
        cqrs.execute::<BankAccount>(
            &account,
            meta.clone(),
            BankAccountCommand::Deposit {
                effective_on: date,
                amount: 10.0,
            },
            &(),
            None,
        )
        .await
        .unwrap();
    }

    let runner = replay_persistence::PolicyRunner::builder(cqrs.clone())
        .register_services::<BankAccount>(())
        .register_policy(CheckpointBatchPolicy {
            read_batch: 4,
            checkpoint_batch: 2,
        })
        .build();

    // Full drain: cursor written at positions 2 and 4 (two checkpoints).
    runner.drain().await.expect("initial drain must succeed");

    let cursor_after_drain: i64 = sqlx::query_scalar(
        "SELECT position FROM policy_cursors WHERE name = 'checkpoint_batch_policy'",
    )
    .fetch_one(&pg_pool)
    .await
    .expect("cursor must exist");
    assert_eq!(
        cursor_after_drain, 4,
        "cursor must be at 4 after full drain"
    );

    // Simulate crash: roll cursor back to the first checkpoint (position 2),
    // as if the process died after that checkpoint but before the final one.
    sqlx::query("UPDATE policy_cursors SET position = 2 WHERE name = 'checkpoint_batch_policy'")
        .execute(&pg_pool)
        .await
        .expect("cursor rollback must succeed");

    // Re-drain from position 2: only events 3 and 4 are reprocessed.
    runner.drain().await.expect("recovery drain must succeed");

    let cursor_after_recovery: i64 = sqlx::query_scalar(
        "SELECT position FROM policy_cursors WHERE name = 'checkpoint_batch_policy'",
    )
    .fetch_one(&pg_pool)
    .await
    .expect("cursor must exist after recovery");
    assert_eq!(
        cursor_after_recovery, 4,
        "cursor must be back at 4 after recovery drain"
    );
}

// ── Closure shortcut: register_policy_fn (issue #87) ─────────────────────────

/// `register_policy_fn` wires the deposit-fee reaction (defined in the example)
/// through the same runner machinery as a full `impl Policy`.
///
/// Scenario:
///   - User registers; opens a BankAccount.
///   - A deposit of $100 is made.
///   - The closure policy (defined in `global_position.rs`) reacts to the
///     `Deposited` event and dispatches `ChargeFee { amount: 1.0 }` to the
///     `PolicyFeeLedger`.
///   - After drain: a `FeeCharged` event exists on the ledger stream.
///   - Drain is idempotent: running it again produces no additional fees.
#[tokio::test]
async fn global_position_closure_policy_charges_deposit_fee_postgres_test() {
    use global_position::{
        BankAccount, BankAccountCommand, BankAccountEvent, BankAccountUrn, PolicyFeeLedger,
        PolicyFeeLedgerUrn, DEPOSIT_FEE_LEDGER_ID, DEPOSIT_FEE_POLICY_NAME, DEPOSIT_FEE_RATE,
    };

    let container = postgres::Postgres::default().start().await.unwrap();
    let host = container.get_host().await.unwrap().to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("Error getting docker port");
    let pg_pool = connect_to_postgres(host, port).await;

    sqlx::migrate!("./tests/migrations")
        .run(&pg_pool)
        .await
        .expect("Failed to run migrations");

    let store = replay_persistence::PostgresEventStore::new(pg_pool.clone());
    let cqrs = replay_persistence::Cqrs::new(store);
    let account = BankAccountUrn::new("closure-test-1").unwrap();

    // Deposit $100 into the account.
    cqrs.execute::<BankAccount>(
        &account,
        replay::Metadata::default(),
        BankAccountCommand::OpenAccount {
            owner: global_position::UserUrn::new("alice").unwrap(),
        },
        &(),
        None,
    )
    .await
    .unwrap();

    let deposit_amount = 100.0_f64;
    cqrs.execute::<BankAccount>(
        &account,
        replay::Metadata::default(),
        BankAccountCommand::Deposit {
            amount: deposit_amount,
        },
        &(),
        None,
    )
    .await
    .unwrap();

    let runner = replay_persistence::PolicyRunner::builder(cqrs.clone())
        .register_services::<BankAccount>(())
        .register_services::<PolicyFeeLedger>(())
        .register_policy_fn::<BankAccountEvent, _>(
            DEPOSIT_FEE_POLICY_NAME,
            replay_persistence::StartAt::Beginning,
            global_position::deposit_fee_react,
        )
        .build();

    // First drain: reacts to the Deposited event and charges the fee.
    let dispatched = runner.drain().await.expect("first drain must succeed");
    assert_eq!(dispatched, 1, "one FeeCharged command must be dispatched");

    // The fee ledger must have a FeeCharged event for the expected amount.
    let expected_fee = deposit_amount * DEPOSIT_FEE_RATE;
    let ledger_urn = PolicyFeeLedgerUrn::new(DEPOSIT_FEE_LEDGER_ID).unwrap();
    let ledger = cqrs
        .fetch_aggregate::<PolicyFeeLedger>(&ledger_urn)
        .await
        .expect("fee ledger must be fetchable");
    assert!(
        (ledger.balance - (-expected_fee)).abs() < 1e-9,
        "fee ledger balance must be -{expected_fee:.2}, got {:.2}",
        ledger.balance
    );

    // Second drain: idempotent — the causation guard absorbs the duplicate.
    let dispatched_again = runner.drain().await.expect("second drain must succeed");
    assert_eq!(
        dispatched_again, 0,
        "second drain must dispatch nothing (cursor already advanced)"
    );
}
