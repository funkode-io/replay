use futures::TryStreamExt;
use serde::{Deserialize, Serialize};

use sqlx::{postgres::PgPoolOptions, PgPool};
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};
use tokio_test::assert_err;
use urn::{Urn, UrnBuilder};

use replay::{Compactable, WithId};
use replay_macros::{Event, Urn};
use replay_persistence::{AggregateVersion, EventStore, PersistedEvent, StreamFilter};

const POSTGRES_PORT: u16 = 5432;

//  bank account stream aggregate model (includes stream id)
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
struct BankAccountAggregate {
    pub id: BankAccountUrn,
    pub balance: f64,
}

// bank account commands
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug)]
enum BankAccountCommand {
    Deposit {
        effective_on: chrono::NaiveDate,
        amount: f64,
    },
    Withdraw {
        effective_on: chrono::NaiveDate,
        amount: f64,
    },
    /// Formally close the month identified by `month`.  The closing balance is
    /// derived from the aggregate's current in-memory state.
    CloseMonth { month: chrono::NaiveDate },
}

// bank account events
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Event)]
enum BankAccountEvent {
    Deposited {
        operation_date: chrono::NaiveDate,
        amount: f64,
    },
    Withdrawn {
        operation_date: chrono::NaiveDate,
        amount: f64,
    },
    /// Summarises all activity up to and including a completed calendar month.
    /// Carries the closing balance so the aggregate can be restored from this
    /// single event without replaying every prior transaction.
    MonthlyClosed {
        month: chrono::NaiveDate,
        closing_balance: f64,
    },
}

// bank account urn
#[derive(Clone, Serialize, Deserialize, Debug, Urn)]
struct BankAccountUrn(Urn);

impl TryFrom<Urn> for BankAccountUrn {
    type Error = String;

    fn try_from(urn: Urn) -> Result<Self, Self::Error> {
        // Enforce namespace validation to be consistent with production behavior
        let expected_nid = "bank-account";
        if urn.nid() == expected_nid {
            Ok(BankAccountUrn(urn))
        } else {
            Err(format!(
                "Invalid namespace for BankAccountUrn: expected '{}', found '{}'",
                expected_nid,
                urn.nid()
            ))
        }
    }
}

// Implement WithId trait
impl replay::WithId for BankAccountAggregate {
    type StreamId = BankAccountUrn;

    fn with_id(id: Self::StreamId) -> Self {
        Self { id, balance: 0.0 }
    }

    fn get_id(&self) -> &Self::StreamId {
        &self.id
    }
}

// bank account stream
impl replay::EventStream for BankAccountAggregate {
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

impl replay::Aggregate for BankAccountAggregate {
    type Command = BankAccountCommand;
    type Error = replay::Error;

    type Services = ();

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> replay::Result<Vec<Self::Event>> {
        match command {
            BankAccountCommand::Deposit {
                effective_on,
                amount,
            } => {
                let event = BankAccountEvent::Deposited {
                    operation_date: effective_on,
                    amount,
                };
                Ok(vec![event])
            }
            BankAccountCommand::Withdraw {
                effective_on,
                amount,
            } => {
                if self.balance < amount {
                    return Err(replay::Error::business_rule_violation("Insufficient funds")
                        .with_operation("Withdraw")
                        .with_context("amount_tried", amount));
                }

                let event = BankAccountEvent::Withdrawn {
                    operation_date: effective_on,
                    amount,
                };
                Ok(vec![event])
            }
            // Close the current month; closing balance is taken from aggregate state.
            BankAccountCommand::CloseMonth { month } => Ok(vec![BankAccountEvent::MonthlyClosed {
                month,
                closing_balance: self.balance,
            }]),
        }
    }
}

impl Compactable for BankAccountAggregate {
    async fn compacted_events(
        &self,
        events: impl futures::TryStream<Ok = BankAccountEvent, Error = replay::Error> + Send,
    ) -> replay::Result<Vec<BankAccountEvent>> {
        use futures::TryStreamExt;
        // Sliding-window scan via try_fold: only the events from the last
        // MonthlyClosed onward are kept in the accumulator at any time.
        // Closed months are never accumulated in memory.
        events
            .try_fold(Vec::new(), |mut tail, event| async move {
                if matches!(event, BankAccountEvent::MonthlyClosed { .. }) {
                    tail.clear(); // drop everything before this summary checkpoint
                }
                tail.push(event);
                Ok(tail)
            })
            .await
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
            self.bank_account.0,
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
        // filter by from / to dates
        replay_persistence::StreamFilter::with_stream_id::<BankAccountAggregate>(&self.bank_account)
    }

    fn update(&mut self, event: PersistedEvent<Self::Event>) {
        // Summary events are not individual transactions and are excluded from statements.
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
            // MonthlyClosed is a compaction marker — not an individual statement line.
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

    let stream_id = BankAccountUrn(UrnBuilder::new("bank-account", "1").build().unwrap());

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

    let id = BankAccountUrn(
        UrnBuilder::new("bank-account", &uuid::Uuid::new_v4().to_string())
            .build()
            .unwrap(),
    );
    let mut bank_account: BankAccountAggregate = BankAccountAggregate::with_id(id);

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
        .execute::<BankAccountAggregate>(
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

    let stream_id = BankAccountUrn(
        UrnBuilder::new("bank-account", "compact-1")
            .build()
            .unwrap(),
    );
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
        cqrs.execute::<BankAccountAggregate>(&stream_id, meta.clone(), cmd, services, None)
            .await
            .unwrap();
    }

    // ── Close January ─────────────────────────────────────────────────────────
    // CloseMonth derives closing_balance from current aggregate state (1300).
    cqrs.execute::<BankAccountAggregate>(
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
        cqrs.execute::<BankAccountAggregate>(&stream_id, meta.clone(), cmd, services, None)
            .await
            .unwrap();
    }

    // Full history at this point: 6 events (3 Jan + MonthlyClosed + 2 Feb).
    let aggregate = cqrs
        .fetch_aggregate::<BankAccountAggregate>(&stream_id, AggregateVersion::Latest, None, None)
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
        .fetch_aggregate::<BankAccountAggregate>(&stream_id, AggregateVersion::Latest, None, None)
        .await
        .unwrap();

    assert_eq!(
        compacted_aggregate.balance, 1600.0,
        "Replaying the compacted stream must yield the same balance"
    );

    // ── Verify: live stream now has exactly 3 events ──────────────────────────
    // Expected: MonthlyClosed(Jan,1300) · Deposited(Feb) · Withdrawn(Feb)
    let live_events: Vec<_> = store
        .stream_events_by_stream_id::<BankAccountAggregate>(
            &stream_id,
            AggregateVersion::Latest,
            None,
            None,
        )
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
        .stream_events_by_stream_id::<BankAccountAggregate>(
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
