use serde::{Deserialize, Serialize};

use sqlx::{postgres::PgPoolOptions, PgPool};
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};
use thiserror::Error;
use tokio_test::assert_err;
use urn::{Urn, UrnBuilder};

use replay::StreamFilter;
use replay_macros::{Event, Urn};

const POSTGRES_PORT: u16 = 5432;

//  bank account stream (id of stream is not part of the model)
#[derive(Default, Serialize, Deserialize, Clone, PartialEq, Debug)]
struct BankAccountAggregate {
    pub balance: f64,
}

// create bank account commands
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
}

// create bank account events enum: Deposited and Withdrawn
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
}

impl BankAccountEvent {
    fn operation_date(&self) -> chrono::NaiveDate {
        match self {
            BankAccountEvent::Deposited { operation_date, .. } => *operation_date,
            BankAccountEvent::Withdrawn { operation_date, .. } => *operation_date,
        }
    }
}

// bank account urn
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug, Urn)]
struct BankAccountUrn(Urn);

// bank account errors
#[derive(Debug, Error)]
enum BankAccountError {
    #[error("Insufficient funds")]
    InsufficientFunds,
    #[error("Persistence error: {source}")]
    PersistenceError {
        #[from]
        source: replay::persistence::EventStoreError,
    },
}

// bank account stream
impl replay::Stream for BankAccountAggregate {
    type Event = BankAccountEvent;
    type StreamId = BankAccountUrn;

    fn stream_type() -> String {
        "BankAccount".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            BankAccountEvent::Deposited {
                operation_date: _,
                amount,
            } => {
                self.balance += amount;
            }
            BankAccountEvent::Withdrawn {
                operation_date: _,
                amount,
            } => {
                self.balance -= amount;
            }
        }
    }
}

impl replay::Aggregate for BankAccountAggregate {
    type Command = BankAccountCommand;
    type Error = BankAccountError;

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
                    return Err(BankAccountError::InsufficientFunds);
                }

                let event = BankAccountEvent::Withdrawn {
                    operation_date: effective_on,
                    amount,
                };
                Ok(vec![event])
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
            self.bank_account.0,
            self.from,
            self.to,
            self.transactions,
            self.balance_change,
            self.total_transactions
        )
    }
}

impl replay::Query for BankAccountStatement {
    type Event = BankAccountEvent;

    fn stream_filter(&self) -> StreamFilter {
        // filter by from / to dates
        StreamFilter::with_stream_id::<BankAccountAggregate>(&self.bank_account)
    }

    fn update(&mut self, event: Self::Event) {
        // right now we don't have filters for "after timestamp" so we need to apply here
        if event.operation_date() > self.to || event.operation_date() < self.from {
            return;
        }

        self.transactions.push(event.clone());

        match event {
            BankAccountEvent::Deposited {
                operation_date: _,
                amount,
            } => {
                self.balance_change += amount;
            }
            BankAccountEvent::Withdrawn {
                operation_date: _,
                amount,
            } => {
                self.balance_change -= amount;
            }
        }

        self.total_transactions += 1;
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

    let store = replay::persistence::PostgresEventStore::new(pg_pool);
    let cqrs = replay::persistence::Cqrs::new(store);

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

    let mut bank_account: BankAccountAggregate = BankAccountAggregate::default();

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
