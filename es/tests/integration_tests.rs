use serde::{Deserialize, Serialize};

use sqlx::{postgres::PgPoolOptions, PgPool};
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};
use thiserror::Error;
use tokio_test::assert_err;
use urn::{Urn, UrnBuilder};

use replay::{persistence::PersistedEvent, StreamFilter};
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
    Deposit { amount: f64 },
    Withdraw { amount: f64 },
}

// create bank account events enum: Deposited and Withdrawn
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Event)]
enum BankAccountEvent {
    Deposited { amount: f64 },
    Withdrawn { amount: f64 },
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
            BankAccountEvent::Deposited { amount } => {
                self.balance += amount;
            }
            BankAccountEvent::Withdrawn { amount } => {
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
            BankAccountCommand::Deposit { amount } => {
                let event = BankAccountEvent::Deposited { amount };
                Ok(vec![event])
            }
            BankAccountCommand::Withdraw { amount } => {
                if self.balance < amount {
                    return Err(BankAccountError::InsufficientFunds);
                }

                let event = BankAccountEvent::Withdrawn { amount };
                Ok(vec![event])
            }
        }
    }
}

struct BankAccountStatement {
    bank_account: BankAccountUrn,
    from: chrono::DateTime<chrono::Utc>,
    to: chrono::DateTime<chrono::Utc>,
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
            .and_at_timestamp(self.from)
    }

    fn update(&mut self, event: PersistedEvent<Self::Event>) {
        // right now we don't have filters for "after timestamp" so we need to apply here
        if event.created > self.to {
            return;
        }

        self.transactions.push(event.data.clone());

        match event.data {
            BankAccountEvent::Deposited { amount } => {
                self.balance_change += amount;
            }
            BankAccountEvent::Withdrawn { amount } => {
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
        BankAccountCommand::Deposit { amount: 100.0 },
        BankAccountCommand::Withdraw { amount: 40.0 },
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
            BankAccountCommand::Withdraw { amount: 100f64 },
            services,
            expected_version,
        )
        .await;

    assert_err!(result, "Insufficient funds");

    // to test the query we get current time and sleep 50 ms, then do couple of transactions
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    let from = chrono::Utc::now();
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    let commands = vec![
        BankAccountCommand::Deposit { amount: 20.0 },
        BankAccountCommand::Withdraw { amount: 40.0 },
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
        from,
        to: chrono::Utc::now() + chrono::Duration::seconds(1),
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
