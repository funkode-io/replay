use serde::{Deserialize, Serialize};

use sqlx::{postgres::PgPoolOptions, PgPool};
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};
use thiserror::Error;
use tokio_test::assert_err;
use urn::{Urn, UrnBuilder};

use replay::{persistence::EventStore, Event};
use replay_macros::Event;

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
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
struct BankAccountUrn(Urn);

impl From<BankAccountUrn> for Urn {
    fn from(urn: BankAccountUrn) -> Self {
        urn.0
    }
}

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

    let stream_id = BankAccountUrn(UrnBuilder::new("bank-account", "1").build().unwrap());

    let commands = vec![
        BankAccountCommand::Deposit { amount: 100.0 },
        BankAccountCommand::Withdraw { amount: 40.0 },
    ];

    let mut bank_account: BankAccountAggregate = BankAccountAggregate::default();

    let services = &();
    let expected_version = None;

    for command in commands {
        bank_account = store
            .apply_command_and_store_events(
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

    let result = store
        .apply_command_and_store_events::<BankAccountAggregate>(
            &stream_id,
            replay::Metadata::default(),
            BankAccountCommand::Withdraw { amount: 100f64 },
            services,
            expected_version,
        )
        .await;

    assert_err!(result, "Insufficient funds");
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
