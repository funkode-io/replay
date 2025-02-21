use futures::TryStreamExt;
use serde::{Deserialize, Serialize};

use sqlx::{postgres::PgPoolOptions, PgPool};
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};
use urn::{Urn, UrnBuilder};

use replay::{
    persistence::{EventStore, StreamFilter},
    Event, Stream,
};
use replay_macros::Event;

const POSTGRES_PORT: u16 = 5432;

//  bank account stream (id of stream is not part of the model)
#[derive(Default)]
struct BankAccountStream {
    pub balance: f64,
}

// create bank account events enum: Deposit and Withdraw
#[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Event)]
enum BankAccountEvent {
    Deposit { amount: f64 },
    Withdraw { amount: f64 },
}

// bank account urn
#[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
struct BankAccountUrn(Urn);

impl From<BankAccountUrn> for Urn {
    fn from(urn: BankAccountUrn) -> Self {
        urn.0
    }
}

// bank account stream
impl replay::Stream for BankAccountStream {
    type Event = BankAccountEvent;
    type StreamId = BankAccountUrn;

    fn stream_type() -> String {
        "BankAccount".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            BankAccountEvent::Deposit { amount } => {
                self.balance += amount;
            }
            BankAccountEvent::Withdraw { amount } => {
                self.balance -= amount;
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

    let stream_id = UrnBuilder::new("bank-account", "1").build().unwrap();

    let events = vec![
        BankAccountEvent::Deposit { amount: 100.0 },
        BankAccountEvent::Withdraw { amount: 40.0 },
    ];

    store
        .store_events(
            &stream_id,
            "BankAccount".to_string(),
            replay::Metadata::default(),
            &events,
            None,
        )
        .await
        .unwrap();

    let stream_events = store
        .stream_events::<BankAccountEvent>(StreamFilter::WithStreamId(stream_id))
        .map_ok(|persisted_event| persisted_event.data)
        .try_collect::<Vec<_>>()
        .await
        .unwrap();

    assert_eq!(stream_events.len(), 2);

    let mut stream = BankAccountStream::default();
    stream.apply_all(stream_events);

    assert_eq!(stream.balance, 60.0);
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
