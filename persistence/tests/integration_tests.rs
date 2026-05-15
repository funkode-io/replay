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
    ) -> replay::Result<Vec<Self::Event>> {
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
