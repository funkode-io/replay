//! Two command middlewares a consumer would write, the runnable companion to the README
//! section *Command middleware*, so the two are kept in lock-step:
//!
//! * [`MaintenanceMode`] holds every command while an operator has maintenance mode on:
//!   it answers `Busy` before the command runs and again at its commit gate, and its
//!   switch waits for the commits already past the gate.
//! * [`ActingUser`] adds the user a command acts for to its metadata, so every event the
//!   command appends records who did it, and audits how each command ended.
//!
//! Run it (no database required) with:
//!
//! ```bash
//! cargo run -p es-replay-persistence --example command_middleware
//! ```
//!
//! Waiting for in-flight commits needs Postgres, so the switch is exercised by the
//! Docker-gated integration test `command_middleware_example_*_postgres_test`; here it is
//! defined so it type-checks under `cargo build --examples`.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use futures::future::BoxFuture;
use replay::prelude::*;
use replay_macros::define_aggregate;
use replay_persistence::{
    CommandContext, CommandContextMut, CommandMiddleware, Next, Outcome, PostgresEventStore,
};

// ── The aggregate the commands run against ─────────────────────────────────

define_aggregate! {
    BankAccount {
        namespace: "bank-account",
        state: {
            balance: f64,
        },
        commands: {
            Deposit { amount: f64 },
        },
        events: {
            Deposited { amount: f64 },
        }
    }
}

impl EventStream for BankAccount {
    type Event = BankAccountEvent;

    fn stream_type() -> String {
        "BankAccount".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        match event {
            BankAccountEvent::Deposited { amount } => self.balance += amount,
        }
    }
}

impl Aggregate for BankAccount {
    type Command = BankAccountCommand;
    type Error = replay::Error;
    type Services = ();

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> replay::Result<Vec<Self::Event>> {
        match command {
            BankAccountCommand::Deposit { amount } => {
                Ok(vec![BankAccountEvent::Deposited { amount }])
            }
        }
    }
}

// ── Maintenance mode ───────────────────────────────────────────────────────

/// Holds every command while maintenance mode is on.
///
/// It answers [`Next::Busy`] in `before`, so a command that arrives during maintenance
/// costs no read and no append, and again in `before_commit`, so a command that checked
/// the flag just before it was switched on still cannot commit. A caller sees
/// `ErrorKind::Busy`; a Policy holds the command and runs it once maintenance is over.
///
/// The flag here is this process's own. A fleet of processes reads it from where every
/// one of them sees it, as it is on each check — a row read on every call, never a cached
/// copy — or `await_in_flight_commits` cannot vouch for the other processes' appends.
#[derive(Clone, Default)]
pub struct MaintenanceMode {
    switched_on: Arc<AtomicBool>,
}

impl MaintenanceMode {
    /// Switch maintenance mode on, and return once no command that checked the flag
    /// while it was off can still commit: from then on the store is quiet.
    pub async fn switch_on(&self, store: &PostgresEventStore) -> replay::Result<()> {
        self.switched_on.store(true, Ordering::SeqCst);
        store.await_in_flight_commits().await
    }

    /// Switch maintenance mode off. Held Policies resume on their own.
    pub fn switch_off(&self) {
        self.switched_on.store(false, Ordering::SeqCst);
    }

    fn check(&self) -> Next {
        if self.switched_on.load(Ordering::SeqCst) {
            Next::Busy("maintenance in progress".to_string())
        } else {
            Next::Continue
        }
    }
}

impl CommandMiddleware for MaintenanceMode {
    fn before<'a>(&'a self, _context: CommandContextMut<'a>) -> BoxFuture<'a, Next> {
        Box::pin(async move { self.check() })
    }

    fn before_commit<'a>(&'a self, _context: &'a CommandContext) -> BoxFuture<'a, Next> {
        Box::pin(async move { self.check() })
    }
}

// ── The acting user ────────────────────────────────────────────────────────

tokio::task_local! {
    /// The user the current request acts for, set by the application's request layer
    /// around the commands it executes.
    pub static ACTING_USER: String;
}

/// The metadata key [`ActingUser`] writes.
pub const ACTING_USER_KEY: &str = "acting_user";

/// Adds the user a command acts for to its metadata, so the events it appends record
/// who did it, and audits how each command ended.
///
/// An application's command acts for the request's [`ACTING_USER`]; a command without
/// one is refused as `Unauthorized`. A Policy's command acts for the Policy, named from
/// the command's causation.
pub struct ActingUser;

impl CommandMiddleware for ActingUser {
    fn before<'a>(&'a self, mut context: CommandContextMut<'a>) -> BoxFuture<'a, Next> {
        Box::pin(async move {
            let acting_user = match context.causation() {
                Some(causation) => format!("policy:{}", causation.policy()),
                None => match ACTING_USER.try_with(Clone::clone) {
                    Ok(user) => user,
                    Err(_) => {
                        return Next::Error(replay::Error::unauthorized(
                            "a command needs an acting user",
                        ))
                    }
                },
            };
            match context.insert_metadata(ACTING_USER_KEY, acting_user) {
                Ok(()) => Next::Continue,
                Err(error) => Next::Error(error),
            }
        })
    }

    fn after<'a>(
        &'a self,
        context: &'a CommandContext,
        outcome: &'a Outcome<'a>,
    ) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            let acting_user = context.metadata().as_json().get(ACTING_USER_KEY);
            tracing::info!(
                command = context.command_type(),
                stream_id = %context.stream_id(),
                acting_user = ?acting_user,
                outcome = ?outcome,
                "command finished"
            );
        })
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Runnable walkthrough (in-memory store, no database required)
// ─────────────────────────────────────────────────────────────────────────────

// Gated out of the `cfg(test)` build so this file can be `#[path]`-included as a
// module by the Docker-gated integration test, which drives the same middlewares
// against Postgres and provides its own test harness.
#[cfg(not(test))]
#[tokio::main]
async fn main() -> replay::Result<()> {
    use replay::ErrorKind;
    use replay_persistence::{Cqrs, InMemoryEventStore};

    let maintenance = MaintenanceMode::default();
    // Maintenance first: a command it holds never reaches the next middleware.
    let cqrs = Cqrs::new(InMemoryEventStore::new())
        .with_middleware(maintenance.clone())
        .with_middleware(ActingUser);

    let account = BankAccountUrn::new("alice-checking").unwrap();
    let deposit = |amount| BankAccountCommand::Deposit { amount };

    // A request acting for Alice: the deposit's event records her as its acting user.
    ACTING_USER
        .scope(
            "alice".to_string(),
            cqrs.execute::<BankAccount>(&account, Default::default(), deposit(100.0), &(), None),
        )
        .await?;

    // Without an acting user the command is refused, and nothing is appended.
    let refused = cqrs
        .execute::<BankAccount>(&account, Default::default(), deposit(1.0), &(), None)
        .await
        .unwrap_err();
    assert_eq!(refused.kind(), ErrorKind::Unauthorized);

    // The in-memory store has no commits in flight to wait for; on Postgres the switch
    // is `maintenance.switch_on(&store).await?`.
    maintenance.switched_on.store(true, Ordering::SeqCst);
    let held = ACTING_USER
        .scope(
            "alice".to_string(),
            cqrs.execute::<BankAccount>(&account, Default::default(), deposit(50.0), &(), None),
        )
        .await
        .unwrap_err();
    assert_eq!(held.kind(), ErrorKind::Busy);
    println!("during maintenance: {held}");

    maintenance.switch_off();
    let account_state = ACTING_USER
        .scope(
            "alice".to_string(),
            cqrs.execute::<BankAccount>(&account, Default::default(), deposit(50.0), &(), None),
        )
        .await?;
    assert_eq!(account_state.balance, 150.0);
    println!("balance after maintenance: {}", account_state.balance);

    Ok(())
}
