//! A policy that cannot advance says so in the log (funkode-io/replay#169).
//!
//! The incident behind funkode-io/replay#164 was not caused by the wait — the feed
//! waiting for a `global_position` that is still committing is the design. It was
//! caused by the wait being *indistinguishable from idleness*: nineteen policies
//! stopped in front of a burned position for three days and emitted, between them,
//! nothing at all.
//!
//! So these tests assert on the log itself rather than on the code that writes it:
//! what an operator would have had on screen, under the four conditions that matter
//! — healthy and idle, just stopped, stopped for a while, and stopped for a while
//! longer.

use std::sync::atomic::{AtomicUsize, Ordering};

use sqlx::{postgres::PgPoolOptions, PgPool};
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};
use tracing_test::traced_test;

use replay_macros::define_aggregate;
use replay_persistence::{Cqrs, PolicyRunner, PostgresEventStore, StartAt};

mod common;
use common::postgres_image::postgres_container;

const POSTGRES_PORT: u16 = 5432;

/// A policy whose cursor is parked long enough for the stop to be an incident.
const BLOCKED_LONG: &str = "long_blocked_audit";
/// A policy parked at the same hole, but only just: indistinguishable from an
/// append still landing, and therefore not yet worth a warning.
const BLOCKED_JUST_NOW: &str = "newly_blocked_audit";

define_aggregate! {
    Ledger {
        namespace: "ledger",
        state: {
            balance: f64,
        },
        commands: {
            Add { amount: f64 },
        },
        events: {
            Added { amount: f64 },
        }
    }
}

impl replay::EventStream for Ledger {
    type Event = LedgerEvent;

    fn stream_type() -> String {
        "Ledger".to_string()
    }

    fn apply(&mut self, event: Self::Event) {
        let LedgerEvent::Added { amount } = event;
        self.balance += amount;
    }
}

impl replay::Aggregate for Ledger {
    type Command = LedgerCommand;
    type Error = replay::Error;
    type Services = ();

    async fn handle(
        &self,
        command: Self::Command,
        _services: &Self::Services,
    ) -> Result<Vec<Self::Event>, Self::Error> {
        let LedgerCommand::Add { amount } = command;
        Ok(vec![LedgerEvent::Added { amount }])
    }
}

async fn start_postgres() -> (
    PgPool,
    testcontainers_modules::testcontainers::ContainerAsync<postgres::Postgres>,
) {
    let container = postgres_container().start().await.unwrap();
    let host = container.get_host().await.unwrap().to_string();
    let port = container
        .get_host_port_ipv4(POSTGRES_PORT)
        .await
        .expect("Error getting docker port");

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&format!(
            "postgres://postgres:postgres@{host}:{port}/postgres"
        ))
        .await
        .expect("Failed to connect to Postgres");

    sqlx::migrate!("./tests/migrations")
        .run(&pool)
        .await
        .expect("Failed to run migrations");

    (pool, container)
}

/// Lines the runner writes about a stopped feed, in the order it wrote them.
fn gap_traces<'a>(lines: &[&'a str]) -> Vec<&'a str> {
    lines
        .iter()
        .copied()
        .filter(|line| line.contains("policy feed stops at a gap"))
        .collect()
}

/// Lines escalating a stop to an incident.
fn blocked_warnings<'a>(lines: &[&'a str]) -> Vec<&'a str> {
    lines
        .iter()
        .copied()
        .filter(|line| line.contains("policy is blocked"))
        .collect()
}

/// The `blocked_for_secs` field of a warning line.
fn blocked_for_secs(line: &str) -> u64 {
    line.split("blocked_for_secs=")
        .nth(1)
        .expect("the warning names how long the policy has been blocked")
        .split_whitespace()
        .next()
        .expect("a value follows the field name")
        .parse()
        .expect("blocked_for_secs is a number of seconds")
}

#[tokio::test]
#[traced_test]
async fn a_blocked_policy_says_so_in_the_log_postgres_test() {
    let (pool, _container) = start_postgres().await;

    let cqrs = Cqrs::new(PostgresEventStore::new(pool.clone()));
    let runner = PolicyRunner::builder(cqrs.clone())
        .register_policy_fn::<LedgerEvent, _>(BLOCKED_LONG, StartAt::Beginning, |_| vec![])
        .register_policy_fn::<LedgerEvent, _>(BLOCKED_JUST_NOW, StartAt::Beginning, |_| vec![])
        .build();

    let ledger = LedgerUrn::new("blocked-policy-log").unwrap();
    let add = |amount: f64| {
        let cqrs = cqrs.clone();
        let ledger = ledger.clone();
        async move {
            cqrs.execute::<Ledger>(
                &ledger,
                replay::Metadata::default(),
                LedgerCommand::Add { amount },
                &(),
                None,
            )
            .await
            .expect("append must succeed");
        }
    };

    // ── Healthy and idle ──────────────────────────────────────────────────────
    // Two events, drained, then drained again with nothing to do. A caught-up
    // policy is the case that must stay silent, or the signal below is worthless.
    add(10.0).await;
    add(5.0).await;
    for _ in 0..4 {
        runner.drain().await.expect("drain must succeed");
    }

    logs_assert(
        |lines| match (gap_traces(lines).len(), blocked_warnings(lines).len()) {
            (0, 0) => Ok(()),
            (gaps, warnings) => Err(format!(
                "a caught-up policy must say nothing, but wrote {gaps} gap traces \
             and {warnings} warnings"
            )),
        },
    );

    // ── Just stopped ──────────────────────────────────────────────────────────
    // The #164 fault, exactly: `nextval` is not transactional, so a position
    // consumed by an aborted append is gone permanently and the next event lands
    // behind a hole that will never fill.
    let burned: i64 = sqlx::query_scalar("SELECT nextval('events_global_position_seq')")
        .fetch_one(&pool)
        .await
        .expect("burning a sequence value must succeed");
    add(1.0).await;

    runner.drain().await.expect("drain must succeed");

    let head = burned + 1;
    let cursor = burned - 1;
    logs_assert(|lines| {
        let traces = gap_traces(lines);
        if traces.len() != 2 {
            return Err(format!(
                "both policies stop at the hole, so both trace it: got {} traces",
                traces.len()
            ));
        }
        for policy in [BLOCKED_LONG, BLOCKED_JUST_NOW] {
            let trace = traces
                .iter()
                .find(|line| line.contains(&format!("policy={policy}")))
                .ok_or_else(|| format!("no gap trace names {policy}"))?;
            for field in [
                "DEBUG",
                &format!("cursor={cursor}"),
                &format!("expected={burned}"),
                &format!("found={head}"),
            ] {
                if !trace.contains(field) {
                    return Err(format!("gap trace for {policy} lacks {field}: {trace}"));
                }
            }
        }
        Ok(())
    });

    // Nothing has been blocked long enough to be an incident yet: a hole this
    // young is what an in-flight commit looks like, and the runner waits for it.
    logs_assert(|lines| match blocked_warnings(lines).len() {
        0 => Ok(()),
        n => Err(format!(
            "a stop of a few milliseconds must not warn, got {n}"
        )),
    });

    // ── Stopped long enough to be an incident ─────────────────────────────────
    // Age one policy's stop past the escalation threshold by backdating the only
    // record of when its cursor last moved.
    sqlx::query(
        "UPDATE policy_cursors SET updated_at = now() - interval '10 minutes' WHERE name = $1",
    )
    .bind(BLOCKED_LONG)
    .execute(&pool)
    .await
    .expect("backdating the cursor must succeed");

    runner.drain().await.expect("drain must succeed");

    logs_assert(|lines| {
        let warnings = blocked_warnings(lines);
        let [warning] = warnings.as_slice() else {
            return Err(format!(
                "exactly the aged policy warns, got {} warnings",
                warnings.len()
            ));
        };
        for field in [
            "WARN",
            &format!("policy={BLOCKED_LONG}"),
            &format!("cursor={cursor}"),
            &format!("head={head}"),
            &format!("missing_position={burned}"),
        ] {
            if !warning.contains(field) {
                return Err(format!("the warning lacks {field}: {warning}"));
            }
        }
        let blocked_for = blocked_for_secs(warning);
        if blocked_for < 600 {
            return Err(format!(
                "the warning must report how long it has really been blocked \
                 (≥600s), got {blocked_for}s"
            ));
        }
        Ok(())
    });

    // ── Still stopped ─────────────────────────────────────────────────────────
    // A blocked policy polls forever. The escalation repeats at a bounded rate, so
    // three more polls inside the interval add nothing to the log.
    for _ in 0..3 {
        runner.drain().await.expect("drain must succeed");
    }

    logs_assert(|lines| match blocked_warnings(lines).len() {
        1 => Ok(()),
        n => Err(format!(
            "the warning must be rate-bounded while the policy stays blocked, got {n}"
        )),
    });

    // ── Recovered ─────────────────────────────────────────────────────────────
    // The operator's fix from the field report: move the parked cursors past the
    // hole. The policies advance again and go quiet.
    sqlx::query("UPDATE policy_cursors SET position = $1, updated_at = now() WHERE position = $2")
        .bind(burned)
        .bind(cursor)
        .execute(&pool)
        .await
        .expect("the operator's correction must succeed");

    let quiet_from = AtomicUsize::new(0);
    logs_assert(|lines| {
        quiet_from.store(lines.len(), Ordering::SeqCst);
        Ok(())
    });
    for _ in 0..3 {
        runner.drain().await.expect("drain must succeed");
    }

    logs_assert(|lines| {
        let after: Vec<&str> = lines
            .iter()
            .copied()
            .skip(quiet_from.load(Ordering::SeqCst))
            .collect();
        match (gap_traces(&after).len(), blocked_warnings(&after).len()) {
            (0, 0) => Ok(()),
            (gaps, warnings) => Err(format!(
                "a recovered policy must go quiet, but wrote {gaps} gap traces \
                 and {warnings} warnings"
            )),
        }
    });

    // Sanity: the correction is what made it quiet, not a stuck runner.
    let positions: Vec<i64> =
        sqlx::query_scalar("SELECT position FROM policy_cursors ORDER BY name")
            .fetch_all(&pool)
            .await
            .expect("reading the cursors must succeed");
    assert_eq!(
        positions,
        vec![head, head],
        "both policies must have drained the event stranded behind the hole"
    );
}
