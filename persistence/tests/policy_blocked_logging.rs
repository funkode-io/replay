//! A Policy that cannot advance says so in the log (funkode-io/replay#169), and a
//! healthy idle one still says nothing (funkode-io/replay#164).
//!
//! What stops a Policy since funkode-io/replay#195 is a write that has not ended: the
//! feed delivers an event only once its transaction has finished, so everything
//! committed after a still-open write is held back until that write commits or aborts.
//! The wait is the design — delivering past it would put those events ahead of what the
//! open write may still publish — and the silence was the bug. So these tests assert on
//! the log itself rather than on the code that writes it, through the states an
//! operator passes: caught up, just stopped, stopped too long, still stopped, cleared.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use sqlx::{postgres::PgPoolOptions, PgPool};
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};
use tracing_test::traced_test;

use replay_macros::define_aggregate;
use replay_persistence::{Cqrs, PolicyRunner, PostgresEventStore, StartAt};

mod common;
use common::held_append::hold_an_append_open;
use common::postgres_image::postgres_container;

const POSTGRES_PORT: u16 = 5432;

/// A policy whose cursor last advanced long ago, then meets a fresh wait: the trap
/// of measuring a stop from the cursor instead of from the wait.
const AGED_CURSOR: &str = "aged_cursor_audit";
/// A policy that was working right up to the same wait.
const FRESH_CURSOR: &str = "fresh_cursor_audit";

/// Escalation threshold and repeat spacing for this test, short enough to elapse.
const WARN_AFTER: Duration = Duration::from_secs(1);

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

/// Lines the runner writes about a feed that is waiting, in the order it wrote them.
fn wait_traces<'a>(lines: &[&'a str]) -> Vec<&'a str> {
    lines
        .iter()
        .copied()
        .filter(|line| line.contains("policy feed is waiting for an open write"))
        .collect()
}

/// Lines escalating a wait to an incident.
fn waiting_warnings<'a>(lines: &[&'a str]) -> Vec<&'a str> {
    lines
        .iter()
        .copied()
        .filter(|line| line.contains("policy is waiting on a write that has not ended"))
        .collect()
}

/// The `waiting_for_secs` field of a warning line.
fn waiting_for_secs(line: &str) -> u64 {
    line.split("waiting_for_secs=")
        .nth(1)
        .expect("the warning names how long the policy has been waiting")
        .split_whitespace()
        .next()
        .expect("a value follows the field name")
        .parse()
        .expect("waiting_for_secs is a number of seconds")
}

#[tokio::test]
#[traced_test]
async fn a_policy_waiting_on_an_open_write_says_so_in_the_log_postgres_test() {
    let (pool, _container) = start_postgres().await;

    // Read once, at build: this binary runs one test, so nothing else sees it.
    std::env::set_var(
        "REPLAY_BLOCKED_WARN_AFTER_SECS",
        WARN_AFTER.as_secs().to_string(),
    );

    let cqrs = Cqrs::new(PostgresEventStore::new(pool.clone()));
    let runner = PolicyRunner::builder(cqrs.clone())
        .register_policy_fn::<LedgerEvent, _>(AGED_CURSOR, StartAt::Beginning, |_| vec![])
        .register_policy_fn::<LedgerEvent, _>(FRESH_CURSOR, StartAt::Beginning, |_| vec![])
        .build();

    let add = |stream: &'static str, amount: f64| {
        let cqrs = cqrs.clone();
        async move {
            cqrs.execute::<Ledger>(
                &LedgerUrn::new(stream).unwrap(),
                replay::Metadata::default(),
                LedgerCommand::Add { amount },
                &(),
                None,
            )
            .await
            .expect("append must succeed");
        }
    };

    // ── Caught up ─────────────────────────────────────────────────────────────
    // A drained policy must stay silent, or the signal below is worthless.
    add("waiting-policy-log", 10.0).await;
    // A second stream for the append held open below, which takes the next version of
    // the stream it is on and holds that stream's lock until the test ends it.
    add("waiting-policy-held", 5.0).await;
    for _ in 0..4 {
        runner.drain().await.expect("drain must succeed");
    }

    logs_assert(
        |lines| match (wait_traces(lines).len(), waiting_warnings(lines).len()) {
            (0, 0) => Ok(()),
            (traces, warnings) => Err(format!(
                "a caught-up policy must say nothing, but wrote {traces} traces \
             and {warnings} warnings"
            )),
        },
    );

    // ── Just stopped ──────────────────────────────────────────────────────────
    // A write is open and has not committed, so the event appended after it cannot be
    // delivered: it was written after a transaction that may still publish events of
    // its own. Waiting for it is the design.
    let holder = hold_an_append_open(&pool, 2).await;
    let held = holder.position;
    add("waiting-policy-log", 1.0).await;
    let withheld = held + 1;

    // One cursor has not advanced in ten minutes — healthy idleness, not a stop.
    // Its wait starts now, with the open write, and must not be dated from the row.
    sqlx::query(
        "UPDATE policy_cursors SET updated_at = now() - interval '10 minutes' WHERE name = $1",
    )
    .bind(AGED_CURSOR)
    .execute(&pool)
    .await
    .expect("backdating the cursor must succeed");

    runner.drain().await.expect("drain must succeed");

    let cursor = 2;
    logs_assert(|lines| {
        let traces = wait_traces(lines);
        if traces.len() != 2 {
            return Err(format!(
                "both policies stop behind the open write, so both trace it: got {} traces",
                traces.len()
            ));
        }
        for policy in [AGED_CURSOR, FRESH_CURSOR] {
            let trace = traces
                .iter()
                .find(|line| line.contains(&format!("policy={policy}")))
                .ok_or_else(|| format!("no trace names {policy}"))?;
            for field in [
                "DEBUG",
                &format!("cursor={cursor}"),
                &format!("withheld_position={withheld}"),
            ] {
                if !trace.contains(field) {
                    return Err(format!("the trace for {policy} lacks {field}: {trace}"));
                }
            }
        }
        Ok(())
    });

    // A wait this young is what an ordinary append looks like — including for the
    // policy whose cursor has been sitting still for ten minutes.
    logs_assert(|lines| match waiting_warnings(lines).len() {
        0 => Ok(()),
        n => Err(format!(
            "a wait of a few milliseconds must not warn, got {n}"
        )),
    });

    // ── Stopped too long ──────────────────────────────────────────────────────
    // The write stays open past the threshold, so both policies escalate.
    tokio::time::sleep(WARN_AFTER + Duration::from_millis(200)).await;
    runner.drain().await.expect("drain must succeed");

    logs_assert(|lines| {
        let warnings = waiting_warnings(lines);
        if warnings.len() != 2 {
            return Err(format!(
                "both waiting policies warn once the write outlives the threshold, got {}",
                warnings.len()
            ));
        }
        for policy in [AGED_CURSOR, FRESH_CURSOR] {
            let warning = warnings
                .iter()
                .find(|line| line.contains(&format!("policy={policy}")))
                .ok_or_else(|| format!("no warning names {policy}"))?;
            for field in [
                "WARN",
                &format!("cursor={cursor}"),
                &format!("head={withheld}"),
                &format!("withheld_position={withheld}"),
            ] {
                if !warning.contains(field) {
                    return Err(format!("the warning for {policy} lacks {field}: {warning}"));
                }
            }
        }

        // The reported duration is the cursor's, so it survives restarts and
        // leadership changes: ten minutes for the aged row, seconds for the other.
        let aged = warnings
            .iter()
            .find(|line| line.contains(&format!("policy={AGED_CURSOR}")))
            .map(|line| waiting_for_secs(line))
            .expect("the aged policy warned");
        if aged < 600 {
            return Err(format!(
                "the warning must report the cursor's real age (≥600s), got {aged}s"
            ));
        }
        Ok(())
    });

    // ── Still stopped ─────────────────────────────────────────────────────────
    // A waiting policy polls forever; three more polls inside the interval add
    // nothing to the log.
    for _ in 0..3 {
        runner.drain().await.expect("drain must succeed");
    }

    logs_assert(|lines| match waiting_warnings(lines).len() {
        2 => Ok(()),
        n => Err(format!(
            "the warning must be rate-bounded while the policy keeps waiting, got {n}"
        )),
    });

    // ── Cleared ───────────────────────────────────────────────────────────────
    // The write ends. Nothing is corrected by hand and no cursor is moved: the wait
    // was never a fault, so it resolves itself and the policies go quiet.
    let quiet_from = AtomicUsize::new(0);
    logs_assert(|lines| {
        quiet_from.store(lines.len(), Ordering::SeqCst);
        Ok(())
    });

    holder.tx.commit().await.expect("committing must succeed");
    for _ in 0..3 {
        runner.drain().await.expect("drain must succeed");
    }

    logs_assert(|lines| {
        let after: Vec<&str> = lines
            .iter()
            .copied()
            .skip(quiet_from.load(Ordering::SeqCst))
            .collect();
        match (wait_traces(&after).len(), waiting_warnings(&after).len()) {
            (0, 0) => Ok(()),
            (traces, warnings) => Err(format!(
                "a policy that caught up must go quiet, but wrote {traces} traces \
                 and {warnings} warnings"
            )),
        }
    });

    // Sanity: the write landing is what made it quiet, not a stuck runner.
    let positions: Vec<i64> =
        sqlx::query_scalar("SELECT position FROM policy_cursors ORDER BY name")
            .fetch_all(&pool)
            .await
            .expect("reading the cursors must succeed");
    assert_eq!(
        positions,
        vec![withheld, withheld],
        "both policies must have drained the event that was held back"
    );
}

/// A daemon keeps its cursor in memory across polls, so an operator's correction is
/// adopted by the refresh on an empty feed — one poll *after* the read that still used
/// the abandoned position. What that read was waiting behind belongs to a cursor that no
/// longer exists and must not be traced as where the policy is stopped.
///
/// The manual drain cannot show this: it reloads the cursor every call.
#[tokio::test]
#[traced_test]
async fn a_correction_adopted_by_a_running_daemon_traces_no_stale_wait_postgres_test() {
    let (pool, _container) = start_postgres().await;

    // Long enough that a poll cannot slip between the correction and the snapshot
    // below, and no NOTIFY to wake one early.
    let poll = Duration::from_secs(2);

    let cqrs = Cqrs::new(PostgresEventStore::new(pool.clone()));
    let runner = PolicyRunner::builder(cqrs.clone())
        .without_notifications()
        .register_policy_fn::<LedgerEvent, _>(FRESH_CURSOR, StartAt::Beginning, |_| vec![])
        .build();
    let daemon = runner.start_polling(poll);

    let add = |stream: &'static str, amount: f64| {
        let cqrs = cqrs.clone();
        async move {
            cqrs.execute::<Ledger>(
                &LedgerUrn::new(stream).unwrap(),
                replay::Metadata::default(),
                LedgerCommand::Add { amount },
                &(),
                None,
            )
            .await
            .expect("append must succeed");
        }
    };
    let stored = || async {
        sqlx::query_scalar::<_, i64>("SELECT position FROM policy_cursors WHERE name = $1")
            .bind(FRESH_CURSOR)
            .fetch_optional(&pool)
            .await
            .expect("reading the cursor must succeed")
            .unwrap_or_default()
    };
    let wait_for_position = |want: i64| async move {
        for _ in 0..100 {
            if stored().await >= want {
                return;
            }
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        panic!("the daemon never reached position {want}");
    };
    let traces_so_far = || {
        let n = AtomicUsize::new(0);
        logs_assert(|lines| {
            n.store(wait_traces(lines).len(), Ordering::SeqCst);
            Ok(())
        });
        n.load(Ordering::SeqCst)
    };

    add("daemon-correction", 10.0).await;
    // As above: the held append is on this second stream, so the append that follows it
    // does not queue behind the transaction under test.
    add("daemon-correction-held", 5.0).await;
    wait_for_position(2).await;

    let holder = hold_an_append_open(&pool, 2).await;
    let held = holder.position;
    add("daemon-correction", 1.0).await;

    // Wait for the daemon to trace the stop. Doubles as proof that this test can see
    // the daemon's records at all, so the absence asserted below means something.
    let mut traced = false;
    for _ in 0..100 {
        if traces_so_far() > 0 {
            traced = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(traced, "the daemon never traced the stop");

    let traces_before = traces_so_far();
    sqlx::query("UPDATE policy_cursors SET position = $1, updated_at = now() WHERE name = $2")
        .bind(held + 1)
        .bind(FRESH_CURSOR)
        .execute(&pool)
        .await
        .expect("the operator's correction must succeed");

    // Waiting on the log, not on the row: the row already holds the corrected position
    // because this test wrote it, and the poll that adopts it is the one under test.
    let mut adopted = false;
    for _ in 0..100 {
        if logs_contain("persisted cursor was moved externally") {
            adopted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    assert!(adopted, "the daemon never adopted the correction in place");

    assert_eq!(
        traces_so_far(),
        traces_before,
        "adopting the correction must not trace a wait read from the abandoned cursor"
    );

    holder.tx.rollback().await.expect("rollback must succeed");
    daemon.shutdown().await;
}
