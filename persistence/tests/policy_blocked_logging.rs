//! A policy that cannot advance says so in the log (funkode-io/replay#169).
//!
//! The wait is the design; the silence was the bug. So this test asserts on the log
//! itself rather than on the code that writes it, through the states an operator
//! passes: caught up, just stopped, stopped too long, still stopped, corrected.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use sqlx::{postgres::PgPoolOptions, PgPool};
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};
use tracing_test::traced_test;

use replay_macros::define_aggregate;
use replay_persistence::{Cqrs, PolicyRunner, PostgresEventStore, StartAt};

mod common;
use common::postgres_image::postgres_container;

const POSTGRES_PORT: u16 = 5432;

/// A policy whose cursor last advanced long ago, then meets a fresh hole: the trap
/// of measuring a blockage from the cursor instead of from the hole.
const AGED_CURSOR: &str = "aged_cursor_audit";
/// A policy that was working right up to the same hole.
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

    // ── Caught up ─────────────────────────────────────────────────────────────
    // A drained policy must stay silent, or the signal below is worthless.
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
    // The #164 fault: `nextval` is not transactional, so a position consumed by an
    // aborted append is gone for good and the next event lands behind the hole.
    let burned: i64 = sqlx::query_scalar("SELECT nextval('events_global_position_seq')")
        .fetch_one(&pool)
        .await
        .expect("burning a sequence value must succeed");
    add(1.0).await;

    // One cursor has not advanced in ten minutes — healthy idleness, not a stop.
    // Its blockage starts now, with the hole, and must not be dated from the row.
    sqlx::query(
        "UPDATE policy_cursors SET updated_at = now() - interval '10 minutes' WHERE name = $1",
    )
    .bind(AGED_CURSOR)
    .execute(&pool)
    .await
    .expect("backdating the cursor must succeed");

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
        for policy in [AGED_CURSOR, FRESH_CURSOR] {
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

    // A hole this young is what an in-flight commit looks like — including for the
    // policy whose cursor has been sitting still for ten minutes.
    logs_assert(|lines| match blocked_warnings(lines).len() {
        0 => Ok(()),
        n => Err(format!(
            "a stop of a few milliseconds must not warn, got {n}"
        )),
    });

    // ── Stopped too long ──────────────────────────────────────────────────────
    // The hole outlives the threshold, so both policies escalate.
    tokio::time::sleep(WARN_AFTER + Duration::from_millis(200)).await;
    runner.drain().await.expect("drain must succeed");

    logs_assert(|lines| {
        let warnings = blocked_warnings(lines);
        if warnings.len() != 2 {
            return Err(format!(
                "both blocked policies warn once the hole persists, got {}",
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
                &format!("head={head}"),
                &format!("missing_position={burned}"),
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
            .map(|line| blocked_for_secs(line))
            .expect("the aged policy warned");
        if aged < 600 {
            return Err(format!(
                "the warning must report the cursor's real age (≥600s), got {aged}s"
            ));
        }
        Ok(())
    });

    // ── Still stopped ─────────────────────────────────────────────────────────
    // A blocked policy polls forever; three more polls inside the interval add
    // nothing to the log.
    for _ in 0..3 {
        runner.drain().await.expect("drain must succeed");
    }

    logs_assert(|lines| match blocked_warnings(lines).len() {
        2 => Ok(()),
        n => Err(format!(
            "the warning must be rate-bounded while the policy stays blocked, got {n}"
        )),
    });

    // ── Corrected ─────────────────────────────────────────────────────────────
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

    // ── Stopping mid-window ───────────────────────────────────────────────────
    // A window with work in it *and* a hole behind that work: the policy advances
    // over the prefix and parks at the hole, so the trace must name where it parks,
    // not where the poll started.
    let traces_before = AtomicUsize::new(0);
    logs_assert(|lines| {
        traces_before.store(gap_traces(lines).len(), Ordering::SeqCst);
        Ok(())
    });

    add(2.0).await;
    let burned_again: i64 = sqlx::query_scalar("SELECT nextval('events_global_position_seq')")
        .fetch_one(&pool)
        .await
        .expect("burning a sequence value must succeed");
    add(3.0).await;

    runner.drain().await.expect("drain must succeed");

    let parks_at = burned_again - 1;
    logs_assert(|lines| {
        for trace in gap_traces(lines)
            .into_iter()
            .skip(traces_before.load(Ordering::SeqCst))
        {
            for field in [
                &format!("cursor={parks_at}"),
                &format!("expected={burned_again}"),
                &format!("found={}", burned_again + 1),
            ] {
                if !trace.contains(field) {
                    return Err(format!("the trace lacks {field}: {trace}"));
                }
            }
        }
        Ok(())
    });

    let positions: Vec<i64> =
        sqlx::query_scalar("SELECT position FROM policy_cursors ORDER BY name")
            .fetch_all(&pool)
            .await
            .expect("reading the cursors must succeed");
    assert_eq!(
        positions,
        vec![parks_at, parks_at],
        "the traced cursor must be the one the policies actually parked at"
    );
}

/// A daemon keeps its cursor in memory across polls, so an operator's correction is
/// adopted by the refresh on an empty feed — one poll *after* the read that still
/// used the abandoned position. That read's gap belongs to a cursor that no longer
/// exists and must not be traced as where the policy is parked.
///
/// The manual drain cannot show this: it reloads the cursor every call.
#[tokio::test]
#[traced_test]
async fn a_correction_adopted_by_a_running_daemon_traces_no_stale_gap_postgres_test() {
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

    let ledger = LedgerUrn::new("daemon-correction").unwrap();
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
            n.store(gap_traces(lines).len(), Ordering::SeqCst);
            Ok(())
        });
        n.load(Ordering::SeqCst)
    };

    add(10.0).await;
    wait_for_position(1).await;

    let burned: i64 = sqlx::query_scalar("SELECT nextval('events_global_position_seq')")
        .fetch_one(&pool)
        .await
        .expect("burning a sequence value must succeed");
    add(1.0).await;

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
        .bind(burned)
        .bind(FRESH_CURSOR)
        .execute(&pool)
        .await
        .expect("the operator's correction must succeed");

    wait_for_position(burned + 1).await;

    assert_eq!(
        traces_so_far(),
        traces_before,
        "adopting the correction must not trace a gap read from the abandoned cursor"
    );
    assert!(
        logs_contain("persisted cursor was moved externally"),
        "the daemon must have adopted the correction in place"
    );

    daemon.shutdown().await;
}
