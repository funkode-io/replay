//! A `global_position` that can never appear no longer wedges a Policy
//! (funkode-io/replay#170).
//!
//! `nextval` is not transactional, so a position taken by an append that then
//! aborts is burned for good. The feed cannot tell that from an append still
//! committing, and in funkode-io/replay#164 one burned position stopped nineteen
//! policies for three days. These tests drive the three shapes that matter: a
//! position burned from the sequence, an append rolled back (the shape of the
//! reported incident), and an append that is genuinely still running and must be
//! waited for.

use std::sync::atomic::{AtomicUsize, Ordering};

use sqlx::{postgres::PgPoolOptions, Executor, PgPool};
use testcontainers_modules::testcontainers::ImageExt;
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};
use tracing_test::traced_test;

use replay_macros::define_aggregate;
use replay_persistence::{Cqrs, PolicyRunner, PostgresEventStore, StartAt};

mod common;
use common::postgres_image::postgres_container;

const POSTGRES_PORT: u16 = 5432;

const AUDIT: &str = "burned_position_audit";

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

/// Lines reporting positions the runner decided can never appear.
fn skip_warnings<'a>(lines: &[&'a str]) -> Vec<&'a str> {
    lines
        .iter()
        .copied()
        .filter(|line| line.contains("policy feed skipped global_position values"))
        .collect()
}

/// The highest `global_position` that exists.
async fn head(pool: &PgPool) -> i64 {
    sqlx::query_scalar::<_, i64>("SELECT COALESCE(MAX(global_position), 0) FROM events")
        .fetch_one(pool)
        .await
        .expect("reading the head must succeed")
}

/// How far the Policy's stored cursor has got.
async fn stored_cursor(pool: &PgPool, policy: &str) -> i64 {
    sqlx::query_scalar::<_, i64>("SELECT position FROM policy_cursors WHERE name = $1")
        .bind(policy)
        .fetch_optional(pool)
        .await
        .expect("reading the cursor must succeed")
        .unwrap_or_default()
}

/// Clone the event at `source` into `tx` as the next version of its own stream,
/// returning the `global_position` the copy took.
///
/// A copy rather than a hand-written row: it is shaped exactly like an event the
/// store appended — valid stream URN, type tag and payload — so what the Policy
/// does with it is the Policy's real behaviour and not an artefact of the fixture.
async fn clone_event_into(tx: &mut sqlx::Transaction<'_, sqlx::Postgres>, source: i64) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO events (id, data, metadata, stream_id, type, version, created) \
         SELECT gen_random_uuid(), data, metadata, stream_id, type, version + 1, now() \
         FROM events WHERE global_position = $1 RETURNING global_position",
    )
    .bind(source)
    .fetch_one(&mut **tx)
    .await
    .expect("cloning an event must succeed")
}

/// Burn `count` sequence values the way an aborted append does, returning the first.
async fn burn_positions(pool: &PgPool, count: i64) -> i64 {
    // `MIN` over the whole series, not `LIMIT 1` over it: a limit would stop the
    // scan after the first `nextval` and burn one position however many were asked
    // for.
    sqlx::query_scalar(
        "SELECT MIN(burned) FROM \
         (SELECT nextval('events_global_position_seq') AS burned FROM generate_series(1, $1)) s",
    )
    .bind(count)
    .fetch_one(pool)
    .await
    .expect("burning sequence values must succeed")
}

/// Drain until the Policy reaches `want`, or give up after `polls`.
async fn drain_until(runner: &PolicyRunner, pool: &PgPool, want: i64, polls: usize) -> usize {
    for poll in 1..=polls {
        runner.drain().await.expect("drain must succeed");
        if stored_cursor(pool, AUDIT).await >= want {
            return poll;
        }
    }
    panic!(
        "the policy never reached position {want} in {polls} polls; it is at {}",
        stored_cursor(pool, AUDIT).await
    );
}

/// A burned position, and then an aborted append: both leave a hole nothing can
/// fill, and the Policy must cross both and deliver what was stranded behind them.
#[tokio::test]
#[traced_test]
async fn a_permanently_burned_position_no_longer_wedges_a_policy_postgres_test() {
    let (pool, _container) = start_postgres().await;

    let reacted = std::sync::Arc::new(AtomicUsize::new(0));
    let counter = std::sync::Arc::clone(&reacted);
    let cqrs = Cqrs::new(PostgresEventStore::new(pool.clone()));
    let runner = PolicyRunner::builder(cqrs.clone())
        .register_policy_fn::<LedgerEvent, _>(AUDIT, StartAt::Beginning, move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
            vec![]
        })
        .build();

    let ledger = LedgerUrn::new("burned-position").unwrap();
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
    add(10.0).await;
    drain_until(&runner, &pool, 1, 4).await;
    assert_eq!(reacted.load(Ordering::SeqCst), 1);

    // ── One burned position ───────────────────────────────────────────────────
    // The minimal reproduction from the field report: burn a value, then append.
    let burned = burn_positions(&pool, 1).await;
    add(1.0).await;

    let polls = drain_until(&runner, &pool, burned + 1, 5).await;
    assert_eq!(
        reacted.load(Ordering::SeqCst),
        2,
        "the event stranded behind the burned position must be delivered"
    );

    logs_assert(|lines| {
        let warnings = skip_warnings(lines);
        if warnings.len() != 1 {
            return Err(format!(
                "crossing a burned position must be reported exactly once, got {}",
                warnings.len()
            ));
        }
        for field in [
            "WARN",
            &format!("policy={AUDIT}"),
            &format!("skipped_from={burned}"),
            &format!("skipped_to={burned}"),
            &format!("next_position={}", burned + 1),
        ] {
            if !warnings[0].contains(field) {
                return Err(format!("the warning lacks {field}: {}", warnings[0]));
            }
        }
        Ok(())
    });
    assert!(
        polls <= 3,
        "a burned position must clear within a couple of polls, took {polls}"
    );

    // ── An aborted append ─────────────────────────────────────────────────────
    // The shape of the reported incident: a transaction inserts an event row and
    // then rolls back, leaving the position it took gone for good.
    let mut aborted = pool.begin().await.expect("beginning must succeed");
    let rolled_back = clone_event_into(&mut aborted, head(&pool).await).await;
    aborted.rollback().await.expect("rollback must succeed");

    add(2.0).await;

    let polls = drain_until(&runner, &pool, rolled_back + 1, 5).await;
    assert_eq!(
        reacted.load(Ordering::SeqCst),
        3,
        "the event appended after the rollback must be delivered"
    );
    assert!(
        polls <= 3,
        "an aborted append must clear within a couple of polls, took {polls}"
    );

    // ── A run of burned positions ─────────────────────────────────────────────
    // A batch append that dies after a hundred rows burns a hundred positions. The
    // whole run is one hole and costs one skip, not one poll per position.
    let first_burned = burn_positions(&pool, 100).await;
    add(3.0).await;

    let polls = drain_until(&runner, &pool, first_burned + 100, 5).await;
    assert_eq!(reacted.load(Ordering::SeqCst), 4);
    assert!(
        polls <= 3,
        "a run of burned positions must cost a bounded number of polls, took {polls}"
    );

    logs_assert(|lines| {
        let warnings = skip_warnings(lines);
        let last = warnings.last().expect("the run was reported");
        for field in [
            &format!("skipped_from={first_burned}"),
            &format!("skipped_to={}", first_burned + 99),
            "skipped=100",
        ] {
            if !last.contains(field) {
                return Err(format!("the warning lacks {field}: {last}"));
            }
        }
        Ok(())
    });
}

/// An append that is still running holds the position it took, so its hole can
/// still fill: the Policy must wait for it however many times it polls, and deliver
/// the event when it commits. Skipping here would be silent event loss, which is
/// the whole reason the feed waits at a hole.
#[tokio::test]
#[traced_test]
async fn an_append_still_in_flight_is_waited_for_not_skipped_postgres_test() {
    let (pool, _container) = start_postgres().await;

    let reacted = std::sync::Arc::new(AtomicUsize::new(0));
    let counter = std::sync::Arc::clone(&reacted);
    let cqrs = Cqrs::new(PostgresEventStore::new(pool.clone()));
    let runner = PolicyRunner::builder(cqrs.clone())
        .register_policy_fn::<LedgerEvent, _>(AUDIT, StartAt::Beginning, move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
            vec![]
        })
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

    add("in-flight-main", 10.0).await;
    // A second stream, so the append held open below cannot collide on
    // `(stream_id, version)` with the append that follows it.
    add("in-flight-held", 5.0).await;
    drain_until(&runner, &pool, 2, 4).await;
    assert_eq!(reacted.load(Ordering::SeqCst), 2);

    // An append in flight: the row exists, its position is taken, and nothing else
    // can see it yet.
    let mut in_flight = pool.begin().await.expect("beginning must succeed");
    let held = clone_event_into(&mut in_flight, 2).await;

    // A later append commits ahead of it, which is what turns the in-flight
    // position into a hole the feed can see.
    add("in-flight-main", 1.0).await;

    for _ in 0..5 {
        runner.drain().await.expect("drain must succeed");
    }

    assert_eq!(
        stored_cursor(&pool, AUDIT).await,
        held - 1,
        "the policy must park in front of an append that is still running"
    );
    logs_assert(|lines| match skip_warnings(lines).len() {
        0 => Ok(()),
        n => Err(format!(
            "a position a running transaction still holds must never be skipped, \
             but {n} skips were reported"
        )),
    });
    assert_eq!(
        reacted.load(Ordering::SeqCst),
        2,
        "nothing past the in-flight position may be delivered before it lands"
    );

    // It commits: the hole fills and both events arrive, in position order.
    in_flight
        .commit()
        .await
        .expect("committing the append must succeed");

    drain_until(&runner, &pool, held + 1, 5).await;
    assert_eq!(
        reacted.load(Ordering::SeqCst),
        4,
        "the event that was in flight and the one behind it must both be delivered"
    );
}

/// A transaction that has taken a sequence value but not yet inserted anything has
/// no `xid` at all, so no transaction snapshot lists it. It can still fill the hole,
/// and the runner must still wait for it — this is the case that rules out deciding
/// permanence from `pg_current_snapshot()`'s `xmin`/`xmax`.
#[tokio::test]
#[traced_test]
async fn a_transaction_holding_a_position_without_an_xid_is_waited_for_postgres_test() {
    let (pool, _container) = start_postgres().await;

    let cqrs = Cqrs::new(PostgresEventStore::new(pool.clone()));
    let runner = PolicyRunner::builder(cqrs.clone())
        .register_policy_fn::<LedgerEvent, _>(AUDIT, StartAt::Beginning, |_| vec![])
        .build();

    let ledger = LedgerUrn::new("xid-less-holder").unwrap();
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

    add(10.0).await;
    drain_until(&runner, &pool, 1, 4).await;

    // Takes the next position and writes nothing: no `xid`, no tuple, no entry in
    // any snapshot's in-progress list.
    let mut holder = pool.begin().await.expect("beginning must succeed");
    let taken: i64 = sqlx::query_scalar("SELECT nextval('events_global_position_seq')")
        .fetch_one(&mut *holder)
        .await
        .expect("taking a position must succeed");
    holder
        .execute("SELECT 1")
        .await
        .expect("keeping the transaction open must succeed");

    add(1.0).await;

    for _ in 0..5 {
        runner.drain().await.expect("drain must succeed");
    }

    assert_eq!(
        stored_cursor(&pool, AUDIT).await,
        taken - 1,
        "a position a transaction still holds is not burned, whatever it has written"
    );
    logs_assert(|lines| match skip_warnings(lines).len() {
        0 => Ok(()),
        n => Err(format!("nothing may be skipped while it is held, got {n}")),
    });

    // Once it ends the position really is burned, and the Policy crosses it.
    holder.rollback().await.expect("rollback must succeed");

    drain_until(&runner, &pool, taken + 1, 5).await;
    logs_assert(|lines| match skip_warnings(lines).len() {
        1 => Ok(()),
        n => Err(format!(
            "the abandoned position must be crossed and reported once, got {n}"
        )),
    });
}

/// The rule this fix rejects, executed, so nobody reinstates it by accident.
///
/// The obvious way to decide permanence is a transaction-snapshot watermark: record
/// `pg_snapshot_xmax(pg_current_snapshot())` when the hole is first seen, and treat
/// the hole as permanent once `pg_snapshot_xmin(pg_current_snapshot())` has reached
/// it, on the reasoning that every transaction in flight at the first reading has
/// then ended. It is wrong, and this test makes it be wrong on demand.
///
/// A snapshot's `xmax` is one past the newest *completed* transaction, and its list
/// of in-flight transactions only reaches that far. An append that is running while
/// nothing newer has completed therefore sits above the watermark and is in no list
/// at all: the reading says "nothing was in flight" about a transaction that is
/// holding the very position in question. One later commit is enough to carry `xmin`
/// to the watermark and complete the false proof.
///
/// The consequence is the reason the feed waits at holes in the first place. The
/// burned-position fix trades a loud, recoverable outage for a quiet one only if it
/// gets this wrong: skipping a position that later commits delivers that event to
/// nobody, ever, with every health signal green.
///
/// If this test ever fails, Postgres has changed what a snapshot reports and the
/// rejected design deserves a fresh look. Until then, it is the evidence for
/// ADR-0015's choice of the sequence's lock holders as the oracle.
#[tokio::test]
#[traced_test]
async fn a_transaction_snapshot_cannot_prove_a_position_is_burned_postgres_test() {
    let (pool, _container) = start_postgres().await;

    let cqrs = Cqrs::new(PostgresEventStore::new(pool.clone()));
    let runner = PolicyRunner::builder(cqrs.clone())
        .register_policy_fn::<LedgerEvent, _>(AUDIT, StartAt::Beginning, |_| vec![])
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

    let snapshot_bounds = || {
        let pool = pool.clone();
        async move {
            sqlx::query_as::<_, (i64, i64)>(
                "SELECT pg_snapshot_xmin(pg_current_snapshot())::text::bigint, \
                 pg_snapshot_xmax(pg_current_snapshot())::text::bigint",
            )
            .fetch_one(&pool)
            .await
            .expect("reading the snapshot must succeed")
        }
    };

    add("snapshot-main", 10.0).await;
    add("snapshot-held", 5.0).await;
    drain_until(&runner, &pool, 2, 4).await;

    // An append in flight, holding the position it took. It has written a row, so it
    // has a transaction id and is exactly the case a snapshot is supposed to cover.
    let mut in_flight = pool.begin().await.expect("beginning must succeed");
    let held = clone_event_into(&mut in_flight, 2).await;
    let holder_xid: i64 = sqlx::query_scalar("SELECT pg_current_xact_id()::text::bigint")
        .fetch_one(&mut *in_flight)
        .await
        .expect("the holder has a transaction id");

    // What the rejected rule would record on the poll that first sees the hole.
    let (_, watermark) = snapshot_bounds().await;
    assert!(
        watermark <= holder_xid,
        "the watermark is one past the newest completed transaction, so a running \
         one at or above it is in no in-flight list: watermark {watermark}, holder \
         {holder_xid}"
    );

    // One ordinary append commits behind the hole. This is what makes the hole
    // visible to the feed at all \u2014 and, incidentally, completes the false proof.
    add("snapshot-main", 1.0).await;

    // Polled rather than read once: any unrelated transaction that happens to be
    // running holds `xmin` below the watermark until it ends, which delays the false
    // verdict without making it any less false.
    let mut ended_by_the_rejected_rule = false;
    for _ in 0..40 {
        if snapshot_bounds().await.0 >= watermark {
            ended_by_the_rejected_rule = true;
            break;
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
    assert!(
        ended_by_the_rejected_rule,
        "the rejected rule must reach its verdict here; if it no longer does, the \
         reasoning in ADR-0015 needs revisiting rather than this assertion relaxing"
    );

    // And it is a false verdict: the position is still missing, and the transaction
    // that can still write it is still running.
    let still_missing: bool =
        sqlx::query_scalar("SELECT NOT EXISTS (SELECT 1 FROM events WHERE global_position = $1)")
            .bind(held)
            .fetch_one(&pool)
            .await
            .expect("checking the position must succeed");
    let still_holds_the_sequence: bool = sqlx::query_scalar(
        "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype = 'relation' \
         AND relation = pg_get_serial_sequence('events', 'global_position')::regclass)",
    )
    .fetch_one(&pool)
    .await
    .expect("reading the locks must succeed");
    assert!(
        still_missing && still_holds_the_sequence,
        "the rejected rule called a position permanent while the append holding it \
         was still running"
    );

    // The runner, asked the right question, waits.
    for _ in 0..5 {
        runner.drain().await.expect("drain must succeed");
    }
    assert_eq!(
        stored_cursor(&pool, AUDIT).await,
        held - 1,
        "the position is not burned and must not be crossed"
    );
    logs_assert(|lines| match skip_warnings(lines).len() {
        0 => Ok(()),
        n => Err(format!(
            "the snapshot's false verdict must not reach the cursor, got {n} skips"
        )),
    });

    // The event the rejected rule would have lost.
    in_flight.commit().await.expect("committing must succeed");
    drain_until(&runner, &pool, held + 1, 5).await;
}

/// Reading the sequence is not holding a position. A long-lived transaction that
/// only looked at it — a monitoring query, or the operator's own diagnostic from the
/// field report — keeps an `AccessShareLock` on it until it ends, and counting that
/// as a candidate would keep a genuinely burned position blocked for as long as the
/// reader lives: the outage this whole mechanism exists to end, re-entered through
/// the mechanism itself.
#[tokio::test]
#[traced_test]
async fn a_long_lived_reader_of_the_sequence_does_not_hold_a_position_postgres_test() {
    let (pool, _container) = start_postgres().await;

    let cqrs = Cqrs::new(PostgresEventStore::new(pool.clone()));
    let runner = PolicyRunner::builder(cqrs.clone())
        .register_policy_fn::<LedgerEvent, _>(AUDIT, StartAt::Beginning, |_| vec![])
        .build();

    let ledger = LedgerUrn::new("sequence-reader").unwrap();
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

    add(10.0).await;
    drain_until(&runner, &pool, 1, 4).await;

    // Open for the rest of the test, reading the sequence and nothing else.
    let mut reader = pool.begin().await.expect("beginning must succeed");
    let _: i64 = sqlx::query_scalar("SELECT last_value FROM events_global_position_seq")
        .fetch_one(&mut *reader)
        .await
        .expect("reading the sequence must succeed");

    let burned = burn_positions(&pool, 1).await;
    add(1.0).await;

    let polls = drain_until(&runner, &pool, burned + 1, 5).await;
    assert!(
        polls <= 3,
        "a reader must not delay crossing a burned position, took {polls} polls"
    );
    logs_assert(|lines| match skip_warnings(lines).len() {
        1 => Ok(()),
        n => Err(format!("the burned position must be crossed once, got {n}")),
    });

    reader.rollback().await.expect("rollback must succeed");
}

/// A prepared transaction still holds the position it took, and is the one holder
/// that cannot be tracked by identity: Postgres keeps its virtual id while the
/// server runs and re-issues it as `-1/<xid>` after a restart, so a candidate
/// recorded before a restart would look like one that had ended — while
/// `COMMIT PREPARED` can still publish the missing event.
///
/// The runner therefore keys on the lock having no backend behind it rather than on
/// who holds it, and this test pins that marker as much as the behaviour: if a
/// future Postgres stops reporting a prepared holder with a null `pid`, the
/// assertion below fails rather than the skip quietly losing an event.
///
/// Two-phase commit is off by default, so this test runs its own server with it on.
#[tokio::test]
#[traced_test]
async fn a_prepared_transaction_holding_a_position_is_waited_for_postgres_test() {
    let container = postgres_container()
        .with_cmd(["postgres", "-c", "max_prepared_transactions=5"])
        .start()
        .await
        .unwrap();
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

    let reacted = std::sync::Arc::new(AtomicUsize::new(0));
    let counter = std::sync::Arc::clone(&reacted);
    let cqrs = Cqrs::new(PostgresEventStore::new(pool.clone()));
    let runner = PolicyRunner::builder(cqrs.clone())
        .register_policy_fn::<LedgerEvent, _>(AUDIT, StartAt::Beginning, move |_| {
            counter.fetch_add(1, Ordering::SeqCst);
            vec![]
        })
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

    add("prepared-main", 10.0).await;
    add("prepared-held", 5.0).await;
    drain_until(&runner, &pool, 2, 4).await;
    assert_eq!(reacted.load(Ordering::SeqCst), 2);

    // An append that has written its event and been prepared: its session is gone,
    // and only `COMMIT PREPARED` or `ROLLBACK PREPARED` decides its fate.
    let mut in_flight = pool.begin().await.expect("beginning must succeed");
    let held = clone_event_into(&mut in_flight, 2).await;
    sqlx::query("PREPARE TRANSACTION 'held-append'")
        .execute(&mut *in_flight)
        .await
        .expect("preparing the append must succeed");
    drop(in_flight);

    // What the runner keys on: a lock with no backend behind it.
    let prepared_holders: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pg_locks WHERE locktype = 'relation' \
         AND mode = 'RowExclusiveLock' AND granted AND pid IS NULL \
         AND relation = pg_get_serial_sequence('events', 'global_position')::regclass",
    )
    .fetch_one(&pool)
    .await
    .expect("reading the locks must succeed");
    assert_eq!(
        prepared_holders, 1,
        "a prepared transaction must still hold the sequence, and report no backend"
    );

    // The event behind it lands, making the hole visible to the feed.
    add("prepared-main", 1.0).await;

    for _ in 0..5 {
        runner.drain().await.expect("drain must succeed");
    }

    assert_eq!(
        stored_cursor(&pool, AUDIT).await,
        held - 1,
        "a position a prepared transaction holds must never be crossed"
    );
    logs_assert(|lines| match skip_warnings(lines).len() {
        0 => Ok(()),
        n => Err(format!(
            "nothing may be skipped while a prepared transaction holds it, got {n}"
        )),
    });

    // It commits, long after its session is gone, exactly as the hazard describes.
    sqlx::query("COMMIT PREPARED 'held-append'")
        .execute(&pool)
        .await
        .expect("committing the prepared append must succeed");

    drain_until(&runner, &pool, held + 1, 5).await;
    assert_eq!(
        reacted.load(Ordering::SeqCst),
        4,
        "the prepared event and the one behind it must both be delivered"
    );
}
