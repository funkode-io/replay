//! The Policy feed reads by commit visibility, not by position contiguity
//! (funkode-io/replay#195).
//!
//! An event is delivered once the transaction that wrote it has ended, in
//! `(commit_txid, global_position)` order. These tests drive the shapes that used to
//! need a theory of holes and now need none — a position burned from the sequence, an
//! insert that was rolled back, a transaction holding a position it never writes — plus
//! the two properties that replace it: an append in flight is delivered in its place
//! when it commits, and the order a Policy walks is the order the log committed in.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use sqlx::{postgres::PgPoolOptions, Executor, PgPool, Row};
use testcontainers_modules::{postgres, testcontainers::runners::AsyncRunner};
use tracing_test::traced_test;
use uuid::Uuid;

use replay_macros::define_aggregate;
use replay_persistence::{Cqrs, PolicyRunner, PostgresEventStore, StartAt};

mod common;
use common::held_append::{append_inside, hold_an_append_open};
use common::postgres_image::{postgres_container, POSTGRES_PORT};

const AUDIT: &str = "commit_visibility_audit";

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
        .max_connections(10)
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

/// Every line the runner writes about a feed that is not advancing. A Policy that is
/// simply walking the log writes none of them.
fn stop_reports<'a>(lines: &[&'a str]) -> Vec<&'a str> {
    lines
        .iter()
        .copied()
        .filter(|line| {
            line.contains("replay_persistence::policy_runner")
                && (line.contains("waiting") || line.contains("feed"))
        })
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

/// The log in the order the feed reads it: the transaction that wrote each event,
/// then its position.
async fn events_in_commit_order(pool: &PgPool) -> Vec<Uuid> {
    sqlx::query("SELECT id FROM events ORDER BY commit_txid, global_position")
        .fetch_all(pool)
        .await
        .expect("reading the log must succeed")
        .into_iter()
        .map(|row| row.get("id"))
        .collect()
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

/// A position no event will ever carry is not a hole in commit order — it is simply
/// not in it. Whatever burned it, the Policy reacts to the next event on its next poll:
/// nothing skipped, nothing warned, nothing to detect.
#[tokio::test]
#[traced_test]
async fn a_position_that_can_never_appear_does_not_delay_a_policy_postgres_test() {
    let (pool, _container) = start_postgres().await;

    let reacted = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&reacted);
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

    let polls = drain_until(&runner, &pool, burned + 1, 2).await;
    assert_eq!(
        reacted.load(Ordering::SeqCst),
        2,
        "the event written after the burned position must be delivered"
    );
    assert_eq!(polls, 1, "and on the first poll after it was appended");

    // ── An append rolled back ─────────────────────────────────────────────────
    // The shape of the reported incident: an append starts, takes its position and
    // rolls back, leaving that position gone for good.
    let aborted = hold_an_append_open(&pool, head(&pool).await).await;
    let rolled_back = aborted.position;
    aborted.tx.rollback().await.expect("rollback must succeed");

    add(2.0).await;

    let polls = drain_until(&runner, &pool, rolled_back + 1, 2).await;
    assert_eq!(reacted.load(Ordering::SeqCst), 3);
    assert_eq!(polls, 1, "a rolled-back append delays nothing either");

    // ── A run of burned positions ─────────────────────────────────────────────
    // A batch append that dies after a hundred rows burns a hundred positions, and
    // costs exactly as much as one: they are not read, so they are not crossed.
    let first_burned = burn_positions(&pool, 100).await;
    add(3.0).await;

    let polls = drain_until(&runner, &pool, first_burned + 100, 2).await;
    assert_eq!(reacted.load(Ordering::SeqCst), 4);
    assert_eq!(polls, 1);

    // ── A position taken and never written ────────────────────────────────────
    // A transaction that takes a position and writes nothing has no transaction id at
    // all, so it holds nothing back; the events appended past it are delivered while
    // it is still open.
    let mut holder = pool.begin().await.expect("beginning must succeed");
    let taken: i64 = sqlx::query_scalar("SELECT nextval('events_global_position_seq')")
        .fetch_one(&mut *holder)
        .await
        .expect("taking a position must succeed");
    holder
        .execute("SELECT 1")
        .await
        .expect("keeping the transaction open must succeed");

    add(4.0).await;

    drain_until(&runner, &pool, taken + 1, 2).await;
    assert_eq!(reacted.load(Ordering::SeqCst), 5);
    holder.rollback().await.expect("rollback must succeed");

    logs_assert(|lines| match stop_reports(lines).len() {
        0 => Ok(()),
        n => Err(format!(
            "a policy that never stopped must report nothing about its feed, got {n} \
             lines: {:?}",
            stop_reports(lines)
        )),
    });
}

/// An append still in flight is never delivered early and never missed. Its events —
/// and everything committed after it, which the feed withholds until it ends — arrive
/// in one batch once it commits.
///
/// Delivering the later append first would be the silent loss the whole order exists to
/// rule out: the in-flight write sits *before* it in commit order, so the Policy would
/// have passed the point its events belong at.
#[tokio::test]
#[traced_test]
async fn an_append_in_flight_is_delivered_in_its_place_when_it_commits_postgres_test() {
    let (pool, _container) = start_postgres().await;

    let reacted = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&reacted);
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

    // An append in flight: it has written its row, so it holds a transaction id, and
    // nothing else can see it yet.
    let in_flight = hold_an_append_open(&pool, 2).await;
    let held = in_flight.position;

    // A later append commits ahead of it. Its transaction is younger, so it belongs
    // behind the one still running and cannot be delivered yet.
    add("in-flight-main", 1.0).await;

    for _ in 0..5 {
        runner.drain().await.expect("drain must succeed");
    }

    assert_eq!(
        stored_cursor(&pool, AUDIT).await,
        held - 1,
        "the policy must wait for an append that is still running"
    );
    assert_eq!(
        reacted.load(Ordering::SeqCst),
        2,
        "nothing written after an open transaction may be delivered before it ends"
    );

    // It commits: both events arrive, in the order the log committed them.
    in_flight
        .tx
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

/// The order a Policy walks is total and stable under concurrent appends: what it was
/// given, in the order it was given, is exactly the log in `(commit_txid,
/// global_position)` order — no event turning up behind a point it had passed, none
/// delivered twice, none missing.
///
/// Concurrency is what makes the assertion worth making: the appends interleave, so
/// positions become visible out of order and the drains run while writes are open.
#[tokio::test]
async fn the_order_a_policy_walks_is_the_order_the_log_committed_in_postgres_test() {
    const WRITERS: usize = 8;
    const APPENDS_PER_WRITER: usize = 5;

    let (pool, _container) = start_postgres().await;

    let delivered: Arc<Mutex<Vec<Uuid>>> = Arc::new(Mutex::new(Vec::new()));
    let recorder = Arc::clone(&delivered);
    let cqrs = Cqrs::new(PostgresEventStore::new(pool.clone()));
    let runner = PolicyRunner::builder(cqrs.clone())
        .register_policy_fn::<LedgerEvent, _>(AUDIT, StartAt::Beginning, move |event| {
            recorder
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(event.id);
            vec![]
        })
        .build();

    let appending = (0..WRITERS).map(|writer| {
        let cqrs = cqrs.clone();
        tokio::spawn(async move {
            for append in 0..APPENDS_PER_WRITER {
                cqrs.execute::<Ledger>(
                    &LedgerUrn::new(format!("concurrent-{writer}")).unwrap(),
                    replay::Metadata::default(),
                    LedgerCommand::Add {
                        amount: append as f64,
                    },
                    &(),
                    None,
                )
                .await
                .expect("append must succeed");
            }
        })
    });

    // Draining while the writers run is the point: a poll that lands mid-append must
    // neither skip what that append will publish nor deliver it twice.
    let draining = {
        let appends = futures::future::join_all(appending);
        let runner = &runner;
        async move {
            let mut appends = Box::pin(appends);
            loop {
                runner.drain().await.expect("drain must succeed");
                if let std::task::Poll::Ready(results) = futures::poll!(&mut appends) {
                    for result in results {
                        result.expect("the writers must finish");
                    }
                    return;
                }
            }
        }
    };
    draining.await;

    // Drained on the count delivered, not on the cursor reaching the numeric head: the
    // cursor walks in commit order, where a later point can hold a lower position, so a
    // correct run need never store `MAX(global_position)`.
    let total = WRITERS * APPENDS_PER_WRITER;
    for _ in 0..10 {
        runner.drain().await.expect("drain must succeed");
        if delivered
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .len()
            == total
        {
            break;
        }
    }

    let delivered = delivered
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
        .clone();
    assert_eq!(
        delivered.len(),
        total,
        "every event is delivered exactly once"
    );
    assert_eq!(
        delivered,
        events_in_commit_order(&pool).await,
        "and in commit order, which is the order the cursor walks"
    );
}

/// The feed resumes from its cursor and stops at the watermark by reading the index
/// built for it (migration 0019), not by sorting the log.
///
/// The pair predicate is the load-bearing part: a cursor written as two `AND`ed
/// comparisons would be neither the same set nor an index scan.
#[tokio::test]
async fn the_feed_resumes_its_cursor_by_an_index_scan_postgres_test() {
    let (pool, _container) = start_postgres().await;

    let cqrs = Cqrs::new(PostgresEventStore::new(pool.clone()));
    cqrs.execute::<Ledger>(
        &LedgerUrn::new("planned").unwrap(),
        replay::Metadata::default(),
        LedgerCommand::Add { amount: 1.0 },
        &(),
        None,
    )
    .await
    .expect("append must succeed");

    let mut conn = pool.acquire().await.expect("a connection for the plan");
    // A handful of rows is cheaper to scan and sort whatever the indexes say, and this
    // test is about which access paths exist, not what the planner costs them at.
    conn.execute("SET enable_seqscan = off")
        .await
        .expect("discouraging the sequential scan");
    conn.execute("SET enable_bitmapscan = off")
        .await
        .expect("discouraging the bitmap scan, which loses the index's order");

    let plan: Vec<String> = sqlx::query_scalar(
        "EXPLAIN SELECT id FROM events \
         WHERE (commit_txid, global_position) > ('0'::xid8, 0) \
           AND commit_txid < pg_snapshot_xmin(pg_current_snapshot()) \
         ORDER BY commit_txid, global_position LIMIT 100",
    )
    .fetch_all(&mut *conn)
    .await
    .expect("explaining must succeed");
    let plan = plan.join("\n");

    assert!(
        plan.contains("idx_events_commit_txid_position"),
        "the feed reads the commit-order index:\n{plan}"
    );
    assert!(
        !plan.contains("Sort"),
        "the index scan already returns commit order, so nothing is sorted:\n{plan}"
    );
}

/// The direction a transaction snapshot may be used in, executed, so nobody reverses
/// it by accident.
///
/// Reading only *below* `pg_snapshot_xmin` is sound: every transaction under it has
/// ended, so nothing new can appear there. The other direction — using a watermark to
/// prove that a specific missing `global_position` can never appear — is not, and
/// [ADR-0015](../../docs/adr/0015-policy-crosses-a-position-no-transaction-can-fill.md)
/// rejects it. This test makes it be wrong on demand.
///
/// A snapshot's `xmax` is one past the newest *completed* transaction, and its list of
/// in-flight transactions only reaches that far. An append that is running while
/// nothing newer has completed therefore sits above the watermark and is in no list at
/// all: the reading says "nothing was in flight" about a transaction that is holding
/// the very position in question. One later commit carries `xmin` to the watermark and
/// completes the false proof.
///
/// The feed, asked the sound question instead, withholds the event behind it until it
/// commits, and then delivers both.
#[tokio::test]
async fn a_transaction_snapshot_cannot_prove_a_position_is_burned_postgres_test() {
    let (pool, _container) = start_postgres().await;

    let reacted = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&reacted);
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

    // An append in flight, holding the position it took. It has written its event, so
    // it
    // has a transaction id and is exactly the case a snapshot is supposed to cover.
    let mut in_flight = hold_an_append_open(&pool, 2).await;
    let held = in_flight.position;
    let holder_xid: i64 = sqlx::query_scalar("SELECT pg_current_xact_id()::text::bigint")
        .fetch_one(&mut *in_flight.tx)
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

    // One ordinary append commits behind the hole — and, incidentally, completes the
    // false proof.
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
    // that will write it is still running.
    let still_missing: bool =
        sqlx::query_scalar("SELECT NOT EXISTS (SELECT 1 FROM events WHERE global_position = $1)")
            .bind(held)
            .fetch_one(&pool)
            .await
            .expect("checking the position must succeed");
    assert!(
        still_missing,
        "the rejected rule called a position permanent while the append holding it \
         was still running"
    );

    // The feed, asked the sound question, waits: the open transaction holds `xmin`
    // down, so everything committed after it is withheld too.
    for _ in 0..5 {
        runner.drain().await.expect("drain must succeed");
    }
    assert_eq!(
        stored_cursor(&pool, AUDIT).await,
        held - 1,
        "the event the rejected rule would have lost is still to come"
    );

    in_flight
        .tx
        .commit()
        .await
        .expect("committing must succeed");
    drain_until(&runner, &pool, held + 1, 5).await;
    assert_eq!(
        reacted.load(Ordering::SeqCst),
        4,
        "and both events are delivered once it lands"
    );
}

/// What `StartAt::Now` means in this order, pinned because the order is what gives it a
/// second reading: a write already in flight when a Policy is first registered is history
/// it skips, not an event it is owed.
///
/// The head the Policy starts at was written by a younger transaction than the one still
/// running, so the in-flight write sits *behind* that start point and is never delivered —
/// while everything committed after it is. Starting at the watermark instead would catch
/// it, at the price of replaying every event committed while any transaction was open; no
/// point in the order does both (ADR-0021).
#[tokio::test]
async fn a_policy_starting_now_skips_a_write_that_was_already_in_flight_postgres_test() {
    let (pool, _container) = start_postgres().await;

    let cqrs = Cqrs::new(PostgresEventStore::new(pool.clone()));
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

    add("start-now-main", 10.0).await;
    add("start-now-held", 5.0).await;

    // In flight when the policy is registered, and committing after it.
    let in_flight = hold_an_append_open(&pool, 2).await;
    let held = in_flight.position;

    // The head the policy will start at: younger transaction, higher position.
    add("start-now-main", 1.0).await;

    let delivered: Arc<Mutex<Vec<f64>>> = Arc::new(Mutex::new(Vec::new()));
    let recorder = Arc::clone(&delivered);
    let runner = PolicyRunner::builder(cqrs.clone())
        .register_policy_fn::<LedgerEvent, _>(AUDIT, StartAt::Now, move |event| {
            let LedgerEvent::Added { amount } = event.data;
            recorder
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(amount);
            vec![]
        })
        .build();
    runner.drain().await.expect("drain must succeed");

    in_flight
        .tx
        .commit()
        .await
        .expect("committing must succeed");
    add("start-now-main", 2.0).await;
    drain_until(&runner, &pool, held + 2, 5).await;

    assert_eq!(
        delivered
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone(),
        vec![2.0],
        "only the event appended after the policy started is delivered"
    );
}

/// The head a Policy starts at is the greatest point in the feed's order, which is not the
/// greatest position: a transaction that writes elsewhere before it appends carries a lower
/// `commit_txid` than one that appended earlier, so the last position and the last point
/// can belong to different rows.
///
/// Starting at `MAX(global_position)` therefore leaves a *committed* event sorting after the
/// cursor, and a Policy registered to skip history delivers it as news.
#[tokio::test]
async fn a_policy_starting_now_skips_a_committed_event_whose_transaction_is_younger_postgres_test()
{
    let (pool, _container) = start_postgres().await;

    let cqrs = Cqrs::new(PostgresEventStore::new(pool.clone()));
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

    add("now-order-early", 10.0).await;
    add("now-order-late", 5.0).await;

    // Takes its transaction id first and appends last: lower `commit_txid`, higher position.
    let mut early = pool.begin().await.expect("beginning must succeed");
    sqlx::query_scalar::<_, String>("SELECT pg_current_xact_id()::text")
        .fetch_one(&mut *early)
        .await
        .expect("assigning a transaction id must succeed");

    // Appends first and so takes the lower position, under a higher `commit_txid`.
    let late = hold_an_append_open(&pool, 2).await;
    let late_position = late.position;
    let early_position = append_inside(&mut early, &pool, 1).await;
    assert!(
        early_position > late_position,
        "the fixture must produce the inversion it is testing: {early_position} <= {late_position}"
    );

    late.tx.commit().await.expect("committing must succeed");
    early.commit().await.expect("committing must succeed");

    let delivered: Arc<Mutex<Vec<f64>>> = Arc::new(Mutex::new(Vec::new()));
    let recorder = Arc::clone(&delivered);
    let runner = PolicyRunner::builder(cqrs.clone())
        .register_policy_fn::<LedgerEvent, _>(AUDIT, StartAt::Now, move |event| {
            let LedgerEvent::Added { amount } = event.data;
            recorder
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .push(amount);
            vec![]
        })
        .build();

    // Everything above is committed, so all of it is history.
    runner.drain().await.expect("drain must succeed");

    // And the policy is started, not parked: the next append arrives.
    add("now-order-early", 1.0).await;
    drain_until(&runner, &pool, early_position + 1, 5).await;

    assert_eq!(
        delivered
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .clone(),
        vec![1.0],
        "only the event appended after the policy started is delivered"
    );
}
