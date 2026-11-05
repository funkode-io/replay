//! A bulk append commits in bounded segments (funkode-io/replay#303, ADR-0034).
//!
//! `Cqrs::execute_bulk` consumes the aggregate's `handle_stream` and commits every
//! `bulk_segment_size` events, each segment one transaction holding its events, their
//! inline-projection rows and their bulk work. What is asserted is what a consumer and
//! an operator can see: the events and work rows a failure leaves behind, the
//! transaction an inline projection wrote each event's row in, and an online Policy
//! delivering while a bulk is still between segments.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use futures::future::BoxFuture;
use futures::stream::BoxStream;
use futures::StreamExt;
use replay::{Aggregate, EventStream, WithId};
use replay_persistence::{
    CommandContext, CommandMiddleware, Cqrs, Lanes, Next, Outcome, PersistedEvent, PolicySettings,
    PostgresEventStore, StartAt, StreamFilter,
};
use tokio::sync::Notify;

use crate::common;

use common::policy_harness::{PolicyDaemonHarness, ProbeEvent, ProbeUrn};

/// The segment size every test here appends with: small, so a dozen events span
/// several segments.
const SEGMENT: usize = 5;

/// The projection flush size of [`projected_store`]: below [`SEGMENT`].
const PROJECTION_FLUSH: usize = 2;

// ── Fixture: an importer whose handler stream can fail or pause part-way ─────

/// A bulk decision of `count` records, one `Pinged` per record, whose stream returns
/// an error in place of record `fail_at`, or waits for [`Importer`]'s services to be
/// notified before yielding record `pause_at`.
struct Import {
    count: usize,
    fail_at: Option<usize>,
    pause_at: Option<usize>,
}

impl Import {
    fn of(count: usize) -> Self {
        Self {
            count,
            fail_at: None,
            pause_at: None,
        }
    }
}

/// An aggregate that implements only `handle_stream`, so the records are produced one
/// at a time as the store pulls them and never collected.
#[derive(Debug)]
struct Importer {
    id: ProbeUrn,
}

impl WithId for Importer {
    type StreamId = ProbeUrn;

    fn with_id(id: Self::StreamId) -> Self {
        Importer { id }
    }

    fn get_id(&self) -> &Self::StreamId {
        &self.id
    }
}

impl EventStream for Importer {
    type Event = ProbeEvent;

    fn stream_type() -> String {
        "Importer".to_string()
    }

    fn apply(&mut self, _event: Self::Event) {}
}

impl Aggregate for Importer {
    type Command = Import;
    type Error = replay::Error;
    /// What releases a paused import.
    type Services = Arc<Notify>;

    async fn handle_stream(
        &self,
        command: Import,
        release: &Arc<Notify>,
    ) -> Result<BoxStream<'static, Result<ProbeEvent, replay::Error>>, replay::Error> {
        let release = Arc::clone(release);
        Ok(futures::stream::iter(0..command.count)
            .then(move |n| {
                let release = Arc::clone(&release);
                async move {
                    if command.pause_at == Some(n) {
                        release.notified().await;
                    }
                    if command.fail_at == Some(n) {
                        return Err(replay::Error::invalid_input(format!(
                            "record {n} does not parse"
                        )));
                    }
                    Ok(ProbeEvent::Pinged {
                        tag: format!("record-{n}"),
                    })
                }
            })
            .boxed())
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

fn bulk() -> PolicySettings {
    PolicySettings::new().on_lanes(Lanes::Bulk)
}

/// A `Cqrs` over the harness's database whose store commits a bulk every [`SEGMENT`]
/// events.
async fn segmented(harness: &PolicyDaemonHarness) -> Cqrs<PostgresEventStore> {
    Cqrs::new(
        PostgresEventStore::builder(harness.pool().clone())
            .bulk_segment_size(SEGMENT)
            .build()
            .await
            .expect("the store must build"),
    )
}

fn importer(name: &str) -> ProbeUrn {
    ProbeUrn::new(name).expect("a valid URN NSS")
}

/// How many events `stream` holds.
async fn events_in(harness: &PolicyDaemonHarness, stream: &ProbeUrn) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM events WHERE stream_id = $1")
        .bind(stream.to_urn().to_string())
        .fetch_one(harness.pool())
        .await
        .expect("the events must be countable")
}

/// How many transactions wrote `stream`'s events.
async fn transactions_of(harness: &PolicyDaemonHarness, stream: &ProbeUrn) -> i64 {
    sqlx::query_scalar("SELECT count(DISTINCT commit_txid) FROM events WHERE stream_id = $1")
        .bind(stream.to_urn().to_string())
        .fetch_one(harness.pool())
        .await
        .expect("the commit stamps must be readable")
}

/// A store committing a bulk every [`SEGMENT`] events, with an inline projection that
/// records, for every event, the transaction it was applied in. Its flush size is
/// [`PROJECTION_FLUSH`], below the segment, so a segment writes projection rows before
/// it ends and a rollback has some to take back.
async fn projected_store(harness: &PolicyDaemonHarness) -> PostgresEventStore {
    sqlx::query(
        "CREATE TABLE bulk_segment_seen (\
             stream_id TEXT NOT NULL, version BIGINT NOT NULL, txid xid8 NOT NULL, \
             PRIMARY KEY (stream_id, version))",
    )
    .execute(harness.pool())
    .await
    .expect("the projection's table must be created");

    PostgresEventStore::builder(harness.pool().clone())
        .bulk_segment_size(SEGMENT)
        .projection_flush_size(PROJECTION_FLUSH)
        .register_postgres_event_handler::<ProbeEvent, _>(
            "bulk_segment_seen",
            1,
            |conn: &mut sqlx::PgConnection, events: &[PersistedEvent<ProbeEvent>]| {
                let rows: Vec<(String, i64)> = events
                    .iter()
                    .map(|event| (event.stream_id.to_string(), event.version))
                    .collect();
                let fut: BoxFuture<'_, Result<(), replay::Error>> = Box::pin(async move {
                    for (stream_id, version) in rows {
                        sqlx::query(
                            "INSERT INTO bulk_segment_seen (stream_id, version, txid) \
                             VALUES ($1, $2, pg_current_xact_id())",
                        )
                        .bind(stream_id)
                        .bind(version)
                        .execute(&mut *conn)
                        .await
                        .map_err(replay_persistence::db_error)?;
                    }
                    Ok(())
                });
                fut
            },
        )
        .build()
        .await
        .expect("the store must build")
}

/// How many of `stream`'s events the projection recorded.
async fn projected_in(harness: &PolicyDaemonHarness, stream: &ProbeUrn) -> i64 {
    sqlx::query_scalar("SELECT count(*) FROM bulk_segment_seen WHERE stream_id = $1")
        .bind(stream.to_urn().to_string())
        .fetch_one(harness.pool())
        .await
        .expect("the projection's rows must be countable")
}

/// A handler stream that fails after two segments leaves those two segments committed,
/// with a row of work for each of their events, and the caller sees the failure. The
/// failing record sits inside the third segment, so what that segment had already
/// written is rolled back with it.
#[tokio::test]
async fn a_handler_error_leaves_the_segments_before_it_committed_with_their_work_postgres_test() {
    let mut harness = PolicyDaemonHarness::start("bulk_segment_error", |builder, policy| {
        builder.register_policy_fn::<ProbeEvent, _>(policy, bulk(), |_| vec![])
    })
    .await;
    let policy = harness.policy_name().to_string();
    harness.await_bulk_registration(&policy).await;
    // No worker runs while the work is counted, so nothing is drained underneath.
    harness.stop().await;

    let cqrs = segmented(&harness).await;
    let stream = importer("failing-import");
    let failed = cqrs
        .execute_bulk::<Importer>(
            &stream,
            replay::Metadata::default(),
            Import {
                fail_at: Some(2 * SEGMENT + 2),
                ..Import::of(3 * SEGMENT)
            },
            &Arc::new(Notify::new()),
            None,
        )
        .await;

    let error = match failed {
        Ok(_) => panic!("the handler's error must reach the caller"),
        Err(error) => error,
    };
    assert!(
        format!("{error:?}").contains("does not parse"),
        "the caller sees the handler's own error, got {error:?}"
    );
    assert_eq!(
        events_in(&harness, &stream).await,
        (2 * SEGMENT) as i64,
        "the two segments before the failure stand, and none of the third"
    );
    assert_eq!(
        transactions_of(&harness, &stream).await,
        2,
        "each segment is a transaction of its own"
    );
    assert_eq!(
        harness.bulk_work(&policy).await,
        (2 * SEGMENT) as i64,
        "every committed event's work is queued with it"
    );

    harness.shutdown().await;
}

/// An inline projection is applied to each segment's events inside that segment's
/// transaction: the row it writes for an event carries the commit stamp of the event
/// itself, and a segment that rolls back takes its projection rows with it.
#[tokio::test]
async fn an_inline_projection_sees_each_segment_inside_its_transaction_postgres_test() {
    let harness = PolicyDaemonHarness::start("bulk_segment_projection", |builder, policy| {
        builder.register_policy_fn::<ProbeEvent, _>(policy, bulk(), |_| vec![])
    })
    .await;
    let cqrs = Cqrs::new(projected_store(&harness).await);

    let stream = importer("projected-import");
    cqrs.execute_bulk::<Importer>(
        &stream,
        replay::Metadata::default(),
        Import::of(2 * SEGMENT + 2),
        &Arc::new(Notify::new()),
        None,
    )
    .await
    .expect("the bulk execution must succeed");

    let (seen, in_its_transaction): (i64, i64) = sqlx::query_as(
        "SELECT count(*), count(*) FILTER (WHERE s.txid = e.commit_txid) \
           FROM bulk_segment_seen s \
           JOIN events e ON e.stream_id = s.stream_id AND e.version = s.version \
          WHERE s.stream_id = $1",
    )
    .bind(stream.to_urn().to_string())
    .fetch_one(harness.pool())
    .await
    .expect("the projection's rows must be readable");
    assert_eq!(seen, (2 * SEGMENT + 2) as i64, "every event is projected");
    assert_eq!(
        in_its_transaction, seen,
        "each event is projected by the transaction that wrote it"
    );
    assert_eq!(
        transactions_of(&harness, &stream).await,
        3,
        "two full segments and the remainder"
    );

    let failing = importer("projected-failing-import");
    cqrs.execute_bulk::<Importer>(
        &failing,
        replay::Metadata::default(),
        Import {
            fail_at: Some(SEGMENT + 3),
            ..Import::of(2 * SEGMENT)
        },
        &Arc::new(Notify::new()),
        None,
    )
    .await
    .expect_err("the handler's error must reach the caller");
    assert_eq!(
        projected_in(&harness, &failing).await,
        SEGMENT as i64,
        "the rolled-back segment's projection rows roll back with it"
    );

    harness.shutdown().await;
}

/// While a bulk execution waits between segments, an online command on an unrelated
/// stream is delivered to its online Policy: the bulk holds no transaction open between
/// segments, so nothing holds back the online feed.
#[tokio::test]
async fn an_online_policy_delivers_while_a_bulk_is_between_segments_postgres_test() {
    let harness = PolicyDaemonHarness::start("bulk_segment_online", |builder, policy| {
        builder.register_policy_fn::<ProbeEvent, _>(
            policy,
            PolicySettings::new()
                .starting_at(StartAt::Beginning)
                .with_stream_filter(StreamFilter::ForStreamTypes(vec!["Probe".to_string()])),
            |event| match &event.data {
                ProbeEvent::Pinged { tag } => {
                    vec![replay::Dispatch::to::<common::policy_harness::Probe>(
                        ProbeUrn::new(format!("echo-{}", event.stream_id.nss()))
                            .expect("a valid URN NSS"),
                        common::policy_harness::ProbeCommand::Echo { tag: tag.clone() },
                    )]
                }
                ProbeEvent::Echoed { .. } => vec![],
            },
        )
    })
    .await;

    let cqrs = segmented(&harness).await;
    let stream = importer("paused-import");
    let release = Arc::new(Notify::new());
    let bulk = tokio::spawn({
        let (cqrs, stream, release) = (cqrs.clone(), stream.clone(), Arc::clone(&release));
        async move {
            cqrs.execute_bulk::<Importer>(
                &stream,
                replay::Metadata::default(),
                Import {
                    pause_at: Some(SEGMENT),
                    ..Import::of(2 * SEGMENT)
                },
                &release,
                None,
            )
            .await
            .map(|_| ())
        }
    });

    harness
        .observe("the bulk's first segment to commit", || async {
            (events_in(&harness, &stream).await == SEGMENT as i64).then_some(())
        })
        .await;

    let online = harness.ping("online-source", "online").await;
    let echo = ProbeUrn::new("echo-online-source").expect("a valid URN NSS");
    harness
        .observe(
            "the online Policy to deliver the online command",
            || async { (events_in(&harness, &echo).await == 1).then_some(()) },
        )
        .await;
    harness.await_passed(online.global_position).await;
    assert!(
        !bulk.is_finished(),
        "the bulk is still appending when the online Policy delivers"
    );

    release.notify_one();
    bulk.await
        .expect("the bulk task must not panic")
        .expect("the bulk execution must succeed");
    assert_eq!(events_in(&harness, &stream).await, (2 * SEGMENT) as i64);

    harness.shutdown().await;
}

/// Answers `Continue` at the first commit gate it is asked and `Busy` at every later
/// one, and records how often it was asked and the outcome `after` was told.
/// A clone shares the record, so the test keeps one while the `Cqrs` owns another.
#[derive(Clone, Default)]
struct BusyFromTheSecondSegment {
    asked: Arc<AtomicUsize>,
    outcome: Arc<Mutex<Option<String>>>,
}

impl CommandMiddleware for BusyFromTheSecondSegment {
    fn before_commit<'a>(&'a self, _: &'a CommandContext) -> BoxFuture<'a, Next> {
        Box::pin(async move {
            match self.asked.fetch_add(1, Ordering::SeqCst) {
                0 => Next::Continue,
                _ => Next::Busy("the import window closed".to_string()),
            }
        })
    }

    fn after<'a>(&'a self, _: &'a CommandContext, outcome: &'a Outcome<'a>) -> BoxFuture<'a, ()> {
        Box::pin(async move {
            *self.outcome.lock().unwrap() = Some(match outcome {
                Outcome::Committed => "committed".to_string(),
                Outcome::Busy(reason) => format!("busy: {reason}"),
                Outcome::Failed(error) => format!("failed: {error}"),
            });
        })
    }
}

/// The commit gate is asked once per segment: a middleware that stops the second
/// segment rolls back that segment alone — its events, projection rows and work —
/// while the first stands, and both the caller and `after` see the `Busy`.
#[tokio::test]
async fn a_gate_that_stops_a_later_segment_rolls_back_that_segment_alone_postgres_test() {
    let mut harness = PolicyDaemonHarness::start("bulk_segment_gate", |builder, policy| {
        builder.register_policy_fn::<ProbeEvent, _>(policy, bulk(), |_| vec![])
    })
    .await;
    let policy = harness.policy_name().to_string();
    harness.await_bulk_registration(&policy).await;
    harness.stop().await;

    let gate = BusyFromTheSecondSegment::default();
    let cqrs = Cqrs::new(projected_store(&harness).await).with_middleware(gate.clone());
    let stream = importer("gated-import");
    let stopped = cqrs
        .execute_bulk::<Importer>(
            &stream,
            replay::Metadata::default(),
            Import::of(2 * SEGMENT + 2),
            &Arc::new(Notify::new()),
            None,
        )
        .await;

    let error = match stopped {
        Ok(_) => panic!("the middleware's Busy must reach the caller"),
        Err(error) => error,
    };
    assert_eq!(error.kind(), replay::ErrorKind::Busy, "got {error:?}");
    assert_eq!(
        gate.asked.load(Ordering::SeqCst),
        2,
        "asked before each segment's commit, and never again after it stopped one"
    );
    assert_eq!(
        gate.outcome.lock().unwrap().as_deref(),
        Some("busy: the import window closed"),
        "after is told the Busy"
    );
    assert_eq!(events_in(&harness, &stream).await, SEGMENT as i64);
    assert_eq!(projected_in(&harness, &stream).await, SEGMENT as i64);
    assert_eq!(harness.bulk_work(&policy).await, SEGMENT as i64);

    harness.shutdown().await;
}
