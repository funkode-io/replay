//! A Policy behind on its feed catches up at the speed of its work, not of its poll
//! interval (funkode-io/replay#322).
//!
//! The poll interval is the safety net for a missed `NOTIFY`, and `read_batch_size`
//! bounds what one poll reads. Neither is meant as a throughput limit: a poll that
//! fills its budget knows more is waiting, and the worker polls again rather than
//! sleeping. These tests run without `NOTIFY` and with reactions that append nothing,
//! so the only thing that can wake the worker early is the worker itself.

use crate::common;

use std::time::{Duration, Instant};

use common::policy_harness::{PolicyDaemonHarness, Probe, ProbeEvent};
use replay_persistence::{PolicySettings, StartAt, StreamFilter};

/// Far longer than the backlog takes to work through, so a worker that sleeps it once
/// between batches is caught by the bound below.
const POLL_INTERVAL: Duration = Duration::from_secs(5);

/// The most one poll reads. The backlog is several of these.
const READ_BATCH: u32 = 10;

/// How long the test waits before calling the backlog stuck. Well past one poll
/// interval, so a worker that sleeps between batches reports how long it took rather
/// than timing out.
const GIVE_UP_AFTER: Duration = Duration::from_secs(60);

/// A backlog several read batches deep, across streams and within one, whose reactions
/// append nothing, is worked through in less than one poll interval.
///
/// Before #322 the worker slept the whole interval after every poll, full or not: with
/// these numbers each batch of ten after the first waited five seconds, and the backlog
/// took twenty.
#[tokio::test]
async fn a_backlog_deeper_than_one_read_batch_drains_without_waiting_out_the_interval_postgres_test(
) {
    let mut harness =
        PolicyDaemonHarness::start_polling_every("backlog", POLL_INTERVAL, |builder, policy| {
            builder
                .without_notifications()
                .register_policy_fn::<ProbeEvent, _>(
                    policy,
                    PolicySettings::new()
                        .starting_at(StartAt::Beginning)
                        .with_read_batch_size(READ_BATCH)
                        .with_checkpoint_batch_size(READ_BATCH),
                    |_| vec![],
                )
        })
        .await;

    // The backlog is appended with the daemon stopped, so it is all there before the first
    // poll: a worker that polled once while it was being written would wait out one
    // interval for it, as it should without a `NOTIFY`, and that is not the wait this is
    // about.
    harness.stop().await;

    // One stream four batches deep, and two more streams beside it: a poll's budget
    // runs out inside a stream and across streams, and both have to be picked up again.
    let mut last = Vec::new();
    for (stream, depth) in [("deep", 4 * READ_BATCH), ("beside-1", 5), ("beside-2", 5)] {
        let mut appended = None;
        for n in 0..depth {
            appended = Some(harness.ping(stream, &format!("{stream}-{n}")).await);
        }
        last.extend(appended);
    }
    harness.resume();
    let resumed = Instant::now();

    let drained = time_until(resumed, || async {
        let mut passed = true;
        for event in &last {
            passed &= harness.has_passed(event.global_position).await;
        }
        passed
    })
    .await;
    harness.shutdown().await;

    eprintln!("backlog drained {drained:?} after the daemon resumed");
    assert!(
        drained < POLL_INTERVAL,
        "a backlog of {} batches took {drained:?} to drain: the worker waited out its \
         {POLL_INTERVAL:?} poll interval between full batches",
        (4 * READ_BATCH + 10).div_ceil(READ_BATCH),
    );
}

/// A Policy that tracks one stream type, behind a log several read batches deep of
/// another, sweeps past it in less than one poll interval.
///
/// The sweep reads its `read_batch_size` of positions whatever streams they name and
/// nominates none of these, so no stream is ever carried: the only thing that says
/// more is waiting is that the sweep filled its page. A worker that slept between such
/// polls passed one batch of the log per interval.
#[tokio::test]
async fn a_full_sweep_of_untracked_streams_sweeps_on_without_waiting_out_the_interval_postgres_test(
) {
    let mut harness =
        PolicyDaemonHarness::start_polling_every("untracked", POLL_INTERVAL, |builder, policy| {
            builder
                .without_notifications()
                .register_policy_fn::<ProbeEvent, _>(
                    policy,
                    PolicySettings::new()
                        .starting_at(StartAt::Beginning)
                        .with_stream_filter(StreamFilter::for_stream_type::<Probe>())
                        .with_read_batch_size(READ_BATCH)
                        .with_checkpoint_batch_size(READ_BATCH),
                    |_| vec![],
                )
        })
        .await;

    // Appended with the daemon stopped, for the reason the backlog above is.
    harness.stop().await;
    let mut last = None;
    for n in 0..5 * READ_BATCH {
        let stream = format!("urn:ledger:untracked-{}", n % 3);
        last = Some(harness.ping_of_type("Ledger", &stream, "ignored").await);
    }
    let last = last.expect("the log is not empty").global_position;
    harness.resume();
    let resumed = Instant::now();

    let swept = time_until(resumed, || async {
        harness.swept_through().await.is_some_and(|at| at >= last)
    })
    .await;
    let places = harness.places().await;
    harness.shutdown().await;

    eprintln!("untracked log swept {swept:?} after the daemon resumed");
    assert!(
        places.is_empty(),
        "the Policy read streams it does not track: {places:?}"
    );
    assert!(
        swept < POLL_INTERVAL,
        "sweeping five batches of untracked log took {swept:?}: the worker waited out its \
         {POLL_INTERVAL:?} poll interval between full sweeps",
    );
}

/// How long after `since` the observation `holds` first held, re-read until it does or
/// [`GIVE_UP_AFTER`] passes.
async fn time_until<F, Fut>(since: Instant, holds: F) -> Duration
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = bool>,
{
    while since.elapsed() < GIVE_UP_AFTER {
        if holds().await {
            return since.elapsed();
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("the observation never held within {GIVE_UP_AFTER:?}");
}
