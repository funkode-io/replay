//! A consumer lists its parked commands, with the event that triggered each, through
//! the status read model rather than through Replay's tables (funkode-io/replay#260).
//!
//! The listing is a stream ([ADR-0029](../../../docs/adr/0029-the-public-api-is-stream-first.md)):
//! the consumer decides how much of it to read, resumes after the id of the last row it
//! showed, and counts through a call of its own.
//!
//! Every dead letter here is parked by a real daemon through
//! `tests/common/policy_harness.rs`, and read back only through
//! [`PolicyStatusStore`](replay_persistence::PolicyStatusStore).

use crate::common;

use futures::{StreamExt, TryStreamExt};

use common::policy_harness::{PolicyDaemonHarness, Probe, ProbeCommand, ProbeEvent, ProbeUrn};
use replay_persistence::{
    DeadLetterFilter, Dispatch, ObservedEvent, Policy, PolicyRunnerBuilder, PolicySettings, StartAt,
};

/// Tag whose reaction dispatches a command the aggregate refuses permanently, to the
/// instance named after the stream the event was written to.
const REFUSED: &str = "refused";

/// The `error_kind` a refused command is parked under.
const PERMANENT_KIND: &str = "Invalid Input";

/// Tag prefix whose reaction echoes the rest of the tag to `<stream>-target`, so the
/// echo is a trigger of a second type, in a second stream.
const ECHO: &str = "echo-";

/// A Policy that parks one row per `REFUSED` event, addressed to `<stream>-target`.
///
/// One that `echoes` also turns an `ECHO` ping into an `Echoed` event and refuses an
/// `Echoed` event whose tag is `REFUSED`: a trigger of another type, in another stream.
struct RefusingPolicy {
    name: String,
    echoes: bool,
}

impl Policy for RefusingPolicy {
    type Event = ProbeEvent;

    fn name(&self) -> &str {
        &self.name
    }

    fn react(&self, event: &ObservedEvent<Self::Event>) -> Vec<Dispatch> {
        let target = ProbeUrn::new(format!("{}-target", event.stream_id.nss())).unwrap();
        let refuse = |tag: &str| {
            vec![Dispatch::to::<Probe>(
                target.clone(),
                ProbeCommand::Refuse {
                    reason: tag.to_string(),
                },
            )]
        };
        match &event.data {
            ProbeEvent::Pinged { tag } if tag.starts_with(REFUSED) => refuse(tag),
            ProbeEvent::Pinged { tag } if self.echoes && tag.starts_with(ECHO) => {
                vec![Dispatch::to::<Probe>(
                    target.clone(),
                    ProbeCommand::Echo {
                        tag: tag[ECHO.len()..].to_string(),
                    },
                )]
            }
            ProbeEvent::Echoed { tag } if self.echoes && tag.starts_with(REFUSED) => refuse(tag),
            _ => vec![],
        }
    }
}

/// The name of the second Policy [`two_refusing_policies`] registers.
fn other(policy: &str) -> String {
    format!("{policy}-other")
}

fn refusing_policy(
) -> impl Fn(PolicyRunnerBuilder, &str) -> PolicyRunnerBuilder + Send + Sync + 'static {
    |builder, policy| {
        builder.register_policy(
            RefusingPolicy {
                name: policy.to_string(),
                echoes: true,
            },
            PolicySettings::new().starting_at(StartAt::Beginning),
        )
    }
}

/// The harness's Policy, which echoes, and [`other`], which only refuses pings.
fn two_refusing_policies(
) -> impl Fn(PolicyRunnerBuilder, &str) -> PolicyRunnerBuilder + Send + Sync + 'static {
    |builder, policy| {
        refusing_policy()(builder, policy).register_policy(
            RefusingPolicy {
                name: other(policy),
                echoes: false,
            },
            PolicySettings::new().starting_at(StartAt::Beginning),
        )
    }
}

/// The ids `filter` lists, in the order it lists them.
async fn listed_ids(harness: &PolicyDaemonHarness, filter: DeadLetterFilter) -> Vec<i64> {
    harness
        .status_store()
        .dead_letters(filter)
        .map_ok(|row| row.id)
        .try_collect()
        .await
        .expect("the listing must be readable")
}

/// What `filter` counts.
async fn counted(harness: &PolicyDaemonHarness, filter: DeadLetterFilter) -> i64 {
    harness
        .status_store()
        .dead_letter_count(&filter)
        .await
        .expect("the count must be readable")
}

/// The URN of the probe instance named `stream`.
fn urn_of(stream: &str) -> String {
    ProbeUrn::new(stream).unwrap().to_urn().to_string()
}

/// A listed dead letter says which policy parked it, what went wrong, the command and
/// instance that failed, and carries the event that triggered the reaction — enough to
/// render "which record failed and why" without reading `policy_dead_letters`.
#[tokio::test]
async fn a_listed_dead_letter_carries_its_triggering_event_and_failed_command_postgres_test() {
    let harness = PolicyDaemonHarness::start("listing", refusing_policy()).await;

    let trigger = harness.ping("customer-1", "refused-import").await;
    let parked = harness.await_dead_letters(1).await;

    let listed: Vec<_> = harness
        .status_store()
        .dead_letters(DeadLetterFilter::new())
        .try_collect()
        .await
        .expect("the listing must be readable");

    assert_eq!(listed.len(), 1, "one parked row, one listed: {listed:#?}");
    let row = &listed[0];
    assert_eq!(row.id, parked[0].id, "the id retry and discard take");
    assert_eq!(row.policy_name, harness.policy_name());
    assert_eq!(row.error_kind, PERMANENT_KIND);
    assert!(
        row.error_message.contains("probe refuses: refused-import"),
        "the error says what went wrong, got {:?}",
        row.error_message
    );
    assert_eq!(row.last_parked_at, parked[0].last_parked_at);
    assert_eq!(row.created_at, parked[0].created_at);
    assert_eq!(row.deliveries, 1);
    assert_eq!(row.retry_count, 0);
    assert_eq!(row.last_retried_at, None);

    assert_eq!(
        row.aggregate_name.as_deref(),
        Some(std::any::type_name::<Probe>())
    );
    assert_eq!(
        row.target_stream_id,
        Some(ProbeUrn::new("customer-1-target").unwrap().to_urn().clone())
    );
    assert_eq!(
        row.command_name.as_deref(),
        Some(std::any::type_name::<ProbeCommand>())
    );
    assert_eq!(row.dispatch_ordinal, Some(0));

    let event = row
        .triggering_event
        .as_ref()
        .expect("the triggering event is still in the log");
    assert_eq!(event.id, trigger.event_id);
    assert_eq!(event.r#type, "Pinged");
    assert_eq!(event.stream_id.to_string(), urn_of("customer-1"));
    assert_eq!(
        event.data,
        serde_json::json!({ "Pinged": { "tag": "refused-import" } })
    );

    harness.shutdown().await;
}

/// Each criterion narrows the listing — the Policy, the trigger's stream, the trigger's
/// type — and the count of a filter is the number of rows it lists.
#[tokio::test]
async fn a_filter_narrows_the_listing_and_its_count_by_policy_trigger_stream_and_type_postgres_test(
) {
    let harness = PolicyDaemonHarness::start("listing-filter", two_refusing_policies()).await;
    let policy = harness.policy_name().to_string();

    harness.ping("customer-1", "refused-a").await;
    harness.ping("customer-2", "refused-b").await;
    harness.ping("customer-3", "echo-refused-c").await;

    // The harness's Policy parks both pings and the echo; the other parks the pings.
    let parked = harness.await_dead_letters(3).await;
    harness
        .observe("the other policy's two parked rows", || async {
            (counted(&harness, DeadLetterFilter::new().policy(other(&policy))).await == 2)
                .then_some(())
        })
        .await;

    let all = listed_ids(&harness, DeadLetterFilter::new()).await;
    assert_eq!(all.len(), 5, "three rows and two: {all:?}");
    assert!(all.is_sorted(), "oldest id first: {all:?}");
    assert_eq!(counted(&harness, DeadLetterFilter::new()).await, 5);

    let mine = DeadLetterFilter::new().policy(&policy);
    let parked_ids: Vec<i64> = parked.iter().map(|row| row.id).collect();
    assert_eq!(listed_ids(&harness, mine.clone()).await, parked_ids);
    assert_eq!(counted(&harness, mine.clone()).await, 3);

    let in_customer_1 = DeadLetterFilter::new().triggered_in(ProbeUrn::new("customer-1").unwrap());
    let rows = harness
        .status_store()
        .dead_letters_all(in_customer_1.clone())
        .await
        .expect("the listing must be readable");
    let mut policies: Vec<&str> = rows.iter().map(|row| row.policy_name.as_str()).collect();
    policies.sort();
    let mut expected = vec![policy.clone(), other(&policy)];
    expected.sort();
    assert_eq!(policies, expected, "both policies parked customer-1's ping");
    assert_eq!(counted(&harness, in_customer_1.clone()).await, 2);
    assert_eq!(
        counted(&harness, in_customer_1.policy(other(&policy))).await,
        1,
        "criteria combine"
    );

    let echoes = DeadLetterFilter::new().triggered_by("Echoed");
    let rows = harness
        .status_store()
        .dead_letters_all(echoes.clone())
        .await
        .expect("the listing must be readable");
    assert_eq!(rows.len(), 1, "only the echo's row: {rows:#?}");
    let trigger = rows[0].triggering_event.as_ref().unwrap();
    assert_eq!(trigger.r#type, "Echoed");
    assert_eq!(trigger.stream_id.to_string(), urn_of("customer-3-target"));
    assert_eq!(counted(&harness, echoes).await, 1);

    assert_eq!(
        counted(
            &harness,
            DeadLetterFilter::new()
                .triggered_by("Echoed")
                .policy(other(&policy))
        )
        .await,
        0,
        "the other policy parked no echo"
    );

    harness.shutdown().await;
}

/// More parked rows than one chunk of the listing reads: [`DEAD_LETTER_CHUNK`] is 100
/// in the library, and the walk must cross it twice without the consumer seeing a seam.
const BACKLOG: usize = 250;

/// The listing walks a backlog larger than one chunk in id order; a consumer that
/// cannot hold the stream resumes after the last id it showed; and a row that leaves
/// the active set leaves the listing and its count, while its id stays a place to
/// resume from.
#[tokio::test]
async fn the_listing_resumes_after_a_key_across_chunks_postgres_test() {
    let harness = PolicyDaemonHarness::start("listing-resume", refusing_policy()).await;

    for n in 0..BACKLOG {
        harness
            .ping(&format!("customer-{n}"), &format!("refused-{n}"))
            .await;
    }
    harness.await_dead_letters(BACKLOG).await;

    let mine = DeadLetterFilter::new().policy(harness.policy_name());
    let all = listed_ids(&harness, mine.clone()).await;
    assert_eq!(all.len(), BACKLOG, "every row, across chunks");
    assert!(
        all.windows(2).all(|pair| pair[0] < pair[1]),
        "strictly ascending ids, none repeated at a chunk seam"
    );
    assert_eq!(counted(&harness, mine.clone()).await, BACKLOG as i64);

    // A paged UI behind HTTP: the second screen starts after the last id the first one
    // showed.
    let screen = 30;
    let first: Vec<i64> = harness
        .status_store()
        .dead_letters(mine.clone())
        .map_ok(|row| row.id)
        .take(screen)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(first, all[..screen]);
    let second: Vec<i64> = harness
        .status_store()
        .dead_letters_after(mine.clone(), *first.last().unwrap())
        .map_ok(|row| row.id)
        .take(screen)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(second, all[screen..2 * screen]);

    // The rest, resumed from inside the second chunk, reaches the end.
    let tail: Vec<i64> = harness
        .status_store()
        .dead_letters_after(mine.clone(), all[149])
        .map_ok(|row| row.id)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(tail, all[150..]);

    // A discarded row leaves the listing and the count; its id is still a key.
    let gone = all[screen - 1];
    assert_eq!(
        harness.discard_parked_row(gone).await,
        replay_persistence::DeadLetterDiscard::Discarded
    );
    let remaining = listed_ids(&harness, mine.clone()).await;
    assert!(!remaining.contains(&gone));
    assert_eq!(remaining.len(), BACKLOG - 1);
    assert_eq!(counted(&harness, mine.clone()).await, (BACKLOG - 1) as i64);
    let resumed: Vec<i64> = harness
        .status_store()
        .dead_letters_after(mine, gone)
        .map_ok(|row| row.id)
        .take(screen)
        .try_collect()
        .await
        .unwrap();
    assert_eq!(resumed, all[screen..2 * screen]);

    harness.shutdown().await;
}

/// A parked command whose triggering event is no longer in the log is still listed,
/// with no event, so the listing agrees with the count. A criterion on the trigger's
/// stream or type cannot match it, in the listing or in the count.
#[tokio::test]
async fn a_parked_command_whose_trigger_left_the_log_is_listed_without_it_postgres_test() {
    let harness = PolicyDaemonHarness::start("listing-gone", refusing_policy()).await;

    let gone = harness.ping("customer-gone", "refused-gone").await;
    let kept = harness.ping("customer-kept", "refused-kept").await;
    harness.await_dead_letters(2).await;
    harness.remove_from_log(&gone).await;

    let store = harness.status_store();
    let mine = DeadLetterFilter::new().policy(harness.policy_name());
    for filter in [DeadLetterFilter::new(), mine] {
        let rows = store.dead_letters_all(filter.clone()).await.unwrap();
        assert_eq!(rows.len(), 2, "both parked commands are listed: {rows:#?}");
        assert_eq!(store.dead_letter_count(&filter).await.unwrap(), 2);
        let orphan = rows
            .iter()
            .find(|row| row.event_id == gone.event_id)
            .expect("the orphaned row is listed");
        assert!(orphan.triggering_event.is_none(), "{orphan:#?}");
        let intact = rows
            .iter()
            .find(|row| row.event_id == kept.event_id)
            .expect("the intact row is listed");
        assert_eq!(
            intact.triggering_event.as_ref().map(|event| event.id),
            Some(kept.event_id)
        );
    }

    let in_gone_stream =
        DeadLetterFilter::new().triggered_in(ProbeUrn::new("customer-gone").unwrap());
    assert!(listed_ids(&harness, in_gone_stream.clone())
        .await
        .is_empty());
    assert_eq!(counted(&harness, in_gone_stream).await, 0);

    let pinged = DeadLetterFilter::new().triggered_by("Pinged");
    let rows = store.dead_letters_all(pinged.clone()).await.unwrap();
    assert_eq!(
        rows.iter().map(|row| row.event_id).collect::<Vec<_>>(),
        vec![kept.event_id]
    );
    assert_eq!(counted(&harness, pinged).await, 1);

    harness.shutdown().await;
}
