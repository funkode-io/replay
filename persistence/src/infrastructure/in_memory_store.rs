use std::{collections::HashMap, sync::RwLock};

use chrono::Utc;
use futures::TryStream;
use serde_json::Value;
use urn::Urn;
use uuid::Uuid;

use crate::{EventStore, PersistedEvent, StreamFilter};
use replay::{Compactable, Event};

/// In-memory event store implementation, only for testing purpose.
///
/// It ignores stream type.
///
/// Events are stored per-stream-URN in insertion order. The `aggregate_version` field on each
/// event distinguishes current events (`None`) from archived compaction snapshots (`Some(n)`).
pub struct InMemoryEventStore {
    events: RwLock<HashMap<Urn, Vec<PersistedEvent<Value>>>>,
}

impl InMemoryEventStore {
    pub fn new() -> Self {
        Self {
            events: RwLock::new(HashMap::new()),
        }
    }

    /// Walk a filter tree and return the first `WithStreamId` URN found (used for fast lookup).
    fn extract_stream_id(filter: &StreamFilter) -> Option<Urn> {
        match filter {
            StreamFilter::WithStreamId(id) => Some(id.clone()),
            StreamFilter::And(left, right) => {
                Self::extract_stream_id(left).or_else(|| Self::extract_stream_id(right))
            }
            _ => None,
        }
    }

    /// Apply a filter to a raw (un-typed) persisted event.
    ///
    /// Returns an error if the filter contains a `ForStreamTypes` variant — that variant
    /// requires knowledge of the concrete `EventStream` type at compile time, which is not
    /// available when working with the raw JSON storage of the in-memory store.
    fn evaluate<E>(
        filter: &StreamFilter,
        event: &PersistedEvent<E>,
    ) -> Result<bool, replay::Error> {
        match filter {
            StreamFilter::All => Ok(true),
            StreamFilter::WithStreamId(stream_id) => Ok(event.stream_id == *stream_id),
            StreamFilter::ForStreamTypes(_) => Err(replay::Error::internal(
                "InMemoryEventStore does not support ForStreamTypes filters; \
                 use stream_events_by_stream_id with a concrete EventStream type instead",
            )
            .with_operation("stream_events")),
            StreamFilter::WithMetadata(metadata) => Ok(event.metadata == *metadata),
            StreamFilter::AfterVersion(version) => Ok(event.version > *version),
            StreamFilter::CreatedAfter(timestamp) => Ok(event.created > *timestamp),
            StreamFilter::WithAggregateVersion(v) => Ok(event.aggregate_version == *v),
            StreamFilter::And(left, right) => {
                Ok(Self::evaluate(left, event)? && Self::evaluate(right, event)?)
            }
            StreamFilter::Or(left, right) => {
                Ok(Self::evaluate(left, event)? || Self::evaluate(right, event)?)
            }
            StreamFilter::Not(inner) => Ok(!Self::evaluate(inner, event)?),
        }
    }
}

impl Default for InMemoryEventStore {
    fn default() -> Self {
        Self::new()
    }
}

impl EventStore for InMemoryEventStore {
    async fn store_events<S: replay::EventStream>(
        &self,
        stream_id: &S::StreamId,
        _stream_type: String,
        metadata: replay::Metadata,
        domain_events: &[S::Event],
        expected_version: Option<i64>,
    ) -> Result<(), replay::Error> {
        let mut store = self.events.write().unwrap();
        let stream_id: Urn = stream_id.clone().into();

        // Only count current (non-archived) events for the sequence version.
        let mut last_version = store
            .get(&stream_id)
            .map(|events| {
                events
                    .iter()
                    .filter(|e| e.aggregate_version.is_none())
                    .last()
                    .map(|e| e.version)
                    .unwrap_or(0)
            })
            .unwrap_or(0);

        if let Some(expected_version) = expected_version {
            if last_version != expected_version {
                return Err(crate::concurrency_error(
                    stream_id.clone(),
                    expected_version,
                    last_version,
                ));
            }
        }

        let serialized_events: Result<Vec<PersistedEvent<Value>>, replay::Error> = domain_events
            .iter()
            .map(|event| {
                let id = Uuid::new_v4();
                let created = Utc::now();
                let r#type = event.event_type();
                let version = last_version + 1;
                last_version = version;
                let data = serde_json::to_value(event).map_err(crate::ser_error)?;
                Ok(PersistedEvent {
                    id,
                    data,
                    stream_id: stream_id.clone(),
                    r#type,
                    version,
                    created,
                    metadata: metadata.clone(),
                    aggregate_version: None,
                })
            })
            .collect::<Result<Vec<_>, replay::Error>>();

        let events = serialized_events?;
        let stream = store.entry(stream_id.clone()).or_default();

        stream.extend(events);
        Ok(())
    }

    fn stream_events<E: Event>(
        &self,
        filter: StreamFilter,
    ) -> impl TryStream<Ok = PersistedEvent<E>, Error = replay::Error> + Send {
        // Optimise: if the filter references a specific stream URN, only scan that stream.
        let candidate_events: Vec<PersistedEvent<Value>> = {
            let store = self.events.read().unwrap();
            if let Some(stream_id) = Self::extract_stream_id(&filter) {
                store.get(&stream_id).cloned().unwrap_or_default()
            } else {
                store.values().flatten().cloned().collect()
            }
        };

        async_stream::stream! {
            for event in candidate_events {
                match Self::evaluate(&filter, &event) {
                    Err(e) => { yield Err(e); return; }
                    Ok(false) => continue,
                    Ok(true) => {}
                }
                let data: E = serde_json::from_value(event.data).map_err(crate::deser_error)?;
                yield Ok(PersistedEvent {
                    id: event.id,
                    data,
                    stream_id: event.stream_id,
                    r#type: event.r#type,
                    version: event.version,
                    created: event.created,
                    metadata: event.metadata,
                    aggregate_version: event.aggregate_version,
                });
            }
        }
    }

    async fn compact<A>(
        &self,
        aggregate: &A,
        metadata: replay::Metadata,
    ) -> Result<u32, replay::Error>
    where
        A: replay::Aggregate + Compactable + Sync,
    {
        let stream_id: Urn = aggregate.get_id().clone().into();

        // 1. Collect current live events while holding the read lock (brief, sync).
        //    Wrap them in a TryStream so that compacted_events can process them
        //    without assuming an in-memory slice is available.
        let current_events: Vec<A::Event> = {
            let store = self.events.read().unwrap();
            match store.get(&stream_id) {
                None => Vec::new(),
                Some(stream) => stream
                    .iter()
                    .filter(|e| e.aggregate_version.is_none())
                    .map(|e| {
                        serde_json::from_value::<A::Event>(e.data.clone())
                            .map_err(crate::deser_error)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            }
        };

        let event_stream =
            futures::stream::iter(current_events.into_iter().map(Ok::<_, replay::Error>));
        let compacted = aggregate.compacted_events(event_stream).await?;

        // 2. Determine the next archive version number and archive all current events.
        {
            let mut store = self.events.write().unwrap();
            let stream = store.entry(stream_id.clone()).or_default();

            let next_version: u32 = stream
                .iter()
                .filter_map(|e| e.aggregate_version)
                .max()
                .unwrap_or(0)
                + 1;

            // Archive: mark every current (aggregate_version = None) event with the new version.
            for event in stream.iter_mut() {
                if event.aggregate_version.is_none() {
                    event.aggregate_version = Some(next_version);
                }
            }

            // Insert compacted events as the new current stream (aggregate_version = None).
            // Sequence versions restart from 1.
            let mut seq: i64 = 0;
            for event in &compacted {
                seq += 1;
                let data = serde_json::to_value(event).map_err(crate::ser_error)?;
                stream.push(PersistedEvent {
                    id: Uuid::new_v4(),
                    data,
                    stream_id: stream_id.clone(),
                    r#type: event.event_type(),
                    version: seq,
                    created: Utc::now(),
                    metadata: metadata.clone(),
                    aggregate_version: None,
                });
            }

            return Ok(next_version);
        }
    }
}

// tests
#[cfg(test)]
mod tests {

    use super::*;
    use replay::{EventStream, WithId};

    use futures::TryStreamExt;

    use replay_macros::Event;
    use serde::{Deserialize, Serialize};
    use urn::{Urn, UrnBuilder};

    //  bank account stream (id of stream is not part of the model)
    struct BankAccountStream {
        pub id: BankAccountUrn,
        pub balance: f64,
    }

    // create bank account events enum: Deposited and Withdrawn
    #[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Event)]
    enum BankAccountEvent {
        Deposited { amount: f64 },
        Withdrawn { amount: f64 },
    }

    // bank account urn
    #[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
    struct BankAccountUrn(Urn);

    impl From<BankAccountUrn> for Urn {
        fn from(urn: BankAccountUrn) -> Self {
            urn.0
        }
    }

    impl TryFrom<Urn> for BankAccountUrn {
        type Error = String;

        fn try_from(urn: Urn) -> Result<Self, Self::Error> {
            Ok(BankAccountUrn(urn))
        }
    }

    impl WithId for BankAccountStream {
        type StreamId = BankAccountUrn;

        fn with_id(id: Self::StreamId) -> Self {
            BankAccountStream {
                id: id.clone(),
                balance: 0.0,
            }
        }

        fn get_id(&self) -> &Self::StreamId {
            &self.id
        }
    }

    // bank account stream
    impl replay::EventStream for BankAccountStream {
        type Event = BankAccountEvent;

        fn stream_type() -> String {
            "BankAccount".to_string()
        }

        fn apply(&mut self, event: Self::Event) {
            match event {
                BankAccountEvent::Deposited { amount } => {
                    self.balance += amount;
                }
                BankAccountEvent::Withdrawn { amount } => {
                    self.balance -= amount;
                }
            }
        }
    }

    #[tokio::test]
    async fn test_store_events() {
        let store = InMemoryEventStore {
            events: RwLock::new(HashMap::new()),
        };

        let stream_id = BankAccountUrn(UrnBuilder::new("bank-account", "1").build().unwrap());

        let events = vec![
            BankAccountEvent::Deposited { amount: 100.0 },
            BankAccountEvent::Withdrawn { amount: 40.0 },
        ];

        store
            .store_events::<BankAccountStream>(
                &stream_id,
                "BankAccount".to_string(),
                replay::Metadata::default(),
                &events,
                None,
            )
            .await
            .unwrap();

        let stream_events = store
            .stream_events::<BankAccountEvent>(StreamFilter::with_stream_id::<BankAccountStream>(
                &stream_id,
            ))
            .map_ok(|persisted_event| persisted_event.data)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(stream_events.len(), 2);

        let mut stream = BankAccountStream {
            id: stream_id.clone(),
            balance: 0.0,
        };
        stream.apply_all(stream_events);

        assert_eq!(stream.balance, 60.0);
    }
}
