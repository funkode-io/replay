use std::{collections::HashMap, sync::RwLock};

use chrono::Utc;
use futures::TryStream;
use serde_json::Value;
use urn::Urn;
use uuid::Uuid;

use crate::{
    persistence::{EventStoreError, LocalEventStore, PersistedEvent, StreamFilter},
    Event,
};

/// In-memory event store implementation, only for testing purpose.
///
/// It ignores stream type and metadata.
///
/// It only supports filter by stream id.
pub struct InMemoryEventStore {
    events: RwLock<HashMap<Urn, Vec<PersistedEvent<Value>>>>,
}

impl LocalEventStore for InMemoryEventStore {
    async fn store_events<E: Event>(
        &self,
        stream_id: &Urn,
        _stream_type: String,
        _metadata: crate::Metadata,
        domain_events: &[E],
        expected_version: Option<i64>,
    ) -> Result<(), EventStoreError> {
        let mut store = self.events.write().unwrap();

        let mut last_version = store
            .get(stream_id)
            .map(|events| events.last().map(|e| e.version).unwrap_or(0))
            .unwrap_or(0);

        if let Some(expected_version) = expected_version {
            if last_version != expected_version {
                return Err(EventStoreError::ConcurrencyError {
                    stream_id: stream_id.clone(),
                    expected_version,
                    actual_version: last_version,
                });
            }
        }

        let serialized_events: Result<Vec<PersistedEvent<Value>>, EventStoreError> = domain_events
            .iter()
            .map(|event| {
                let id = Uuid::new_v4();
                let created = Utc::now();
                let r#type = event.event_type();
                let version = last_version + 1;
                last_version = version;
                let data = serde_json::to_value(event)?;
                Ok(PersistedEvent {
                    id,
                    data,
                    stream_id: stream_id.clone(),
                    r#type,
                    version,
                    created,
                })
            })
            .collect::<Result<Vec<_>, EventStoreError>>();

        let events = serialized_events?;
        let stream = store.entry(stream_id.clone()).or_default();

        stream.extend(events);
        Ok(())
    }

    fn stream_events<E: Event>(
        &self,
        filter: StreamFilter,
    ) -> impl TryStream<Ok = PersistedEvent<E>, Error = EventStoreError> {
        // right now we only support filtering by stream id

        async_stream::stream! {

            if let StreamFilter::WithStreamId(stream_id) = filter {
                let stream = self.events.read().unwrap().get(&stream_id).cloned().unwrap_or_default();

                for event in stream {
                    let data: E = serde_json::from_value(event.data).map_err(EventStoreError::deser_error)?;
                    yield Ok(PersistedEvent {
                        id: event.id,
                        data,
                        stream_id: event.stream_id,
                        r#type: event.r#type,
                        version: event.version,
                        created: event.created,
                    });
                }

            } else {
                yield Err(EventStoreError::DatabaseError(anyhow::anyhow!("Unsupported filter")));
            };
        }
    }
}

// tests
#[cfg(test)]
mod tests {
    use super::*;
    use crate::Stream;

    use futures::TryStreamExt;
    use replay_macros::Event;
    use serde::{Deserialize, Serialize};
    use urn::{Urn, UrnBuilder};

    //  bank account stream (id of stream is not part of the model)
    #[derive(Default)]
    struct BankAccountStream {
        pub balance: f64,
    }

    // create bank account events enum: Deposit and Withdraw
    #[derive(Serialize, Deserialize, Clone, PartialEq, Debug, Event)]
    enum BankAccountEvent {
        Deposit { amount: f64 },
        Withdraw { amount: f64 },
    }

    // bank account urn
    #[derive(Clone, Serialize, Deserialize, PartialEq, Debug)]
    struct BankAccountUrn(Urn);

    impl From<BankAccountUrn> for Urn {
        fn from(urn: BankAccountUrn) -> Self {
            urn.0
        }
    }

    // bank account stream
    impl crate::Stream for BankAccountStream {
        type Event = BankAccountEvent;
        type StreamId = BankAccountUrn;

        fn stream_type() -> String {
            "BankAccount".to_string()
        }

        fn apply(&mut self, event: Self::Event) {
            match event {
                BankAccountEvent::Deposit { amount } => {
                    self.balance += amount;
                }
                BankAccountEvent::Withdraw { amount } => {
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

        let stream_id = UrnBuilder::new("bank-account", "1").build().unwrap();

        let events = vec![
            BankAccountEvent::Deposit { amount: 100.0 },
            BankAccountEvent::Withdraw { amount: 40.0 },
        ];

        store
            .store_events(
                &stream_id,
                "BankAccount".to_string(),
                crate::Metadata::default(),
                &events,
                None,
            )
            .await
            .unwrap();

        let stream_events = store
            .stream_events::<BankAccountEvent>(StreamFilter::WithStreamId(stream_id))
            .map_ok(|persisted_event| persisted_event.data)
            .try_collect::<Vec<_>>()
            .await
            .unwrap();

        assert_eq!(stream_events.len(), 2);

        let mut stream = BankAccountStream::default();
        stream.apply_all(stream_events);

        assert_eq!(stream.balance, 60.0);
    }
}
