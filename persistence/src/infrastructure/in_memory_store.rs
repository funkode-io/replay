use std::{collections::HashMap, sync::RwLock};

use chrono::Utc;
use futures::TryStream;
use serde_json::Value;
use urn::Urn;
use uuid::Uuid;

use crate::{EventStore, PersistedEvent, StreamFilter};
use replay::Event;

/// In-memory event store implementation, only for testing purpose.
///
/// It ignores stream type and metadata.
///
/// It only supports filter by stream id.
pub struct InMemoryEventStore {
    events: RwLock<HashMap<Urn, Vec<PersistedEvent<Value>>>>,
}

impl EventStore for InMemoryEventStore {
    async fn store_events<S: replay::Stream>(
        &self,
        stream_id: &S::StreamId,
        _stream_type: String,
        metadata: replay::Metadata,
        domain_events: &[S::Event],
        expected_version: Option<i64>,
    ) -> Result<(), replay::Error> {
        let mut store = self.events.write().unwrap();
        let stream_id: Urn = stream_id.clone().into();

        let mut last_version = store
            .get(&stream_id)
            .map(|events| events.last().map(|e| e.version).unwrap_or(0))
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
        // right now we only support filtering by stream id

        async_stream::stream! {

            if let StreamFilter::WithStreamId(stream_id) = filter {
                let stream = self.events.read().unwrap().get(&stream_id).cloned().unwrap_or_default();

                for event in stream {
                    let data: E = serde_json::from_value(event.data).map_err(crate::deser_error)?;
                    yield Ok(PersistedEvent {
                        id: event.id,
                        data,
                        stream_id: event.stream_id,
                        r#type: event.r#type,
                        version: event.version,
                        created: event.created,
                        metadata: event.metadata,
                    });
                }

            } else {
                yield Err(replay::Error::internal("Unsupported filter").with_operation("stream_events"));
            };
        }
    }
}

// tests
#[cfg(test)]
mod tests {
    use super::*;
    use replay::Stream;

    use futures::TryStreamExt;

    use replay_macros::Event;
    use serde::{Deserialize, Serialize};
    use urn::{Urn, UrnBuilder};

    //  bank account stream (id of stream is not part of the model)
    #[derive(Default)]
    struct BankAccountStream {
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

    // bank account stream
    impl replay::Stream for BankAccountStream {
        type Event = BankAccountEvent;
        type StreamId = BankAccountUrn;

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

        let mut stream = BankAccountStream::default();
        stream.apply_all(stream_events);

        assert_eq!(stream.balance, 60.0);
    }
}
