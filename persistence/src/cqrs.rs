use std::sync::Arc;

use futures::{StreamExt, TryStreamExt};

use replay::{Aggregate, Event};

use super::EventStore;

#[derive(Clone)]
pub struct Cqrs<ES: EventStore> {
    store: Arc<ES>,
}

impl<ES: EventStore> Cqrs<ES> {
    pub fn new(event_store: ES) -> Self {
        Self {
            store: Arc::new(event_store),
        }
    }

    pub async fn fetch_aggregate<A: Aggregate + Sync>(
        &self,
        id: &A::StreamId,
        at_stream_version: Option<i64>,
        at_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> Result<A, A::Error> {
        let events = self
            .store
            .stream_events_by_stream_id::<A>(id, at_stream_version, at_timestamp)
            .map_err(A::Error::from);

        let mut stream = A::with_id(id.clone());

        futures::pin_mut!(events);

        while let Some(event) = events.try_next().await? {
            stream.apply(event.data);
        }

        Ok(stream)
    }

    pub async fn execute<A: Aggregate>(
        &self,
        id: &A::StreamId,
        metadata: replay::Metadata,
        command: A::Command,
        services: &A::Services,
        expected_version: Option<i64>,
    ) -> Result<A, A::Error> {
        // get stream from store
        let mut aggregate = self
            .fetch_aggregate::<A>(id, expected_version, None)
            .await?;

        let stream_type = A::stream_type();

        let events = aggregate.handle(command, services).await?;

        self.store
            .store_events::<A>(id, stream_type, metadata, &events, expected_version)
            .await
            .map_err(A::Error::from)?;

        aggregate.apply_all(events);

        Ok(aggregate)
    }

    pub async fn run_query<'a, Q, E>(&'a self, query: &'a mut Q) -> Result<(), replay::Error>
    where
        E: Event + 'a,
        Q: crate::Query<Event = E>,
    {
        let events = self
            .store
            .stream_events::<E>(query.stream_filter())
            .into_stream();

        futures::pin_mut!(events);

        while let Some(Ok(event)) = events.next().await {
            query.update(event);
        }

        Ok(())
    }
}
