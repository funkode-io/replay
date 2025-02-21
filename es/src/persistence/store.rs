use std::future::Future;

use futures::{TryStream, TryStreamExt};

use crate::{Aggregate, Event};

use super::{EventStoreError, PersistedEvent, StreamFilter};

pub trait EventStore: Send + Sync {
    fn store_events<S: crate::Stream>(
        &self,
        stream_id: &S::StreamId,
        stream_type: String,
        metadata: crate::Metadata,
        domain_events: &[S::Event],
        expected_version: Option<i64>,
    ) -> impl Future<Output = Result<(), EventStoreError>> + Send;

    fn stream_events<E: Event>(
        &self,
        filter: StreamFilter,
    ) -> impl TryStream<Ok = PersistedEvent<E>, Error = EventStoreError> + Send;

    fn fetch_aggregate<A: Aggregate + Send + Sync>(
        &self,
        id: &A::StreamId,
        at_stream_version: Option<i64>,
        at_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> impl Future<Output = Result<A, A::Error>> + Send {
        async move {
            let events = self
                .stream_events_by_stream_id::<A>(id, at_stream_version, at_timestamp)
                .map_err(A::Error::from);

            let mut stream = A::default();

            futures::pin_mut!(events);

            while let Some(event) = events.try_next().await? {
                stream.apply(event.data);
            }

            Ok(stream)
        }
    }

    fn apply_command_and_store_events<A: Aggregate>(
        &self,
        id: &A::StreamId,
        metadata: crate::Metadata,
        command: A::Command,
        services: &A::Services,
        expected_version: Option<i64>,
    ) -> impl Future<Output = Result<A, A::Error>> + Send {
        async move {
            // get stream from store
            let mut aggregate = self
                .fetch_aggregate::<A>(id, expected_version, None)
                .await?;

            let stream_type = A::stream_type();

            let events = aggregate.handle(command, services).await?;

            self.store_events::<A>(id, stream_type, metadata, &events, expected_version)
                .await
                .map_err(A::Error::from)?;

            aggregate.apply_all(events);

            Ok(aggregate)
        }
    }

    fn stream_events_by_stream_id<S: crate::Stream>(
        &self,
        stream_id: &S::StreamId,
        at_stream_version: Option<i64>,
        at_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> impl TryStream<Ok = PersistedEvent<S::Event>, Error = EventStoreError> + Send {
        let filter = StreamFilter::WithStreamId(stream_id.clone().into())
            .and_at_stream_version_optional(at_stream_version)
            .and_at_timestamp_optional(at_timestamp);

        self.stream_events::<S::Event>(filter)
    }
}
