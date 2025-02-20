use std::future::Future;

use futures::TryStream;

use urn::Urn;

use crate::Event;

use super::{EventStoreError, PersistedEvent, StreamFilter};

#[trait_variant::make(EventStore: Send)]
pub trait LocalEventStore {
    fn store_events<E: Event>(
        &mut self,
        stream_id: &Urn,
        stream_type: String,
        metadata: crate::Metadata,
        domain_events: &[E],
        expected_version: Option<i64>,
    ) -> impl Future<Output = Result<(), EventStoreError>>;

    fn stream_events<E: Event>(
        &self,
        filter: StreamFilter,
    ) -> impl TryStream<Ok = PersistedEvent<E>, Error = EventStoreError>;

    fn stream_events_by_stream_id<E: Event>(
        &self,
        stream_id: Urn,
        at_stream_version: Option<i64>,
        at_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> impl TryStream<Ok = PersistedEvent<E>, Error = EventStoreError> {
        let filter = StreamFilter::WithStreamId(stream_id)
            .and_at_stream_version_optional(at_stream_version)
            .and_at_timestamp_optional(at_timestamp);

        self.stream_events::<E>(filter)
    }
}
