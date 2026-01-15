use std::future::Future;

use futures::TryStream;

use replay::Event;

use super::PersistedEvent;

pub trait EventStore: Send + Sync {
    fn store_events<S: replay::Stream>(
        &self,
        stream_id: &S::StreamId,
        stream_type: String,
        metadata: replay::Metadata,
        domain_events: &[S::Event],
        expected_version: Option<i64>,
    ) -> impl Future<Output = Result<(), replay::Error>> + Send;

    fn stream_events<E: Event>(
        &self,
        filter: crate::StreamFilter,
    ) -> impl TryStream<Ok = PersistedEvent<E>, Error = replay::Error> + Send;

    fn stream_events_by_stream_id<S: replay::Stream>(
        &self,
        stream_id: &S::StreamId,
        at_stream_version: Option<i64>,
        at_timestamp: Option<chrono::DateTime<chrono::Utc>>,
    ) -> impl TryStream<Ok = PersistedEvent<S::Event>, Error = replay::Error> + Send {
        let filter = crate::StreamFilter::WithStreamId(stream_id.clone().into())
            .and_at_stream_version_optional(at_stream_version)
            .and_at_timestamp_optional(at_timestamp);

        self.stream_events::<S::Event>(filter)
    }
}
