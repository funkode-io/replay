use chrono::{DateTime, Utc};
use urn::Urn;
use uuid::Uuid;

use replay::{Event, Metadata};

#[derive(Debug, Clone)]
pub struct PersistedEvent<E> {
    pub id: Uuid,
    pub data: E,
    pub stream_id: Urn,
    pub r#type: String,
    /// Monotonic position of this event within the aggregate's current stream.
    pub version: i64,
    pub created: DateTime<Utc>,
    pub metadata: Metadata,
    /// `None` identifies events belonging to the current (latest) stream.
    /// `Some(n)` identifies events that were archived during the nth compaction.
    pub aggregate_version: Option<u32>,
}

impl<E> PersistedEvent<E> {
    pub fn wrap_data_with<Other: From<E>>(self) -> PersistedEvent<Other> {
        PersistedEvent {
            id: self.id,
            data: Other::from(self.data),
            stream_id: self.stream_id,
            r#type: self.r#type,
            version: self.version,
            created: self.created,
            metadata: self.metadata,
            aggregate_version: self.aggregate_version,
        }
    }

    pub fn with_data<Other: Event>(self, data: Other) -> PersistedEvent<Other> {
        PersistedEvent {
            id: self.id,
            data,
            stream_id: self.stream_id,
            r#type: self.r#type,
            version: self.version,
            created: self.created,
            metadata: self.metadata,
            aggregate_version: self.aggregate_version,
        }
    }
}
