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
    /// Matches the `INTEGER` column type in the database.
    pub aggregate_version: Option<i32>,
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

    /// Re-envelope a *borrowed* event with new data.
    ///
    /// The counterpart to [`with_data`](Self::with_data) for the erasure bridges, which
    /// only ever hold `&PersistedEvent<Value>`: the payload is not copied at all (the
    /// caller supplies it, typically deserialized by borrow from `self.data`), and the
    /// envelope's owned fields are cloned individually rather than by cloning the whole
    /// event — which would drag the JSON payload along with them.
    pub(crate) fn with_data_from<Other: Event>(&self, data: Other) -> PersistedEvent<Other> {
        PersistedEvent {
            id: self.id,
            data,
            stream_id: self.stream_id.clone(),
            r#type: self.r#type.clone(),
            version: self.version,
            created: self.created,
            metadata: self.metadata.clone(),
            aggregate_version: self.aggregate_version,
        }
    }
}
