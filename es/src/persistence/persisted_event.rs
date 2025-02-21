use chrono::{DateTime, Utc};
use urn::Urn;
use uuid::Uuid;

use crate::Event;

#[derive(Debug, Clone)]
pub struct PersistedEvent<E> {
    pub id: Uuid,
    pub data: E,
    pub stream_id: Urn,
    pub r#type: String,
    pub version: i64,
    pub created: DateTime<Utc>,
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
        }
    }
}
