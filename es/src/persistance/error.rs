use thiserror::Error;
use urn::Urn;

#[derive(Debug, Error)]
pub enum EventStoreError {
    #[error("Serialization error: {0}")]
    Serialization(anyhow::Error),
    #[error("Deserialization error: {0}")]
    Deserialization(anyhow::Error),
    #[error("Database error: {0}")]
    DatabaseError(anyhow::Error),
    #[error("Concurrency error: stream_id: {stream_id}, expected_version: {expected_version}, actual_version: {actual_version}")]
    ConcurrencyError {
        stream_id: Urn,
        expected_version: i64,
        actual_version: i64,
    },
}

impl EventStoreError {
    pub fn deser_error(error: serde_json::Error) -> Self {
        EventStoreError::Deserialization(error.into())
    }
}

impl From<serde_json::Error> for EventStoreError {
    fn from(error: serde_json::Error) -> Self {
        EventStoreError::Serialization(error.into())
    }
}

impl From<sqlx::Error> for EventStoreError {
    fn from(error: sqlx::Error) -> Self {
        EventStoreError::DatabaseError(error.into())
    }
}
