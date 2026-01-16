use urn::Urn;

/// Convert a deserialization error to replay::Error
pub fn deser_error(error: serde_json::Error) -> replay::Error {
    replay::Error::internal(format!("Deserialization failed: {}", error))
        .with_operation("deserialize")
}

/// Convert a serialization error to replay::Error
pub fn ser_error(error: serde_json::Error) -> replay::Error {
    replay::Error::internal(format!("Serialization failed: {}", error)).with_operation("serialize")
}

/// Convert a sqlx error to replay::Error
pub fn db_error(error: sqlx::Error) -> replay::Error {
    match error {
        sqlx::Error::RowNotFound => {
            replay::Error::not_found("Row not found").with_operation("database_query")
        }
        sqlx::Error::PoolTimedOut => {
            replay::Error::unavailable("Database connection pool timed out")
                .with_operation("database_connect")
        }
        _ => replay::Error::internal(format!("Database error: {}", error))
            .with_operation("database_operation"),
    }
}

/// Create a concurrency conflict error
pub fn concurrency_error(
    stream_id: Urn,
    expected_version: i64,
    actual_version: i64,
) -> replay::Error {
    replay::Error::conflict("Stream version mismatch")
        .with_operation("store_events")
        .with_context("stream_id", stream_id.to_string())
        .with_context("expected_version", expected_version)
        .with_context("actual_version", actual_version)
}
