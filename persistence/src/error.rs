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
///
/// A `55P03` is the server abandoning a wait for a row somebody else holds — a
/// [`crate::PostgresEventStore`]'s stream-lock wait, or a `lock_timeout` the
/// consumer set. It is temporary and retryable: the row is contended, not broken.
/// Classifying it here rather than only where this crate takes the stream row is
/// what makes it retryable from an inline projection's own write too, since a
/// projection handler maps its failures with this function.
pub fn db_error(error: sqlx::Error) -> replay::Error {
    if crate::lock_wait::is_lock_not_available(&error) {
        return replay::Error::unavailable(format!("Row lock not available: {error}"))
            .with_operation("database_operation");
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::lock_wait::test_support::refused;
    use crate::lock_wait::LOCK_NOT_AVAILABLE;

    /// The failure this crate's own stream-lock wait produces, and the one a
    /// consumer's `lock_timeout` produces inside an inline projection's write. A
    /// contended row is somebody else's transaction, not a defect: the Policy
    /// runner retries it, which it would not do for an `Internal`.
    #[test]
    fn a_row_lock_that_was_not_available_is_temporary() {
        let error = db_error(refused(
            LOCK_NOT_AVAILABLE,
            "canceling statement due to lock timeout",
        ));

        assert_eq!(error.kind(), replay::ErrorKind::Unavailable);
        assert!(error.is_temporary());
    }

    #[test]
    fn any_other_database_failure_stays_internal() {
        let error = db_error(refused("42703", "column \"nope\" does not exist"));

        assert_eq!(error.kind(), replay::ErrorKind::Internal);
    }
}
