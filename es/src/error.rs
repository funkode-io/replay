use std::fmt;

pub type Result<T> = std::result::Result<T, Error>;

/// Categorizes errors by what the caller can do about them, not by their origin.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorKind {
    /// The requested resource was not found. Don't retry.
    NotFound,
    /// Validation failed. The input is invalid. Don't retry without fixing the input.
    InvalidInput,
    /// A conflict occurred (e.g., optimistic concurrency check failed). May retry.
    Conflict,
    /// An external dependency is temporarily unavailable. Safe to retry.
    Unavailable,
    /// An internal error occurred. Don't retry without investigation.
    Internal,
    /// A business rule was violated. Don't retry without changing the request.
    BusinessRuleViolation,
    /// Authentication is required but missing or invalid.
    Unauthorized,
    /// The authenticated user doesn't have permission for this operation.
    Forbidden,
    /// Too many requests, rate limit exceeded. Safe to retry after delay.
    RateLimited,
}

/// Indicates whether an error is worth retrying.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ErrorStatus {
    /// Don't retry - the error is permanent (e.g., validation failure, not found)
    Permanent,
    /// Safe to retry - the error is temporary (e.g., network timeout, rate limit)
    Temporary,
    /// Was retried but still failing - escalate or give up
    Persistent,
}

/// A flat, actionable error structure designed for both machine and human consumers.
///
/// This design follows the principles from: https://fast.github.io/blog/stop-forwarding-errors-start-designing-them/
///
/// Key principles:
/// - Categorize by action (ErrorKind), not by origin
/// - Make retryability explicit (ErrorStatus)
/// - Capture rich context for debugging
/// - Keep structure flat and queryable
///
/// # Error Chaining
///
/// The `with_source()` method automatically detects when the source is a `replay::Error`
/// and preserves the full error chain properly. For other error types, it attaches them as-is.
///
/// ```
/// # use replay::Error;
/// // Works seamlessly with replay::Error - preserves full chain
/// let db_error = Error::unavailable("Database connection timeout")
///     .with_operation("connect")
///     .with_context("host", "localhost:5432");
///
/// // Just use with_source() - automatic detection handles everything
/// let service_error = Error::internal("Failed to fetch user data")
///     .with_operation("get_user")
///     .with_context("user_id", "123")
///     .with_source(db_error);
///
/// // Also works with external errors (std::io::Error, etc.)
/// let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "config missing");
/// let config_error = Error::internal("Failed to read config")
///     .with_source(io_err);
/// ```
///
/// Alternatively, you can use explicit wrapper methods for more control:
/// - `wrap_internal()` - Wrap as an internal error
/// - `wrap_unavailable()` - Wrap as an unavailable error
/// - `wrap_conflict()` - Wrap as a conflict error
#[derive(Debug)]
pub struct Error {
    /// What kind of error occurred - guides the caller's response
    kind: ErrorKind,
    /// Whether this error is worth retrying
    status: ErrorStatus,
    /// Human-readable error message
    message: String,
    /// The operation that was being performed
    operation: &'static str,
    /// Additional context as key-value pairs
    context: Vec<(&'static str, String)>,
    /// The underlying error, if any (type-erased for flexibility)
    source: Option<Box<dyn std::error::Error + Send + Sync>>,
    /// Location where the error was created (file:line:column)
    location: String,
}

impl Error {
    /// Create a new error with the given kind, status, and message.
    #[track_caller]
    pub fn new(kind: ErrorKind, status: ErrorStatus, message: impl Into<String>) -> Self {
        Self {
            kind,
            status,
            message: message.into(),
            operation: "",
            context: Vec::new(),
            source: None,
            location: std::panic::Location::caller().to_string(),
        }
    }

    /// Create a permanent error (don't retry).
    #[track_caller]
    pub fn permanent(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self::new(kind, ErrorStatus::Permanent, message)
    }

    /// Create a temporary error (safe to retry).
    #[track_caller]
    pub fn temporary(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self::new(kind, ErrorStatus::Temporary, message)
    }

    /// Create a "not found" error.
    #[track_caller]
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::permanent(ErrorKind::NotFound, message)
    }

    /// Create an "invalid input" error.
    #[track_caller]
    pub fn invalid_input(message: impl Into<String>) -> Self {
        Self::permanent(ErrorKind::InvalidInput, message)
    }

    /// Create a "conflict" error (e.g., optimistic concurrency failure).
    #[track_caller]
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::new(ErrorKind::Conflict, ErrorStatus::Temporary, message)
    }

    /// Create an "unavailable" error (external dependency temporarily down).
    #[track_caller]
    pub fn unavailable(message: impl Into<String>) -> Self {
        Self::temporary(ErrorKind::Unavailable, message)
    }

    /// Create an "internal" error (unexpected, needs investigation).
    #[track_caller]
    pub fn internal(message: impl Into<String>) -> Self {
        Self::permanent(ErrorKind::Internal, message)
    }

    #[track_caller]
    pub fn business_rule_violation(message: impl Into<String>) -> Self {
        Self::permanent(ErrorKind::BusinessRuleViolation, message)
    }

    #[track_caller]
    pub fn unauthorized(message: impl Into<String>) -> Self {
        Self::permanent(ErrorKind::Unauthorized, message)
    }

    /// Set the operation being performed when the error occurred.
    pub fn with_operation(mut self, operation: &'static str) -> Self {
        self.operation = operation;
        self
    }

    /// Add context as a key-value pair.
    pub fn with_context(mut self, key: &'static str, value: impl fmt::Display) -> Self {
        self.context.push((key, value.to_string()));
        self
    }

    /// Attach a source error.
    ///
    /// This method automatically detects if the source is a `replay::Error` and preserves
    /// the full error chain properly. For other error types, it attaches them as-is.
    ///
    /// # Example
    /// ```
    /// # use replay::Error;
    /// // Works with replay::Error - preserves full chain
    /// let db_err = Error::unavailable("Connection failed");
    /// let wrapped = Error::internal("Database operation failed")
    ///     .with_source(db_err);
    ///
    /// // Works with other error types too
    /// let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file missing");
    /// let wrapped2 = Error::internal("Failed to read file")
    ///     .with_source(io_err);
    /// ```
    pub fn with_source(mut self, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        // Box the error first
        let boxed: Box<dyn std::error::Error + Send + Sync> = Box::new(source);

        // Try to downcast to replay::Error to detect if we're wrapping one of our own
        // If successful, we re-box it to preserve the concrete type information
        self.source = Some(match boxed.downcast::<Error>() {
            Ok(replay_error) => replay_error as Box<dyn std::error::Error + Send + Sync>,
            Err(original_box) => original_box,
        });
        self
    }

    /// Wrap an existing replay::Error with a new error context.
    /// This preserves the full error chain and generates proper "caused by" messages.
    #[track_caller]
    pub fn wrap(
        kind: ErrorKind,
        status: ErrorStatus,
        message: impl Into<String>,
        source: Error,
    ) -> Self {
        Self {
            kind,
            status,
            message: message.into(),
            operation: "",
            context: Vec::new(),
            source: Some(Box::new(source)),
            location: std::panic::Location::caller().to_string(),
        }
    }

    /// Wrap an existing replay::Error as a permanent error.
    #[track_caller]
    pub fn wrap_permanent(kind: ErrorKind, message: impl Into<String>, source: Error) -> Self {
        Self::wrap(kind, ErrorStatus::Permanent, message, source)
    }

    /// Wrap an existing replay::Error as a temporary error.
    #[track_caller]
    pub fn wrap_temporary(kind: ErrorKind, message: impl Into<String>, source: Error) -> Self {
        Self::wrap(kind, ErrorStatus::Temporary, message, source)
    }

    /// Wrap an existing replay::Error as an internal error.
    #[track_caller]
    pub fn wrap_internal(message: impl Into<String>, source: Error) -> Self {
        Self::wrap_permanent(ErrorKind::Internal, message, source)
    }

    /// Wrap an existing replay::Error as an unavailable error.
    #[track_caller]
    pub fn wrap_unavailable(message: impl Into<String>, source: Error) -> Self {
        Self::wrap_temporary(ErrorKind::Unavailable, message, source)
    }

    /// Wrap an existing replay::Error as a conflict error.
    #[track_caller]
    pub fn wrap_conflict(message: impl Into<String>, source: Error) -> Self {
        Self::wrap(ErrorKind::Conflict, ErrorStatus::Temporary, message, source)
    }

    /// Get the error kind.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Get the error status.
    pub fn status(&self) -> ErrorStatus {
        self.status
    }

    /// Check if this error is temporary (worth retrying).
    pub fn is_temporary(&self) -> bool {
        self.status == ErrorStatus::Temporary
    }

    /// Check if this error is permanent (don't retry).
    pub fn is_permanent(&self) -> bool {
        self.status == ErrorStatus::Permanent
    }

    /// Get the operation being performed.
    pub fn operation(&self) -> &str {
        self.operation
    }

    /// Get the context key-value pairs.
    pub fn context(&self) -> &[(&'static str, String)] {
        &self.context
    }

    /// Get the source error, if any.
    pub fn source(&self) -> Option<&(dyn std::error::Error + Send + Sync)> {
        self.source.as_ref().map(|e| e.as_ref())
    }

    /// Get the location where the error was created.
    pub fn location(&self) -> &str {
        &self.location
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Error kind with emoji for visual scanning
        let kind_str = match self.kind {
            ErrorKind::NotFound => "🔍 Not Found",
            ErrorKind::Unauthorized => "🔒 Unauthorized",
            ErrorKind::Forbidden => "🚫 Forbidden",
            ErrorKind::RateLimited => "⏱️  Rate Limited",
            ErrorKind::InvalidInput => "❌ Invalid Input",
            ErrorKind::Internal => "💥 Internal Error",
            ErrorKind::Conflict => "⚔️  Conflict",
            ErrorKind::Unavailable => "⚠️  Unavailable",
            ErrorKind::BusinessRuleViolation => "📋 Business Rule Violation",
        };

        // Status indicator
        let status_indicator = match self.status {
            ErrorStatus::Permanent => "🛑",
            ErrorStatus::Temporary => "🔄",
            ErrorStatus::Persistent => "⛔",
        };

        // Main error line with kind and status
        writeln!(f, "{} {} {}", status_indicator, kind_str, self.message)?;

        // Operation - what was being attempted
        if !self.operation.is_empty() {
            writeln!(f, "  Operation: {}", self.operation)?;
        }

        // Business context - most important for debugging
        if !self.context.is_empty() {
            writeln!(f, "  Context:")?;
            for (key, value) in &self.context {
                writeln!(f, "    • {}: {}", key, value)?;
            }
        }

        // Location - for developers to find the code
        writeln!(f, "  Location: {}", self.location)?;

        // Source chain - technical details
        if let Some(source) = &self.source {
            writeln!(f, "  Caused by: {}", source)?;

            // Walk the error chain
            let mut current_source = source.source();
            let mut depth = 1;
            while let Some(src) = current_source {
                writeln!(f, "  {}└─ {}", "  ".repeat(depth), src)?;
                current_source = src.source();
                depth += 1;
                if depth > 5 {
                    writeln!(f, "  {}└─ ...", "  ".repeat(depth))?;
                    break;
                }
            }
        }

        // Actionable hint based on error kind
        let hint = match (self.kind, self.status) {
            (ErrorKind::RateLimited, _) => {
                Some("💡 Wait before retrying, consider exponential backoff")
            }
            (ErrorKind::Unavailable, ErrorStatus::Temporary) => {
                Some("💡 Safe to retry immediately")
            }
            (ErrorKind::Unavailable, ErrorStatus::Persistent) => {
                Some("💡 Multiple retries failed, may need manual intervention")
            }
            (ErrorKind::NotFound, _) => Some("💡 Verify the resource identifier is correct"),
            (ErrorKind::Unauthorized, _) => Some("💡 Check authentication credentials"),
            (ErrorKind::Forbidden, _) => Some("💡 Insufficient permissions for this operation"),
            (ErrorKind::InvalidInput, _) => Some("💡 Review and fix the request parameters"),
            (ErrorKind::BusinessRuleViolation, _) => {
                Some("💡 Review business logic and request parameters")
            }
            _ => None,
        };

        if let Some(hint) = hint {
            writeln!(f, "  {}", hint)?;
        }

        Ok(())
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source
            .as_ref()
            .map(|e| e.as_ref() as &(dyn std::error::Error + 'static))
    }
}

#[cfg(test)]
mod tests {
    use tracing_test::traced_test;

    use super::*;

    #[test]
    fn test_error_creation() {
        let err = Error::not_found("user not found")
            .with_operation("fetch_user")
            .with_context("user_id", "123");

        assert_eq!(err.kind(), ErrorKind::NotFound);
        assert_eq!(err.status(), ErrorStatus::Permanent);
        assert!(err.is_permanent());
        assert!(!err.is_temporary());
        assert_eq!(err.operation(), "fetch_user");
        assert_eq!(err.context().len(), 1);
    }

    #[test]
    fn test_temporary_error() {
        let err = Error::unavailable("database connection failed");

        assert_eq!(err.kind(), ErrorKind::Unavailable);
        assert!(err.is_temporary());
        assert!(!err.is_permanent());
    }

    #[test]
    #[traced_test]
    fn test_error_display() {
        let err = Error::conflict("version mismatch")
            .with_operation("update_aggregate")
            .with_context("stream_id", "urn:user:123")
            .with_context("expected_version", "5");

        let display = format!("{}", err);
        tracing::info!("Error display:\n{}", err);

        assert!(display.contains("version mismatch"));
        assert!(display.contains("Operation: update_aggregate"));
        assert!(display.contains("stream_id: urn:user:123"));
        assert!(display.contains("expected_version: 5"));
    }

    #[test]
    fn test_error_wrapping() {
        // Create an initial error
        let original = Error::not_found("user record not found")
            .with_operation("fetch_user")
            .with_context("user_id", "123");

        // Wrap it in a new error
        let wrapped = Error::wrap_internal("Failed to load user profile", original);

        assert_eq!(wrapped.kind(), ErrorKind::Internal);
        assert_eq!(wrapped.status(), ErrorStatus::Permanent);

        // Check that the wrapped error is in the source chain
        let source = wrapped.source().expect("Should have a source error");
        assert!(source.to_string().contains("user record not found"));
    }

    #[test]
    #[traced_test]
    fn test_error_chain_display() {
        // Create a chain of errors
        let db_error = Error::unavailable("connection timeout")
            .with_operation("connect_to_db")
            .with_context("host", "localhost:5432");

        let query_error = Error::wrap_unavailable("Failed to query user data", db_error);

        let service_error = Error::wrap_internal("User service unavailable", query_error)
            .with_operation("get_user_profile")
            .with_context("request_id", "abc-123");

        let display = format!("{}", service_error);
        tracing::info!("Error chain display:\n{}", service_error);

        // Verify the top-level error information
        assert!(display.contains("User service unavailable"));
        assert!(display.contains("Operation: get_user_profile"));
        assert!(display.contains("request_id: abc-123"));

        // Verify the "Caused by" chain is present
        assert!(display.contains("Caused by:"));
        assert!(display.contains("Failed to query user data"));
        assert!(display.contains("connection timeout"));
    }

    #[test]
    fn test_wrap_methods() {
        // Test wrap_internal
        let original =
            Error::invalid_input("Invalid email format").with_context("email", "invalid@");
        let wrapped_internal = Error::wrap_internal("Validation failed", original);
        assert_eq!(wrapped_internal.kind(), ErrorKind::Internal);
        assert!(wrapped_internal.is_permanent());

        // Test wrap_unavailable
        let original2 = Error::internal("Service error");
        let wrapped_unavailable = Error::wrap_unavailable("External service down", original2);
        assert_eq!(wrapped_unavailable.kind(), ErrorKind::Unavailable);
        assert!(wrapped_unavailable.is_temporary());

        // Test wrap_conflict
        let original3 = Error::not_found("Resource missing");
        let wrapped_conflict = Error::wrap_conflict("Concurrent modification detected", original3);
        assert_eq!(wrapped_conflict.kind(), ErrorKind::Conflict);
        assert!(wrapped_conflict.is_temporary());
    }

    #[test]
    fn test_with_source_also_works_for_replay_errors() {
        // Verify that the existing with_source method also works for chaining replay::Error
        let original = Error::not_found("item not found").with_context("item_id", "456");

        let wrapped = Error::internal("Failed to retrieve item")
            .with_operation("get_item")
            .with_source(original);

        assert!(wrapped.source().is_some());
        let source = wrapped.source().unwrap();
        assert!(source.to_string().contains("item not found"));
    }

    #[test]
    fn test_with_source_auto_detection() {
        // Test that with_source automatically detects replay::Error
        let replay_err = Error::unavailable("Service down")
            .with_operation("call_api")
            .with_context("service", "payment-api");

        let wrapped = Error::internal("Payment processing failed")
            .with_operation("process_payment")
            .with_source(replay_err);

        // Verify the error chain is preserved
        assert!(wrapped.source().is_some());
        let source = wrapped.source().unwrap();

        // The source should be a replay::Error with full information
        assert!(source.to_string().contains("Service down"));
        assert!(source.to_string().contains("call_api"));
        assert!(source.to_string().contains("payment-api"));
    }

    #[test]
    fn test_with_source_works_with_other_error_types() {
        // Test that with_source still works with non-replay errors
        let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "config.json not found");

        let wrapped = Error::internal("Failed to load configuration")
            .with_operation("load_config")
            .with_source(io_err);

        assert!(wrapped.source().is_some());
        let source = wrapped.source().unwrap();
        assert!(source.to_string().contains("config.json not found"));
    }
}
