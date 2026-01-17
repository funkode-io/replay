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
    pub fn with_source(mut self, source: impl std::error::Error + Send + Sync + 'static) -> Self {
        self.source = Some(Box::new(source));
        self
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
}
