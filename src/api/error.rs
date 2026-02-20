use thiserror::Error;

/// Structured error type for pgmold library operations.
///
/// This enum is marked `#[non_exhaustive]` to allow adding new variants
/// in future versions without breaking semver.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// Failed to parse schema SQL.
    #[error("Parse error: {message}")]
    Parse { message: String },

    /// Failed to connect to database.
    #[error("Database connection failed: {message}")]
    Connection { message: String },

    /// Failed to introspect database schema.
    #[error("Introspection failed: {message}")]
    Introspection { message: String },

    /// Invalid glob pattern in filter.
    #[error("Invalid filter pattern: {pattern}")]
    InvalidFilter { pattern: String },

    /// Migration validation failed on temp database.
    #[error("Migration validation failed: {message}")]
    Validation {
        message: String,
        errors: Vec<ValidationError>,
    },

    /// Migration statement execution failed.
    #[error("Migration execution failed at statement {statement_index}: {message}")]
    Execution {
        message: String,
        statement_index: usize,
        sql: String,
    },

    /// Lint check found errors.
    #[error("Lint check failed with {count} error(s)")]
    LintFailed {
        count: usize,
        issues: Vec<crate::lint::LintResult>,
    },

    /// Failed to create tokio runtime (blocking API only).
    #[error("Runtime error: {message}")]
    Runtime { message: String },
}

/// Details about a validation error during migration testing.
#[derive(Debug, Clone)]
pub struct ValidationError {
    pub statement_index: usize,
    pub sql: String,
    pub error_message: String,
}

impl Error {
    /// Create a parse error.
    pub fn parse(message: impl Into<String>) -> Self {
        Self::Parse {
            message: message.into(),
        }
    }

    /// Create a connection error.
    pub fn connection(message: impl Into<String>) -> Self {
        Self::Connection {
            message: message.into(),
        }
    }

    /// Create an introspection error.
    pub fn introspection(message: impl Into<String>) -> Self {
        Self::Introspection {
            message: message.into(),
        }
    }

    /// Create an invalid filter error.
    pub fn invalid_filter(pattern: impl Into<String>) -> Self {
        Self::InvalidFilter {
            pattern: pattern.into(),
        }
    }

    /// Create a runtime error.
    pub fn runtime(message: impl Into<String>) -> Self {
        Self::Runtime {
            message: message.into(),
        }
    }
}
