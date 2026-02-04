use thiserror::Error;

/// Structured error type for pgmold library operations.
#[derive(Debug, Error)]
pub enum Error {
    #[error("Parse error: {message}")]
    Parse { message: String },

    #[error("Database connection failed: {message}")]
    Connection { message: String },

    #[error("Introspection failed: {message}")]
    Introspection { message: String },

    #[error("Invalid filter pattern: {pattern}")]
    InvalidFilter { pattern: String },

    #[error("Migration validation failed: {message}")]
    Validation {
        message: String,
        errors: Vec<ValidationError>,
    },

    #[error("Migration execution failed at statement {statement_index}: {message}")]
    Execution {
        message: String,
        statement_index: usize,
        sql: String,
    },

    #[error("Lint check failed with {count} error(s)")]
    LintFailed {
        count: usize,
        issues: Vec<crate::lint::LintResult>,
    },

    #[error("Invalid schema source: {schema_source}")]
    InvalidSource { schema_source: String },

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
    pub fn parse(message: impl Into<String>) -> Self {
        Self::Parse {
            message: message.into(),
        }
    }

    pub fn connection(message: impl Into<String>) -> Self {
        Self::Connection {
            message: message.into(),
        }
    }

    pub fn introspection(message: impl Into<String>) -> Self {
        Self::Introspection {
            message: message.into(),
        }
    }

    pub fn invalid_filter(pattern: impl Into<String>) -> Self {
        Self::InvalidFilter {
            pattern: pattern.into(),
        }
    }

    pub fn invalid_source(schema_source: impl Into<String>) -> Self {
        Self::InvalidSource {
            schema_source: schema_source.into(),
        }
    }

    pub fn runtime(message: impl Into<String>) -> Self {
        Self::Runtime {
            message: message.into(),
        }
    }
}
