use thiserror::Error;

/// Error type for pgrs operations
#[derive(Debug, Error)]
pub enum PgRsError {
    #[error("Connection failed: {0}")]
    ConnectionFailed(String),

    #[error("Query failed: {0}")]
    QueryFailed(String),

    #[error("Expected {expected} row(s), got {actual}")]
    UnexpectedRowCount { expected: usize, actual: usize },

    #[error("Column not found: {0}")]
    ColumnNotFound(String),
}

/// Result type alias for pgrs operations
pub type Result<T> = std::result::Result<T, PgRsError>;
