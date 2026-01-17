use async_trait::async_trait;

use crate::error::Result;
use crate::types::{RawQueryResult, SqlValue};

/// Trait for database driver implementations.
/// Drivers are responsible for:
/// - Connecting to the database
/// - Converting SqlValue parameters to native types
/// - Executing queries and converting results to RawQueryResult
#[async_trait]
pub trait DatabaseDriver: Send + Sync {
    /// Execute a SQL query with the given parameters.
    /// Parameters use PostgreSQL-style placeholders ($1, $2, etc.)
    async fn execute(&self, sql: &str, params: &[SqlValue]) -> Result<RawQueryResult>;
}
