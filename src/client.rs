use std::sync::Arc;

use crate::drivers::TokioPostgresDriver;
use crate::error::Result;
use crate::querier::Querier;
use crate::traits::DatabaseDriver;

/// Main entry point for pgrs.
/// Holds a database connection and provides query building capabilities.
pub struct PgRsClient {
    driver: Arc<dyn DatabaseDriver>,
}

impl PgRsClient {
    /// Connect to a PostgreSQL database using the provided connection string.
    ///
    /// # Example
    /// ```ignore
    /// let client = PgRsClient::connect("postgres://user:pass@localhost/mydb").await?;
    /// ```
    pub async fn connect(connection_string: &str) -> Result<Self> {
        let driver = TokioPostgresDriver::connect(connection_string).await?;
        Ok(Self {
            driver: Arc::new(driver),
        })
    }

    /// Create a new client with a custom driver.
    /// Useful for testing or using alternative database drivers.
    pub fn with_driver(driver: Arc<dyn DatabaseDriver>) -> Self {
        Self { driver }
    }

    /// Create a Querier for building and executing queries.
    pub fn querier(&self) -> Querier {
        Querier::new(Arc::clone(&self.driver))
    }
}
