use std::sync::Arc;

use crate::builders::SelectBuilder;
use crate::traits::DatabaseDriver;

/// Query builder factory.
/// Created from a PgRsClient and used to build and execute queries.
pub struct Querier {
    driver: Arc<dyn DatabaseDriver>,
}

impl Querier {
    pub(crate) fn new(driver: Arc<dyn DatabaseDriver>) -> Self {
        Self { driver }
    }

    /// Start building a SELECT query.
    pub fn select(&self) -> SelectBuilder {
        SelectBuilder::new(Arc::clone(&self.driver))
    }
}
