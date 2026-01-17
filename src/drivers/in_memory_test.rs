use std::collections::VecDeque;
use std::sync::Mutex;

use async_trait::async_trait;

use crate::error::Result;
use crate::traits::DatabaseDriver;
use crate::types::{RawQueryResult, SqlValue};

/// A recorded query execution for verification.
#[derive(Debug, Clone, PartialEq)]
pub struct RecordedQuery {
    pub sql: String,
    pub params: Vec<SqlValue>,
}

/// An in-memory database driver for testing.
///
/// Allows configuring expected responses and verifying executed queries.
///
/// # Example
/// ```
/// use std::sync::Arc;
/// use pgrs::drivers::{InMemoryTestDriver, InMemoryTestResponseBuilder};
/// use pgrs::traits::DatabaseDriver;
///
/// let driver = Arc::new(
///     InMemoryTestDriver::new().with_response(
///         InMemoryTestResponseBuilder::new()
///             .columns(&["id", "name"])
///             .row(&["1", "Alice"])
///             .build(),
///     ),
/// );
/// ```
pub struct InMemoryTestDriver {
    responses: Mutex<VecDeque<RawQueryResult>>,
    recorded_queries: Mutex<Vec<RecordedQuery>>,
    default_response: RawQueryResult,
}

impl InMemoryTestDriver {
    /// Create a new in-memory test driver with no pre-configured responses.
    pub fn new() -> Self {
        Self {
            responses: Mutex::new(VecDeque::new()),
            recorded_queries: Mutex::new(Vec::new()),
            default_response: RawQueryResult::empty(),
        }
    }

    /// Add a response to be returned by the next query.
    /// Responses are returned in FIFO order.
    pub fn with_response(self, response: RawQueryResult) -> Self {
        self.responses.lock().unwrap().push_back(response);
        self
    }

    /// Add multiple responses to be returned by subsequent queries.
    pub fn with_responses(self, responses: impl IntoIterator<Item = RawQueryResult>) -> Self {
        let mut queue = self.responses.lock().unwrap();
        for response in responses {
            queue.push_back(response);
        }
        drop(queue);
        self
    }

    /// Set a default response to use when no queued responses remain.
    pub fn with_default_response(mut self, response: RawQueryResult) -> Self {
        self.default_response = response;
        self
    }

    /// Get all recorded queries that have been executed.
    pub fn recorded_queries(&self) -> Vec<RecordedQuery> {
        self.recorded_queries.lock().unwrap().clone()
    }

    /// Get the last recorded query, if any.
    pub fn last_query(&self) -> Option<RecordedQuery> {
        self.recorded_queries.lock().unwrap().last().cloned()
    }

    /// Clear all recorded queries.
    pub fn clear_recorded_queries(&self) {
        self.recorded_queries.lock().unwrap().clear();
    }

    /// Assert that the last query matches the expected SQL and parameters.
    pub fn assert_last_query(&self, expected_sql: &str, expected_params: &[SqlValue]) {
        let last = self.last_query().expect("No queries were recorded");
        assert_eq!(
            last.sql, expected_sql,
            "SQL mismatch.\nExpected: {}\nActual: {}",
            expected_sql, last.sql
        );
        assert_eq!(
            last.params, expected_params,
            "Parameters mismatch.\nExpected: {:?}\nActual: {:?}",
            expected_params, last.params
        );
    }

    /// Assert that exactly n queries were executed.
    pub fn assert_query_count(&self, expected: usize) {
        let actual = self.recorded_queries.lock().unwrap().len();
        assert_eq!(
            actual, expected,
            "Query count mismatch. Expected: {}, Actual: {}",
            expected, actual
        );
    }
}

impl Default for InMemoryTestDriver {
    fn default() -> Self {
        Self::new()
    }
}

#[async_trait]
impl DatabaseDriver for InMemoryTestDriver {
    async fn execute(&self, sql: &str, params: &[SqlValue]) -> Result<RawQueryResult> {
        // Record the query
        self.recorded_queries.lock().unwrap().push(RecordedQuery {
            sql: sql.to_string(),
            params: params.to_vec(),
        });

        // Return next queued response or default
        let response = self
            .responses
            .lock()
            .unwrap()
            .pop_front()
            .unwrap_or_else(|| self.default_response.clone());

        Ok(response)
    }
}

/// Builder for creating test responses easily.
pub struct InMemoryTestResponseBuilder {
    columns: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl InMemoryTestResponseBuilder {
    pub fn new() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }

    /// Set the column names for the response.
    pub fn columns(mut self, cols: &[&str]) -> Self {
        self.columns = cols.iter().map(|s| s.to_string()).collect();
        self
    }

    /// Add a row of string values.
    pub fn row(mut self, values: &[&str]) -> Self {
        self.rows
            .push(values.iter().map(|s| s.to_string()).collect());
        self
    }

    /// Build the RawQueryResult.
    pub fn build(self) -> RawQueryResult {
        RawQueryResult::new(self.columns, self.rows)
    }
}

impl Default for InMemoryTestResponseBuilder {
    fn default() -> Self {
        Self::new()
    }
}
