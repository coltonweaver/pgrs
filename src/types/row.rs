use std::collections::HashMap;

use crate::{
    error::{PgRsError, Result},
    Column,
};

/// Driver-agnostic raw result from a database query.
/// All values are converted to strings by the driver.
#[derive(Debug, Clone)]
pub struct RawQueryResult {
    /// Column names in order
    pub columns: Vec<String>,
    /// Rows, where each row is a vector of string values in column order
    pub rows: Vec<Vec<String>>,
}

impl RawQueryResult {
    pub fn new(columns: Vec<String>, rows: Vec<Vec<String>>) -> Self {
        Self { columns, rows }
    }

    pub fn empty() -> Self {
        Self {
            columns: Vec::new(),
            rows: Vec::new(),
        }
    }
}

/// A single row result from a query.
/// Values are stored as strings and accessed by column name.
#[derive(Debug, Clone)]
pub struct Row {
    values: HashMap<String, String>,
}

impl Row {
    /// Creates a new Row from column names and values.
    pub(crate) fn new(columns: &[String], values: Vec<String>) -> Self {
        let values = columns
            .iter()
            .zip(values.into_iter())
            .map(|(col, val)| (col.clone(), val))
            .collect();
        Self { values }
    }

    /// Gets a value by column name.
    pub fn get<T: Column + ?Sized>(&self, column: &T) -> Result<&str> {
        self.values
            .get(column.column_name())
            .map(|s| s.as_str())
            .ok_or_else(|| PgRsError::ColumnNotFound(column.qualified_name()))
    }

    /// Returns all column names in this row.
    pub fn columns(&self) -> Vec<&str> {
        self.values.keys().map(|s| s.as_str()).collect()
    }

    /// Returns the number of columns in this row.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns true if this row has no columns.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }
}

/// Result of a query execution, containing zero or more rows.
#[derive(Debug)]
pub struct QueryResult {
    columns: Vec<String>,
    rows: Vec<Row>,
}

impl QueryResult {
    /// Creates a QueryResult from a RawQueryResult.
    pub fn from_raw(raw: RawQueryResult) -> Self {
        let rows = raw
            .rows
            .into_iter()
            .map(|values| Row::new(&raw.columns, values))
            .collect();
        Self {
            columns: raw.columns,
            rows,
        }
    }

    /// Extracts a single row from the result.
    /// Returns an error if the result contains zero or more than one row.
    pub fn single_row(self) -> Result<Row> {
        if self.rows.len() != 1 {
            return Err(PgRsError::UnexpectedRowCount {
                expected: 1,
                actual: self.rows.len(),
            });
        }
        Ok(self.rows.into_iter().next().unwrap())
    }

    /// Returns all rows from the result.
    pub fn rows(self) -> Vec<Row> {
        self.rows
    }

    /// Returns a reference to the rows without consuming the result.
    pub fn rows_ref(&self) -> &[Row] {
        &self.rows
    }

    /// Returns the column names from this result.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Returns the number of rows in this result.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Returns true if this result contains no rows.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test column implementations
    struct IdColumn;
    struct NameColumn;
    struct MissingColumn;

    impl Column for IdColumn {
        fn column_name(&self) -> &'static str {
            "id"
        }
        fn table_name(&self) -> &'static str {
            "test"
        }
    }

    impl Column for NameColumn {
        fn column_name(&self) -> &'static str {
            "name"
        }
        fn table_name(&self) -> &'static str {
            "test"
        }
    }

    impl Column for MissingColumn {
        fn column_name(&self) -> &'static str {
            "missing"
        }
        fn table_name(&self) -> &'static str {
            "test"
        }
    }

    #[test]
    fn test_row_get() {
        let columns = vec!["id".to_string(), "name".to_string()];
        let values = vec!["1".to_string(), "John".to_string()];
        let row = Row::new(&columns, values);

        assert_eq!(row.get(&IdColumn).unwrap(), "1");
        assert_eq!(row.get(&NameColumn).unwrap(), "John");
        assert!(row.get(&MissingColumn).is_err());
    }

    #[test]
    fn test_query_result_single_row() {
        let raw = RawQueryResult {
            columns: vec!["id".to_string()],
            rows: vec![vec!["1".to_string()]],
        };
        let result = QueryResult::from_raw(raw);
        let row = result.single_row().unwrap();
        assert_eq!(row.get(&IdColumn).unwrap(), "1");
    }

    #[test]
    fn test_query_result_single_row_error_on_empty() {
        let raw = RawQueryResult {
            columns: vec!["id".to_string()],
            rows: vec![],
        };
        let result = QueryResult::from_raw(raw);
        let err = result.single_row().unwrap_err();
        match err {
            PgRsError::UnexpectedRowCount { expected, actual } => {
                assert_eq!(expected, 1);
                assert_eq!(actual, 0);
            }
            _ => panic!("Expected UnexpectedRowCount error"),
        }
    }

    #[test]
    fn test_query_result_single_row_error_on_multiple() {
        let raw = RawQueryResult {
            columns: vec!["id".to_string()],
            rows: vec![vec!["1".to_string()], vec!["2".to_string()]],
        };
        let result = QueryResult::from_raw(raw);
        let err = result.single_row().unwrap_err();
        match err {
            PgRsError::UnexpectedRowCount { expected, actual } => {
                assert_eq!(expected, 1);
                assert_eq!(actual, 2);
            }
            _ => panic!("Expected UnexpectedRowCount error"),
        }
    }
}
