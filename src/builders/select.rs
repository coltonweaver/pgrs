use std::sync::Arc;

use crate::clauses::WhereClause;
use crate::error::{PgRsError, Result};
use crate::traits::{Column, ColumnRef, DatabaseDriver, Table};
use crate::types::{QueryResult, SqlValue};

/// Builder for SELECT queries.
///
/// Use the fluent API to construct a query, then call `execute()` to run it.
/// Required fields (columns, table) are validated at execution time.
pub struct SelectBuilder {
    driver: Arc<dyn DatabaseDriver>,
    columns: Vec<ColumnRef>,
    table: Option<String>,
    where_clause: Option<WhereClause>,
    limit: Option<u64>,
}

impl SelectBuilder {
    pub(crate) fn new(driver: Arc<dyn DatabaseDriver>) -> Self {
        Self {
            driver,
            columns: Vec::new(),
            table: None,
            where_clause: None,
            limit: None,
        }
    }

    /// Specify the columns to select.
    pub fn columns(mut self, cols: &[&dyn Column]) -> Self {
        self.columns = cols.iter().map(|c| ColumnRef::from_column(*c)).collect();
        self
    }

    /// Specify the table to select from.
    pub fn from<T: Table>(mut self, _table: T) -> Self {
        self.table = Some(T::qualified_name());
        self
    }

    /// Add a WHERE clause to the query.
    pub fn where_(mut self, clause: WhereClause) -> Self {
        self.where_clause = Some(clause);
        self
    }

    /// Add a LIMIT to the query.
    pub fn limit(mut self, n: u64) -> Self {
        self.limit = Some(n);
        self
    }

    /// Build the SQL query string and parameters.
    fn build_sql(&self) -> Result<(String, Vec<SqlValue>)> {
        if self.columns.is_empty() {
            return Err(PgRsError::NoColumnsSpecified);
        }

        let table = self.table.as_ref().ok_or(PgRsError::NoTableSpecified)?;

        let mut sql = String::with_capacity(256);
        let mut params = Vec::new();

        // SELECT clause
        sql.push_str("SELECT ");
        for (i, col) in self.columns.iter().enumerate() {
            if i > 0 {
                sql.push_str(", ");
            }
            sql.push_str(&col.qualified_name());
        }

        // FROM clause
        sql.push_str(" FROM ");
        sql.push_str(table);

        // WHERE clause
        if let Some(ref where_clause) = self.where_clause {
            sql.push_str(" WHERE ");
            let where_sql = where_clause.build_sql(0, &mut params);
            sql.push_str(&where_sql);
        }

        // LIMIT clause
        if let Some(limit) = self.limit {
            sql.push_str(" LIMIT ");
            sql.push_str(&limit.to_string());
        }

        Ok((sql, params))
    }

    /// Execute the query and return the result.
    pub async fn execute(self) -> Result<QueryResult> {
        let (sql, params) = self.build_sql()?;
        let raw_result = self.driver.execute(&sql, &params).await?;
        Ok(QueryResult::from_raw(raw_result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::RawQueryResult;
    use async_trait::async_trait;

    // Mock driver for testing
    struct MockDriver {
        result: RawQueryResult,
    }

    #[async_trait]
    impl DatabaseDriver for MockDriver {
        async fn execute(&self, _sql: &str, _params: &[SqlValue]) -> Result<RawQueryResult> {
            Ok(self.result.clone())
        }
    }

    // Test table and columns
    struct Users;
    struct UsersColumns {
        pub id: UsersId,
        pub name: UsersName,
    }
    struct UsersId;
    struct UsersName;

    impl Table for Users {
        type Columns = UsersColumns;
        fn table_name() -> &'static str {
            "users"
        }
        fn columns() -> Self::Columns {
            UsersColumns {
                id: UsersId,
                name: UsersName,
            }
        }
    }

    impl Column for UsersId {
        fn column_name(&self) -> &'static str {
            "id"
        }
        fn table_name(&self) -> &'static str {
            "users"
        }
    }

    impl Column for UsersName {
        fn column_name(&self) -> &'static str {
            "name"
        }
        fn table_name(&self) -> &'static str {
            "users"
        }
    }

    #[test]
    fn test_build_simple_select() {
        let driver = Arc::new(MockDriver {
            result: RawQueryResult::empty(),
        });

        let builder = SelectBuilder::new(driver)
            .columns(&[&Users::columns().id, &Users::columns().name])
            .from(Users);

        let (sql, params) = builder.build_sql().unwrap();
        assert_eq!(sql, "SELECT users.id, users.name FROM users");
        assert!(params.is_empty());
    }

    #[test]
    fn test_build_select_with_where() {
        let driver = Arc::new(MockDriver {
            result: RawQueryResult::empty(),
        });

        let builder = SelectBuilder::new(driver)
            .columns(&[&Users::columns().id])
            .from(Users)
            .where_(WhereClause::eq(&Users::columns().name, "John"));

        let (sql, params) = builder.build_sql().unwrap();
        assert_eq!(sql, "SELECT users.id FROM users WHERE users.name = $1");
        assert_eq!(params.len(), 1);
        assert_eq!(params[0], SqlValue::Text("John".to_string()));
    }

    #[test]
    fn test_build_select_with_limit() {
        let driver = Arc::new(MockDriver {
            result: RawQueryResult::empty(),
        });

        let builder = SelectBuilder::new(driver)
            .columns(&[&Users::columns().id])
            .from(Users)
            .limit(10);

        let (sql, params) = builder.build_sql().unwrap();
        assert_eq!(sql, "SELECT users.id FROM users LIMIT 10");
        assert!(params.is_empty());
    }

    #[test]
    fn test_build_select_with_where_and_limit() {
        let driver = Arc::new(MockDriver {
            result: RawQueryResult::empty(),
        });

        let builder = SelectBuilder::new(driver)
            .columns(&[&Users::columns().id])
            .from(Users)
            .where_(WhereClause::eq(&Users::columns().name, "John"))
            .limit(10);

        let (sql, params) = builder.build_sql().unwrap();
        assert_eq!(
            sql,
            "SELECT users.id FROM users WHERE users.name = $1 LIMIT 10"
        );
        assert_eq!(params.len(), 1);
    }

    #[test]
    fn test_build_fails_without_columns() {
        let driver = Arc::new(MockDriver {
            result: RawQueryResult::empty(),
        });

        let builder = SelectBuilder::new(driver).from(Users);

        let err = builder.build_sql().unwrap_err();
        assert!(matches!(err, PgRsError::NoColumnsSpecified));
    }

    #[test]
    fn test_build_fails_without_table() {
        let driver = Arc::new(MockDriver {
            result: RawQueryResult::empty(),
        });

        let builder = SelectBuilder::new(driver).columns(&[&Users::columns().id]);

        let err = builder.build_sql().unwrap_err();
        assert!(matches!(err, PgRsError::NoTableSpecified));
    }
}
