use crate::traits::{Column, ColumnRef};
use crate::types::SqlValue;

/// Represents a WHERE clause condition.
/// Supports basic comparison operations and logical combinations.
#[derive(Debug, Clone)]
pub enum WhereClause {
    /// column = value
    Eq(ColumnRef, SqlValue),
    /// clause AND clause
    And(Box<WhereClause>, Box<WhereClause>),
    /// clause OR clause
    Or(Box<WhereClause>, Box<WhereClause>),
}

impl WhereClause {
    /// Creates an equality condition: column = value
    pub fn eq<C: Column, V: Into<SqlValue>>(column: &C, value: V) -> Self {
        WhereClause::Eq(ColumnRef::from_column(column), value.into())
    }

    /// Combines this clause with another using AND
    pub fn and(self, other: WhereClause) -> Self {
        WhereClause::And(Box::new(self), Box::new(other))
    }

    /// Combines this clause with another using OR
    pub fn or(self, other: WhereClause) -> Self {
        WhereClause::Or(Box::new(self), Box::new(other))
    }

    /// Builds the SQL string and collects parameters.
    /// Returns the SQL fragment and updates the params vector.
    /// `param_offset` is the starting parameter number (1-indexed for PostgreSQL).
    pub fn build_sql(&self, param_offset: usize, params: &mut Vec<SqlValue>) -> String {
        match self {
            WhereClause::Eq(col, value) => {
                params.push(value.clone());
                format!(
                    "{} = ${}",
                    col.qualified_name(),
                    param_offset + params.len()
                )
            }
            WhereClause::And(left, right) => {
                let left_sql = left.build_sql(param_offset, params);
                let right_sql = right.build_sql(param_offset, params);
                format!("({}) AND ({})", left_sql, right_sql)
            }
            WhereClause::Or(left, right) => {
                let left_sql = left.build_sql(param_offset, params);
                let right_sql = right.build_sql(param_offset, params);
                format!("({}) OR ({})", left_sql, right_sql)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test column implementation
    struct TestColumn {
        table: &'static str,
        column: &'static str,
    }

    impl Column for TestColumn {
        fn column_name(&self) -> &'static str {
            self.column
        }
        fn table_name(&self) -> &'static str {
            self.table
        }
    }

    #[test]
    fn test_eq_clause() {
        let col = TestColumn {
            table: "users",
            column: "name",
        };
        let clause = WhereClause::eq(&col, "John");
        let mut params = Vec::new();
        let sql = clause.build_sql(0, &mut params);

        assert_eq!(sql, "users.name = $1");
        assert_eq!(params.len(), 1);
        assert_eq!(params[0], SqlValue::Text("John".to_string()));
    }

    #[test]
    fn test_and_clause() {
        let name_col = TestColumn {
            table: "users",
            column: "name",
        };
        let age_col = TestColumn {
            table: "users",
            column: "age",
        };

        let clause = WhereClause::eq(&name_col, "John").and(WhereClause::eq(&age_col, 30));

        let mut params = Vec::new();
        let sql = clause.build_sql(0, &mut params);

        assert_eq!(sql, "(users.name = $1) AND (users.age = $2)");
        assert_eq!(params.len(), 2);
    }
}
