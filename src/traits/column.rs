/// Trait representing a database column.
/// Implementations are typically generated from schema definitions.
pub trait Column {
    /// Returns the column name as it appears in the database.
    fn column_name(&self) -> &'static str;

    /// Returns the table name this column belongs to.
    fn table_name(&self) -> &'static str;

    /// Returns the fully qualified column name (table.column).
    fn qualified_name(&self) -> String {
        format!("{}.{}", self.table_name(), self.column_name())
    }
}

/// A reference to a column, used internally by query builders.
/// This allows storing column information without requiring the original Column type.
#[derive(Debug, Clone, PartialEq)]
pub struct ColumnRef {
    pub table: String,
    pub column: String,
}

impl ColumnRef {
    pub fn new(table: impl Into<String>, column: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            column: column.into(),
        }
    }

    pub fn from_column<C: Column + ?Sized>(col: &C) -> Self {
        Self {
            table: col.table_name().to_string(),
            column: col.column_name().to_string(),
        }
    }

    pub fn qualified_name(&self) -> String {
        format!("{}.{}", self.table, self.column)
    }
}
