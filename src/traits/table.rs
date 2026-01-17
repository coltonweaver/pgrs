/// Trait representing a database table.
/// Implementations are typically generated from schema definitions.
pub trait Table {
    /// The type containing all column accessors for this table.
    type Columns;

    /// Returns the table name as it appears in the database.
    fn table_name() -> &'static str;

    /// Returns the schema name, if any.
    fn schema() -> Option<&'static str> {
        None
    }

    /// Returns the fully qualified table name (schema.table or just table).
    fn qualified_name() -> String {
        match Self::schema() {
            Some(schema) => format!("{}.{}", schema, Self::table_name()),
            None => Self::table_name().to_string(),
        }
    }

    /// Returns an instance of the columns accessor for this table.
    fn columns() -> Self::Columns;
}
