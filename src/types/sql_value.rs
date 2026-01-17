/// Represents a SQL parameter value in a driver-agnostic way.
/// Drivers are responsible for converting these to their native types.
#[derive(Debug, Clone, PartialEq)]
pub enum SqlValue {
    Null,
    Text(String),
    Int32(i32),
    Int64(i64),
    Bool(bool),
}

impl From<&str> for SqlValue {
    fn from(value: &str) -> Self {
        SqlValue::Text(value.to_string())
    }
}

impl From<String> for SqlValue {
    fn from(value: String) -> Self {
        SqlValue::Text(value)
    }
}

impl From<i32> for SqlValue {
    fn from(value: i32) -> Self {
        SqlValue::Int32(value)
    }
}

impl From<i64> for SqlValue {
    fn from(value: i64) -> Self {
        SqlValue::Int64(value)
    }
}

impl From<bool> for SqlValue {
    fn from(value: bool) -> Self {
        SqlValue::Bool(value)
    }
}

impl<T: Into<SqlValue>> From<Option<T>> for SqlValue {
    fn from(value: Option<T>) -> Self {
        match value {
            Some(v) => v.into(),
            None => SqlValue::Null,
        }
    }
}
