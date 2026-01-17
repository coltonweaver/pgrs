use async_trait::async_trait;
use tokio_postgres::{types::ToSql, Client, NoTls};

use crate::error::{PgRsError, Result};
use crate::traits::DatabaseDriver;
use crate::types::{RawQueryResult, SqlValue};

/// PostgreSQL driver implementation using tokio-postgres.
pub struct TokioPostgresDriver {
    client: Client,
}

impl TokioPostgresDriver {
    /// Connect to a PostgreSQL database.
    pub async fn connect(connection_string: &str) -> Result<Self> {
        let (client, connection) = tokio_postgres::connect(connection_string, NoTls)
            .await
            .map_err(|e| PgRsError::ConnectionFailed(e.to_string()))?;

        // Spawn the connection handler
        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("PostgreSQL connection error: {}", e);
            }
        });

        Ok(Self { client })
    }
}

#[async_trait]
impl DatabaseDriver for TokioPostgresDriver {
    async fn execute(&self, sql: &str, params: &[SqlValue]) -> Result<RawQueryResult> {
        // Convert SqlValue params to tokio-postgres compatible types
        let converted_params: Vec<Box<dyn ToSql + Sync + Send>> =
            params.iter().map(|v| sql_value_to_tosql(v)).collect();

        let param_refs: Vec<&(dyn ToSql + Sync)> = converted_params
            .iter()
            .map(|b| b.as_ref() as &(dyn ToSql + Sync))
            .collect();

        let rows = self
            .client
            .query(sql, &param_refs)
            .await
            .map_err(|e| PgRsError::QueryFailed(e.to_string()))?;

        // Extract column names
        let columns: Vec<String> = if rows.is_empty() {
            Vec::new()
        } else {
            rows[0]
                .columns()
                .iter()
                .map(|c| c.name().to_string())
                .collect()
        };

        // Convert rows to string values
        let result_rows: Vec<Vec<String>> = rows
            .iter()
            .map(|row| {
                row.columns()
                    .iter()
                    .enumerate()
                    .map(|(i, col)| row_value_to_string(row, i, col.type_()))
                    .collect()
            })
            .collect();

        Ok(RawQueryResult::new(columns, result_rows))
    }
}

/// Convert a SqlValue to a boxed ToSql trait object.
fn sql_value_to_tosql(value: &SqlValue) -> Box<dyn ToSql + Sync + Send> {
    match value {
        SqlValue::Null => Box::new(None::<String>),
        SqlValue::Text(s) => Box::new(s.clone()),
        SqlValue::Int32(i) => Box::new(*i),
        SqlValue::Int64(i) => Box::new(*i),
        SqlValue::Bool(b) => Box::new(*b),
    }
}

/// Convert a row value at a given index to a string.
fn row_value_to_string(
    row: &tokio_postgres::Row,
    index: usize,
    _type_: &tokio_postgres::types::Type,
) -> String {
    // Try common types and convert to string
    // This is a simplified implementation - a production version would handle more types

    // Try as i32
    if let Ok(val) = row.try_get::<_, i32>(index) {
        return val.to_string();
    }

    // Try as i64
    if let Ok(val) = row.try_get::<_, i64>(index) {
        return val.to_string();
    }

    // Try as String
    if let Ok(val) = row.try_get::<_, String>(index) {
        return val;
    }

    // Try as bool
    if let Ok(val) = row.try_get::<_, bool>(index) {
        return val.to_string();
    }

    // Try as f64
    if let Ok(val) = row.try_get::<_, f64>(index) {
        return val.to_string();
    }

    // Try as Option<String> for NULL handling
    if let Ok(val) = row.try_get::<_, Option<String>>(index) {
        return val.unwrap_or_else(|| "NULL".to_string());
    }

    // Fallback
    "UNKNOWN".to_string()
}
