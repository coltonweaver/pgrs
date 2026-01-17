//! pgrs - A type-safe, driver-agnostic PostgreSQL query builder
//!
//! # Example
//! ```ignore
//! use pgrs::{PgRsClient, WhereClause, Table, Column};
//!
//! // Connect to database
//! let client = PgRsClient::connect("postgres://localhost/mydb").await?;
//! let querier = client.querier();
//!
//! // Execute a SELECT query
//! let row = querier
//!     .select()
//!     .columns(&[&Users::columns().id, &Users::columns().name])
//!     .from(Users)
//!     .r#where(WhereClause::eq(&Users::columns().name, "John"))
//!     .execute()
//!     .await?
//!     .single_row()?;
//!
//! let id = row.get("id")?;
//! let name = row.get("name")?;
//! ```

pub mod builders;
pub mod clauses;
pub mod drivers;
pub mod error;
pub mod querier;
pub mod traits;
pub mod types;

mod client;

// Re-export main types for convenient access
pub use clauses::WhereClause;
pub use client::PgRsClient;
pub use error::{PgRsError, Result};
pub use querier::Querier;
pub use traits::{Column, ColumnRef, DatabaseDriver, Table};
pub use types::{QueryResult, RawQueryResult, Row, SqlValue};
