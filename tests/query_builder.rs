use std::sync::Arc;

use pgrs::drivers::{InMemoryTestDriver, InMemoryTestResponseBuilder};
use pgrs::error::PgRsError;
use pgrs::traits::{Column, DatabaseDriver, Table};
use pgrs::types::{QueryResult, SqlValue};
use pgrs::{PgRsClient, WhereClause};

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

#[tokio::test]
async fn test_simple_select_single_column() {
    let in_memory_test_driver = Arc::new(
        InMemoryTestDriver::new().with_response(
            InMemoryTestResponseBuilder::new()
                .columns(&["id"])
                .row(&["1"])
                .build(),
        ),
    );
    let driver: Arc<dyn DatabaseDriver> =
        Arc::clone(&in_memory_test_driver) as Arc<dyn DatabaseDriver>;
    let client = PgRsClient::with_driver(driver);
    let querier = client.querier();

    let result = querier
        .select()
        .columns(&[&Users::columns().id])
        .from(Users)
        .execute()
        .await
        .unwrap();

    // Verify the query that was executed
    in_memory_test_driver.assert_last_query("SELECT users.id FROM users", &[]);
    in_memory_test_driver.assert_query_count(1);

    // Verify the result
    let row = result.single_row().unwrap();
    assert_eq!(row.get(&Users::columns().id).unwrap(), "1");
}

#[tokio::test]
async fn test_select_with_where() {
    let in_memory_test_driver = Arc::new(
        InMemoryTestDriver::new().with_response(
            InMemoryTestResponseBuilder::new()
                .columns(&["id"])
                .row(&["42"])
                .build(),
        ),
    );
    let driver: Arc<dyn DatabaseDriver> =
        Arc::clone(&in_memory_test_driver) as Arc<dyn DatabaseDriver>;
    let client = PgRsClient::with_driver(driver);
    let querier = client.querier();

    let result = querier
        .select()
        .columns(&[&Users::columns().id])
        .from(Users)
        .where_(WhereClause::eq(&Users::columns().name, "Bob"))
        .execute()
        .await
        .unwrap();

    // Verify the query
    in_memory_test_driver.assert_last_query(
        "SELECT users.id FROM users WHERE users.name = $1",
        &[SqlValue::Text("Bob".to_string())],
    );

    // Verify the result
    let row = result.single_row().unwrap();
    assert_eq!(row.get(&Users::columns().id).unwrap(), "42");
}

#[tokio::test]
async fn test_select_with_where_and_limit() {
    let in_memory_test_driver = Arc::new(
        InMemoryTestDriver::new().with_response(
            InMemoryTestResponseBuilder::new()
                .columns(&["id"])
                .row(&["1"])
                .row(&["2"])
                .row(&["3"])
                .build(),
        ),
    );
    let driver: Arc<dyn DatabaseDriver> =
        Arc::clone(&in_memory_test_driver) as Arc<dyn DatabaseDriver>;
    let client = PgRsClient::with_driver(driver);
    let querier = client.querier();

    let result = querier
        .select()
        .columns(&[&Users::columns().id])
        .from(Users)
        .where_(WhereClause::eq(&Users::columns().name, "Test"))
        .limit(3)
        .execute()
        .await
        .unwrap();

    // Verify the query
    in_memory_test_driver.assert_last_query(
        "SELECT users.id FROM users WHERE users.name = $1 LIMIT 3",
        &[SqlValue::Text("Test".to_string())],
    );

    // Verify we got multiple rows
    let rows = result.rows();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get(&Users::columns().id).unwrap(), "1");
    assert_eq!(rows[1].get(&Users::columns().id).unwrap(), "2");
    assert_eq!(rows[2].get(&Users::columns().id).unwrap(), "3");
}

#[tokio::test]
async fn test_select_empty_result() {
    let in_memory_test_driver = Arc::new(InMemoryTestDriver::new().with_response(
        InMemoryTestResponseBuilder::new().columns(&["id"]).build(), // No rows
    ));
    let driver: Arc<dyn DatabaseDriver> =
        Arc::clone(&in_memory_test_driver) as Arc<dyn DatabaseDriver>;
    let client = PgRsClient::with_driver(driver);
    let querier = client.querier();

    let result = querier
        .select()
        .columns(&[&Users::columns().id])
        .from(Users)
        .where_(WhereClause::eq(&Users::columns().id, 999))
        .execute()
        .await
        .unwrap();

    assert!(result.is_empty());
    assert_eq!(result.len(), 0);

    // single_row should fail on empty result
    let err = QueryResult::from_raw(InMemoryTestResponseBuilder::new().columns(&["id"]).build())
        .single_row()
        .unwrap_err();

    match err {
        PgRsError::UnexpectedRowCount { expected, actual } => {
            assert_eq!(expected, 1);
            assert_eq!(actual, 0);
        }
        _ => panic!("Expected UnexpectedRowCount error"),
    }
}

#[tokio::test]
async fn test_multiple_queries() {
    let in_memory_test_driver = Arc::new(
        InMemoryTestDriver::new()
            .with_response(
                InMemoryTestResponseBuilder::new()
                    .columns(&["id"])
                    .row(&["1"])
                    .build(),
            )
            .with_response(
                InMemoryTestResponseBuilder::new()
                    .columns(&["name"])
                    .row(&["Alice"])
                    .build(),
            ),
    );
    let driver: Arc<dyn DatabaseDriver> =
        Arc::clone(&in_memory_test_driver) as Arc<dyn DatabaseDriver>;
    let client = PgRsClient::with_driver(driver);
    let querier = client.querier();

    // First query
    let result1 = querier
        .select()
        .columns(&[&Users::columns().id])
        .from(Users)
        .execute()
        .await
        .unwrap();

    // Second query
    let result2 = querier
        .select()
        .columns(&[&Users::columns().name])
        .from(Users)
        .execute()
        .await
        .unwrap();

    // Verify both queries were recorded
    in_memory_test_driver.assert_query_count(2);

    let queries = in_memory_test_driver.recorded_queries();
    assert_eq!(queries[0].sql, "SELECT users.id FROM users");
    assert_eq!(queries[1].sql, "SELECT users.name FROM users");

    // Verify results
    assert_eq!(
        result1
            .single_row()
            .unwrap()
            .get(&Users::columns().id)
            .unwrap(),
        "1"
    );
    assert_eq!(
        result2
            .single_row()
            .unwrap()
            .get(&Users::columns().name)
            .unwrap(),
        "Alice"
    );
}

#[tokio::test]
async fn test_compound_where_clause() {
    let in_memory_test_driver = Arc::new(
        InMemoryTestDriver::new().with_response(
            InMemoryTestResponseBuilder::new()
                .columns(&["name"])
                .row(&["Admin"])
                .build(),
        ),
    );
    let driver: Arc<dyn DatabaseDriver> =
        Arc::clone(&in_memory_test_driver) as Arc<dyn DatabaseDriver>;
    let client = PgRsClient::with_driver(driver);
    let querier = client.querier();

    let result = querier
        .select()
        .columns(&[&Users::columns().name])
        .from(Users)
        .where_(
            WhereClause::eq(&Users::columns().name, "Admin")
                .and(WhereClause::eq(&Users::columns().id, 1)),
        )
        .execute()
        .await
        .unwrap();

    // Verify compound WHERE clause
    in_memory_test_driver.assert_last_query(
        "SELECT users.name FROM users WHERE (users.name = $1) AND (users.id = $2)",
        &[SqlValue::Text("Admin".to_string()), SqlValue::Int32(1)],
    );

    let row = result.single_row().unwrap();
    assert_eq!(row.get(&Users::columns().name).unwrap(), "Admin");
}
