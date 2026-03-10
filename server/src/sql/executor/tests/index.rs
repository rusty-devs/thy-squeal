use crate::sql::Executor;
use crate::storage::{Database, Value};
use std::sync::Arc;

#[tokio::test]
async fn test_hash_index() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor
        .execute("CREATE TABLE users (id INT, name TEXT)", vec![], None, None)
        .await
        .unwrap();
    executor
        .execute(
            "CREATE UNIQUE INDEX idx_name ON users (name) USING HASH",
            vec![],
            None,
            None,
        )
        .await
        .unwrap();

    executor
        .execute("INSERT INTO users VALUES (1, 'Alice')", vec![], None, None)
        .await
        .unwrap();
    executor
        .execute("INSERT INTO users VALUES (2, 'Bob')", vec![], None, None)
        .await
        .unwrap();

    let result = executor
        .execute("SELECT * FROM users WHERE name = 'Bob'", vec![], None, None)
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int(2));

    // Test unique constraint
    let result = executor
        .execute("INSERT INTO users VALUES (3, 'Alice')", vec![], None, None)
        .await;
    assert!(result.is_err());
}

#[tokio::test]
async fn test_composite_btree_index() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor
        .execute(
            "CREATE TABLE users (first_name TEXT, last_name TEXT)",
            vec![],
            None,
            None,
        )
        .await
        .unwrap();
    executor
        .execute(
            "CREATE INDEX idx_name ON users (last_name, first_name)",
            vec![],
            None,
            None,
        )
        .await
        .unwrap();

    executor
        .execute("INSERT INTO users VALUES ('Alice', 'Smith')", vec![], None, None)
        .await
        .unwrap();
    executor
        .execute("INSERT INTO users VALUES ('Bob', 'Smith')", vec![], None, None)
        .await
        .unwrap();
    executor
        .execute("INSERT INTO users VALUES ('Charlie', 'Brown')", vec![], None, None)
        .await
        .unwrap();

    let result = executor
        .execute(
            "SELECT * FROM users WHERE last_name = 'Smith'",
            vec![],
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[tokio::test]
async fn test_json_path_index() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor
        .execute("CREATE TABLE events (data JSON)", vec![], None, None)
        .await
        .unwrap();
    executor
        .execute(
            "CREATE INDEX idx_user_id ON events (data.user.id)",
            vec![],
            None,
            None,
        )
        .await
        .unwrap();

    executor
        .execute(
            "INSERT INTO events VALUES ('{\"user\": {\"id\": 123}, \"type\": \"login\"}')",
            vec![],
            None,
            None,
        )
        .await
        .unwrap();
    executor
        .execute(
            "INSERT INTO events VALUES ('{\"user\": {\"id\": 456}, \"type\": \"logout\"}')",
            vec![],
            None,
            None,
        )
        .await
        .unwrap();

    let result = executor
        .execute(
            "SELECT * FROM events WHERE data.user.id = 123",
            vec![],
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[tokio::test]
async fn test_functional_index() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor
        .execute("CREATE TABLE users (email TEXT)", vec![], None, None)
        .await
        .unwrap();
    executor
        .execute(
            "CREATE INDEX idx_lower_email ON users (LOWER(email))",
            vec![],
            None,
            None,
        )
        .await
        .unwrap();

    executor
        .execute(
            "INSERT INTO users VALUES ('Alice@Example.Com')",
            vec![],
            None,
            None,
        )
        .await
        .unwrap();

    let result = executor
        .execute(
            "SELECT * FROM users WHERE LOWER(email) = 'alice@example.com'",
            vec![],
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[tokio::test]
async fn test_partial_index() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor
        .execute("CREATE TABLE orders (id INT, status TEXT)", vec![], None, None)
        .await
        .unwrap();
    executor
        .execute(
            "CREATE UNIQUE INDEX idx_active_orders ON orders (id) WHERE status = 'pending'",
            vec![],
            None,
            None,
        )
        .await
        .unwrap();

    executor
        .execute(
            "INSERT INTO orders VALUES (1, 'shipped')",
            vec![],
            None,
            None,
        )
        .await
        .unwrap(); // OK, status not pending
    executor
        .execute(
            "INSERT INTO orders VALUES (1, 'shipped')",
            vec![],
            None,
            None,
        )
        .await
        .unwrap(); // OK, status not pending

    let res = executor
        .execute(
            "INSERT INTO orders VALUES (1, 'pending')",
            vec![],
            None,
            None,
        )
        .await;
    assert!(res.is_ok());

    let res = executor
        .execute(
            "INSERT INTO orders VALUES (1, 'pending')",
            vec![],
            None,
            None,
        )
        .await;
    assert!(res.is_err());
}

#[tokio::test]
async fn test_index_selectivity() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor
        .execute("CREATE TABLE test (val INT)", vec![], None, None)
        .await
        .unwrap();
    executor
        .execute("CREATE INDEX idx_val ON test (val)", vec![], None, None)
        .await
        .unwrap();

    // Insert 100 rows: 50 rows with val=1, 50 rows with val=2..51
    for i in 0..50 {
        executor
            .execute("INSERT INTO test VALUES (1)", vec![], None, None)
            .await
            .unwrap();
        executor
            .execute(
                &format!("INSERT INTO test VALUES ({})", i + 2),
                vec![],
                None,
                None,
            )
            .await
            .unwrap();
    }

    // Check statistics via info_schema
    let stats = executor
        .execute(
            "SELECT cardinality, total_rows FROM information_schema.statistics WHERE table_name = 'test'",
            vec![],
            None,
            None,
        )
        .await
        .unwrap();
    println!("DEBUG STATS: {:?}", stats.rows);

    // val=1 is NOT selective (50% of rows). Should use Full Table Scan.
    let res = executor
        .execute(
            "EXPLAIN SELECT * FROM test WHERE val = 1",
            vec![],
            None,
            None,
        )
        .await
        .unwrap();
    println!("EXPLAIN non-selective: {:?}", res.rows);
    assert!(res.rows[0][1]
        .as_text()
        .unwrap()
        .contains("Full Table Scan"));

    // val=10 is highly selective (1% of rows). Should use Index.
    let res = executor
        .execute(
            "EXPLAIN SELECT * FROM test WHERE val = 10",
            vec![],
            None,
            None,
        )
        .await
        .unwrap();
    println!("EXPLAIN selective: {:?}", res.rows);
    assert!(res.rows[0][1]
        .as_text()
        .unwrap()
        .contains("Index Lookup (BTree)"));
}
