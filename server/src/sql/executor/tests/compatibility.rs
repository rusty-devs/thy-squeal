use crate::sql::Executor;
use crate::storage::{Database, Value};
use std::sync::Arc;

#[tokio::test]
async fn test_auto_increment() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    // Test with AUTO_INCREMENT keyword
    executor
        .execute(
            "CREATE TABLE users (id INT AUTO_INCREMENT, name TEXT)",
            vec![],
            None,
        )
        .await
        .unwrap();

    // Insert without ID
    executor
        .execute("INSERT INTO users (name) VALUES ('Alice')", vec![], None)
        .await
        .unwrap();
    executor
        .execute("INSERT INTO users (name) VALUES ('Bob')", vec![], None)
        .await
        .unwrap();

    let result = executor
        .execute("SELECT * FROM users ORDER BY id", vec![], None)
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], Value::Int(1));
    assert_eq!(result.rows[0][1], Value::Text("Alice".to_string()));
    assert_eq!(result.rows[1][0], Value::Int(2));
    assert_eq!(result.rows[1][1], Value::Text("Bob".to_string()));

    // Insert with explicit NULL
    executor
        .execute("INSERT INTO users VALUES (NULL, 'Charlie')", vec![], None)
        .await
        .unwrap();

    let result = executor
        .execute("SELECT * FROM users WHERE name = 'Charlie'", vec![], None)
        .await
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Int(3));
}

#[tokio::test]
async fn test_serial_shorthand() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    // Test with SERIAL shorthand
    executor
        .execute("CREATE TABLE tasks (id SERIAL, task TEXT)", vec![], None)
        .await
        .unwrap();

    executor
        .execute("INSERT INTO tasks (task) VALUES ('Task 1')", vec![], None)
        .await
        .unwrap();

    let result = executor
        .execute("SELECT id FROM tasks", vec![], None)
        .await
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Int(1));
}

#[tokio::test]
async fn test_alter_table() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor
        .execute("CREATE TABLE users (id INT, name TEXT)", vec![], None)
        .await
        .unwrap();
    executor
        .execute("INSERT INTO users VALUES (1, 'Alice')", vec![], None)
        .await
        .unwrap();

    // 1. ADD COLUMN
    executor
        .execute("ALTER TABLE users ADD COLUMN age INT", vec![], None)
        .await
        .unwrap();

    let result = executor
        .execute("SELECT age FROM users", vec![], None)
        .await
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Null); // Existing rows get NULL

    executor
        .execute("UPDATE users SET age = 30 WHERE id = 1", vec![], None)
        .await
        .unwrap();
    let result = executor
        .execute("SELECT age FROM users", vec![], None)
        .await
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Int(30));

    // 2. RENAME COLUMN
    executor
        .execute(
            "ALTER TABLE users RENAME COLUMN name TO full_name",
            vec![],
            None,
        )
        .await
        .unwrap();
    let result = executor
        .execute("SELECT full_name FROM users", vec![], None)
        .await
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Text("Alice".to_string()));

    // 3. DROP COLUMN
    executor
        .execute("ALTER TABLE users DROP COLUMN age", vec![], None)
        .await
        .unwrap();
    let result = executor
        .execute("SELECT * FROM users", vec![], None)
        .await
        .unwrap();
    assert_eq!(result.columns.len(), 2); // id, full_name
    assert_eq!(result.rows[0].len(), 2);

    // 4. RENAME TABLE
    executor
        .execute("ALTER TABLE users RENAME TO members", vec![], None)
        .await
        .unwrap();
    let result = executor
        .execute("SELECT * FROM members", vec![], None)
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 1);

    // Original table should be gone
    let err = executor
        .execute("SELECT * FROM users", vec![], None)
        .await
        .unwrap_err();
    assert!(matches!(err, crate::sql::error::SqlError::TableNotFound(_)));
}
