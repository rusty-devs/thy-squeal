use crate::sql::Executor;
use crate::storage::{Database, Value};
use std::sync::Arc;

#[tokio::test]
async fn test_parameterized_select() {
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
    executor
        .execute("INSERT INTO users VALUES (2, 'Bob')", vec![], None)
        .await
        .unwrap();

    // Test with ? placeholder
    let result = executor
        .execute(
            "SELECT * FROM users WHERE id = ?",
            vec![Value::Int(2)],
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][1], Value::Text("Bob".to_string()));

    // Test with $1 placeholder
    let result = executor
        .execute(
            "SELECT * FROM users WHERE name = $1",
            vec![Value::Text("Alice".to_string())],
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int(1));
}

#[tokio::test]
async fn test_parameterized_insert() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor
        .execute("CREATE TABLE users (id INT, name TEXT)", vec![], None)
        .await
        .unwrap();

    // Test parameterized INSERT
    executor
        .execute(
            "INSERT INTO users VALUES (?, $2)",
            vec![Value::Int(1), Value::Text("Alice".to_string())],
            None,
        )
        .await
        .unwrap();

    let result = executor
        .execute("SELECT * FROM users", vec![], None)
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int(1));
    assert_eq!(result.rows[0][1], Value::Text("Alice".to_string()));
}

#[tokio::test]
async fn test_parameterized_insert_with_columns() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor
        .execute(
            "CREATE TABLE users (id INT, name TEXT, email TEXT)",
            vec![],
            None,
        )
        .await
        .unwrap();

    // Test parameterized INSERT with specific columns
    executor
        .execute(
            "INSERT INTO users (name, id) VALUES (?, ?)",
            vec![Value::Text("Bob".to_string()), Value::Int(2)],
            None,
        )
        .await
        .unwrap();

    let result = executor
        .execute("SELECT id, name, email FROM users", vec![], None)
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int(2));
    assert_eq!(result.rows[0][1], Value::Text("Bob".to_string()));
    assert_eq!(result.rows[0][2], Value::Null);
}

#[tokio::test]
async fn test_prepare_execute() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor
        .execute("CREATE TABLE users (id INT, name TEXT)", vec![], None)
        .await
        .unwrap();

    // PREPARE
    executor
        .execute(
            "PREPARE inst FROM 'INSERT INTO users VALUES (?, ?)'",
            vec![],
            None,
        )
        .await
        .unwrap();

    // EXECUTE
    executor
        .execute("EXECUTE inst USING 1, 'Alice'", vec![], None)
        .await
        .unwrap();
    executor
        .execute("EXECUTE inst USING 2, 'Bob'", vec![], None)
        .await
        .unwrap();

    let result = executor
        .execute("SELECT COUNT(*) FROM users", vec![], None)
        .await
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Int(2));

    // EXECUTE with params passed directly (protocol-level)
    executor
        .execute(
            "PREPARE sel FROM 'SELECT name FROM users WHERE id = ?'",
            vec![],
            None,
        )
        .await
        .unwrap();

    let result = executor
        .execute("EXECUTE sel", vec![Value::Int(2)], None)
        .await
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Text("Bob".to_string()));

    // DEALLOCATE
    executor
        .execute("DEALLOCATE PREPARE inst", vec![], None)
        .await
        .unwrap();
}

#[tokio::test]
async fn test_parameterized_update_delete() {
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

    // UPDATE
    executor
        .execute(
            "UPDATE users SET name = ? WHERE id = $2",
            vec![Value::Text("Bob".to_string()), Value::Int(1)],
            None,
        )
        .await
        .unwrap();

    let result = executor
        .execute("SELECT name FROM users WHERE id = 1", vec![], None)
        .await
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Text("Bob".to_string()));

    // DELETE
    executor
        .execute("DELETE FROM users WHERE id = ?", vec![Value::Int(1)], None)
        .await
        .unwrap();

    let result = executor
        .execute("SELECT COUNT(*) FROM users", vec![], None)
        .await
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Int(0));
}
