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
            None,
        )
        .await
        .unwrap();

    // Insert without ID
    executor
        .execute(
            "INSERT INTO users (name) VALUES ('Alice')",
            vec![],
            None,
            None,
        )
        .await
        .unwrap();
    executor
        .execute(
            "INSERT INTO users (name) VALUES ('Bob')",
            vec![],
            None,
            None,
        )
        .await
        .unwrap();

    let result = executor
        .execute("SELECT * FROM users ORDER BY id", vec![], None, None)
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], Value::Int(1));
    assert_eq!(result.rows[0][1], Value::Text("Alice".to_string()));
    assert_eq!(result.rows[1][0], Value::Int(2));
    assert_eq!(result.rows[1][1], Value::Text("Bob".to_string()));

    // Insert with explicit NULL
    executor
        .execute(
            "INSERT INTO users VALUES (NULL, 'Charlie')",
            vec![],
            None,
            None,
        )
        .await
        .unwrap();

    let result = executor
        .execute(
            "SELECT * FROM users WHERE name = 'Charlie'",
            vec![],
            None,
            None,
        )
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
        .execute(
            "CREATE TABLE tasks (id SERIAL, task TEXT)",
            vec![],
            None,
            None,
        )
        .await
        .unwrap();

    executor
        .execute(
            "INSERT INTO tasks (task) VALUES ('Task 1')",
            vec![],
            None,
            None,
        )
        .await
        .unwrap();

    let result = executor
        .execute("SELECT id FROM tasks", vec![], None, None)
        .await
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Int(1));
}

#[tokio::test]
async fn test_alter_table() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor
        .execute("CREATE TABLE users (id INT, name TEXT)", vec![], None, None)
        .await
        .unwrap();
    executor
        .execute("INSERT INTO users VALUES (1, 'Alice')", vec![], None, None)
        .await
        .unwrap();

    // 1. ADD COLUMN
    executor
        .execute("ALTER TABLE users ADD COLUMN age INT", vec![], None, None)
        .await
        .unwrap();

    let result = executor
        .execute("SELECT age FROM users", vec![], None, None)
        .await
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Null); // Existing rows get NULL

    executor
        .execute("UPDATE users SET age = 30 WHERE id = 1", vec![], None, None)
        .await
        .unwrap();
    let result = executor
        .execute("SELECT age FROM users", vec![], None, None)
        .await
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Int(30));

    // 2. RENAME COLUMN
    executor
        .execute(
            "ALTER TABLE users RENAME COLUMN name TO full_name",
            vec![],
            None,
            None,
        )
        .await
        .unwrap();
    let result = executor
        .execute("SELECT full_name FROM users", vec![], None, None)
        .await
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Text("Alice".to_string()));

    // 3. DROP COLUMN
    executor
        .execute("ALTER TABLE users DROP COLUMN age", vec![], None, None)
        .await
        .unwrap();
    let result = executor
        .execute("SELECT * FROM users", vec![], None, None)
        .await
        .unwrap();
    assert_eq!(result.columns.len(), 2); // id, full_name
    assert_eq!(result.rows[0].len(), 2);

    // 4. RENAME TABLE
    executor
        .execute("ALTER TABLE users RENAME TO members", vec![], None, None)
        .await
        .unwrap();
    let result = executor
        .execute("SELECT * FROM members", vec![], None, None)
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 1);

    // Original table should be gone
    let err = executor
        .execute("SELECT * FROM users", vec![], None, None)
        .await
        .unwrap_err();
    assert!(matches!(err, crate::sql::error::SqlError::TableNotFound(_)));
}

#[tokio::test]
async fn test_sql_functions() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    // CONCAT
    let result = executor
        .execute("SELECT CONCAT('Hello', ' ', 'World')", vec![], None, None)
        .await
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Text("Hello World".to_string()));

    // COALESCE
    let result = executor
        .execute(
            "SELECT COALESCE(NULL, NULL, 'Found', 'Not this')",
            vec![],
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Text("Found".to_string()));

    // REPLACE
    let result = executor
        .execute("SELECT REPLACE('banana', 'a', 'o')", vec![], None, None)
        .await
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Text("bonono".to_string()));

    // NOW (just check it returns a DateTime)
    let result = executor
        .execute("SELECT NOW()", vec![], None, None)
        .await
        .unwrap();
    assert!(matches!(result.rows[0][0], Value::DateTime(_)));
}

#[tokio::test]
async fn test_constraints() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor
        .execute(
            "CREATE TABLE groups (id INT PRIMARY KEY (id), name TEXT)",
            vec![],
            None,
            None,
        )
        .await
        .unwrap();

    executor
        .execute("INSERT INTO groups VALUES (1, 'Admin')", vec![], None, None)
        .await
        .unwrap();

    // 1. PRIMARY KEY uniqueness
    let err = executor
        .execute(
            "INSERT INTO groups VALUES (1, 'Duplicate')",
            vec![],
            None,
            None,
        )
        .await
        .unwrap_err();
    assert!(err.to_string().contains("Duplicate key"));

    executor
        .execute(
            "CREATE TABLE users (id INT, group_id INT, FOREIGN KEY (group_id) REFERENCES groups(id))",
            vec![],
            None,
            None,
        )
        .await
        .unwrap();

    // 2. FOREIGN KEY existence
    executor
        .execute("INSERT INTO users VALUES (101, 1)", vec![], None, None)
        .await
        .unwrap(); // Works

    let err = executor
        .execute("INSERT INTO users VALUES (102, 999)", vec![], None, None)
        .await
        .unwrap_err(); // Fails
    assert!(err.to_string().contains("Foreign key constraint violation"));
}

#[tokio::test]
async fn test_ctes() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    let sql = "WITH t AS (SELECT 1 AS val) SELECT * FROM t";
    let result = executor.execute(sql, vec![], None, None).await.unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int(1));
    assert_eq!(result.columns[0], "val");

    // Multiple CTEs and JOIN
    executor
        .execute("CREATE TABLE users (id INT, name TEXT)", vec![], None, None)
        .await
        .unwrap();
    executor
        .execute("INSERT INTO users VALUES (1, 'Alice')", vec![], None, None)
        .await
        .unwrap();

    let sql = "WITH a AS (SELECT * FROM users), b AS (SELECT 2 AS id) SELECT a.name FROM a JOIN b ON a.id = b.id - 1";
    let result = executor.execute(sql, vec![], None, None).await.unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Text("Alice".to_string()));
}
