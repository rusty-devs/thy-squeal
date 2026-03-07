use crate::sql::Executor;
use crate::sql::error::SqlError;
use crate::storage::{Database, Value};
use std::sync::Arc;

#[tokio::test]
async fn test_create_table_insert_select() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor
        .execute("CREATE TABLE users (id INT, name TEXT)", None)
        .await
        .unwrap();
    executor
        .execute("INSERT INTO users VALUES (1, 'Alice')", None)
        .await
        .unwrap();
    let result = executor.execute("SELECT * FROM users", None).await.unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int(1));
    assert_eq!(result.rows[0][1], Value::Text("Alice".to_string()));
}

#[tokio::test]
async fn test_drop_table() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor
        .execute("CREATE TABLE users (id INT)", None)
        .await
        .unwrap();
    executor.execute("DROP TABLE users", None).await.unwrap();

    let result = executor.execute("SELECT * FROM users", None).await;
    assert!(matches!(result, Err(SqlError::TableNotFound(_))));
}

#[tokio::test]
async fn test_select_where() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor
        .execute("CREATE TABLE users (id INT, name TEXT)", None)
        .await
        .unwrap();
    executor
        .execute("INSERT INTO users VALUES (1, 'Alice')", None)
        .await
        .unwrap();
    executor
        .execute("INSERT INTO users VALUES (2, 'Bob')", None)
        .await
        .unwrap();

    let result = executor
        .execute("SELECT * FROM users WHERE id = 2", None)
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][1], Value::Text("Bob".to_string()));

    let result = executor
        .execute("SELECT * FROM users WHERE name = 'Alice'", None)
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Int(1));
}

#[tokio::test]
async fn test_update() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor
        .execute("CREATE TABLE users (id INT, name TEXT)", None)
        .await
        .unwrap();
    executor
        .execute("INSERT INTO users VALUES (1, 'Alice')", None)
        .await
        .unwrap();

    executor
        .execute("UPDATE users SET name = 'Bob' WHERE id = 1", None)
        .await
        .unwrap();

    let result = executor
        .execute("SELECT name FROM users WHERE id = 1", None)
        .await
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Text("Bob".to_string()));
}

#[tokio::test]
async fn test_delete() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor
        .execute("CREATE TABLE users (id INT, name TEXT)", None)
        .await
        .unwrap();
    executor
        .execute("INSERT INTO users VALUES (1, 'Alice')", None)
        .await
        .unwrap();

    executor
        .execute("DELETE FROM users WHERE id = 1", None)
        .await
        .unwrap();

    let result = executor.execute("SELECT * FROM users", None).await.unwrap();
    assert!(result.rows.is_empty());
}

#[tokio::test]
async fn test_order_by() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor
        .execute("CREATE TABLE t (v INT)", None)
        .await
        .unwrap();
    executor.execute("INSERT INTO t VALUES (3)", None).await.unwrap();
    executor.execute("INSERT INTO t VALUES (1)", None).await.unwrap();
    executor.execute("INSERT INTO t VALUES (2)", None).await.unwrap();

    let result = executor
        .execute("SELECT v FROM t ORDER BY v ASC", None)
        .await
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Int(1));
    assert_eq!(result.rows[1][0], Value::Int(2));
    assert_eq!(result.rows[2][0], Value::Int(3));

    let result = executor
        .execute("SELECT v FROM t ORDER BY v DESC", None)
        .await
        .unwrap();
    assert_eq!(result.rows[0][0], Value::Int(3));
    assert_eq!(result.rows[1][0], Value::Int(2));
    assert_eq!(result.rows[2][0], Value::Int(1));
}

#[tokio::test]
async fn test_limit_offset() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor
        .execute("CREATE TABLE t (v INT)", None)
        .await
        .unwrap();
    for i in 1..=10 {
        executor
            .execute(&format!("INSERT INTO t VALUES ({})", i), None)
            .await
            .unwrap();
    }

    let result = executor
        .execute("SELECT v FROM t ORDER BY v ASC LIMIT 3 OFFSET 2", None)
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 3);
    assert_eq!(result.rows[0][0], Value::Int(3));
    assert_eq!(result.rows[1][0], Value::Int(4));
    assert_eq!(result.rows[2][0], Value::Int(5));
}

#[tokio::test]
async fn test_aggregations_and_aliases() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor
        .execute("CREATE TABLE sales (amount FLOAT)", None)
        .await
        .unwrap();
    executor
        .execute("INSERT INTO sales VALUES (10.5)", None)
        .await
        .unwrap();
    executor
        .execute("INSERT INTO sales VALUES (20.0)", None)
        .await
        .unwrap();
    executor
        .execute("INSERT INTO sales VALUES (5.5)", None)
        .await
        .unwrap();

    let result = executor
            .execute("SELECT COUNT(*) as total_count, SUM(amount) as total_sum, AVG(amount), MIN(amount), MAX(amount) FROM sales", None)
            .await
            .unwrap();

    assert_eq!(result.columns[0], "total_count");
    assert_eq!(result.columns[1], "total_sum");
    assert_eq!(result.rows[0][0], Value::Int(3));
    assert_eq!(result.rows[0][1], Value::Float(36.0));
    assert_eq!(result.rows[0][2], Value::Float(12.0));
    assert_eq!(result.rows[0][3], Value::Float(5.5));
    assert_eq!(result.rows[0][4], Value::Float(20.0));
}

#[tokio::test]
async fn test_group_by_having() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor
        .execute("CREATE TABLE sales (dept TEXT, amount INT)", None)
        .await
        .unwrap();
    executor
        .execute("INSERT INTO sales VALUES ('A', 10)", None)
        .await
        .unwrap();
    executor
        .execute("INSERT INTO sales VALUES ('A', 20)", None)
        .await
        .unwrap();
    executor
        .execute("INSERT INTO sales VALUES ('B', 5)", None)
        .await
        .unwrap();
    executor
        .execute("INSERT INTO sales VALUES ('B', 15)", None)
        .await
        .unwrap();

    let result = executor
            .execute("SELECT dept, SUM(amount) FROM sales GROUP BY dept HAVING SUM(amount) > 25", None)
            .await
            .unwrap();

    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Text("A".to_string()));
    assert_eq!(result.rows[0][1], Value::Int(30));
}

#[tokio::test]
async fn test_distinct() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor
        .execute("CREATE TABLE t (v INT)", None)
        .await
        .unwrap();
    executor.execute("INSERT INTO t VALUES (1)", None).await.unwrap();
    executor.execute("INSERT INTO t VALUES (1)", None).await.unwrap();
    executor.execute("INSERT INTO t VALUES (2)", None).await.unwrap();

    let result = executor.execute("SELECT DISTINCT v FROM t", None).await.unwrap();
    assert_eq!(result.rows.len(), 2);
}

#[tokio::test]
async fn test_inner_join() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor
        .execute("CREATE TABLE users (id INT, name TEXT)", None)
        .await
        .unwrap();
    executor
        .execute("CREATE TABLE posts (id INT, user_id INT, title TEXT)", None)
        .await
        .unwrap();

    executor
        .execute("INSERT INTO users VALUES (1, 'Alice')", None)
        .await
        .unwrap();
    executor
        .execute("INSERT INTO users VALUES (2, 'Bob')", None)
        .await
        .unwrap();

    executor
        .execute("INSERT INTO posts VALUES (101, 1, 'Hello')", None)
        .await
        .unwrap();
    executor
        .execute("INSERT INTO posts VALUES (102, 1, 'World')", None)
        .await
        .unwrap();

    let result = executor
            .execute("SELECT users.name, posts.title FROM users JOIN posts ON users.id = posts.user_id", None)
            .await
            .unwrap();

    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], Value::Text("Alice".to_string()));
    assert_eq!(result.rows[0][1], Value::Text("Hello".to_string()));
}

#[tokio::test]
async fn test_left_join() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor
        .execute("CREATE TABLE users (id INT, name TEXT)", None)
        .await
        .unwrap();
    executor
        .execute("CREATE TABLE posts (id INT, user_id INT, title TEXT)", None)
        .await
        .unwrap();

    executor
        .execute("INSERT INTO users VALUES (1, 'Alice')", None)
        .await
        .unwrap();
    executor
        .execute("INSERT INTO users VALUES (2, 'Bob')", None)
        .await
        .unwrap();

    executor
        .execute("INSERT INTO posts VALUES (101, 1, 'Hello')", None)
        .await
        .unwrap();

    let result = executor
            .execute("SELECT users.name, posts.title FROM users LEFT JOIN posts ON users.id = posts.user_id ORDER BY users.id", None)
            .await
            .unwrap();

    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], Value::Text("Alice".to_string()));
    assert_eq!(result.rows[0][1], Value::Text("Hello".to_string()));
    assert_eq!(result.rows[1][0], Value::Text("Bob".to_string()));
    assert_eq!(result.rows[1][1], Value::Null);
}

#[tokio::test]
async fn test_subqueries() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor
        .execute("CREATE TABLE users (id INT, name TEXT)", None)
        .await
        .unwrap();
    executor
        .execute("INSERT INTO users VALUES (1, 'Alice')", None)
        .await
        .unwrap();
    executor
        .execute("INSERT INTO users VALUES (2, 'Bob')", None)
        .await
        .unwrap();

    // Subquery in WHERE (IN)
    let result = executor
            .execute("SELECT name FROM users WHERE id IN (SELECT id FROM users WHERE name = 'Bob')", None)
            .await
            .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Text("Bob".to_string()));

    // Correlated subquery in SELECT
    let result = executor
            .execute("SELECT name, (SELECT COUNT(*) FROM users u2 WHERE u2.id <= users.id) as rank FROM users ORDER BY id", None)
            .await
            .unwrap();
    assert_eq!(result.rows[0][1], Value::Int(1));
    assert_eq!(result.rows[1][1], Value::Int(2));
}

#[tokio::test]
async fn test_explain() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor
        .execute("CREATE TABLE users (id INT, name TEXT)", None)
        .await
        .unwrap();
    let result = executor
        .execute("EXPLAIN SELECT * FROM users WHERE id = 1", None)
        .await
        .unwrap();

    assert!(!result.rows.is_empty());
    assert_eq!(result.columns[0], "stage");
}

#[tokio::test]
async fn test_hash_index() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor.execute("CREATE TABLE users (id INT, name TEXT)", None).await.unwrap();
    executor.execute("CREATE INDEX idx_id ON users (id) USING HASH", None).await.unwrap();
    executor.execute("INSERT INTO users VALUES (1, 'Alice')", None).await.unwrap();

    let result = executor.execute("SELECT name FROM users WHERE id = 1", None).await.unwrap();
    assert_eq!(result.rows[0][0], Value::Text("Alice".to_string()));
}

#[tokio::test]
async fn test_unique_index() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor.execute("CREATE TABLE users (id INT, name TEXT)", None).await.unwrap();
    executor.execute("CREATE UNIQUE INDEX idx_id ON users (id)", None).await.unwrap();
    executor.execute("INSERT INTO users VALUES (1, 'Alice')", None).await.unwrap();

    let res = executor.execute("INSERT INTO users VALUES (1, 'Duplicate')", None).await;
    assert!(res.is_err());
}

#[tokio::test]
async fn test_composite_index() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor.execute("CREATE TABLE users (first_name TEXT, last_name TEXT)", None).await.unwrap();
    executor.execute("CREATE INDEX idx_name ON users (last_name, first_name)", None).await.unwrap();
    executor.execute("INSERT INTO users VALUES ('Alice', 'Smith')", None).await.unwrap();

    let result = executor.execute("SELECT * FROM users WHERE last_name = 'Smith'", None).await.unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[tokio::test]
async fn test_json_path_index() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor.execute("CREATE TABLE events (data JSON)", None).await.unwrap();
    executor.execute("CREATE INDEX idx_user_id ON events (data.user.id)", None).await.unwrap();
    
    executor.execute("INSERT INTO events VALUES ('{\"user\": {\"id\": 123}, \"type\": \"login\"}')", None).await.unwrap();

    let result = executor.execute("SELECT * FROM events WHERE data.user.id = 123", None).await.unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[tokio::test]
async fn test_functional_index() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor.execute("CREATE TABLE users (email TEXT)", None).await.unwrap();
    executor.execute("CREATE INDEX idx_lower_email ON users (LOWER(email))", None).await.unwrap();
    executor.execute("INSERT INTO users VALUES ('Alice@Example.Com')", None).await.unwrap();

    let result = executor.execute("SELECT * FROM users WHERE LOWER(email) = 'alice@example.com'", None).await.unwrap();
    assert_eq!(result.rows.len(), 1);
}

#[tokio::test]
async fn test_partial_index() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor.execute("CREATE TABLE orders (id INT, status TEXT)", None).await.unwrap();
    executor.execute("CREATE UNIQUE INDEX idx_active_orders ON orders (id) WHERE status = 'pending'", None).await.unwrap();
    
    executor.execute("INSERT INTO orders VALUES (1, 'pending')", None).await.unwrap();
    executor.execute("INSERT INTO orders VALUES (1, 'shipped')", None).await.unwrap(); // OK, status not pending
    
    let res = executor.execute("INSERT INTO orders VALUES (1, 'pending')", None).await;
    assert!(res.is_err());
}

#[tokio::test]
async fn test_select_columns() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor.execute("CREATE TABLE users (id INT, name TEXT)", None).await.unwrap();
    executor.execute("INSERT INTO users VALUES (1, 'Alice')", None).await.unwrap();

    // Test specific columns
    let result = executor.execute("SELECT name, id FROM users", None).await.unwrap();
    assert_eq!(result.columns, vec!["name", "id"]);
    assert_eq!(result.rows[0][0], Value::Text("Alice".to_string()));
    assert_eq!(result.rows[0][1], Value::Int(1));

    // Test expressions in SELECT
    let result = executor.execute("SELECT id + 10, UPPER(name) FROM users", None).await.unwrap();
    assert_eq!(result.rows[0][0], Value::Int(11));
    assert_eq!(result.rows[0][1], Value::Text("ALICE".to_string()));
}
