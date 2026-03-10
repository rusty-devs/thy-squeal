use crate::sql::Executor;
use crate::storage::{Database, Value};
use std::sync::Arc;

#[tokio::test]
async fn test_inner_join() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor
        .execute("CREATE TABLE users (id INT, name TEXT)", vec![], None)
        .await
        .unwrap();
    executor
        .execute(
            "CREATE TABLE posts (id INT, user_id INT, title TEXT)",
            vec![],
            None,
        )
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

    executor
        .execute("INSERT INTO posts VALUES (101, 1, 'Hello')", vec![], None)
        .await
        .unwrap();
    executor
        .execute("INSERT INTO posts VALUES (102, 1, 'World')", vec![], None)
        .await
        .unwrap();

    let result = executor
        .execute(
            "SELECT users.name, posts.title FROM users JOIN posts ON users.id = posts.user_id",
            vec![],
            None,
        )
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
        .execute("CREATE TABLE users (id INT, name TEXT)", vec![], None)
        .await
        .unwrap();
    executor
        .execute(
            "CREATE TABLE posts (id INT, user_id INT, title TEXT)",
            vec![],
            None,
        )
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

    executor
        .execute("INSERT INTO posts VALUES (101, 1, 'Hello')", vec![], None)
        .await
        .unwrap();

    let result = executor
            .execute("SELECT users.name, posts.title FROM users LEFT JOIN posts ON users.id = posts.user_id ORDER BY users.id", vec![], None)
            .await
            .unwrap();

    assert_eq!(result.rows.len(), 2);
    assert_eq!(result.rows[0][0], Value::Text("Alice".to_string()));
    assert_eq!(result.rows[0][1], Value::Text("Hello".to_string()));
    assert_eq!(result.rows[1][0], Value::Text("Bob".to_string()));
    assert_eq!(result.rows[1][1], Value::Null);
}
