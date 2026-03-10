use crate::sql::Executor;
use crate::storage::{Database, Value};
use std::sync::Arc;

#[tokio::test]
async fn test_subqueries() {
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
    executor
        .execute("INSERT INTO users VALUES (2, 'Bob')", vec![], None, None)
        .await
        .unwrap();

    // Subquery in WHERE (IN)
    let result = executor
        .execute(
            "SELECT name FROM users WHERE id IN (SELECT id FROM users WHERE name = 'Bob')",
            vec![],
            None,
            None,
        )
        .await
        .unwrap();
    assert_eq!(result.rows.len(), 1);
    assert_eq!(result.rows[0][0], Value::Text("Bob".to_string()));

    // Correlated subquery in SELECT
    let result = executor
            .execute("SELECT name, (SELECT COUNT(*) FROM users u2 WHERE u2.id <= users.id) as rank FROM users ORDER BY id", vec![], None, None)
            .await
            .unwrap();
    assert_eq!(result.rows[0][1], Value::Int(1));
    assert_eq!(result.rows[1][1], Value::Int(2));
}
