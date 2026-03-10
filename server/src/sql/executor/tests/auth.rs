use crate::sql::Executor;
use crate::storage::Database;
use std::sync::Arc;

#[tokio::test]
async fn test_rbac_basic() {
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    // 1. Root can do everything
    executor
        .execute(
            "CREATE TABLE test (id INT)",
            vec![],
            None,
            Some("root".to_string()),
        )
        .await
        .unwrap();
    executor
        .execute(
            "CREATE USER 'bob' IDENTIFIED BY 'pass'",
            vec![],
            None,
            Some("root".to_string()),
        )
        .await
        .unwrap();

    // 2. Bob cannot select without permission
    let res = executor
        .execute("SELECT * FROM test", vec![], None, Some("bob".to_string()))
        .await;
    assert!(res.is_err());
    assert!(
        res.unwrap_err()
            .to_string()
            .contains("does not have Select privilege")
    );

    // 3. Grant permission
    executor
        .execute(
            "GRANT SELECT ON test TO 'bob'",
            vec![],
            None,
            Some("root".to_string()),
        )
        .await
        .unwrap();

    // 4. Bob can now select
    let res = executor
        .execute("SELECT * FROM test", vec![], None, Some("bob".to_string()))
        .await;
    assert!(res.is_ok());

    // 5. Bob still cannot insert
    let res = executor
        .execute(
            "INSERT INTO test VALUES (1)",
            vec![],
            None,
            Some("bob".to_string()),
        )
        .await;
    assert!(res.is_err());

    // 6. Grant INSERT globally
    executor
        .execute(
            "GRANT INSERT ON ALL PRIVILEGES TO 'bob'",
            vec![],
            None,
            Some("root".to_string()),
        )
        .await
        .unwrap();
    let res = executor
        .execute(
            "INSERT INTO test VALUES (1)",
            vec![],
            None,
            Some("bob".to_string()),
        )
        .await;
    assert!(res.is_ok());

    // 7. Revoke
    executor
        .execute(
            "REVOKE SELECT ON test FROM 'bob'",
            vec![],
            None,
            Some("root".to_string()),
        )
        .await
        .unwrap();
    let res = executor
        .execute("SELECT * FROM test", vec![], None, Some("bob".to_string()))
        .await;
    assert!(res.is_err());
}
