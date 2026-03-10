use super::common::setup;
use crate::sql::Executor;
use crate::storage::Database;
use std::sync::Arc;

#[tokio::test]
async fn test_dump_restore() {
    setup();
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    // 1. Create schema and data
    executor
        .execute("CREATE TABLE dump_test (id INT, name TEXT)", vec![], None)
        .await
        .unwrap();
    executor
        .execute(
            "CREATE UNIQUE INDEX idx_dump_id ON dump_test (id)",
            vec![],
            None,
        )
        .await
        .unwrap();
    executor
        .execute("INSERT INTO dump_test VALUES (1, 'alice')", vec![], None)
        .await
        .unwrap();
    executor
        .execute("INSERT INTO dump_test VALUES (2, 'bob')", vec![], None)
        .await
        .unwrap();

    // 2. Perform dump
    let sql_dump = executor.dump().await.unwrap();
    assert!(sql_dump.contains("CREATE TABLE dump_test"));
    assert!(sql_dump.contains("INSERT INTO dump_test VALUES (1, 'alice')"));
    assert!(sql_dump.contains("CREATE UNIQUE INDEX idx_dump_id"));

    // 3. Start fresh database and restore
    let db2 = Database::new();
    let executor2 = Arc::new(Executor::new(db2));

    executor2.execute_batch(&sql_dump).await.unwrap();

    // 4. Verify data and index
    let r = executor2
        .execute("SELECT name FROM dump_test WHERE id = 1", vec![], None)
        .await
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0].as_text(), Some("alice"));

    let r = executor2
        .execute("SELECT name FROM dump_test WHERE id = 2", vec![], None)
        .await
        .unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0].as_text(), Some("bob"));

    // Verify unique index works after restore
    let res = executor2
        .execute(
            "INSERT INTO dump_test VALUES (1, 'duplicate')",
            vec![],
            None,
        )
        .await;
    assert!(res.is_err());
}
