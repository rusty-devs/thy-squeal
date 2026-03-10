use super::common::setup;
use crate::sql::Executor;
use crate::storage::persistence::SledPersister;
use crate::storage::{Database, Value};
use std::sync::Arc;

#[tokio::test]
async fn test_persistence() {
    setup();
    let temp_dir = std::env::temp_dir().join(format!("thy-squeal-test-{}", uuid::Uuid::new_v4()));
    let data_dir = temp_dir.to_str().unwrap().to_string();

    {
        let persister = Box::new(SledPersister::new(&data_dir).unwrap());
        let db = Database::with_persister(persister, data_dir.clone()).unwrap();

        let executor = Arc::new(Executor::new(db).with_data_dir(data_dir.clone()));
        executor
            .execute(
                "CREATE TABLE test_table (id INT, name TEXT)",
                vec![],
                None,
                None,
            )
            .await
            .unwrap();
        executor
            .execute(
                "INSERT INTO test_table VALUES (1, 'alice')",
                vec![],
                None,
                None,
            )
            .await
            .unwrap();
    }

    // Re-open
    {
        let persister = Box::new(SledPersister::new(&data_dir).unwrap());
        let db = Database::with_persister(persister, data_dir.clone()).unwrap();
        let executor = Arc::new(Executor::new(db).with_data_dir(data_dir.clone()));

        let res = executor
            .execute(
                "SELECT name FROM test_table WHERE id = 1",
                vec![],
                None,
                None,
            )
            .await
            .unwrap();
        assert_eq!(res.rows[0][0], Value::Text("alice".to_string()));
    }

    let _ = std::fs::remove_dir_all(temp_dir);
}

#[tokio::test]
async fn test_wal_recovery() {
    setup();
    let temp_dir =
        std::env::temp_dir().join(format!("thy-squeal-wal-test-{}", uuid::Uuid::new_v4()));
    let data_dir = temp_dir.to_str().unwrap().to_string();

    // 1. Create table and insert data (will be in WAL)
    {
        let persister = Box::new(SledPersister::new(&data_dir).unwrap());
        let db = Database::with_persister(persister, data_dir.clone()).unwrap();
        let executor = Arc::new(Executor::new(db).with_data_dir(data_dir.clone()));

        executor
            .execute("CREATE TABLE w (id INT, v TEXT)", vec![], None, None)
            .await
            .unwrap();
        executor
            .execute("INSERT INTO w VALUES (1, 'wal_data')", vec![], None, None)
            .await
            .unwrap();
    }

    // 2. Re-open and verify
    {
        let persister = Box::new(SledPersister::new(&data_dir).unwrap());
        let db = Database::with_persister(persister, data_dir.clone()).unwrap();
        let executor = Arc::new(Executor::new(db).with_data_dir(data_dir.clone()));

        let res = executor
            .execute("SELECT v FROM w WHERE id = 1", vec![], None, None)
            .await
            .unwrap();
        assert_eq!(res.rows[0][0], Value::Text("wal_data".to_string()));
    }

    let _ = std::fs::remove_dir_all(temp_dir);
}
