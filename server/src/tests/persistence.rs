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
        let mut db = Database::with_persister(persister, data_dir.clone()).unwrap();

        db.create_table(
            "test_table".to_string(),
            vec![
                crate::storage::Column {
                    name: "id".to_string(),
                    data_type: crate::storage::DataType::Int,
                    is_auto_increment: false,
                },
                crate::storage::Column {
                    name: "name".to_string(),
                    data_type: crate::storage::DataType::Text,
                    is_auto_increment: false,
                },
            ],
            None,
            vec![],
        )
        .unwrap();

        let executor = Arc::new(Executor::new(db));
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
        let executor = Arc::new(Executor::new(db));

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
        let executor = Arc::new(Executor::new(db));

        executor
            .execute("CREATE TABLE w (id INT, v TEXT)", vec![], None, None)
            .await
            .unwrap();
        executor
            .execute("INSERT INTO w VALUES (1, 'wal_data')", vec![], None, None)
            .await
            .unwrap();

        // We don't call save() manually here to ensure it's in WAL only if not auto-saved
        // But our implementation saves on every mutation for now.
        // To truly test WAL, we'd need to simulate a crash before save().
        // However, with Sled, WAL is written immediately.
    }

    // 2. Re-open and verify
    {
        let persister = Box::new(SledPersister::new(&data_dir).unwrap());
        let db = Database::with_persister(persister, data_dir.clone()).unwrap();
        let executor = Arc::new(Executor::new(db));

        let res = executor
            .execute("SELECT v FROM w WHERE id = 1", vec![], None, None)
            .await
            .unwrap();
        assert_eq!(res.rows[0][0], Value::Text("wal_data".to_string()));
    }

    let _ = std::fs::remove_dir_all(temp_dir);
}
