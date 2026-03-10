use super::common::setup;
use crate::{http::create_app, sql::Executor};
use axum::{body::Body, http::Request};
use serde_json::{Value, json};
use std::sync::Arc;
use tower::ServiceExt; // for `oneshot`

#[tokio::test]
async fn test_persistence() {
    setup();
    let temp_dir = std::env::temp_dir().join(format!("thy-squeal-test-{}", uuid::Uuid::new_v4()));
    let data_dir = temp_dir.to_str().unwrap().to_string();

    let config = crate::config::Config {
        server: crate::config::ServerConfig {
            host: "127.0.0.1".to_string(),
            sql_port: 3306,
            http_port: 9200,
        },
        storage: crate::config::StorageConfig {
            max_memory_mb: 1024,
            default_cache_size: 1000,
            default_eviction: "LRU".to_string(),
            snapshot_interval_sec: 300,
            data_dir: data_dir.clone(),
        },
        security: crate::config::SecurityConfig {
            auth_enabled: false,
            tls_enabled: false,
        },
        logging: crate::config::LoggingConfig {
            level: "info".to_string(),
        },
    };

    // 1. Create table and insert data in first instance
    {
        let persister =
            Box::new(crate::storage::persistence::SledPersister::new(&data_dir).unwrap());
        let db = crate::storage::Database::with_persister(persister, data_dir.clone()).unwrap();
        let executor = Arc::new(Executor::new(db));
        let app = create_app(executor, config.clone());

        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/_query")
                    .header("Content-Type", "application/json")
                    .body(Body::from(
                        json!({"sql": "CREATE TABLE p (id INT, v TEXT)"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        app.clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/_query")
                    .header("Content-Type", "application/json")
                    .body(Body::from(
                        json!({"sql": "INSERT INTO p (id, v) VALUES (1, 'persisted')"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();
    }

    // 2. Start a second instance and verify data exists
    {
        let persister =
            Box::new(crate::storage::persistence::SledPersister::new(&data_dir).unwrap());
        let db = crate::storage::Database::with_persister(persister, data_dir.clone()).unwrap();
        let executor = Arc::new(Executor::new(db));
        let app = create_app(executor, config);

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/_query")
                    .header("Content-Type", "application/json")
                    .body(Body::from(
                        json!({"sql": "SELECT v FROM p WHERE id = 1"}).to_string(),
                    ))
                    .unwrap(),
            )
            .await
            .unwrap();

        let body = axum::body::to_bytes(response.into_body(), usize::MAX)
            .await
            .unwrap();
        let body: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(body["data"][0][0], "persisted");
    }

    // Cleanup
    let _ = std::fs::remove_dir_all(temp_dir);
}

#[tokio::test]
async fn test_wal_recovery() {
    setup();
    let temp_dir =
        std::env::temp_dir().join(format!("thy-squeal-wal-test-{}", uuid::Uuid::new_v4()));
    let data_dir = temp_dir.to_str().unwrap().to_string();

    // 1. Create table and insert data (it will be logged to WAL and applied to in-memory)
    {
        let persister =
            Box::new(crate::storage::persistence::SledPersister::new(&data_dir).unwrap());
        let db = crate::storage::Database::with_persister(persister, data_dir.clone()).unwrap();
        let executor = Arc::new(Executor::new(db));

        executor
            .execute("CREATE TABLE w (id INT, v TEXT)", vec![], None)
            .await
            .unwrap();
        executor
            .execute("INSERT INTO w VALUES (1, 'wal_data')", vec![], None)
            .await
            .unwrap();
    }

    // 2. Start a second instance and verify data exists
    {
        let persister =
            Box::new(crate::storage::persistence::SledPersister::new(&data_dir).unwrap());
        let db = crate::storage::Database::with_persister(persister, data_dir.clone()).unwrap();
        let executor = Arc::new(Executor::new(db));

        let r = executor
            .execute("SELECT v FROM w WHERE id = 1", vec![], None)
            .await
            .unwrap();
        assert_eq!(r.rows[0][0].as_text(), Some("wal_data"));
    }

    let _ = std::fs::remove_dir_all(temp_dir);
}
