use crate::{http::create_app, sql::Executor};
use crate::storage::Database;
use axum::{
    body::Body,
    http::{Request, StatusCode},
    response::Response,
};
use serde_json::{Value, json};
use std::sync::Arc;
use std::sync::Once;
use tower::ServiceExt; // for `oneshot`

static INIT: Once = Once::new();

fn setup() {
    INIT.call_once(|| {
        let _ = tracing_subscriber::fmt::try_init();
    });
}

#[tokio::test]
async fn test_sql_lifecycle() {
    setup();
    let temp_dir = std::env::temp_dir().join(format!(
        "thy-squeal-lifecycle-test-{}",
        uuid::Uuid::new_v4()
    ));
    let data_dir = temp_dir.to_str().unwrap().to_string();

    let db = crate::storage::Database::with_persister(
        Box::new(crate::storage::persistence::SledPersister::new(&data_dir).unwrap()),
        data_dir.clone(),
    )
    .unwrap();
    let executor = Arc::new(Executor::new(db));

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
    let app = create_app(executor, config);

    // 1. CREATE TABLE
    let response: Response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({
                        "sql": "CREATE TABLE users (id INT, name TEXT)"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert!(body["success"].as_bool().unwrap());

    // 2. INSERT
    let response: Response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({
                        "sql": "INSERT INTO users (id, name) VALUES (1, 'alice')"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert!(body["success"].as_bool().unwrap());
    assert_eq!(body["rows_affected"].as_u64().unwrap(), 1);

    // 3. SELECT
    let response: Response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({
                        "sql": "SELECT * FROM users WHERE name = 'alice'"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert!(body["success"].as_bool().unwrap());
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
    assert_eq!(body["data"][0][1], "alice");

    // 4. UPDATE
    let response: Response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({
                        "sql": "UPDATE users SET name = 'bob' WHERE id = 1"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert!(body["success"].as_bool().unwrap());
    assert_eq!(body["rows_affected"].as_u64().unwrap(), 1);

    // 5. SELECT again to verify update
    let response: Response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({
                        "sql": "SELECT name FROM users WHERE id = 1"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["data"][0][0], "bob");

    let _ = std::fs::remove_dir_all(temp_dir);
}

#[tokio::test]
async fn test_persistence() {
    setup();
    let temp_dir =
        std::env::temp_dir().join(format!("thy-squeal-test-{}", uuid::Uuid::new_v4()));
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
                        json!({"sql": "INSERT INTO p (id, v) VALUES (1, 'persisted')"})
                            .to_string(),
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
async fn test_full_text_search() {
    setup();
    let temp_dir =
        std::env::temp_dir().join(format!("thy-squeal-search-test-{}", uuid::Uuid::new_v4()));
    let data_dir = temp_dir.to_str().unwrap().to_string();

    let db = crate::storage::Database::with_persister(
        Box::new(crate::storage::persistence::SledPersister::new(&data_dir).unwrap()),
        data_dir.clone(),
    )
    .unwrap();
    let executor = Arc::new(Executor::new(db));

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
    let app = create_app(executor, config);

    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({"sql": "CREATE TABLE articles (id INT, content TEXT)"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    app.clone().oneshot(
        Request::builder()
            .method("POST")
            .uri("/_query")
            .header("Content-Type", "application/json")
            .body(Body::from(json!({"sql": "INSERT INTO articles (id, content) VALUES (1, 'Rust is a great systems programming language')"}).to_string()))
            .unwrap(),
    ).await.unwrap();

    app.clone().oneshot(
        Request::builder()
            .method("POST")
            .uri("/_query")
            .header("Content-Type", "application/json")
            .body(Body::from(json!({"sql": "INSERT INTO articles (id, content) VALUES (2, 'SQL databases are powerful tools for data management')"}).to_string()))
            .unwrap(),
    ).await.unwrap();

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({"sql": "SEARCH articles 'programming'"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();

    assert!(body["success"].as_bool().unwrap());
    let data = body["data"].as_array().unwrap();
    assert_eq!(data.len(), 1);
    assert_eq!(data[0][0], 1);
    assert!(data[0][2].as_f64().unwrap() > 0.0);

    let _ = std::fs::remove_dir_all(temp_dir);
}

#[tokio::test]
async fn test_error_handling() {
    setup();
    let executor = Arc::new(Executor::new(crate::storage::Database::new()));
    let config = crate::config::Config::default();
    let app = create_app(executor, config);

    // Table not found error
    let response: Response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({
                        "sql": "SELECT * FROM non_existent"
                    })
                    .to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert!(!body["success"].as_bool().unwrap());
    assert!(body["error"].as_str().unwrap().contains("TableNotFound"));
}

#[tokio::test]
async fn test_transactions() {
    setup();
    let executor = Arc::new(Executor::new(crate::storage::Database::new()));
    let config = crate::config::Config::default();
    let app = create_app(executor, config);

    // 1. Create table
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({"sql": "CREATE TABLE accounts (id INT, balance FLOAT)"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    // 2. BEGIN
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(json!({"sql": "BEGIN"}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();

    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    let tx_id = body["transaction_id"].as_str().unwrap().to_string();

    // 3. INSERT in TX
    app.clone().oneshot(
        Request::builder()
            .method("POST")
            .uri("/_query")
            .header("Content-Type", "application/json")
            .body(Body::from(json!({"sql": "INSERT INTO accounts VALUES (1, 100.0)", "transaction_id": tx_id}).to_string()))
            .unwrap(),
    ).await.unwrap();

    // 4. Verify NOT visible globally
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({"sql": "SELECT * FROM accounts"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert!(body["data"].as_array().unwrap().is_empty());

    // 5. COMMIT
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({"sql": "COMMIT", "transaction_id": tx_id}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    // 6. Verify visible globally
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({"sql": "SELECT * FROM accounts"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["data"].as_array().unwrap().len(), 1);
}

#[tokio::test]
async fn test_rollback() {
    setup();
    let executor = Arc::new(Executor::new(crate::storage::Database::new()));
    let config = crate::config::Config::default();
    let app = create_app(executor, config);

    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({"sql": "CREATE TABLE t (id INT)"}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(json!({"sql": "BEGIN"}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    let tx_id = body["transaction_id"].as_str().unwrap().to_string();

    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(
                    json!({"sql": "INSERT INTO t VALUES (1)", "transaction_id": tx_id})
                        .to_string(),
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
                    json!({"sql": "ROLLBACK", "transaction_id": tx_id}).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/_query")
                .header("Content-Type", "application/json")
                .body(Body::from(json!({"sql": "SELECT * FROM t"}).to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert!(body["data"].as_array().unwrap().is_empty());
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
        let db =
            crate::storage::Database::with_persister(persister, data_dir.clone()).unwrap();
        let executor = Arc::new(Executor::new(db));

        executor
            .execute("CREATE TABLE w (id INT, v TEXT)", None)
            .await
            .unwrap();
        executor
            .execute("INSERT INTO w VALUES (1, 'wal_data')", None)
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
            .execute("SELECT v FROM w WHERE id = 1", None)
            .await
            .unwrap();
        assert_eq!(r.rows[0][0].as_text(), Some("wal_data"));
    }

    let _ = std::fs::remove_dir_all(temp_dir);
}

#[tokio::test]
async fn test_info_schema() {
    setup();
    let db = Database::new();
    let executor = Arc::new(Executor::new(db));

    executor.execute("CREATE TABLE info_test (id INT, name TEXT)", None).await.unwrap();
    executor.execute("CREATE UNIQUE INDEX idx_info_id ON info_test (id)", None).await.unwrap();

    // 1. Check tables
    let r = executor.execute("SELECT table_name, table_type FROM information_schema.tables WHERE table_name = 'info_test'", None).await.unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0].as_text(), Some("info_test"));
    assert_eq!(r.rows[0][1].as_text(), Some("BASE TABLE"));

    // 2. Check columns
    let r = executor.execute("SELECT column_name, data_type FROM information_schema.columns WHERE table_name = 'info_test' ORDER BY ordinal_position", None).await.unwrap();
    assert_eq!(r.rows.len(), 2);
    assert_eq!(r.rows[0][0].as_text(), Some("id"));
    assert_eq!(r.rows[0][1].as_text(), Some("INT"));
    assert_eq!(r.rows[1][0].as_text(), Some("name"));
    assert_eq!(r.rows[1][1].as_text(), Some("TEXT"));

    // 3. Check indexes
    let r = executor.execute("SELECT index_name, is_unique FROM information_schema.indexes WHERE table_name = 'info_test'", None).await.unwrap();
    assert_eq!(r.rows.len(), 1);
    assert_eq!(r.rows[0][0].as_text(), Some("idx_info_id"));
    assert_eq!(r.rows[0][1].as_bool(), Some(true));
}
