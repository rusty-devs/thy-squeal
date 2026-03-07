use crate::{http::create_app, sql::Executor};
use crate::storage::Database;
use axum::{
    body::Body,
    http::{Request, StatusCode},
    response::Response,
};
use serde_json::{Value, json};
use std::sync::Arc;
use tower::ServiceExt; // for `oneshot`
use super::common::setup;

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
