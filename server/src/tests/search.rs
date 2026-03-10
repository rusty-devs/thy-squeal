use super::common::setup;
use crate::{http::create_app, sql::Executor};
use axum::{body::Body, http::Request};
use serde_json::{Value, json};
use std::sync::Arc;
use tower::ServiceExt; // for `oneshot`

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
    let executor = Arc::new(Executor::new(db).with_data_dir(data_dir.clone()));

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
