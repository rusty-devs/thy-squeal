use std::sync::Arc;
use axum::{
    Router,
    routing::{get, post},
    extract::State,
    Json,
};
use serde::{Deserialize, Serialize};
use tower_http::cors::{Any, CorsLayer};
use crate::sql::{self, Executor};
use crate::config;
use crate::storage;

#[derive(Clone)]
pub struct AppState {
    pub _config: config::Config,
    pub executor: Arc<Executor>,
}

#[derive(Deserialize)]
pub struct QueryRequest {
    pub sql: String,
    #[allow(dead_code)]
    #[serde(rename = "params", default)]
    pub _params: Vec<serde_json::Value>,
}

#[derive(Serialize)]
pub struct QueryResponse {
    pub success: bool,
    #[serde(default)]
    pub columns: Vec<String>,
    #[serde(default)]
    pub data: Vec<serde_json::Value>,
    #[serde(default)]
    pub rows_affected: u64,
    #[serde(default)]
    pub execution_time_ms: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<sql::SqlError>,
}

pub async fn root() -> &'static str {
    "thy-squeal SQL server"
}

pub async fn health() -> &'static str {
    "OK"
}

pub async fn execute_query(
    State(state): State<AppState>,
    Json(req): Json<QueryRequest>,
) -> Json<QueryResponse> {
    let start = std::time::Instant::now();
    
    match state.executor.execute(&req.sql).await {
        Ok(result) => {
            let data: Vec<serde_json::Value> = result.rows.iter().map(|row| {
                let mut map = serde_json::Map::new();
                for (i, col) in result.columns.iter().enumerate() {
                    if let Some(val) = row.get(i) {
                        map.insert(col.clone(), value_to_json(val));
                    }
                }
                serde_json::Value::Object(map)
            }).collect();

            Json(QueryResponse {
                success: true,
                columns: result.columns,
                data,
                rows_affected: result.rows_affected,
                execution_time_ms: start.elapsed().as_millis() as u64,
                error: None,
            })
        }
        Err(e) => {
            tracing::error!("Query error: {:?}", e);
            Json(QueryResponse {
                success: false,
                columns: vec![],
                data: vec![],
                rows_affected: 0,
                execution_time_ms: start.elapsed().as_millis() as u64,
                error: Some(e),
            })
        }
    }
}

pub fn value_to_json(val: &storage::Value) -> serde_json::Value {
    match val {
        storage::Value::Null => serde_json::Value::Null,
        storage::Value::Int(i) => serde_json::Value::Number((*i).into()),
        storage::Value::Float(f) => serde_json::Value::Number(serde_json::Number::from_f64(*f).unwrap()),
        storage::Value::Bool(b) => serde_json::Value::Bool(*b),
        storage::Value::Text(s) => serde_json::Value::String(s.clone()),
        storage::Value::Date(d) => serde_json::Value::String(d.to_string()),
        storage::Value::DateTime(dt) => serde_json::Value::String(dt.to_string()),
        storage::Value::Blob(b) => serde_json::Value::String(base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b)),
        storage::Value::Json(j) => j.clone(),
    }
}

pub fn create_app(executor: Arc<Executor>, config: config::Config) -> Router {
    let state = AppState { 
        _config: config,
        executor,
    };

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
        .route("/", get(root))
        .route("/health", get(health))
        .route("/_query", post(execute_query))
        .layer(cors)
        .with_state(state)
}
