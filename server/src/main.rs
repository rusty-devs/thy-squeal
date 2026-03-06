mod config;
mod storage;
mod sql;

use std::net::SocketAddr;
use std::sync::Arc;
use axum::{
    Router,
    routing::{get, post},
    extract::State,
    Json,
};
use serde::{Deserialize, Serialize};
use tower_http::cors::{CorsLayer, Any};
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

#[derive(Clone)]
struct AppState {
    config: config::Config,
    executor: Arc<sql::Executor>,
}

#[derive(Deserialize)]
struct QueryRequest {
    sql: String,
    #[serde(default)]
    params: Vec<serde_json::Value>,
}

#[derive(Serialize)]
struct QueryResponse {
    success: bool,
    #[serde(default)]
    columns: Vec<String>,
    #[serde(default)]
    data: Vec<serde_json::Value>,
    #[serde(default)]
    rows_affected: u64,
    #[serde(default)]
    execution_time_ms: u64,
    #[serde(default)]
    error: Option<QueryError>,
}

#[derive(Serialize)]
struct QueryError {
    code: String,
    message: String,
}

async fn root() -> &'static str {
    "thy-squeal SQL server"
}

async fn health() -> &'static str {
    "OK"
}

async fn execute_query(
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
            Json(QueryResponse {
                success: false,
                columns: vec![],
                data: vec![],
                rows_affected: 0,
                execution_time_ms: start.elapsed().as_millis() as u64,
                error: Some(QueryError {
                    code: "EXECUTION_ERROR".to_string(),
                    message: e,
                }),
            })
        }
    }
}

fn value_to_json(value: &storage::Value) -> serde_json::Value {
    use storage::Value;
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Int(i) => serde_json::Value::Number((*i).into()),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::Bool(b) => serde_json::Value::Bool(*b),
        Value::Date(d) => serde_json::Value::String(d.to_string()),
        Value::DateTime(dt) => serde_json::Value::String(dt.to_string()),
        Value::Text(s) => serde_json::Value::String(s.clone()),
        Value::Blob(b) => serde_json::Value::String(base64::Engine::encode(&base64::engine::general_purpose::STANDARD, b)),
        Value::Json(j) => j.clone(),
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .finish();
    
    tracing::subscriber::set_global_default(subscriber)
        .expect("Failed to set tracing subscriber");

    info!("Starting thy-squeal server v{}", env!("CARGO_PKG_VERSION"));

    let config = config::load_config()?;
    info!("Configuration loaded: sql_port={}, http_port={}", 
          config.server.sql_port, config.server.http_port);

    let executor = Arc::new(sql::Executor::new());
    
    let state = AppState { 
        config: config.clone(),
        executor,
    };

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let app = Router::new()
        .route("/", get(root))
        .route("/health", get(health))
        .route("/_query", post(execute_query))
        .layer(cors)
        .with_state(state);

    let addr = SocketAddr::from(([0, 0, 0, 0], config.server.http_port));
    
    info!("HTTP server listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
