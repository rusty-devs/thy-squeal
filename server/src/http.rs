use crate::config::Config;
use crate::sql::executor::{Executor, QueryResult};
use crate::storage::Value;
use axum::{
    Json, Router,
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tracing::error;

#[derive(Deserialize)]
struct QueryRequest {
    sql: String,
    transaction_id: Option<String>,
}

#[derive(Serialize)]
struct QueryResponse {
    success: bool,
    columns: Vec<String>,
    data: Vec<Vec<serde_json::Value>>,
    rows_affected: u64,
    transaction_id: Option<String>,
    error: Option<String>,
}

pub struct HttpServer {
    #[allow(dead_code)]
    executor: Arc<Executor>,
}

impl HttpServer {
    #[allow(dead_code)]
    pub fn new(executor: Arc<Executor>) -> Self {
        Self { executor }
    }

    #[allow(dead_code)]
    pub async fn run(&self, port: u16) -> anyhow::Result<()> {
        let app = create_app(self.executor.clone(), Config::default());
        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
        axum::serve(listener, app).await?;
        Ok(())
    }

    async fn root() -> &'static str {
        "thy-squeal SQL Server"
    }

    async fn health() -> &'static str {
        "OK"
    }

    async fn query(
        State(executor): State<Arc<Executor>>,
        Json(payload): Json<QueryRequest>,
    ) -> impl IntoResponse {
        match executor
            .execute(&payload.sql, vec![], payload.transaction_id)
            .await
        {
            Ok(result) => (StatusCode::OK, Json(Self::map_result(result, None))),
            Err(e) => {
                error!("Query error: {:?}", e);
                (
                    StatusCode::BAD_REQUEST,
                    Json(Self::map_result(
                        QueryResult {
                            columns: vec![],
                            rows: vec![],
                            rows_affected: 0,
                            transaction_id: None,
                        },
                        Some(format!("{:?}", e)),
                    )),
                )
            }
        }
    }

    async fn dump(State(executor): State<Arc<Executor>>) -> impl IntoResponse {
        match executor.dump().await {
            Ok(sql) => (StatusCode::OK, sql),
            Err(e) => {
                error!("Dump error: {:?}", e);
                (StatusCode::INTERNAL_SERVER_ERROR, format!("{:?}", e))
            }
        }
    }

    async fn restore(State(executor): State<Arc<Executor>>, body: String) -> impl IntoResponse {
        match executor.execute_batch(&body).await {
            Ok(result) => (StatusCode::OK, Json(Self::map_result(result, None))),
            Err(e) => {
                error!("Restore error: {:?}", e);
                (
                    StatusCode::BAD_REQUEST,
                    Json(Self::map_result(
                        QueryResult {
                            columns: vec![],
                            rows: vec![],
                            rows_affected: 0,
                            transaction_id: None,
                        },
                        Some(format!("{:?}", e)),
                    )),
                )
            }
        }
    }

    fn map_result(result: QueryResult, error: Option<String>) -> QueryResponse {
        let data = result
            .rows
            .into_iter()
            .map(|row: Vec<Value>| row.into_iter().map(Self::value_to_json).collect())
            .collect();

        QueryResponse {
            success: error.is_none(),
            columns: result.columns,
            data,
            rows_affected: result.rows_affected,
            transaction_id: result.transaction_id,
            error,
        }
    }

    fn value_to_json(v: Value) -> serde_json::Value {
        match v {
            Value::Null => serde_json::Value::Null,
            Value::Int(i) => serde_json::Value::Number(i.into()),
            Value::Float(f) => serde_json::Value::Number(
                serde_json::Number::from_f64(f).unwrap_or_else(|| 0.into()),
            ),
            Value::Text(s) => serde_json::Value::String(s),
            Value::Bool(b) => serde_json::Value::Bool(b),
            Value::DateTime(d) => serde_json::Value::String(d.to_rfc3339()),
            Value::Json(j) => j,
        }
    }
}

pub fn create_app(executor: Arc<Executor>, _config: Config) -> Router {
    Router::new()
        .route("/", get(HttpServer::root))
        .route("/health", get(HttpServer::health))
        .route("/_query", post(HttpServer::query))
        .route("/_dump", get(HttpServer::dump))
        .route("/_restore", post(HttpServer::restore))
        .with_state(executor)
}
