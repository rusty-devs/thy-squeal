mod config;
mod storage;
mod sql;
mod http;
#[cfg(test)]
mod tests;

use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{info, Level};
use tracing_subscriber::FmtSubscriber;

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

    let db = if !config.storage.data_dir.is_empty() {
        info!("Initializing persistence at {}", config.storage.data_dir);
        let persister = Box::new(storage::persistence::SledPersister::new(&config.storage.data_dir).expect("Failed to open database"));
        storage::Database::with_persister(persister).expect("Failed to load database")
    } else {
        storage::Database::new()
    };

    let executor = Arc::new(sql::Executor::new(db));
    
    let app = http::create_app(executor, config.clone());

    let addr = SocketAddr::from(([0, 0, 0, 0], config.server.http_port));
    
    info!("HTTP server listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}
