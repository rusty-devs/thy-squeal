mod config;
mod http;
mod mysql;
mod sql;
mod squeal;
mod storage;
#[cfg(test)]
mod tests;

use crate::mysql::MySqlProtocol;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{Level, error, info};
use tracing_subscriber::FmtSubscriber;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let subscriber = FmtSubscriber::builder()
        .with_max_level(Level::INFO)
        .with_target(false)
        .finish();

    tracing::subscriber::set_global_default(subscriber).expect("Failed to set tracing subscriber");

    info!("Starting thy-squeal server v{}", env!("CARGO_PKG_VERSION"));

    let config = config::load_config()?;
    info!(
        "Configuration loaded: sql_port={}, http_port={}",
        config.server.sql_port, config.server.http_port
    );

    let db = if !config.storage.data_dir.is_empty() {
        info!("Initializing persistence at {}", config.storage.data_dir);
        let persister = Box::new(
            storage::persistence::SledPersister::new(&config.storage.data_dir)
                .expect("Failed to open database"),
        );
        storage::Database::with_persister(persister, config.storage.data_dir.clone())
            .expect("Failed to load database")
    } else {
        storage::Database::new()
    };

    let executor = Arc::new(sql::Executor::new(db).with_data_dir(config.storage.data_dir.clone()));

    // 1. MySQL Protocol Task
    let mysql_executor = executor.clone();
    let mysql_addr = format!("{}:{}", config.server.host, config.server.sql_port);
    let mysql_handle = tokio::spawn(async move {
        let protocol = MySqlProtocol::new(mysql_executor);
        if let Err(e) = protocol.run(&mysql_addr).await {
            error!("MySQL protocol error: {}", e);
        }
    });

    // 2. HTTP Server Task
    let http_executor = executor.clone();
    let http_addr: SocketAddr =
        format!("{}:{}", config.server.host, config.server.http_port).parse()?;
    let http_handle = tokio::spawn(async move {
        let app = http::create_app(http_executor, config);
        info!("HTTP server listening on http://{}", http_addr);
        let listener = match tokio::net::TcpListener::bind(http_addr).await {
            Ok(l) => l,
            Err(e) => {
                error!("Failed to bind HTTP listener: {}", e);
                return;
            }
        };
        if let Err(e) = axum::serve(listener, app).await {
            error!("HTTP server error: {}", e);
        }
    });

    // Wait for both tasks
    let _ = tokio::join!(mysql_handle, http_handle);

    Ok(())
}
