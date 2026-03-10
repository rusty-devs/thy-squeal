pub mod connection;
pub mod packet;

use self::connection::handle_connection;
use crate::sql::executor::Executor;
use anyhow::Result;
use std::sync::Arc;
use tokio::net::TcpListener;
use tracing::{error, info};

/// MySQL Protocol Handler
pub struct MySqlProtocol {
    executor: Arc<Executor>,
}

impl MySqlProtocol {
    pub fn new(executor: Arc<Executor>) -> Self {
        Self { executor }
    }

    pub async fn run(&self, addr: &str) -> Result<()> {
        let listener = TcpListener::bind(addr).await?;
        info!("MySQL Protocol listening on {}", addr);

        loop {
            let (socket, _) = listener.accept().await?;
            let executor = self.executor.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_connection(socket, executor).await {
                    error!("MySQL connection error: {}", e);
                }
            });
        }
    }
}
