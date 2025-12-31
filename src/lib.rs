use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::TcpListener;
use log::{info, error};

pub mod protocol;
pub mod lexer;
pub mod parser;
pub mod executor;
pub mod engines;
pub mod storage;
pub mod types;
pub mod error;
pub mod join;

use crate::executor::Executor;
use crate::protocol::ConnectionHandler;
use crate::storage::StorageEngine;
use crate::engines::TransactionManager;

/// Server configuration
#[derive(Clone)]
pub struct Config {
    /// Address to bind the server to
    pub bind_addr: SocketAddr,
    /// Directory for data files
    pub data_dir: PathBuf,
    /// Fixed username for authentication (simplified)
    pub username: String,
    /// Fixed password for authentication (simplified)
    pub password: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            bind_addr: "127.0.0.1:3306".parse().expect("invalid default bind address"),
            data_dir: PathBuf::from("./data"),
            username: "root".to_string(),
            password: "password".to_string(),
        }
    }
}

/// The main SQL server
pub struct Server {
    config: Config,
    executor: Arc<Executor>,
}

impl Server {
    /// Create a new server with the given configuration
    pub async fn new(config: Config) -> Result<Self, Box<dyn std::error::Error>> {
        // Ensure data directory exists
        std::fs::create_dir_all(&config.data_dir)?;

        // Initialize storage engine
        let storage = StorageEngine::new(config.data_dir.clone())?;
        
        // Initialize transaction manager (performs crash recovery)
        let txn_manager = TransactionManager::new(config.data_dir.clone())?;
        
        // Perform crash recovery
        txn_manager.recover(&storage)?;
        
        // Create executor with storage and transaction manager
        // Enable both Granite and Sandstone engines by default
        let sandstone_config = crate::engines::SandstoneConfig::default();
        let executor = Arc::new(Executor::with_sandstone(storage, txn_manager, sandstone_config)?);

        
        Ok(Self { config, executor })
    }

    /// Run the server, accepting connections
    pub async fn run(&self) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(&self.config.bind_addr).await?;
        info!("MiniSQL server listening on {}", self.config.bind_addr);
        info!("Data directory: {:?}", self.config.data_dir);

        loop {
            match listener.accept().await {
                Ok((stream, addr)) => {
                    info!("New connection from {}", addr);
                    // Disable Nagle on the accepted socket to avoid batching small
                    // protocol packets which can introduce ~100ms latency.
                    if let Err(e) = stream.set_nodelay(true) {
                        error!("Failed to set TCP_NODELAY for {}: {}", addr, e);
                    }

                    let executor = Arc::clone(&self.executor);
                    let config = self.config.clone();
                    
                    // Spawn a task to handle this connection
                    tokio::spawn(async move {
                        let handler = ConnectionHandler::new(stream, executor, config);
                        if let Err(e) = handler.run().await {
                            error!("Connection error from {}: {}", addr, e);
                        }
                        info!("Connection closed: {}", addr);
                    });
                }
                Err(e) => {
                    error!("Failed to accept connection: {}", e);
                }
            }
        }
    }
}
