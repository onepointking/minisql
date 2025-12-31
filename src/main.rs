//! MiniSQL - A minimal MySQL-compatible SQL serverfn main() {

//!    println!("Hello, world!");

//! This is an educational implementation demonstrating core database concepts:}

//! - MySQL protocol handling
//! - SQL parsing and execution
//! - ACID transactions with WAL
//! - Concurrent client connections
//! - JSON column support
//!
//! Architecture Overview:
//! ┌─────────────────────────────────────────────────────────────┐
//! │                     MySQL Clients                           │
//! └─────────────────────────────────────────────────────────────┘
//!                              │
//!                              ▼
//! ┌─────────────────────────────────────────────────────────────┐
//! │                   Protocol Handler                          │
//! │         (MySQL wire protocol, authentication)               │
//! └─────────────────────────────────────────────────────────────┘
//!                              │
//!                              ▼
//! ┌─────────────────────────────────────────────────────────────┐
//! │                     SQL Parser                              │
//! │    (Lexer → Parser → AST for SQL subset)                    │
//! └─────────────────────────────────────────────────────────────┘
//!                              │
//!                              ▼
//! ┌─────────────────────────────────────────────────────────────┐
//! │                   Query Executor                            │
//! │         (Plans and executes parsed queries)                 │
//! └─────────────────────────────────────────────────────────────┘
//!                              │
//!                    ┌─────────┴─────────┐
//!                    ▼                   ▼
//! ┌──────────────────────────┐ ┌────────────────────────────────┐
//! │   Transaction Manager    │ │      Storage Engine            │
//! │  (WAL, ACID, isolation)  │ │  (Tables, rows, JSON, files)   │
//! └──────────────────────────┘ └────────────────────────────────┘
//!                    │                   │
//!                    └─────────┬─────────┘
//!                              ▼
//! ┌─────────────────────────────────────────────────────────────┐
//! │                     File System                             │
//! │              (WAL files, table data files)                  │
//! └─────────────────────────────────────────────────────────────┘

use std::path::PathBuf;
// Removed unused imports that were triggering warnings when building the binary.
use log::info;

use minisql::{Config, Server};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Initialize logging
    env_logger::Builder::from_env(
        env_logger::Env::default().default_filter_or("info")
    ).init();

    // Parse command line arguments (simple)
    let args: Vec<String> = std::env::args().collect();
    
    let mut config = Config::default();
    
    // Simple argument parsing
    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "--port" | "-p" => {
                if i + 1 < args.len() {
                    let port: u16 = args[i + 1].parse()?;
                    config.bind_addr = format!("127.0.0.1:{}", port).parse()?;
                    i += 1;
                }
            }
            "--data-dir" | "-d" => {
                if i + 1 < args.len() {
                    config.data_dir = PathBuf::from(&args[i + 1]);
                    i += 1;
                }
            }
            "--user" | "-u" => {
                if i + 1 < args.len() {
                    config.username = args[i + 1].clone();
                    i += 1;
                }
            }
            "--password" | "-P" => {
                if i + 1 < args.len() {
                    config.password = args[i + 1].clone();
                    i += 1;
                }
            }
            "--help" | "-h" => {
                println!("MiniSQL - A minimal MySQL-compatible SQL server");
                println!();
                println!("Usage: minisql [OPTIONS]");
                println!();
                println!("Options:");
                println!("  -p, --port PORT       Port to listen on (default: 3306)");
                println!("  -d, --data-dir DIR    Data directory (default: ./data)");
                println!("  -u, --user USER       Username (default: root)");
                println!("  -P, --password PASS   Password (default: password)");
                println!("  -h, --help            Show this help");
                return Ok(());
            }
            _ => {}
        }
        i += 1;
    }

    info!("Starting MiniSQL server...");
    
    let server = Server::new(config).await?;
    server.run().await?;
    
    Ok(())
}
