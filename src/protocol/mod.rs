//! MySQL Protocol Handler for MiniSQL
//!
//! Implements the MySQL client/server protocol to allow standard MySQL clients to connect.
//!
//! ## Protocol Overview
//!
//! The MySQL protocol consists of:
//! 1. **Handshake**: Server sends greeting → Client responds with auth → Server confirms
//! 2. **Command Phase**: Client sends commands → Server responds with results
//!
//! ## Packet Format
//! ```text
//! +-------------------+------------------+------------------------------------------+
//! | 3 bytes           | 1 byte           | N bytes                                  |
//! | Payload Length    | Sequence ID      | Payload                                  |
//! +-------------------+------------------+------------------------------------------+
//! ```
//!
//! ## Architecture
//!
//! This module is organized into several submodules:
//! - `constants`: MySQL protocol constants
//! - `packet`: Low-level packet I/O and encoding/decoding
//! - `handshake`: Authentication and handshake logic
//! - `resultset`: Result set formatting and sending
//! - `commands`: Query execution and special query handlers
//! - `prepared`: Prepared statement handling and binary protocol

use std::io;
use std::sync::Arc;
use std::time::Instant;
use tokio::net::TcpStream;

use crate::error::{MiniSqlError, Result};
use crate::executor::{Executor, Session};
use crate::Config;

mod constants;
mod packet;
mod handshake;
mod resultset;
mod commands;
mod prepared;

use constants::*;
use packet::PacketIO;
use handshake::HandshakeHandler;
use commands::CommandHandler;
use prepared::PreparedStatementHandler;

// Re-export the build_select_metadata function for tests
pub use prepared::build_select_metadata;

/// Handles a single client connection
pub struct ConnectionHandler {
    io: PacketIO,
    executor: Arc<Executor>,
    config: Config,
    session: Session,
    command_handler: Option<CommandHandler>,
    prepared_handler: Option<PreparedStatementHandler>,
    // Store the original handshake challenge so we can validate COM_CHANGE_USER auth responses
    auth_challenge: [u8; 20],
}

impl ConnectionHandler {
    /// Create a new connection handler
    pub fn new(stream: TcpStream, executor: Arc<Executor>, config: Config) -> Self {
        Self {
            io: PacketIO::new(stream),
            executor,
            config,
            session: Session::new(),
            command_handler: None,
            prepared_handler: None,
            auth_challenge: [0u8; 20],
        }
    }

    /// Run the connection handler (main loop)
    pub async fn run(mut self) -> Result<()> {
        // Perform handshake
        let client_capabilities = self.do_handshake().await?;

        // Initialize handlers with client capabilities
        self.command_handler = Some(CommandHandler::new(
            Arc::clone(&self.executor),
            client_capabilities,
        ));
        self.prepared_handler = Some(PreparedStatementHandler::new(
            Arc::clone(&self.executor),
            client_capabilities,
        ));

        // Command loop
        loop {
            match self.handle_command().await {
                Ok(true) => continue,
                Ok(false) => break, // Client quit
                Err(e) => {
                    log::error!("Command error: {}", e);
                    // Try to send error to client
                    if let Err(send_err) = self.send_error(&e).await {
                        log::error!("Failed to send error: {}", send_err);
                        break;
                    }
                }
            }
        }

        Ok(())
    }

    /// Perform the MySQL handshake
    async fn do_handshake(&mut self) -> Result<u32> {
        let handshake = HandshakeHandler::new();

        // Optionally profile handshake duration
        let profiling = std::env::var("MINISQL_PROFILE").map(|v| v != "0").unwrap_or(false);
        let t0 = if profiling { Some(Instant::now()) } else { None };

        // Send server greeting
        handshake.send_handshake(&mut self.io).await?;

        // Receive and validate client response
        let response = self.io.read_packet().await?;
        let client_capabilities = handshake.parse_and_validate(&response, &self.config)?;

    // keep a copy of the auth challenge so COM_CHANGE_USER can validate client responses
    self.auth_challenge = handshake.auth_data();

        // Send OK packet to confirm authentication
        let result_sender = resultset::ResultSetSender::new(client_capabilities);
        result_sender
            .send_ok(&mut self.io, 0, 0, "", false)
            .await?;

        if let Some(start) = t0 {
            let elapsed = start.elapsed();
            log::info!("Handshake completed in {} ms", elapsed.as_millis());
        }

        log::info!("Client authenticated successfully");
        Ok(client_capabilities)
    }

    /// Handle a single command from the client
    /// Returns Ok(true) to continue, Ok(false) to quit
    async fn handle_command(&mut self) -> Result<bool> {
        let packet = match self.io.read_packet().await {
            Ok(p) => p,
            Err(MiniSqlError::Io(e)) if e.kind() == io::ErrorKind::UnexpectedEof => {
                return Ok(false);
            }
            Err(e) => return Err(e),
        };

        if packet.is_empty() {
            return Ok(false);
        }

        let command = packet[0];
        let data = &packet[1..];

        match command {
            COM_QUIT => {
                log::info!("Client sent QUIT");
                Ok(false)
            }
            COM_PING => {
                self.send_ok(0, 0, "").await?;
                Ok(true)
            }
            COM_INIT_DB => {
                // We ignore database selection (single-database system)
                let db_name = String::from_utf8_lossy(data);
                log::info!("Client selected database: {}", db_name);
                self.send_ok(0, 0, "").await?;
                Ok(true)
            }
            COM_QUERY => {
                let sql = String::from_utf8_lossy(data).to_string();
                // Optionally measure query execution time per command
                let profiling = std::env::var("MINISQL_PROFILE").map(|v| v != "0").unwrap_or(false);
                if profiling {
                    let start = Instant::now();
                    self.command_handler
                        .as_ref()
                        .unwrap()
                        .handle_query(&mut self.io, &sql, &mut self.session)
                        .await?;
                    let elapsed = start.elapsed();
                    log::info!("COM_QUERY finished: sql='{}' time_ms={}", sql, elapsed.as_millis());
                } else {
                    self.command_handler
                        .as_ref()
                        .unwrap()
                        .handle_query(&mut self.io, &sql, &mut self.session)
                        .await?;
                }
                Ok(true)
            }
            COM_CHANGE_USER => {
                // Parse and validate the change-user payload using the original handshake challenge
                let client_caps = self
                    .command_handler
                    .as_ref()
                    .map(|h| h.client_capabilities())
                    .unwrap_or(CLIENT_PROTOCOL_41);

                match handshake::parse_and_validate_change_user(
                    data,
                    client_caps,
                    &self.auth_challenge,
                    &self.config,
                ) {
                    Ok(_) => {
                        // Reset session-scoped state per COM_CHANGE_USER semantics
                        self.session.prepared_statements.clear();
                        self.session.next_stmt_id = 1;
                        self.session.txn_id = None;
                        self.session.last_insert_id = 0;

                        // Acknowledge success
                        self.send_ok(0, 0, "").await?;
                    }
                    Err(e) => {
                        // Send error back to client
                        self.send_error(&e).await?;
                    }
                }

                Ok(true)
            }
            COM_STMT_PREPARE => {
                let sql = String::from_utf8_lossy(data).to_string();
                self.prepared_handler
                    .as_ref()
                    .unwrap()
                    .handle_prepare(&mut self.io, &sql, &mut self.session)
                    .await?;
                Ok(true)
            }
            COM_STMT_EXECUTE => {
                self.prepared_handler
                    .as_ref()
                    .unwrap()
                    .handle_execute(&mut self.io, data, &mut self.session)
                    .await?;
                Ok(true)
            }
            COM_STMT_CLOSE => {
                self.prepared_handler
                    .as_ref()
                    .unwrap()
                    .handle_close(data, &mut self.session)?;
                Ok(true)
            }
            COM_STMT_RESET => {
                // Reset is a no-op for us (no cursor state to reset)
                self.send_ok(0, 0, "").await?;
                Ok(true)
            }
            COM_FIELD_LIST => {
                // Not supported, send empty result
                self.send_eof().await?;
                Ok(true)
            }
            COM_SET_OPTION => {
                // Ignore option changes
                self.send_ok(0, 0, "").await?;
                Ok(true)
            }
            _ => {
                log::warn!("Unknown command: 0x{:02X}", command);
                self.send_error(&MiniSqlError::Protocol(format!(
                    "Unknown command: 0x{:02X}",
                    command
                )))
                .await?;
                Ok(true)
            }
        }
    }

    /// Send an OK packet
    async fn send_ok(&mut self, affected_rows: u64, last_insert_id: u64, info: &str) -> Result<()> {
        self.command_handler
            .as_ref()
            .unwrap()
            .result_sender()
            .send_ok(
                &mut self.io,
                affected_rows,
                last_insert_id,
                info,
                self.session.txn_id.is_some(),
            )
            .await
    }

    /// Send an EOF packet
    async fn send_eof(&mut self) -> Result<()> {
        self.command_handler
            .as_ref()
            .unwrap()
            .result_sender()
            .send_eof(&mut self.io, self.session.txn_id.is_some())
            .await
    }

    /// Send an error packet
    async fn send_error(&mut self, error: &MiniSqlError) -> Result<()> {
        let mut packet = Vec::new();

        // Header (ERR)
        packet.push(ERR_PACKET);

        // Error code (2 bytes)
        packet.extend_from_slice(&error.mysql_error_code().to_le_bytes());

        // Get client capabilities to determine packet format
        let client_capabilities = if let Some(handler) = &self.command_handler {
            // Use the actual client capabilities from the command handler
            handler.client_capabilities()
        } else {
            CLIENT_PROTOCOL_41
        };

        if client_capabilities & CLIENT_PROTOCOL_41 != 0 {
            // SQL state marker
            packet.push(b'#');
            // SQL state (5 bytes)
            packet.extend_from_slice(error.sql_state().as_bytes());
        }

        // Error message
        packet.extend_from_slice(error.to_string().as_bytes());

        self.io.write_packet(&packet).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::Executor;
    use crate::parser::Parser;
    use crate::storage::StorageEngine;
    use crate::engines::TransactionManager;
    use tempfile::tempdir;

    fn setup() -> (Arc<Executor>, Config) {
        let dir = tempdir().unwrap();
        let storage = StorageEngine::new(dir.path().to_path_buf()).unwrap();
        let txn_manager = TransactionManager::new(dir.path().to_path_buf()).unwrap();
        let executor = Executor::new(storage, txn_manager);

        let exec_arc = Arc::new(executor);
        let mut cfg = Config::default();
        cfg.data_dir = dir.path().to_path_buf();

        (exec_arc, cfg)
    }

    #[test]
    fn test_prepare_select_star_simple() {
        let (executor, _cfg) = setup();
        let mut _session = Session::new();

        // Create table schema in-memory to avoid filesystem IO in unit test
        let schema = crate::types::TableSchema {
            name: "prep_test".to_string(),
            columns: vec![
                crate::types::ColumnDef {
                    name: "id".to_string(),
                    data_type: crate::types::DataType::Integer,
                    nullable: false,
                    default: None,
                    primary_key: true,
                    auto_increment: false,
                },
                crate::types::ColumnDef {
                    name: "name".to_string(),
                    data_type: crate::types::DataType::Text,
                    nullable: false,
                    default: None,
                    primary_key: false,
                    auto_increment: false,
                },
                crate::types::ColumnDef {
                    name: "value".to_string(),
                    data_type: crate::types::DataType::Integer,
                    nullable: false,
                    default: None,
                    primary_key: false,
                    auto_increment: false,
                },
            ],
            auto_increment_counter: 1, engine_type: crate::engines::EngineType::default(),
        };
        (&*executor)
            .storage
            .create_table_in_memory(schema)
            .unwrap();

        let stmt = Parser::parse("SELECT * FROM prep_test").unwrap();
        if let crate::parser::Statement::Select(s) = stmt {
            let (count, names, types) = build_select_metadata(&*executor, &s).unwrap();
            assert_eq!(count, 3);
            assert_eq!(names, vec!["id", "name", "value"]);
            use crate::types::DataType;
            assert_eq!(
                types,
                vec![DataType::Integer, DataType::Text, DataType::Integer]
            );
        } else {
            panic!("Not a select")
        }
    }

    #[tokio::test]
    async fn test_com_change_user_clears_prepared_statements() {
        let (executor, mut cfg) = setup();
        // Allow empty password so we can avoid computing auth_response in the test
        cfg.password = "".to_string();

        // Start a listener and spawn the connection handler for a single connection
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let exec_arc = Arc::clone(&executor);
        let cfg_server = cfg.clone();
        let server_task = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept failed");
            let handler = ConnectionHandler::new(stream, exec_arc, cfg_server);
            // Run handler - it will return when client closes connection
            handler.run().await.expect("handler run failed");
        });

        // Connect as client
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        stream.set_nodelay(true).unwrap();
        let mut client = PacketIO::new(stream);

        // Read server handshake
        let _handshake = client.read_packet().await.unwrap();

        // Send handshake response with empty auth (matches cfg.password="")
        let client_caps: u32 = CLIENT_PROTOCOL_41 | CLIENT_SECURE_CONNECTION | CLIENT_PLUGIN_AUTH;
        let mut resp = Vec::new();
        resp.extend_from_slice(&client_caps.to_le_bytes()); // capabilities
        resp.extend_from_slice(&0u32.to_le_bytes()); // max packet size
        resp.push(45u8); // charset
        resp.extend_from_slice(&[0u8; 23]); // reserved
        resp.extend_from_slice(b"root\0"); // username
        // auth response: secure connection -> 1-byte length (0)
        resp.push(0u8);
        // database (empty)
        resp.push(0u8);
        // auth plugin name
        resp.extend_from_slice(b"mysql_native_password\0");

        client.write_packet(&resp).await.unwrap();

        // Expect OK from server after handshake
        let ok = client.read_packet().await.unwrap();
        assert_eq!(ok[0], OK_PACKET);

        // Send COM_STMT_PREPARE to create a prepared statement in the session
        let mut prep = Vec::new();
        prep.push(COM_STMT_PREPARE);
        prep.extend_from_slice(b"SELECT 1");
        client.write_packet(&prep).await.unwrap();

        // Read response packets from server until we see COM_STMT_PREPARE_OK (0x00)
        let mut found_prepare_ok = false;
        for _ in 0..10 {
            let pkt = client.read_packet().await.unwrap();
            if !pkt.is_empty() && pkt[0] == 0x00 {
                found_prepare_ok = true;
                break;
            }
        }
        assert!(found_prepare_ok, "did not receive COM_STMT_PREPARE_OK from server");

        // Drain any remaining packets from the server that were produced by the prepare step.
        // We attempt to read packets with a short timeout until no more data is available.
        use tokio::time::{timeout, Duration};
        loop {
            match timeout(Duration::from_millis(50), client.read_packet()).await {
                Ok(Ok(_pkt)) => {
                    // consumed a packet; continue draining
                    continue;
                }
                Ok(Err(e)) => {
                    panic!("error reading packet while draining: {}", e);
                }
                Err(_) => {
                    // timeout: no more immediate packets to read
                    break;
                }
            }
        }

        // Now send COM_CHANGE_USER (0x11) with empty auth and empty database
        let mut change = Vec::new();
        change.push(COM_CHANGE_USER);
        change.extend_from_slice(b"root\0");
        // secure connection -> auth length byte 0
        change.push(0u8);
        // database (empty)
        change.push(0u8);
        client.write_packet(&change).await.unwrap();

        // Expect OK packet from server acknowledging change user
        let pkt2 = client.read_packet().await.unwrap();
        assert_eq!(pkt2[0], OK_PACKET);

        // Attempt to execute the previously prepared statement id=1
        let mut exec = Vec::new();
        exec.push(COM_STMT_EXECUTE);
        exec.extend_from_slice(&1u32.to_le_bytes()); // statement id 1
        exec.push(0u8); // flags
        exec.extend_from_slice(&1u32.to_le_bytes()); // iteration count
        client.write_packet(&exec).await.unwrap();

        // Expect an ERR packet since prepared statements should have been cleared
        let pkt3 = client.read_packet().await.unwrap();
        assert_eq!(pkt3[0], ERR_PACKET);

        // Close client and wait for server task to finish
        drop(client);
        // Give server a moment to exit gracefully
        let _ = tokio::time::timeout(std::time::Duration::from_millis(200), server_task).await;
    }

    #[test]
    fn test_prepare_select_exprs_and_alias() {
        let (executor, _cfg) = setup();
        let mut _session = Session::new();

        let schema2 = crate::types::TableSchema {
            name: "prep_test2".to_string(),
            columns: vec![
                crate::types::ColumnDef {
                    name: "id".to_string(),
                    data_type: crate::types::DataType::Integer,
                    nullable: false,
                    default: None,
                    primary_key: true,
                    auto_increment: false,
                },
                crate::types::ColumnDef {
                    name: "name".to_string(),
                    data_type: crate::types::DataType::Text,
                    nullable: false,
                    default: None,
                    primary_key: false,
                    auto_increment: false,
                },
            ],
            auto_increment_counter: 1, engine_type: crate::engines::EngineType::default(),
        };
        (&*executor)
            .storage
            .create_table_in_memory(schema2)
            .unwrap();

        let stmt = Parser::parse("SELECT id, name AS username FROM prep_test2").unwrap();
        if let crate::parser::Statement::Select(s) = stmt {
            let (_count, names, _types) = build_select_metadata(&*executor, &s).unwrap();
            assert_eq!(names, vec!["id", "username"]);
        } else {
            panic!("Not a select")
        }
    }

    #[test]
    fn test_prepare_count_star() {
        let (executor, _cfg) = setup();
        let mut _session = Session::new();

        let items_schema = crate::types::TableSchema {
            name: "items".to_string(),
            columns: vec![
                crate::types::ColumnDef {
                    name: "id".to_string(),
                    data_type: crate::types::DataType::Integer,
                    nullable: false,
                    default: None,
                    primary_key: true,
                    auto_increment: false,
                },
                crate::types::ColumnDef {
                    name: "value".to_string(),
                    data_type: crate::types::DataType::Integer,
                    nullable: false,
                    default: None,
                    primary_key: false,
                    auto_increment: false,
                },
            ],
            auto_increment_counter: 1, engine_type: crate::engines::EngineType::default(),
        };
        (&*executor)
            .storage
            .create_table_in_memory(items_schema)
            .unwrap();

        let stmt = Parser::parse("SELECT COUNT(*) FROM items").unwrap();
        if let crate::parser::Statement::Select(s) = stmt {
            let (_count, names, types) = build_select_metadata(&*executor, &s).unwrap();
            assert_eq!(names, vec!["COUNT(*)"]);
            use crate::types::DataType;
            assert_eq!(types, vec![DataType::Integer]);
        } else {
            panic!("Not a select")
        }
    }

    #[test]
    fn test_prepare_select_join_star() {
        let (executor, _cfg) = setup();
        let mut _session = Session::new();

        let t1_schema = crate::types::TableSchema {
            name: "t1".to_string(),
            columns: vec![
                crate::types::ColumnDef {
                    name: "id".to_string(),
                    data_type: crate::types::DataType::Integer,
                    nullable: false,
                    default: None,
                    primary_key: true,
                    auto_increment: false,
                },
                crate::types::ColumnDef {
                    name: "a".to_string(),
                    data_type: crate::types::DataType::Text,
                    nullable: false,
                    default: None,
                    primary_key: false,
                    auto_increment: false,
                },
            ],
            auto_increment_counter: 1, engine_type: crate::engines::EngineType::default(),
        };
        let t2_schema = crate::types::TableSchema {
            name: "t2".to_string(),
            columns: vec![
                crate::types::ColumnDef {
                    name: "id".to_string(),
                    data_type: crate::types::DataType::Integer,
                    nullable: false,
                    default: None,
                    primary_key: true,
                    auto_increment: false,
                },
                crate::types::ColumnDef {
                    name: "b".to_string(),
                    data_type: crate::types::DataType::Text,
                    nullable: false,
                    default: None,
                    primary_key: false,
                    auto_increment: false,
                },
                crate::types::ColumnDef {
                    name: "t1_id".to_string(),
                    data_type: crate::types::DataType::Integer,
                    nullable: false,
                    default: None,
                    primary_key: false,
                    auto_increment: false,
                },
            ],
            auto_increment_counter: 1, engine_type: crate::engines::EngineType::default(),
        };
        (&*executor)
            .storage
            .create_table_in_memory(t1_schema)
            .unwrap();
        (&*executor)
            .storage
            .create_table_in_memory(t2_schema)
            .unwrap();

        let stmt = Parser::parse("SELECT * FROM t1 JOIN t2 ON t1.id = t2.t1_id").unwrap();
        if let crate::parser::Statement::Select(s) = stmt {
            let (_count, names, _types) = build_select_metadata(&*executor, &s).unwrap();
            // Expect names prefixed with table alias (lowercased)
            assert!(names.contains(&"t1.id".to_string()));
            assert!(names.contains(&"t1.a".to_string()));
            assert!(names.contains(&"t2.id".to_string()));
            assert!(names.contains(&"t2.b".to_string()));
        } else {
            panic!("Not a select")
        }
    }
}
