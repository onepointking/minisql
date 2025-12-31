//! MySQL command handling

use std::sync::Arc;

use crate::error::Result;
use crate::executor::{Executor, Session};
use crate::parser::Parser;
use crate::types::{DataType, QueryResult, ResultSet, Value};

use super::packet::PacketIO;
use super::resultset::ResultSetSender;

/// Handles SQL query execution and special MySQL queries
pub struct CommandHandler {
    executor: Arc<Executor>,
    result_sender: ResultSetSender,
}

impl CommandHandler {
    /// Create a new command handler
    pub fn new(executor: Arc<Executor>, client_capabilities: u32) -> Self {
        Self {
            executor,
            result_sender: ResultSetSender::new(client_capabilities),
        }
    }

    /// Handle a SQL query
    pub async fn handle_query(
        &self,
        io: &mut PacketIO,
        sql: &str,
        session: &mut Session,
    ) -> Result<()> {
        log::debug!("Query: {}", sql);

        // Handle some special queries that MySQL clients send
        let sql_upper = sql.trim().to_uppercase();

        // Handle SET and SELECT variable queries
        if sql_upper.starts_with("SET ") {
            self.result_sender
                .send_ok(io, 0, 0, "", session.txn_id.is_some())
                .await?;
            return Ok(());
        }

        if sql_upper.starts_with("SELECT @@") || sql_upper.starts_with("SELECT VERSION") {
            return self.handle_variable_query(io, sql, session).await;
        }

        if sql_upper == "SELECT DATABASE()" {
            return self.handle_database_query(io, session).await;
        }

        // Parse and execute the SQL
        let stmt = Parser::parse(sql)?;
        let result = self.executor.execute(stmt, session)?;

        // Send result
        match result {
            QueryResult::Select(result_set) => {
                self.result_sender.send_result_set(io, &result_set).await?;
            }
            QueryResult::Modified { rows_affected, last_insert_id } => {
                self.result_sender
                    .send_ok(io, rows_affected, last_insert_id, "", session.txn_id.is_some())
                    .await?;
            }
            QueryResult::Ok => {
                self.result_sender
                    .send_ok(io, 0, 0, "", session.txn_id.is_some())
                    .await?;
            }
            QueryResult::TransactionStarted => {
                self.result_sender
                    .send_ok(io, 0, 0, "", session.txn_id.is_some())
                    .await?;
            }
            QueryResult::TransactionCommitted => {
                self.result_sender
                    .send_ok(io, 0, 0, "", session.txn_id.is_some())
                    .await?;
            }
            QueryResult::TransactionRolledBack => {
                self.result_sender
                    .send_ok(io, 0, 0, "", session.txn_id.is_some())
                    .await?;
            }
        }

        Ok(())
    }

    /// Handle SELECT @@variable queries
    async fn handle_variable_query(
        &self,
        io: &mut PacketIO,
        sql: &str,
        session: &Session,
    ) -> Result<()> {
        // Extract variable name and return appropriate value
        let mut result = ResultSet::new(vec!["@@variable".to_string()], vec![DataType::Text]);

        let sql_lower = sql.to_lowercase();

        if sql_lower.contains("version") {
            result.columns = vec!["version()".to_string()];
            result.add_row(vec![Value::String("5.7.0-MiniSQL".to_string())]);
        } else if sql_lower.contains("autocommit") {
            result.columns = vec!["@@autocommit".to_string()];
            let autocommit = if session.txn_id.is_none() { "1" } else { "0" };
            result.add_row(vec![Value::String(autocommit.to_string())]);
        } else if sql_lower.contains("sql_mode") {
            result.columns = vec!["@@sql_mode".to_string()];
            result.add_row(vec![Value::String(
                "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES".to_string(),
            )]);
        } else if sql_lower.contains("max_allowed_packet") {
            result.columns = vec!["@@max_allowed_packet".to_string()];
            result.add_row(vec![Value::String("67108864".to_string())]);
        } else if sql_lower.contains("character_set") || sql_lower.contains("collation") {
            result.columns = vec!["@@character_set_client".to_string()];
            result.add_row(vec![Value::String("utf8mb4".to_string())]);
        } else {
            result.add_row(vec![Value::Null]);
        }

        self.result_sender.send_result_set(io, &result).await
    }

    /// Handle SELECT DATABASE() query
    async fn handle_database_query(&self, io: &mut PacketIO, _session: &Session) -> Result<()> {
        let mut result = ResultSet::new(vec!["database()".to_string()], vec![DataType::Text]);
        result.add_row(vec![Value::String("minisql".to_string())]);
        self.result_sender.send_result_set(io, &result).await
    }

    /// Get reference to result sender
    pub fn result_sender(&self) -> &ResultSetSender {
        &self.result_sender
    }

    /// Return the client capabilities used by this handler's result sender
    pub fn client_capabilities(&self) -> u32 {
        self.result_sender.client_capabilities()
    }
}

#[cfg(test)]
mod tests {
}