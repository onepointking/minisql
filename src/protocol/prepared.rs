//! Prepared statement handling and binary protocol

use std::sync::Arc;

use crate::error::{MiniSqlError, Result};
use crate::executor::{Executor, PreparedStatement, Session};
use crate::parser::{self, Parser};
use crate::types::{DataType, QueryResult, TableSchema, Value};

use super::constants::*;
use super::packet::{LenencInt, LenencString, PacketIO};
use super::resultset::ResultSetSender;

/// Handles MySQL prepared statements
pub struct PreparedStatementHandler {
    executor: Arc<Executor>,
    result_sender: ResultSetSender,
}

impl PreparedStatementHandler {
    /// Create a new prepared statement handler
    pub fn new(executor: Arc<Executor>, client_capabilities: u32) -> Self {
        Self {
            executor,
            result_sender: ResultSetSender::new(client_capabilities),
        }
    }

    /// Handle COM_STMT_PREPARE - prepare a statement for execution
    pub async fn handle_prepare(
        &self,
        io: &mut PacketIO,
        sql: &str,
        session: &mut Session,
    ) -> Result<()> {
        log::debug!("Preparing statement: {}", sql);

        // Parse the SQL and count placeholders
        let (statement, param_count) = Parser::parse_prepared(sql)?;

        // Assign a statement ID
        let stmt_id = session.next_stmt_id;
        session.next_stmt_id += 1;

        // Determine column info for SELECT statements
        let (column_count, column_names, column_types) = match &statement {
            parser::Statement::Select(select) => {
                build_select_metadata(&self.executor, select)?
            }
            _ => (0, vec![], vec![]),
        };

        // Store the prepared statement
        let prepared = PreparedStatement {
            id: stmt_id,
            sql: sql.to_string(),
            statement,
            param_count,
            column_types: column_types.clone(),
            column_names: column_names.clone(),
        };
        session.prepared_statements.insert(stmt_id, prepared);

        // Send COM_STMT_PREPARE_OK response
        self.send_prepare_ok(io, stmt_id, column_count as u16, param_count as u16)
            .await?;

        // Send parameter definitions if any
        for i in 0..param_count {
            self.send_param_definition(io, &format!("param_{}", i))
                .await?;
        }
        if param_count > 0 {
            self.result_sender
                .send_eof(io, session.txn_id.is_some())
                .await?;
        }

        // Send column definitions if any
        for i in 0..column_count {
            let name = column_names.get(i).map(|s| s.as_str()).unwrap_or("?");
            let dtype = column_types.get(i).unwrap_or(&DataType::Text);
            self.send_column_definition(io, name, dtype).await?;
        }
        if column_count > 0 {
            self.result_sender
                .send_eof(io, session.txn_id.is_some())
                .await?;
        }

        log::debug!(
            "Prepared statement {} with {} params, {} columns",
            stmt_id,
            param_count,
            column_count
        );
        Ok(())
    }

    /// Handle COM_STMT_EXECUTE - execute a prepared statement with parameters
    pub async fn handle_execute(
        &self,
        io: &mut PacketIO,
        data: &[u8],
        session: &mut Session,
    ) -> Result<()> {
        if data.len() < 9 {
            return Err(MiniSqlError::Protocol(
                "COM_STMT_EXECUTE packet too short".into(),
            ));
        }

        // Parse the execute packet
        let stmt_id = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        let _flags = data[4]; // cursor flags, we ignore these
        let _iteration_count = u32::from_le_bytes([data[5], data[6], data[7], data[8]]);

        log::debug!("Executing prepared statement {}", stmt_id);

        // Get the prepared statement
        let prepared = session
            .prepared_statements
            .get(&stmt_id)
            .ok_or_else(|| {
                MiniSqlError::Protocol(format!("Unknown prepared statement ID: {}", stmt_id))
            })?
            .clone();

        // Parse parameters if any
        let params = if prepared.param_count > 0 {
            parse_execute_params(data, prepared.param_count)?
        } else {
            vec![]
        };

        log::debug!("Parsed {} parameters: {:?}", params.len(), params);

        // Substitute parameters into the statement
        let stmt_with_params = substitute_statement_params(&prepared.statement, &params)?;

        // Execute the statement
        let result = self.executor.execute(stmt_with_params, session)?;

        // Send result using binary protocol for prepared statements
        match result {
            QueryResult::Select(result_set) => {
                self.result_sender.send_binary_result_set(io, &result_set).await?;
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

    /// Handle COM_STMT_CLOSE - close a prepared statement
    pub fn handle_close(&self, data: &[u8], session: &mut Session) -> Result<()> {
        if data.len() < 4 {
            return Err(MiniSqlError::Protocol(
                "COM_STMT_CLOSE packet too short".into(),
            ));
        }

        let stmt_id = u32::from_le_bytes([data[0], data[1], data[2], data[3]]);
        log::debug!("Closing prepared statement {}", stmt_id);

        // Remove from session
        session.prepared_statements.remove(&stmt_id);

        // COM_STMT_CLOSE doesn't send a response
        Ok(())
    }

    /// Send COM_STMT_PREPARE_OK response
    async fn send_prepare_ok(
        &self,
        io: &mut PacketIO,
        stmt_id: u32,
        num_columns: u16,
        num_params: u16,
    ) -> Result<()> {
        let mut packet = Vec::new();

        // Status [00] OK
        packet.push(0x00);

        // Statement ID (4 bytes)
        packet.extend_from_slice(&stmt_id.to_le_bytes());

        // Number of columns (2 bytes)
        packet.extend_from_slice(&num_columns.to_le_bytes());

        // Number of params (2 bytes)
        packet.extend_from_slice(&num_params.to_le_bytes());

        // Reserved (1 byte)
        packet.push(0x00);

        // Warning count (2 bytes)
        packet.extend_from_slice(&0u16.to_le_bytes());

        io.write_packet(&packet).await
    }

    /// Send parameter definition packet
    async fn send_param_definition(&self, io: &mut PacketIO, name: &str) -> Result<()> {
        self.send_column_definition(io, name, &DataType::Text)
            .await
    }

    /// Send a column definition packet
    async fn send_column_definition(
        &self,
        io: &mut PacketIO,
        name: &str,
        data_type: &DataType,
    ) -> Result<()> {
        let mut packet = Vec::new();

        // Catalog (lenenc string) - always "def"
        LenencString::write(&mut packet, "def");

        // Schema (lenenc string)
        LenencString::write(&mut packet, "minisql");

        // Virtual table (lenenc string)
        LenencString::write(&mut packet, "");

        // Physical table (lenenc string)
        LenencString::write(&mut packet, "");

        // Virtual column (lenenc string)
        LenencString::write(&mut packet, name);

        // Physical column (lenenc string)
        LenencString::write(&mut packet, name);

        // Fixed length fields marker
        packet.push(0x0C);

        // Character set (2 bytes) - binary for numeric types, utf8mb4 for text
        let charset: u16 = match data_type {
            DataType::Integer | DataType::Float | DataType::Boolean => 63, // binary
            DataType::Varchar(_) | DataType::Text => 45, // utf8mb4
            DataType::Json => 45, // utf8mb4
        };
        packet.extend_from_slice(&charset.to_le_bytes());

        // Column length (4 bytes)
        let col_len: u32 = match data_type {
            DataType::Integer => 11,
            DataType::Float => 22,
            DataType::Varchar(Some(len)) => *len,
            DataType::Varchar(None) => 255,
            DataType::Text => 65535,
            DataType::Boolean => 1,
            DataType::Json => 1073741824,
        };
        packet.extend_from_slice(&col_len.to_le_bytes());

        // Column type (1 byte)
        let col_type = match data_type {
            DataType::Integer => MYSQL_TYPE_LONGLONG,
            DataType::Float => MYSQL_TYPE_DOUBLE,
            DataType::Varchar(_) => MYSQL_TYPE_VAR_STRING,
            DataType::Text => MYSQL_TYPE_BLOB,
            DataType::Boolean => MYSQL_TYPE_TINY,
            DataType::Json => MYSQL_TYPE_JSON,
        };
        packet.push(col_type);

        // Flags (2 bytes)
        let flags: u16 = match data_type {
            DataType::Integer => NUM_FLAG,
            DataType::Float => NUM_FLAG,
            DataType::Boolean => NUM_FLAG,
            DataType::Varchar(_) | DataType::Text | DataType::Json => 0,
        };
        packet.extend_from_slice(&flags.to_le_bytes());

        // Decimals (1 byte)
        packet.push(0);

        // Filler (2 bytes)
        packet.extend_from_slice(&0u16.to_le_bytes());

        io.write_packet(&packet).await
    }
}

/// Build column metadata (count, names, types) for a SELECT statement
pub fn build_select_metadata(
    executor: &Executor,
    select: &parser::SelectStmt,
) -> Result<(usize, Vec<String>, Vec<DataType>)> {
    // Simple SELECT (no joins): expand * using table schema
    if select.joins.is_empty() {
        let (schema, table_alias) = if let Some(ref from) = select.from {
            let schema = executor.storage.get_schema(&from.name)?;
            let table_alias = from.effective_name().to_string();
            (schema, table_alias)
        } else {
            (TableSchema { name: "dual".to_string(), columns: Vec::new(), auto_increment_counter: 1, engine_type: crate::engines::EngineType::default() }, "dual".to_string())
        };
        let (names, types) =
            crate::executor::schema::resolve_select_columns_simple(&select.columns, &schema, &table_alias)?;
        return Ok((names.len(), names, types));
    }

    // Joins present: build JoinTableInfo and resolve
    let mut join_info = crate::executor::schema::JoinTableInfo::new();
    if let Some(ref from) = select.from {
        if let Ok(from_schema) = executor.storage.get_schema(&from.name) {
            let from_alias = from.effective_name().to_string();
            join_info.add_table(&from_alias, from_schema)?;
        }
    }
    for j in &select.joins {
        if let Ok(jschema) = executor.storage.get_schema(&j.table.name) {
            let jalias = j.table.effective_name().to_string();
            join_info.add_table(&jalias, jschema)?;
        }
    }

    let (names, types) =
        crate::executor::schema::resolve_select_columns_join(&select.columns, &join_info)?;
    Ok((names.len(), names, types))
}

/// Parse parameters from COM_STMT_EXECUTE packet
fn parse_execute_params(data: &[u8], param_count: usize) -> Result<Vec<Value>> {
    if param_count == 0 {
        return Ok(vec![]);
    }

    let mut pos = 9; // Skip header (4 stmt_id + 1 flags + 4 iteration_count)

    // NULL bitmap
    let null_bitmap_len = (param_count + 7) / 8;
    if pos + null_bitmap_len > data.len() {
        return Err(MiniSqlError::Protocol("Truncated NULL bitmap".into()));
    }
    let null_bitmap = &data[pos..pos + null_bitmap_len];
    pos += null_bitmap_len;

    // new-params-bound-flag
    if pos >= data.len() {
        return Err(MiniSqlError::Protocol(
            "Missing new-params-bound-flag".into(),
        ));
    }
    let new_params_bound = data[pos];
    pos += 1;

    let mut param_types = Vec::new();
    if new_params_bound == 1 {
        // Read parameter types
        for _ in 0..param_count {
            if pos + 2 > data.len() {
                return Err(MiniSqlError::Protocol("Truncated parameter types".into()));
            }
            let type_byte = data[pos];
            let _unsigned_flag = data[pos + 1];
            param_types.push(type_byte);
            pos += 2;
        }
    } else {
        // Use default types (assume all strings)
        param_types = vec![MYSQL_TYPE_VAR_STRING; param_count];
    }

    // Read parameter values
    let mut params = Vec::new();
    for i in 0..param_count {
        // Check NULL bitmap
        let byte_idx = i / 8;
        let bit_idx = i % 8;
        if (null_bitmap[byte_idx] >> bit_idx) & 1 == 1 {
            params.push(Value::Null);
            continue;
        }

        let type_byte = param_types[i];
        let value = read_binary_value(type_byte, &data[pos..])?;
        pos += binary_value_length(type_byte, &data[pos..]);
        params.push(value);
    }

    Ok(params)
}

/// Read a value from binary format
fn read_binary_value(type_byte: u8, data: &[u8]) -> Result<Value> {
    match type_byte {
        MYSQL_TYPE_TINY => {
            if data.is_empty() {
                return Err(MiniSqlError::Protocol("Missing TINY value".into()));
            }
            Ok(Value::Integer(data[0] as i64))
        }
        MYSQL_TYPE_SHORT => {
            if data.len() < 2 {
                return Err(MiniSqlError::Protocol("Missing SHORT value".into()));
            }
            let val = i16::from_le_bytes([data[0], data[1]]);
            Ok(Value::Integer(val as i64))
        }
        MYSQL_TYPE_LONG | MYSQL_TYPE_INT24 => {
            if data.len() < 4 {
                return Err(MiniSqlError::Protocol("Missing LONG value".into()));
            }
            let val = i32::from_le_bytes([data[0], data[1], data[2], data[3]]);
            Ok(Value::Integer(val as i64))
        }
        MYSQL_TYPE_LONGLONG => {
            if data.len() < 8 {
                return Err(MiniSqlError::Protocol("Missing LONGLONG value".into()));
            }
            let val = i64::from_le_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ]);
            Ok(Value::Integer(val))
        }
        MYSQL_TYPE_FLOAT => {
            if data.len() < 4 {
                return Err(MiniSqlError::Protocol("Missing FLOAT value".into()));
            }
            let val = f32::from_le_bytes([data[0], data[1], data[2], data[3]]);
            Ok(Value::Float(val as f64))
        }
        MYSQL_TYPE_DOUBLE => {
            if data.len() < 8 {
                return Err(MiniSqlError::Protocol("Missing DOUBLE value".into()));
            }
            let val = f64::from_le_bytes([
                data[0], data[1], data[2], data[3], data[4], data[5], data[6], data[7],
            ]);
            Ok(Value::Float(val))
        }
        MYSQL_TYPE_VARCHAR
        | MYSQL_TYPE_VAR_STRING
        | MYSQL_TYPE_STRING
        | MYSQL_TYPE_BLOB
        | MYSQL_TYPE_DECIMAL => {
            // Length-encoded string
            let (len, bytes_read) = LenencInt::read(data)?;
            let start = bytes_read;
            let end = start + len as usize;
            if data.len() < end {
                return Err(MiniSqlError::Protocol("Truncated string value".into()));
            }
            let s = String::from_utf8_lossy(&data[start..end]).to_string();
            Ok(Value::String(s))
        }
        MYSQL_TYPE_NULL => Ok(Value::Null),
        _ => {
            // For unknown types, try to read as string
            let (len, bytes_read) = LenencInt::read(data)?;
            let start = bytes_read;
            let end = start + len as usize;
            if data.len() < end {
                return Err(MiniSqlError::Protocol("Truncated value".into()));
            }
            let s = String::from_utf8_lossy(&data[start..end]).to_string();
            Ok(Value::String(s))
        }
    }
}

/// Calculate length of a binary value
fn binary_value_length(type_byte: u8, data: &[u8]) -> usize {
    match type_byte {
        MYSQL_TYPE_TINY => 1,
        MYSQL_TYPE_SHORT => 2,
        MYSQL_TYPE_LONG | MYSQL_TYPE_INT24 => 4,
        MYSQL_TYPE_LONGLONG => 8,
        MYSQL_TYPE_FLOAT => 4,
        MYSQL_TYPE_DOUBLE => 8,
        MYSQL_TYPE_VARCHAR
        | MYSQL_TYPE_VAR_STRING
        | MYSQL_TYPE_STRING
        | MYSQL_TYPE_BLOB
        | MYSQL_TYPE_DECIMAL
        | MYSQL_TYPE_NULL => {
            // Length-encoded string
            if let Ok((len, bytes_read)) = LenencInt::read(data) {
                bytes_read + len as usize
            } else {
                0
            }
        }
        _ => {
            // Unknown type, try length-encoded
            if let Ok((len, bytes_read)) = LenencInt::read(data) {
                bytes_read + len as usize
            } else {
                0
            }
        }
    }
}

/// Substitute parameters into a statement
fn substitute_statement_params(
    stmt: &parser::Statement,
    params: &[Value],
) -> Result<parser::Statement> {
    use crate::executor::evaluator::substitute_placeholders;
    use parser::*;

    match stmt {
        Statement::Select(select) => {
            let columns: Result<Vec<SelectColumn>> = select
                .columns
                .iter()
                .map(|c| match c {
                    SelectColumn::Star => Ok(SelectColumn::Star),
                    SelectColumn::QualifiedStar { table } => {
                        Ok(SelectColumn::QualifiedStar {
                            table: table.clone(),
                        })
                    }
                    SelectColumn::Expr { expr, alias } => Ok(SelectColumn::Expr {
                        expr: substitute_placeholders(expr, params)?,
                        alias: alias.clone(),
                    }),
                })
                .collect();

            let where_clause = match &select.where_clause {
                Some(expr) => Some(substitute_placeholders(expr, params)?),
                None => None,
            };

            Ok(Statement::Select(SelectStmt {
                columns: columns?,
                from: select.from.clone(),
                joins: select.joins.clone(),
                where_clause,
                group_by: select.group_by.clone(),
                order_by: select.order_by.clone(),
                limit: select.limit,
            }))
        }
        Statement::Insert(insert) => {
            let values: Result<Vec<Vec<Expr>>> = insert
                .values
                .iter()
                .map(|row| {
                    row.iter()
                        .map(|expr| substitute_placeholders(expr, params))
                        .collect()
                })
                .collect();
            Ok(Statement::Insert(InsertStmt {
                table_name: insert.table_name.clone(),
                columns: insert.columns.clone(),
                values: values?,
            }))
        }
        Statement::Update(update) => {
            let assignments: Result<Vec<(String, Expr)>> = update
                .assignments
                .iter()
                .map(|(col, expr)| Ok((col.clone(), substitute_placeholders(expr, params)?)))
                .collect();
            let where_clause = match &update.where_clause {
                Some(expr) => Some(substitute_placeholders(expr, params)?),
                None => None,
            };
            Ok(Statement::Update(UpdateStmt {
                table_name: update.table_name.clone(),
                assignments: assignments?,
                where_clause,
            }))
        }
        Statement::Delete(delete) => {
            let where_clause = match &delete.where_clause {
                Some(expr) => Some(substitute_placeholders(expr, params)?),
                None => None,
            };
            Ok(Statement::Delete(DeleteStmt {
                table_name: delete.table_name.clone(),
                where_clause,
            }))
        }
        // Statements that don't have parameters
        _ => Ok(stmt.clone()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_binary_value_length() {
        assert_eq!(binary_value_length(MYSQL_TYPE_TINY, &[]), 1);
        assert_eq!(binary_value_length(MYSQL_TYPE_SHORT, &[]), 2);
        assert_eq!(binary_value_length(MYSQL_TYPE_LONG, &[]), 4);
        assert_eq!(binary_value_length(MYSQL_TYPE_LONGLONG, &[]), 8);
        assert_eq!(binary_value_length(MYSQL_TYPE_FLOAT, &[]), 4);
        assert_eq!(binary_value_length(MYSQL_TYPE_DOUBLE, &[]), 8);
    }

    #[test]
    fn test_read_binary_value_integer_types() {
        // TINY
        let data = vec![42];
        assert_eq!(
            read_binary_value(MYSQL_TYPE_TINY, &data).unwrap(),
            Value::Integer(42)
        );

        // SHORT
        let data = vec![0xFF, 0x00]; // 255
        assert_eq!(
            read_binary_value(MYSQL_TYPE_SHORT, &data).unwrap(),
            Value::Integer(255)
        );

        // LONG
        let data = vec![0x00, 0x00, 0x01, 0x00]; // 65536
        assert_eq!(
            read_binary_value(MYSQL_TYPE_LONG, &data).unwrap(),
            Value::Integer(65536)
        );

        // LONGLONG
        let data = vec![0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0x7F]; // i64::MAX
        let result = read_binary_value(MYSQL_TYPE_LONGLONG, &data).unwrap();
        assert!(matches!(result, Value::Integer(_)));
    }

    #[test]
    fn test_read_binary_value_float_types() {
        // FLOAT
        let val = 3.14f32;
        let data = val.to_le_bytes().to_vec();
        let result = read_binary_value(MYSQL_TYPE_FLOAT, &data).unwrap();
        if let Value::Float(f) = result {
            assert!((f - 3.14).abs() < 0.01);
        } else {
            panic!("Expected Float value");
        }

        // DOUBLE
        let val = 3.14159265359f64;
        let data = val.to_le_bytes().to_vec();
        let result = read_binary_value(MYSQL_TYPE_DOUBLE, &data).unwrap();
        assert_eq!(result, Value::Float(val));
    }

    #[test]
    fn test_read_binary_value_string() {
        // Length-encoded string: [length, ...bytes]
        let data = vec![5, b'h', b'e', b'l', b'l', b'o'];
        let result = read_binary_value(MYSQL_TYPE_VAR_STRING, &data).unwrap();
        assert_eq!(result, Value::String("hello".to_string()));
    }

    #[test]
    fn test_read_binary_value_null() {
        let result = read_binary_value(MYSQL_TYPE_NULL, &[]).unwrap();
        assert_eq!(result, Value::Null);
    }

    #[test]
    fn test_read_binary_value_truncated() {
        // SHORT but only 1 byte
        let data = vec![42];
        assert!(read_binary_value(MYSQL_TYPE_SHORT, &data).is_err());

        // LONG but only 2 bytes
        let data = vec![42, 0];
        assert!(read_binary_value(MYSQL_TYPE_LONG, &data).is_err());
    }
}
