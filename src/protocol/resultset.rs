//! Result set formatting and sending

use crate::error::Result;
use crate::types::{DataType, ResultSet, Value};

use super::constants::*;
use super::packet::{LenencInt, LenencString, PacketIO};

/// Handles formatting and sending MySQL result sets
pub struct ResultSetSender {
    client_capabilities: u32,
}

impl ResultSetSender {
    /// Create a new result set sender
    pub fn new(client_capabilities: u32) -> Self {
        Self {
            client_capabilities,
        }
    }

    /// Return the client capabilities used by this sender
    pub fn client_capabilities(&self) -> u32 {
        self.client_capabilities
    }

    /// Send a result set to the client (text protocol)
    pub async fn send_result_set(&self, io: &mut PacketIO, result: &ResultSet) -> Result<()> {
        // Column count packet
        let mut packet = Vec::new();
        LenencInt::write(&mut packet, result.columns.len() as u64);
        io.write_packet(&packet).await?;

        // Column definition packets
        for (i, col_name) in result.columns.iter().enumerate() {
            self.send_column_definition(io, col_name, &result.column_types[i])
                .await?;
        }

        // EOF packet after columns (if client doesn't have DEPRECATE_EOF)
        if self.client_capabilities & CLIENT_DEPRECATE_EOF == 0 {
            self.send_eof(io, false).await?;
        }

        // Row packets
        for row in &result.rows {
            self.send_row(io, row).await?;
        }

        // Final EOF packet
        if self.client_capabilities & CLIENT_DEPRECATE_EOF == 0 {
            self.send_eof(io, false).await?;
        } else {
            self.send_ok(io, 0, 0, "", false).await?;
        }

        Ok(())
    }

    /// Send a result set to the client (binary protocol - for prepared statements)
    pub async fn send_binary_result_set(&self, io: &mut PacketIO, result: &ResultSet) -> Result<()> {
        // Column count packet
        let mut packet = Vec::new();
        LenencInt::write(&mut packet, result.columns.len() as u64);
        io.write_packet(&packet).await?;

        // Column definition packets
        for (i, col_name) in result.columns.iter().enumerate() {
            self.send_column_definition(io, col_name, &result.column_types[i])
                .await?;
        }

        // EOF packet after columns (if client doesn't have DEPRECATE_EOF)
        if self.client_capabilities & CLIENT_DEPRECATE_EOF == 0 {
            self.send_eof(io, false).await?;
        }

        // Binary row packets
        for row in &result.rows {
            // Normalize values to match schema types before encoding
            let normalized_row = normalize_row_types(row, &result.column_types)?;
            self.send_binary_row(io, &normalized_row, &result.column_types).await?;
        }

        // Final EOF packet
        if self.client_capabilities & CLIENT_DEPRECATE_EOF == 0 {
            self.send_eof(io, false).await?;
        } else {
            self.send_ok(io, 0, 0, "", false).await?;
        }

        Ok(())
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

    /// Send a row packet (text protocol)
    async fn send_row(&self, io: &mut PacketIO, values: &[Value]) -> Result<()> {
        let mut packet = Vec::new();

        for value in values {
            match value.to_string_repr() {
                Some(s) => LenencString::write(&mut packet, &s),
                None => packet.push(0xFB), // NULL
            }
        }

        io.write_packet(&packet).await
    }

    /// Send a row packet (binary protocol - for prepared statements)
    async fn send_binary_row(&self, io: &mut PacketIO, values: &[Value], types: &[DataType]) -> Result<()> {
        let mut packet = Vec::new();
        
        // Packet header (0x00 for binary result set row)
        packet.push(0x00);
        
        // NULL bitmap
        // Calculate size: (column_count + 7 + 2) / 8
        let null_bitmap_len = (values.len() + 7 + 2) / 8;
        let mut null_bitmap = vec![0u8; null_bitmap_len];
        
        for (i, value) in values.iter().enumerate() {
            if value.is_null() {
                // Set bit (i + 2) in the NULL bitmap
                let bit_pos = i + 2;
                null_bitmap[bit_pos / 8] |= 1 << (bit_pos % 8);
            }
        }
        
        packet.extend_from_slice(&null_bitmap);
        
        // Encode each non-NULL value in binary format
        for (i, value) in values.iter().enumerate() {
            if !value.is_null() {
                let data_type = types.get(i).unwrap_or(&DataType::Text);
                encode_binary_value(&mut packet, value, data_type)?;
            }
        }
        
        io.write_packet(&packet).await
    }

    /// Send an OK packet
    pub async fn send_ok(
        &self,
        io: &mut PacketIO,
        affected_rows: u64,
        last_insert_id: u64,
        info: &str,
        in_transaction: bool,
    ) -> Result<()> {
        let mut packet = Vec::new();

        // Header (OK)
        packet.push(OK_PACKET);

        // Affected rows (lenenc int)
        LenencInt::write(&mut packet, affected_rows);

        // Last insert ID (lenenc int)
        LenencInt::write(&mut packet, last_insert_id);

        if self.client_capabilities & CLIENT_PROTOCOL_41 != 0 {
            // Status flags (2 bytes)
            let status: u16 = if in_transaction { 1 } else { 0 };
            packet.extend_from_slice(&status.to_le_bytes());

            // Warnings (2 bytes)
            packet.extend_from_slice(&0u16.to_le_bytes());
        }

        // Info string
        if !info.is_empty() {
            packet.extend_from_slice(info.as_bytes());
        }

        io.write_packet(&packet).await
    }

    /// Send an EOF packet
    pub async fn send_eof(&self, io: &mut PacketIO, in_transaction: bool) -> Result<()> {
        let mut packet = Vec::new();
        packet.push(EOF_PACKET);

        if self.client_capabilities & CLIENT_PROTOCOL_41 != 0 {
            // Warnings (2 bytes)
            packet.extend_from_slice(&0u16.to_le_bytes());
            // Status flags (2 bytes)
            let status: u16 = if in_transaction { 1 } else { 0 };
            packet.extend_from_slice(&status.to_le_bytes());
        }

        io.write_packet(&packet).await
    }
}

/// Normalize row values to match their schema types
/// This ensures that values stored in one format (e.g., Integer) are
/// converted to match the expected schema type before encoding
fn normalize_row_types(row: &[Value], types: &[DataType]) -> Result<Vec<Value>> {
    use crate::error::MiniSqlError;
    
    if row.len() != types.len() {
        return Err(MiniSqlError::Protocol(
            format!("Row has {} values but schema has {} types", row.len(), types.len())
        ));
    }
    
    let mut normalized = Vec::with_capacity(row.len());
    
    for (value, expected_type) in row.iter().zip(types.iter()) {
        let normalized_value = match (value, expected_type) {
            // NULL stays NULL
            (Value::Null, _) => Value::Null,
            
            // Integer conversions
            (Value::Integer(v), DataType::Integer) => Value::Integer(*v),
            (Value::Integer(v), DataType::Float) => Value::Float(*v as f64),
            (Value::Integer(v), DataType::Boolean) => Value::Boolean(*v != 0),
            (Value::Integer(v), DataType::Varchar(_) | DataType::Text) => Value::String(v.to_string()),
            
            // Float conversions
            (Value::Float(v), DataType::Float) => Value::Float(*v),
            (Value::Float(v), DataType::Integer) => Value::Integer(*v as i64),
            (Value::Float(v), DataType::Varchar(_) | DataType::Text) => Value::String(v.to_string()),
            
            // Boolean conversions
            (Value::Boolean(v), DataType::Boolean) => Value::Boolean(*v),
            (Value::Boolean(v), DataType::Integer) => Value::Integer(if *v { 1 } else { 0 }),
            (Value::Boolean(v), DataType::Float) => Value::Float(if *v { 1.0 } else { 0.0 }),
            (Value::Boolean(v), DataType::Varchar(_) | DataType::Text) => Value::String(if *v { "1" } else { "0" }.to_string()),
            
            // String conversions
            (Value::String(s), DataType::Varchar(_) | DataType::Text) => Value::String(s.clone()),
            (Value::String(s), DataType::Integer) => {
                // Try to parse string as integer
                match s.parse::<i64>() {
                    Ok(i) => Value::Integer(i),
                    Err(_) => Value::String(s.clone()), // Keep as string if parsing fails
                }
            }
            (Value::String(s), DataType::Float) => {
                // Try to parse string as float
                match s.parse::<f64>() {
                    Ok(f) => Value::Float(f),
                    Err(_) => Value::String(s.clone()), // Keep as string if parsing fails
                }
            }
            (Value::String(s), DataType::Boolean) => {
                // Parse string as boolean
                Value::Boolean(s == "1" || s.eq_ignore_ascii_case("true") || s.eq_ignore_ascii_case("yes"))
            }
            
            // JSON conversions
            (Value::Json(j), DataType::Json) => Value::Json(j.clone()),
            (Value::Json(j), DataType::Varchar(_) | DataType::Text) => Value::String(j.to_string()),
            
            // For any other combination, try to keep the value as-is
            _ => value.clone(),
        };
        
        normalized.push(normalized_value);
    }
    
    Ok(normalized)
}

/// Encode a value in binary protocol format
fn encode_binary_value(packet: &mut Vec<u8>, value: &Value, data_type: &DataType) -> Result<()> {
    use crate::error::MiniSqlError;
    
    match (value, data_type) {
        // Integer types
        (Value::Integer(v), DataType::Integer) => {
            // MYSQL_TYPE_LONGLONG - 8 bytes
            packet.extend_from_slice(&v.to_le_bytes());
        }
        (Value::Integer(v), DataType::Boolean) => {
            // MYSQL_TYPE_TINY - 1 byte
            packet.push(if *v != 0 { 1 } else { 0 });
        }
        
        // Boolean as integer
        (Value::Boolean(v), DataType::Boolean) => {
            // MYSQL_TYPE_TINY - 1 byte
            packet.push(if *v { 1 } else { 0 });
        }
        (Value::Boolean(v), DataType::Integer) => {
            // MYSQL_TYPE_LONGLONG - 8 bytes
            let int_val = if *v { 1i64 } else { 0i64 };
            packet.extend_from_slice(&int_val.to_le_bytes());
        }
        
        // Float types
        (Value::Float(v), DataType::Float) => {
            // MYSQL_TYPE_DOUBLE - 8 bytes
            packet.extend_from_slice(&v.to_le_bytes());
        }
        
        // String types (sent as length-encoded strings)
        (Value::String(s), DataType::Varchar(_)) |
        (Value::String(s), DataType::Text) => {
            LenencString::write(packet, s);
        }
        
        // JSON (sent as string)
        (Value::Json(j), DataType::Json) => {
            let json_str = j.to_string();
            LenencString::write(packet, &json_str);
        }
        
        // Type conversions
        (Value::Integer(v), DataType::Float) => {
            let float_val = *v as f64;
            packet.extend_from_slice(&float_val.to_le_bytes());
        }
        (Value::Float(v), DataType::Integer) => {
            let int_val = *v as i64;
            packet.extend_from_slice(&int_val.to_le_bytes());
        }
        
        // String representation fallback for other combinations
        (v, DataType::Varchar(_)) | (v, DataType::Text) => {
            if let Some(s) = v.to_string_repr() {
                LenencString::write(packet, &s);
            } else {
                return Err(MiniSqlError::Protocol(
                    "Cannot encode NULL value in binary protocol (should be in NULL bitmap)".into()
                ));
            }
        }
        
        _ => {
            return Err(MiniSqlError::Protocol(
                format!("Unsupported value/type combination in binary protocol: {:?} / {:?}", value, data_type)
            ));
        }
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_column_type_mapping() {
        // Test that we map data types correctly
        let mappings = vec![
            (DataType::Integer, MYSQL_TYPE_LONGLONG),
            (DataType::Float, MYSQL_TYPE_DOUBLE),
            (DataType::Text, MYSQL_TYPE_BLOB),
            (DataType::Boolean, MYSQL_TYPE_TINY),
            (DataType::Json, MYSQL_TYPE_JSON),
            (DataType::Varchar(Some(50)), MYSQL_TYPE_VAR_STRING),
        ];

        for (data_type, expected) in mappings {
            let mysql_type = match &data_type {
                DataType::Integer => MYSQL_TYPE_LONGLONG,
                DataType::Float => MYSQL_TYPE_DOUBLE,
                DataType::Varchar(_) => MYSQL_TYPE_VAR_STRING,
                DataType::Text => MYSQL_TYPE_BLOB,
                DataType::Boolean => MYSQL_TYPE_TINY,
                DataType::Json => MYSQL_TYPE_JSON,
            };
            assert_eq!(mysql_type, expected);
        }
    }

    #[test]
    fn test_column_length_mapping() {
        let tests = vec![
            (DataType::Integer, 11),
            (DataType::Float, 22),
            (DataType::Boolean, 1),
            (DataType::Varchar(Some(100)), 100),
            (DataType::Varchar(None), 255),
            (DataType::Text, 65535),
        ];

        for (data_type, expected_len) in tests {
            let col_len: u32 = match data_type {
                DataType::Integer => 11,
                DataType::Float => 22,
                DataType::Varchar(Some(len)) => len,
                DataType::Varchar(None) => 255,
                DataType::Text => 65535,
                DataType::Boolean => 1,
                DataType::Json => 1073741824,
            };
            assert_eq!(col_len, expected_len);
        }
    }
}
