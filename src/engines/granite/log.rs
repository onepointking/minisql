//! WAL log record structures and operations

use serde::{Deserialize, Serialize};
use crate::types::{TableSchema, Value};
use super::types::{Lsn, TxnId};

/// A WAL log record
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LogRecord {
    /// Log sequence number
    pub lsn: Lsn,
    /// Transaction ID
    pub txn_id: TxnId,
    /// The operation
    pub op: LogOperation,
    /// Timestamp (Unix epoch millis)
    pub timestamp: u64,
}

/// Operations that can be logged
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum LogOperation {
    /// Transaction started
    Begin,
    /// Transaction committed
    Commit,
    /// Transaction rolled back
    Rollback,
    /// Insert a row
    Insert {
        table: String,
        row_id: u64,
        values: Vec<Value>,
    },
    /// Update a row (with old values for rollback)
    Update {
        table: String,
        row_id: u64,
        old_values: Vec<Value>,
        new_values: Vec<Value>,
    },
    /// Delete a row (with old values for rollback)
    Delete {
        table: String,
        row_id: u64,
        old_values: Vec<Value>,
    },
    /// Create table
    CreateTable {
        schema: TableSchema,
    },
    /// Drop table
    DropTable {
        table: String,
    },
    /// Truncate table
    TruncateTable {
        table: String,
    },
    /// Checkpoint marker
    Checkpoint {
        active_txns: Vec<TxnId>,
    },
}

#[cfg(test)]
mod tests {
    use super::*;


    #[test]
    fn test_log_operation_serialization() {
        let op = LogOperation::Begin;
        let serialized = bincode::serialize(&op).unwrap();
        let deserialized: LogOperation = bincode::deserialize(&serialized).unwrap();
        
        match deserialized {
            LogOperation::Begin => {},
            _ => panic!("Expected Begin operation"),
        }
    }

    #[test]
    fn test_log_record_creation() {
        let record = LogRecord {
            lsn: 1,
            txn_id: 100,
            op: LogOperation::Commit,
            timestamp: 1234567890,
        };
        
        assert_eq!(record.lsn, 1);
        assert_eq!(record.txn_id, 100);
        assert_eq!(record.timestamp, 1234567890);
    }

    #[test]
    fn test_insert_operation() {
        let op = LogOperation::Insert {
            table: "users".to_string(),
            row_id: 1,
            values: vec![Value::Integer(42)],
        };
        
        match op {
            LogOperation::Insert { table, row_id, values } => {
                assert_eq!(table, "users");
                assert_eq!(row_id, 1);
                assert_eq!(values.len(), 1);
            },
            _ => panic!("Expected Insert operation"),
        }
    }

    #[test]
    fn test_update_operation() {
        let op = LogOperation::Update {
            table: "users".to_string(),
            row_id: 1,
            old_values: vec![Value::Integer(10)],
            new_values: vec![Value::Integer(20)],
        };
        
        match op {
            LogOperation::Update { old_values, new_values, .. } => {
                assert_eq!(old_values.len(), 1);
                assert_eq!(new_values.len(), 1);
            },
            _ => panic!("Expected Update operation"),
        }
    }

    #[test]
    fn test_checkpoint_operation() {
        let op = LogOperation::Checkpoint {
            active_txns: vec![1, 2, 3],
        };
        
        match op {
            LogOperation::Checkpoint { active_txns } => {
                assert_eq!(active_txns.len(), 3);
                assert_eq!(active_txns[0], 1);
            },
            _ => panic!("Expected Checkpoint operation"),
        }
    }
}
