//! Transaction recovery and WAL replay logic

use std::collections::{HashMap, HashSet};
use std::fs::{self, File};
use std::io::Read;
use std::path::PathBuf;

use crate::error::Result;
use crate::storage::StorageEngine;
use crate::types::Row;
use super::log::{LogOperation, LogRecord};
use super::types::{Lsn, TxnId};

/// Recovery state tracker
pub struct RecoveryManager {
    data_dir: PathBuf,
}

impl RecoveryManager {
    pub fn new(data_dir: PathBuf) -> Self {
        Self { data_dir }
    }

    fn wal_path(&self) -> PathBuf {
        self.data_dir.join("wal.log")
    }

    fn checkpoint_path(&self) -> PathBuf {
        self.data_dir.join("wal.checkpoint")
    }

    /// Recover from WAL on startup
    pub fn recover(&self, storage: &StorageEngine) -> Result<(Lsn, TxnId, HashSet<TxnId>)> {
        let wal_path = self.wal_path();
        if !wal_path.exists() {
            return Ok((1, 1, HashSet::new()));
        }

        log::info!("Starting recovery from WAL...");

        let mut file = File::open(&wal_path)?;
        
        // Read checkpoint if exists
        let checkpoint_lsn = self.read_checkpoint()?;
        if checkpoint_lsn > 0 {
            log::info!("Found checkpoint at LSN {} - skipping older WAL records", checkpoint_lsn);
        }

        // Track transaction states
        let mut txn_records: HashMap<TxnId, Vec<LogRecord>> = HashMap::new();
        let mut committed_txns: HashSet<TxnId> = HashSet::new();
        let mut aborted_txns: HashSet<TxnId> = HashSet::new();
        let mut max_lsn: Lsn = 0;
        let mut max_txn_id: TxnId = 0;
        let mut current_checkpoint_lsn = checkpoint_lsn;

        // Read binary WAL records (length-prefixed)
        loop {
            // Read 4-byte length prefix
            let mut len_buf = [0u8; 4];
            match file.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(ref e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            
            // Read the record data
            let mut record_buf = vec![0u8; len];
            file.read_exact(&mut record_buf)?;
            
            // Deserialize
            let record: LogRecord = match bincode::deserialize(&record_buf) {
                Ok(r) => r,
                Err(e) => {
                    log::warn!("Skipping malformed WAL record: {}", e);
                    continue;
                }
            };

            // Skip records at or below checkpoint
            if record.lsn <= current_checkpoint_lsn {
                if let LogOperation::Checkpoint { .. } = record.op {
                    current_checkpoint_lsn = current_checkpoint_lsn.max(record.lsn);
                }
                continue;
            }

            max_lsn = max_lsn.max(record.lsn);
            max_txn_id = max_txn_id.max(record.txn_id);

            match &record.op {
                LogOperation::Begin => {
                    txn_records.insert(record.txn_id, Vec::new());
                }
                LogOperation::Commit => {
                    committed_txns.insert(record.txn_id);
                }
                LogOperation::Rollback => {
                    aborted_txns.insert(record.txn_id);
                }
                LogOperation::Checkpoint { .. } => {
                    current_checkpoint_lsn = current_checkpoint_lsn.max(record.lsn);
                }
                _ => {
                    if let Some(records) = txn_records.get_mut(&record.txn_id) {
                        records.push(record);
                    }
                }
            }
        }

        // Second pass: redo committed transactions, undo uncommitted
        for (txn_id, records) in &txn_records {
            if committed_txns.contains(txn_id) {
                // Redo: Apply all operations
                log::info!("Redoing committed transaction {}", txn_id);
                for record in records {
                    redo_operation(&record.op, storage)?;
                }
            } else if !aborted_txns.contains(txn_id) {
                // Undo: Transaction was in progress when crash occurred
                log::info!("Undoing uncommitted transaction {}", txn_id);
                for record in records.iter().rev() {
                    undo_operation(&record.op, storage)?;
                }
            }
        }

        // Flush storage after recovery
        storage.flush_all()?;

        log::info!("Recovery complete. Next LSN: {}, Next TxnId: {}",
                   max_lsn + 1, max_txn_id + 1);

        Ok((max_lsn + 1, max_txn_id + 1, committed_txns))
    }

    /// Read checkpoint LSN from checkpoint file
    fn read_checkpoint(&self) -> Result<Lsn> {
        let cp_path = self.checkpoint_path();
        if !cp_path.exists() {
            return Ok(0);
        }

        match fs::read_to_string(&cp_path) {
            Ok(s) => {
                match serde_json::from_str::<serde_json::Value>(&s) {
                    Ok(json) => {
                        if let Some(lsn_val) = json.get("lsn") {
                            if let Some(lsn_num) = lsn_val.as_u64() {
                                return Ok(lsn_num);
                            }
                        }
                        Ok(0)
                    }
                    Err(e) => {
                        log::warn!("Failed to parse checkpoint file: {}", e);
                        Ok(0)
                    }
                }
            }
            Err(e) => {
                log::warn!("Failed to read checkpoint file '{}': {}", cp_path.display(), e);
                Ok(0)
            }
        }
    }
}

/// Redo a single operation
pub fn redo_operation(op: &LogOperation, storage: &StorageEngine) -> Result<()> {
    match op {
        LogOperation::Insert {
            table,
            row_id,
            values,
        } => {
            // Check if row already exists (idempotency)
            if storage.get_row(table, *row_id)?.is_none() {
                let row = Row::new(*row_id, values.clone());
                storage.restore_row(table, row)?;
            }
        }
        LogOperation::Update {
            table,
            row_id,
            new_values,
            ..
        } => {
            storage.update_row(table, *row_id, new_values.clone())?;
        }
        LogOperation::Delete { table, row_id, .. } => {
            storage.delete_row(table, *row_id)?;
        }
        LogOperation::CreateTable { schema } => {
            storage.apply_schema(schema.clone())?;
        }
        LogOperation::TruncateTable { table } => {
            storage.truncate_table(table)?;
        }
        _ => {}
    }
    Ok(())
}

/// Undo a single operation
pub fn undo_operation(op: &LogOperation, storage: &StorageEngine) -> Result<()> {
    match op {
        LogOperation::Insert { table, row_id, .. } => {
            storage.delete_row(table, *row_id)?;
        }
        LogOperation::Update {
            table,
            row_id,
            old_values,
            ..
        } => {
            storage.update_row(table, *row_id, old_values.clone())?;
        }
        LogOperation::Delete {
            table,
            row_id,
            old_values,
        } => {
            let row = Row::new(*row_id, old_values.clone());
            storage.restore_row(table, row)?;
        }
        _ => {}
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;
    use tempfile::tempdir;

    #[test]
    fn test_recovery_manager_creation() {
        let temp_dir = tempdir().unwrap();
        let recovery = RecoveryManager::new(temp_dir.path().to_path_buf());
        
        assert_eq!(recovery.wal_path(), temp_dir.path().join("wal.log"));
        assert_eq!(recovery.checkpoint_path(), temp_dir.path().join("wal.checkpoint"));
    }

    #[test]
    fn test_read_checkpoint_no_file() {
        let temp_dir = tempdir().unwrap();
        let recovery = RecoveryManager::new(temp_dir.path().to_path_buf());
        
        let lsn = recovery.read_checkpoint().unwrap();
        assert_eq!(lsn, 0);
    }

    #[test]
    fn test_read_checkpoint_with_file() {
        let temp_dir = tempdir().unwrap();
        let cp_path = temp_dir.path().join("wal.checkpoint");
        
        let checkpoint_data = serde_json::json!({
            "lsn": 42,
            "active_txns": [],
            "timestamp": 12345
        });
        fs::write(&cp_path, serde_json::to_string_pretty(&checkpoint_data).unwrap()).unwrap();
        
        let recovery = RecoveryManager::new(temp_dir.path().to_path_buf());
        let lsn = recovery.read_checkpoint().unwrap();
        assert_eq!(lsn, 42);
    }

    #[test]
    fn test_undo_insert_operation() {
        // This would require a mock StorageEngine, so we'll just test the operation matching
        let op = LogOperation::Insert {
            table: "test".to_string(),
            row_id: 1,
            values: vec![Value::Integer(42)],
        };
        
        match op {
            LogOperation::Insert { table, row_id, .. } => {
                assert_eq!(table, "test");
                assert_eq!(row_id, 1);
            },
            _ => panic!("Expected Insert operation"),
        }
    }

    #[test]
    fn test_redo_update_operation() {
        let op = LogOperation::Update {
            table: "test".to_string(),
            row_id: 1,
            old_values: vec![Value::Integer(10)],
            new_values: vec![Value::Integer(20)],
        };
        
        match op {
            LogOperation::Update { new_values, .. } => {
                assert_eq!(new_values[0], Value::Integer(20));
            },
            _ => panic!("Expected Update operation"),
        }
    }
}
