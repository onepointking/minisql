//! Transaction Manager for MiniSQL
//!
//! ## Transaction Engines
//!
//! MiniSQL supports pluggable transaction engines. The default engine is **Granite**,
//! which provides full ACID transactions using Write-Ahead Logging (WAL).
//!
//! ## Granite Engine (Default)
//!
//! The Granite engine implements ACID transactions with high throughput:
//!
//! ### Why Granite?
//! - **Solid**: Full ACID guarantees with crash recovery
//! - **Durable**: Write-ahead logging ensures data survives crashes
//! - **Strong**: High throughput via deferred fsync with commit latches
//!
//! ### Isolation Level: Read Committed
//! - Transactions only see committed data
//! - No dirty reads (reading uncommitted changes from other transactions)
//! - Phantom reads possible (new rows can appear during transaction)
//!
//! ### Implementation
//! 1. BEGIN: Allocate transaction ID, create in-memory change buffer
//! 2. INSERT/UPDATE/DELETE: Log operation to WAL, apply to in-memory data
//! 3. COMMIT: Write COMMIT record, wait for durable fsync, make changes visible
//! 4. ROLLBACK: Discard changes, write ROLLBACK record
//!
//! ### WAL Format
//! Each log entry is binary-encoded (bincode):
//! - 4-byte length prefix
//! - Serialized LogRecord
//!
//! ### Deferred Fsync with Commit Latches
//!
//! For high-throughput workloads, Granite uses PostgreSQL-style deferred fsync:
//! - Writes go to OS buffer immediately (fast)
//! - Fsyncs happen periodically (every `fsync_interval_ms`) or when buffer is full
//! - COMMIT waits on a "commit latch" until the fsync completes
//! - This allows many commits to share a single fsync, dramatically improving throughput
//!
//! ### Recovery
//! On startup:
//! 1. Read WAL from last checkpoint
//! 2. Replay committed transactions
//! 3. Rollback uncommitted transactions
//! 4. Checkpoint to truncate WAL

// Import from sibling modules in granite/
pub use super::types::{Lsn, TxnId, TxnState, Transaction};
pub use super::log::{LogRecord, LogOperation};
pub use super::wal::{GraniteConfig, GraniteWorkerHandle, GraniteWriteRequest, GraniteMessage};
use super::recovery;

use std::collections::{HashMap, HashSet};
use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::error::{MiniSqlError, Result};
use crate::storage::StorageEngine;
use crate::types::{TableSchema, Value};
use crate::engines::EngineType;

// EngineConfig for backward compatibility
#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub engine_type: EngineType,
    pub granite: GraniteConfig,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            engine_type: EngineType::Granite,
            granite: GraniteConfig::default(),
        }
    }
}

impl EngineConfig {
    pub fn granite(config: GraniteConfig) -> Self {
        Self {
            engine_type: EngineType::Granite,
            granite: config,
        }
    }
    
    pub fn engine_type(&self) -> EngineType {
        self.engine_type
    }
}

/// The Transaction Manager
///
/// Manages ACID transactions using the configured engine (Granite by default).
pub struct TransactionManager {
    /// Data directory for WAL files
    data_dir: PathBuf,
    /// Handle to the Granite engine worker task
    granite_worker: GraniteWorkerHandle,
    /// Current LSN (atomic for lock-free reads)
    current_lsn: AtomicU64,
    /// Next transaction ID
    next_txn_id: AtomicU64,
    /// Active transactions (txn_id -> Transaction)
    active_txns: RwLock<HashMap<TxnId, Transaction>>,
    /// Committed transaction IDs (for visibility checks)
    committed_txns: RwLock<HashSet<TxnId>>,
    /// Engine configuration
    config: EngineConfig,
    /// Bytes written since last checkpoint (approximate)
    bytes_since_checkpoint: AtomicU64,
}

impl TransactionManager {
    /// Create a new transaction manager with default Granite engine
    pub fn new(data_dir: PathBuf) -> Result<Self> {
        Self::new_with_engine(data_dir, EngineConfig::default())
    }

    /// Create a new transaction manager with custom Granite config (backward compatible)
    #[allow(deprecated)]
    pub fn new_with_config(data_dir: PathBuf, config: GraniteConfig) -> Result<Self> {
        Self::new_with_engine(data_dir, EngineConfig::granite(config))
    }

    /// Create a new transaction manager with custom engine configuration
    pub fn new_with_engine(data_dir: PathBuf, config: EngineConfig) -> Result<Self> {
        let wal_path = data_dir.join("wal.log");
        let granite_worker = GraniteWorkerHandle::new(wal_path, config.granite.clone())?;

        Ok(Self {
            data_dir,
            granite_worker,
            current_lsn: AtomicU64::new(1),
            next_txn_id: AtomicU64::new(1),
            active_txns: RwLock::new(HashMap::new()),
            committed_txns: RwLock::new(HashSet::new()),
            config,
            bytes_since_checkpoint: AtomicU64::new(0),
        })
    }



    /// Get the checkpoint file path  
    fn checkpoint_path(&self) -> PathBuf {
        self.data_dir.join("wal.checkpoint")
    }

    /// Get current timestamp in milliseconds
    fn timestamp() -> u64 {
        match SystemTime::now().duration_since(UNIX_EPOCH) {
            Ok(dur) => dur.as_millis() as u64,
            Err(_) => 0,
        }
    }

    /// Allocate a new LSN
    fn alloc_lsn(&self) -> Lsn {
        self.current_lsn.fetch_add(1, Ordering::SeqCst)
    }

    /// Write a log record (non-blocking after write, does NOT wait for fsync).
    /// Use `write_log_durable` for operations that require durability guarantee.
    fn write_log(&self, record: LogRecord) -> Result<()> {
        let (tx, rx) = mpsc::sync_channel(1);
        let req = GraniteWriteRequest {
            record: record.clone(),
            responder: tx,
        };
        
        // Estimate bytes written
        let approx_bytes = std::mem::size_of::<LogRecord>() as u64;
        self.bytes_since_checkpoint.fetch_add(approx_bytes, Ordering::Relaxed);
        
        self.granite_worker.sender.send(GraniteMessage::Write(req))
            .map_err(|e| MiniSqlError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Granite worker channel closed: {}", e)
            )))?;
        
        // Wait for write to complete (but NOT fsync in deferred mode)
        rx.recv()
            .map_err(|e| MiniSqlError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Granite worker response lost: {}", e)
            )))?
    }

    /// Write log record and WAIT for durable fsync (for COMMIT and other critical operations).
    /// This guarantees the record is on disk before returning.
    fn write_log_durable(&self, record: LogRecord) -> Result<()> {
        let lsn = record.lsn;
        
        // First, write the record (non-blocking write)
        self.write_log(record)?;
        
        // Then wait for the LSN to become durable
        self.granite_worker.wait_for_durable(lsn)
    }



    /// Send a truncate request to the Granite worker and wait for completion.
    fn truncate_wal_worker(&self) -> Result<()> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.granite_worker.sender.send(GraniteMessage::Truncate(tx))
            .map_err(|e| MiniSqlError::Io(std::io::Error::new(std::io::ErrorKind::Other, format!("Granite worker channel closed: {}", e))))?;

        rx.recv().map_err(|e| MiniSqlError::Io(std::io::Error::new(std::io::ErrorKind::Other, format!("Granite truncate response lost: {}", e))))?
    }

    /// Force an immediate fsync and return the durable LSN.
    /// Useful for checkpointing or before shutdown.
    pub fn force_sync(&self) -> Result<u64> {
        self.granite_worker.force_sync()
    }

    /// Get the current durable LSN (highest LSN that has been fsynced).
    pub fn durable_lsn(&self) -> Lsn {
        self.granite_worker.durable_lsn()
    }

    /// Get the engine type being used
    pub fn engine_type(&self) -> EngineType {
        self.config.engine_type()
    }

    /// Begin a new transaction
    pub fn begin(&self) -> Result<TxnId> {
        let txn_id = self.next_txn_id.fetch_add(1, Ordering::SeqCst);
        let lsn = self.alloc_lsn();

        let record = LogRecord {
            lsn,
            txn_id,
            op: LogOperation::Begin,
            timestamp: Self::timestamp(),
        };

        // BEGIN does not need durable write - if we crash before commit,
        // the transaction is rolled back anyway
        self.write_log(record)?;

        let txn = Transaction::new(txn_id, lsn);
        self.active_txns.write().expect("active_txns lock poisoned").insert(txn_id, txn);

        Ok(txn_id)
    }

    /// Commit a transaction (waits for durable fsync)
    /// 
    /// This is the original commit method, preserved for backward compatibility.
    /// For engine-aware commits, use commit_durable + finalize_commit instead.
    pub fn commit(&self, txn_id: TxnId, storage: &StorageEngine) -> Result<()> {
        self.commit_durable(txn_id)?;
        self.finalize_commit(txn_id, storage)
    }

    /// Perform the durable WAL commit for a transaction.
    /// 
    /// Writes the COMMIT record to WAL and waits for fsync.
    /// This is the expensive part that ensures durability.
    /// 
    /// Returns early if the transaction has no logged operations (empty undo_log).
    pub fn commit_durable(&self, txn_id: TxnId) -> Result<()> {
        // Check transaction exists and is active, and check if there's work to do
        let undo_log_empty = {
            let txns = self.active_txns.read().expect("active_txns lock poisoned");
            let txn = txns.get(&txn_id).ok_or_else(|| {
                MiniSqlError::Transaction(format!("Transaction {} not found", txn_id))
            })?;
            if txn.state != TxnState::Active {
                return Err(MiniSqlError::Transaction(format!(
                    "Transaction {} is not active",
                    txn_id
                )));
            }
            txn.undo_log.is_empty()
        };

        // Skip durable write if no operations were logged
        // This is the key optimization - empty transactions don't need WAL fsync
        if undo_log_empty {
            log::debug!("Transaction {} has empty undo_log, skipping durable WAL write", txn_id);
            return Ok(());
        }

        // Write COMMIT record and WAIT FOR DURABLE FSYNC
        // This is the critical path for durability - we must wait for fsync
        let lsn = self.alloc_lsn();
        let record = LogRecord {
            lsn,
            txn_id,
            op: LogOperation::Commit,
            timestamp: Self::timestamp(),
        };
        self.write_log_durable(record)?;

        Ok(())
    }

    /// Finalize a transaction commit by cleaning up state.
    /// 
    /// This should be called after commit_durable (or after engines have committed).
    /// Handles checkpoint logic and transaction cleanup.
    pub fn finalize_commit(&self, txn_id: TxnId, storage: &StorageEngine) -> Result<()> {
        // Update transaction state
        {
            let mut txns = self.active_txns.write().expect("active_txns lock poisoned");
            if let Some(txn) = txns.get_mut(&txn_id) {
                txn.state = TxnState::Committed;
            }
            txns.remove(&txn_id);
        }

        // Mark as committed
        self.committed_txns.write().expect("committed_txns lock poisoned").insert(txn_id);

        // Check if automatic checkpoint is needed
        let bytes_written = self.bytes_since_checkpoint.load(Ordering::Relaxed);
        if bytes_written > self.config.granite.checkpoint_threshold_bytes {
            // Granite checkpoint threshold reached, triggering automatic checkpoint
            if let Err(_e) = self.checkpoint(storage) {
                // Automatic checkpoint failed (silently continue)
            }
        }

        // If there are no active transactions we can attempt to checkpoint
        // OPTIMIZATION: Only checkpoint if we've accumulated significant log data (>4KB)
        // This prevents fsync loops on empty/Sandstone-only transactions which only write a small BEGIN record
        let active_empty = { self.active_txns.read().unwrap().is_empty() };
        if active_empty && bytes_written > 4096 {
            if let Err(_e) = self.checkpoint(storage) {
                // Immediate post-commit checkpoint failed (silently continue)
            }
        }

        Ok(())
    }

    /// Rollback a transaction
    pub fn rollback(&self, txn_id: TxnId, storage: &StorageEngine) -> Result<()> {
        let undo_log = {
            let txns = self.active_txns.read().unwrap();
            let txn = txns.get(&txn_id).ok_or_else(|| {
                MiniSqlError::Transaction(format!("Transaction {} not found", txn_id))
            })?;
            if txn.state != TxnState::Active {
                return Err(MiniSqlError::Transaction(format!(
                    "Transaction {} is not active",
                    txn_id
                )));
            }
            txn.undo_log.clone()
        };

        // Apply undo operations in reverse order
        for record in undo_log.into_iter().rev() {
            recovery::undo_operation(&record.op, storage)?;
        }

        // Write ROLLBACK record (does not need durable write - rollback is idempotent)
        let lsn = self.alloc_lsn();
        let record = LogRecord {
            lsn,
            txn_id,
            op: LogOperation::Rollback,
            timestamp: Self::timestamp(),
        };
        self.write_log(record)?;

        // Remove from active transactions
        {
            let mut txns = self.active_txns.write().unwrap();
            if let Some(txn) = txns.get_mut(&txn_id) {
                txn.state = TxnState::Aborted;
            }
            txns.remove(&txn_id);
        }

        // Checkpoint if no active transactions
        // OPTIMIZATION: Only checkpoint if we've accumulated significant log data (>4KB)
        let bytes_written = self.bytes_since_checkpoint.load(Ordering::Relaxed);
        let active_empty = { self.active_txns.read().unwrap().is_empty() };
        if active_empty && bytes_written > 4096 {
            if let Err(_e) = self.checkpoint(storage) {
                // Immediate post-rollback checkpoint failed (silently continue)
            }
        }

        Ok(())
    }

    /// Log an insert operation (non-durable write - durability comes at commit time)
    pub fn log_insert(
        &self,
        txn_id: TxnId,
        table: &str,
        row_id: u64,
        values: &[Value],
    ) -> Result<()> {
        let lsn = self.alloc_lsn();
        let op = LogOperation::Insert {
            table: table.to_string(),
            row_id,
            values: values.to_vec(),
        };

        let record = LogRecord {
            lsn,
            txn_id,
            op: op.clone(),
            timestamp: Self::timestamp(),
        };

        // Non-durable write - if crash happens before commit, we rollback anyway
        self.write_log(record)?;

        // Add to undo log
        let mut txns = self.active_txns.write().unwrap();
        if let Some(txn) = txns.get_mut(&txn_id) {
            txn.undo_log.push(LogRecord {
                lsn,
                txn_id,
                op,
                timestamp: Self::timestamp(),
            });
        }

        Ok(())
    }

    /// Log an update operation (non-durable write)
    pub fn log_update(
        &self,
        txn_id: TxnId,
        table: &str,
        row_id: u64,
        old_values: &[Value],
        new_values: &[Value],
    ) -> Result<()> {
        let lsn = self.alloc_lsn();
        let op = LogOperation::Update {
            table: table.to_string(),
            row_id,
            old_values: old_values.to_vec(),
            new_values: new_values.to_vec(),
        };

        let record = LogRecord {
            lsn,
            txn_id,
            op: op.clone(),
            timestamp: Self::timestamp(),
        };

        self.write_log(record)?;

        // Add to undo log
        let mut txns = self.active_txns.write().unwrap();
        if let Some(txn) = txns.get_mut(&txn_id) {
            txn.undo_log.push(LogRecord {
                lsn,
                txn_id,
                op,
                timestamp: Self::timestamp(),
            });
        }

        Ok(())
    }

    /// Log a delete operation (non-durable write)
    pub fn log_delete(
        &self,
        txn_id: TxnId,
        table: &str,
        row_id: u64,
        old_values: &[Value],
    ) -> Result<()> {
        let lsn = self.alloc_lsn();
        let op = LogOperation::Delete {
            table: table.to_string(),
            row_id,
            old_values: old_values.to_vec(),
        };

        let record = LogRecord {
            lsn,
            txn_id,
            op: op.clone(),
            timestamp: Self::timestamp(),
        };

        self.write_log(record)?;

        // Add to undo log
        let mut txns = self.active_txns.write().unwrap();
        if let Some(txn) = txns.get_mut(&txn_id) {
            txn.undo_log.push(LogRecord {
                lsn,
                txn_id,
                op,
                timestamp: Self::timestamp(),
            });
        }

        Ok(())
    }

    /// Log a CREATE TABLE operation (non-durable write)
    pub fn log_create_table(&self, txn_id: TxnId, schema: &TableSchema) -> Result<()> {
        let lsn = self.alloc_lsn();
        let record = LogRecord {
            lsn,
            txn_id,
            op: LogOperation::CreateTable {
                schema: schema.clone(),
            },
            timestamp: Self::timestamp(),
        };
        self.write_log(record)?;
        Ok(())
    }

    /// Log a DROP TABLE operation (non-durable write)
    pub fn log_drop_table(&self, txn_id: TxnId, table: &str) -> Result<()> {
        let lsn = self.alloc_lsn();
        let record = LogRecord {
            lsn,
            txn_id,
            op: LogOperation::DropTable {
                table: table.to_string(),
            },
            timestamp: Self::timestamp(),
        };
        self.write_log(record)?;
        Ok(())
    }

    /// Log a TRUNCATE TABLE operation (non-durable write)
    pub fn log_truncate_table(&self, txn_id: TxnId, table: &str) -> Result<()> {
        let lsn = self.alloc_lsn();
        let record = LogRecord {
            lsn,
            txn_id,
            op: LogOperation::TruncateTable {
                table: table.to_string(),
            },
            timestamp: Self::timestamp(),
        };
        self.write_log(record)?;
        Ok(())
    }

    /// Check if a transaction is active
    pub fn is_active(&self, txn_id: TxnId) -> bool {
        let txns = self.active_txns.read().unwrap();
        txns.contains_key(&txn_id)
    }

    /// Recover from WAL on startup
    pub fn recover(&self, storage: &StorageEngine) -> Result<()> {
        let recovery_mgr = recovery::RecoveryManager::new(self.data_dir.clone());
        let (max_lsn, max_txn_id, committed_txns) = recovery_mgr.recover(storage)?;

        // Update LSN and TxnId counters
        self.current_lsn.store(max_lsn, Ordering::SeqCst);
        self.next_txn_id.store(max_txn_id, Ordering::SeqCst);

        // Add committed txns to the set
        let mut committed = self.committed_txns.write().unwrap();
        for txn_id in committed_txns {
            committed.insert(txn_id);
        }

        // Checkpoint after recovery
        if let Err(_e) = self.checkpoint(storage) {
            // Post-recovery checkpoint failed (silently continue)
        }

        Ok(())
    }

    /// Checkpoint: flush all data and truncate WAL
    pub fn checkpoint(&self, storage: &StorageEngine) -> Result<()> {
        // Flush all data to disk
        storage.flush_all()?;

        // Force fsync of any pending WAL writes
        self.force_sync()?;

        // Get list of active transactions
        let active_txn_ids: Vec<TxnId> = {
            let txns = self.active_txns.read().unwrap();
            txns.keys().cloned().collect()
        };

        // Write checkpoint record (durable since it's a critical marker)
        let lsn = self.alloc_lsn();
        let record = LogRecord {
            lsn,
            txn_id: 0,
            op: LogOperation::Checkpoint {
                active_txns: active_txn_ids.clone(),
            },
            timestamp: Self::timestamp(),
        };
        self.write_log_durable(record)?;

        // Write checkpoint marker
        let checkpoint_data = serde_json::json!({
            "lsn": lsn,
            "active_txns": active_txn_ids,
            "timestamp": Self::timestamp(),
            "durable_lsn": self.durable_lsn()
        });
        fs::write(
            self.checkpoint_path(),
            serde_json::to_string_pretty(&checkpoint_data)?,
        )?;

        // Truncate WAL file if no active transactions
        if active_txn_ids.is_empty() {
            self.truncate_wal()?;
            self.bytes_since_checkpoint.store(0, Ordering::Relaxed);
            // Checkpoint complete at LSN, WAL truncated
        } else {
            // Checkpoint complete at LSN with active transactions
        }

        Ok(())
    }

    /// Truncate the WAL file (called after checkpoint when safe)
    fn truncate_wal(&self) -> Result<()> {
        match self.truncate_wal_worker() {
            Ok(()) => {
                self.bytes_since_checkpoint.store(0, Ordering::Relaxed);
                // WAL truncated successfully by worker
                Ok(())
            }
            Err(e) => {
                // WAL truncation failed
                Err(e)
            }
        }
    }

    /// Get a dummy transaction ID for auto-commit operations
    pub fn auto_commit_txn(&self) -> TxnId {
        0 // Special ID for auto-commit
    }
}

// Note: TransactionManager is not Clone due to the channel-based WAL worker.
// The Arc<TransactionManager> pattern should be used for sharing across threads.

#[cfg(test)]
mod tests;
