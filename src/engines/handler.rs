use crate::error::Result;
use crate::types::{Row, Value};
use crate::engines::granite::TxnId;

/// Trait defining the standard interface for all storage engines.
/// This allows the Executor to interact with different engines (Granite, Sandstone)
/// through a uniform API, similar to MySQL's handler API.
pub trait EngineHandler: Send + Sync {
    /// Initialize a table (e.g. load into memory)
    fn init_table(&self, table_name: &str) -> Result<()>;

    /// Insert a row
    fn insert(&self, txn_id: TxnId, table_name: &str, values: Vec<Value>) -> Result<u64>;

    /// Update a row
    /// Returns true if the row was found and updated, false otherwise.
    fn update(
        &self, 
        txn_id: TxnId, 
        table_name: &str, 
        row_id: u64, 
        old_values: &[Value], 
        new_values: Vec<Value>
    ) -> Result<bool>;

    /// Delete a row
    /// Returns true if the row was found and deleted, false otherwise.
    fn delete(
        &self, 
        txn_id: TxnId, 
        table_name: &str, 
        row_id: u64, 
        old_values: &[Value]
    ) -> Result<bool>;

    /// Scan a table returning all rows
    fn scan(&self, table_name: &str) -> Result<Vec<Row>>;
    
    /// Optional: Flush changes to disk (for eventually consistent engines)
    fn flush(&self, _table_name: &str) -> Result<()> { 
        Ok(()) 
    }

    /// Check if this engine supports transactional semantics (BEGIN/COMMIT/ROLLBACK).
    /// Engines that return false will silently ignore transaction boundaries (MySQL MyISAM behavior).
    fn supports_transactions(&self) -> bool {
        true  // Default: most engines support transactions
    }

    /// Check if this engine supports secondary indexes for optimized lookups.
    /// Engines that return false will always use full table scans.
    fn supports_indexes(&self) -> bool {
        true  // Default: most engines support indexes
    }

    /// Called when a transaction begins. Engine can initialize per-transaction state.
    fn begin_transaction(&self, _txn_id: TxnId) -> Result<()> {
        Ok(())  // Default: no-op
    }

    /// Called when a transaction commits.
    /// Returns true if the engine had work to commit (e.g., WAL records to fsync).
    /// Returns false if there was nothing to commit for this engine.
    fn commit_transaction(&self, _txn_id: TxnId) -> Result<bool> {
        Ok(false)  // Default: no engine-specific commit work
    }

    /// Called when a transaction rolls back.
    fn rollback_transaction(&self, _txn_id: TxnId) -> Result<()> {
        Ok(())  // Default: no-op
    }
}
