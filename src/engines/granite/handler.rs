use std::sync::Arc;
use crate::error::{MiniSqlError, Result};
use crate::types::{Row, Value};
use crate::engines::handler::EngineHandler;
use crate::engines::granite::{TransactionManager, TxnId};
use crate::storage::StorageEngine;

/// Granite Handler: The default ACID engine handler.
/// 
/// Responsibilities:
/// - Logging operations to WAL via TransactionManager
/// - Applying changes to StorageEngine
/// - Enforcing primary key constraints
/// - Managing durability (fsync, checkpoints)
pub struct GraniteHandler {
    storage: Arc<StorageEngine>,
    txn_manager: Arc<TransactionManager>,
}

impl GraniteHandler {
    pub fn new(storage: Arc<StorageEngine>, txn_manager: Arc<TransactionManager>) -> Self {
        Self {
            storage,
            txn_manager,
        }
    }
}

impl EngineHandler for GraniteHandler {
    fn init_table(&self, _table_name: &str) -> Result<()> {
        // Granite tables are always ready (on disk or loaded by storage engine)
        Ok(())
    }

    fn insert(&self, txn_id: TxnId, table_name: &str, values: Vec<Value>) -> Result<u64> {
        // 1. Check uniqueness constraints (PK)
        let schema = self.storage.get_schema(table_name)?;
        let pk_columns = schema.primary_key_columns();
        
        if !pk_columns.is_empty() {
            if self.storage.check_unique_violation(table_name, &pk_columns, &values, None)? {
                let pk_values: Vec<String> = pk_columns.iter()
                    .filter_map(|&idx| values.get(idx).map(|v| v.to_string()))
                    .collect();
                return Err(MiniSqlError::Constraint(format!(
                    "Duplicate entry '{}' for key 'PRIMARY'",
                    pk_values.join("-")
                )));
            }
        }

        // 2. Apply to storage (generates row_id)
        let row_id = self.storage.insert_row(table_name, values.clone())?;
        
        // 3. Log to WAL
        self.txn_manager.log_insert(txn_id, table_name, row_id, &values)?;

        Ok(row_id)
    }

    fn update(
        &self, 
        txn_id: TxnId, 
        table_name: &str, 
        row_id: u64, 
        old_values: &[Value], 
        new_values: Vec<Value>
    ) -> Result<bool> {
        // 1. Check uniqueness constraints if PK is changed
        let schema = self.storage.get_schema(table_name)?;
        let pk_columns = schema.primary_key_columns();
        
        // Check if any PK columns are modified
        // Note: Ideally we'd compare old vs new values here, but for now we just verify global uniqueness 
        // if PK columns are involved in the update.
        // A smarter check would be: if PK changed AND uniqueness violated.
        
        // Simple check: if we are updating, verify the new PK doesn't conflict
        if !pk_columns.is_empty() {
             if self.storage.check_unique_violation(table_name, &pk_columns, &new_values, Some(row_id))? {
                let pk_values: Vec<String> = pk_columns.iter()
                    .filter_map(|&idx| new_values.get(idx).map(|v| v.to_string()))
                    .collect();
                return Err(MiniSqlError::Constraint(format!(
                    "Duplicate entry '{}' for key 'PRIMARY'",
                    pk_values.join("-")
                )));
            }
        }

        // 2. Log to WAL
        self.txn_manager.log_update(txn_id, table_name, row_id, old_values, &new_values)?;

        // 3. Apply to storage
        self.storage.update_row(table_name, row_id, new_values)
    }

    fn delete(
        &self, 
        txn_id: TxnId, 
        table_name: &str, 
        row_id: u64, 
        old_values: &[Value]
    ) -> Result<bool> {
        // 1. Log to WAL
        self.txn_manager.log_delete(txn_id, table_name, row_id, old_values)?;

        // 2. Apply to storage
        self.storage.delete_row(table_name, row_id)
    }

    fn scan(&self, table_name: &str) -> Result<Vec<Row>> {
        self.storage.scan_table(table_name)
    }

    fn flush(&self, table_name: &str) -> Result<()> {
        // Check if we should do an async save
        let async_saves = std::env::var("MINISQL_ASYNC_SAVES")
            .map(|v| v != "0")
            .unwrap_or(false);
            
        if async_saves {
            let _ = self.storage.save_table_async(table_name);
            Ok(())
        } else {
            self.storage.save_table(table_name)
        }
    }

    fn supports_transactions(&self) -> bool {
        true  // Granite is fully transactional
    }

    fn commit_transaction(&self, txn_id: TxnId) -> Result<bool> {
        // Perform the durable WAL commit for Granite
        // This waits for fsync to ensure durability
        self.txn_manager.commit_durable(txn_id)?;
        Ok(true)  // We always have potential work (WAL fsync)
    }

    fn rollback_transaction(&self, txn_id: TxnId) -> Result<()> {
        // Rollback is handled by the TransactionManager
        // The undo log is applied there
        // Note: This is called before txn_manager.rollback() in execute_rollback
        // so we don't need to do anything here - the main rollback handles it
        let _ = txn_id;  // Acknowledge the parameter
        Ok(())
    }
}

