//! Sandstone Engine - Eventual Consistency with Delta-CRDTs
//!
//! Sandstone prioritizes speed over durability using delta-state CRDTs.
//! It does NOT support transactions.
//!
//! ## Architecture
//! - Writes go to in-memory page table (fast)
//! - Background worker periodically flushes to disk
//! - Delta-CRDTs enable conflict-free replication

mod config;
mod page_table;
mod shared_state;
mod delta_crdt;
mod worker;

use std::sync::Arc;
use std::thread::{self, JoinHandle};
use std::time::Duration;

use crate::error::Result;
use crate::storage::StorageEngine;
use crate::types::{Row, Value};
use crate::engines::handler::EngineHandler;
use crate::engines::granite::TxnId;

// Public re-exports
pub use config::SandstoneConfig;
pub use delta_crdt::*;

// Internal imports
use shared_state::SandstoneSharedState;

/// Handle to Sandstone background worker
pub struct SandstoneEngine {
    /// Shared state with background worker
    state: Arc<SandstoneSharedState>,
    /// Reference to storage engine for persistence
    storage: Arc<StorageEngine>,
    /// Background worker thread handle
    worker_handle: Option<JoinHandle<()>>,
    /// Configuration
    config: SandstoneConfig,
}

impl SandstoneEngine {
    /// Create a new Sandstone engine with background worker
    pub fn new(storage: Arc<StorageEngine>, config: SandstoneConfig) -> Result<Self> {
        let state = Arc::new(SandstoneSharedState::new());
        
        // Spawn background worker
        let worker_state = Arc::clone(&state);
        let worker_storage = Arc::clone(&storage);
        let flush_interval = Duration::from_millis(config.flush_interval_ms);
        
        let worker_handle = thread::spawn(move || {
            worker::worker_loop(worker_state, worker_storage, flush_interval);
        });

        Ok(Self {
            state,
            storage,
            worker_handle: Some(worker_handle),
            config,
        })
    }

    /// Initialize a table in Sandstone (load from disk into memory)
    pub fn init_table(&self, table_name: &str) -> Result<()> {
        // Load existing rows from storage
        let rows = self.storage.scan_table(table_name)?;
        
        // Load into page table
        {
            let mut pages = self.state.pages.write().unwrap();
            pages.load_from_storage(table_name, &rows);
        }
        
        // Initialize CRDT state
        if self.config.enable_delta_crdt {
            let mut crdt_states = self.state.crdt_states.write().unwrap();
            crdt_states.entry(table_name.to_string()).or_insert_with(TableDeltaState::new);
        }
        
        Ok(())
    }

    /// Check if this engine owns the given table (based on schema engine type)
    pub fn owns_table(&self, schema: &crate::types::TableSchema) -> bool {
        schema.engine_type == crate::engines::EngineType::Sandstone
    }

    /// Insert a row (fast - memory only)
    pub fn insert_row(&self, table_name: &str, values: Vec<Value>) -> Result<u64> {
        let row_id = {
            let mut pages = self.state.pages.write().unwrap();
            pages.insert(table_name, values.clone())
        };

        // Mark dirty
        self.state.dirty_tables.lock().unwrap().insert(table_name.to_string());

        // Record CRDT delta
        if self.config.enable_delta_crdt {
            let mut crdt_states = self.state.crdt_states.write().unwrap();
            if let Some(state) = crdt_states.get_mut(table_name) {
                state.record_operation(
                    table_name.to_string(),
                    DeltaOperation::Upsert {
                        row_id,
                        values,
                        timestamp: 0, // Will be set by record_operation
                    },
                );
            }
        }

        Ok(row_id)
    }

    /// Update a row (fast - memory only)
    pub fn update_row(&self, table_name: &str, row_id: u64, values: Vec<Value>) -> Result<bool> {
        let updated = {
            let mut pages = self.state.pages.write().unwrap();
            pages.update(table_name, row_id, values.clone())
        };

        if updated {
            self.state.dirty_tables.lock().unwrap().insert(table_name.to_string());

            if self.config.enable_delta_crdt {
                let mut crdt_states = self.state.crdt_states.write().unwrap();
                if let Some(state) = crdt_states.get_mut(table_name) {
                    state.record_operation(
                        table_name.to_string(),
                        DeltaOperation::Upsert {
                            row_id,
                            values,
                            timestamp: 0,
                        },
                    );
                }
            }
        }

        Ok(updated)
    }

    /// Delete a row (fast - memory only)
    pub fn delete_row(&self, table_name: &str, row_id: u64) -> Result<bool> {
        let deleted = {
            let mut pages = self.state.pages.write().unwrap();
            pages.delete(table_name, row_id)
        };

        if deleted {
            self.state.dirty_tables.lock().unwrap().insert(table_name.to_string());

            if self.config.enable_delta_crdt {
                let mut crdt_states = self.state.crdt_states.write().unwrap();
                if let Some(state) = crdt_states.get_mut(table_name) {
                    state.record_operation(
                        table_name.to_string(),
                        DeltaOperation::Delete {
                            row_id,
                            timestamp: 0,
                        },
                    );
                }
            }
        }

        Ok(deleted)
    }

    /// Scan all rows in a table
    pub fn scan_table(&self, table_name: &str) -> Vec<Row> {
        let pages = self.state.pages.read().unwrap();
        pages.scan(table_name)
    }

    /// Get a single row by ID
    pub fn get_row(&self, table_name: &str, row_id: u64) -> Option<Row> {
        let pages = self.state.pages.read().unwrap();
        pages.get(table_name, row_id)
    }

    /// Force immediate flush of all dirty tables
    pub fn flush_all(&self) -> Result<()> {
        worker::flush_dirty_tables(&self.state, &self.storage);
        Ok(())
    }

    /// Get delta state for replication
    pub fn get_pending_deltas(&self, table_name: &str) -> Vec<DeltaState> {
        if !self.config.enable_delta_crdt {
            return Vec::new();
        }

        let mut crdt_states = self.state.crdt_states.write().unwrap();
        if let Some(state) = crdt_states.get_mut(table_name) {
            state.drain_pending_deltas()
        } else {
            Vec::new()
        }
    }

    /// Merge incoming delta from another replica
    pub fn merge_delta(&self, table_name: &str, delta: DeltaState) -> Result<()> {
        if !self.config.enable_delta_crdt {
            return Ok(());
        }

        let operations = {
            let mut crdt_states = self.state.crdt_states.write().unwrap();
            let state = crdt_states.entry(table_name.to_string()).or_insert_with(TableDeltaState::new);
            state.merge_delta(delta)
        };

        // Apply merged operations to page table
        {
            let mut pages = self.state.pages.write().unwrap();
            for op in operations {
                match op {
                    DeltaOperation::Upsert { row_id, values, .. } => {
                        if !pages.update(table_name, row_id, values.clone()) {
                            // Row doesn't exist, insert it
                            pages.pages_mut()
                                .entry(table_name.to_string())
                                .or_default()
                                .insert(row_id, values);
                        }
                    }
                    DeltaOperation::Delete { row_id, .. } => {
                        pages.delete(table_name, row_id);
                    }
                }
            }
        }

        self.state.dirty_tables.lock().unwrap().insert(table_name.to_string());
        Ok(())
    }

    /// Shutdown the background worker
    pub fn shutdown(&mut self) {
        *self.state.shutdown.lock().unwrap() = true;
        
        if let Some(handle) = self.worker_handle.take() {
            let _ = handle.join();
        }
    }
}

impl Drop for SandstoneEngine {
    fn drop(&mut self) {
        self.shutdown();
    }
}

impl EngineHandler for SandstoneEngine {
    fn init_table(&self, table_name: &str) -> Result<()> {
        self.init_table(table_name)
    }

    fn insert(&self, _txn_id: TxnId, table_name: &str, values: Vec<Value>) -> Result<u64> {
        self.insert_row(table_name, values)
    }

    fn update(
        &self, 
        _txn_id: TxnId, 
        table_name: &str, 
        row_id: u64, 
        _old_values: &[Value], 
        new_values: Vec<Value>
    ) -> Result<bool> {
        self.update_row(table_name, row_id, new_values)
    }

    fn delete(
        &self, 
        _txn_id: TxnId, 
        table_name: &str, 
        row_id: u64, 
        _old_values: &[Value]
    ) -> Result<bool> {
        self.delete_row(table_name, row_id)
    }

    fn scan(&self, table_name: &str) -> Result<Vec<Row>> {
        Ok(self.scan_table(table_name))
    }

    fn flush(&self, _table_name: &str) -> Result<()> {
        self.flush_all()
    }

    fn supports_transactions(&self) -> bool {
        false  // Sandstone uses eventual consistency, no transaction support
    }

    fn supports_indexes(&self) -> bool {
        false  // Sandstone uses in-memory page table, no secondary indexes
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use super::page_table::MemoryPageTable;

    #[test]
    fn test_sandstone_config_defaults() {
        let config = SandstoneConfig::default();
        assert_eq!(config.flush_interval_ms, 1000);
        assert_eq!(config.max_dirty_tables, None);
        assert!(config.enable_delta_crdt);
    }
    
    #[test]
    fn test_sandstone_config_high_throughput() {
        let config = SandstoneConfig::high_throughput();
        assert_eq!(config.flush_interval_ms, 5000);
        assert!(config.enable_delta_crdt);
    }
    
    #[test]
    fn test_sandstone_config_low_latency() {
        let config = SandstoneConfig::low_latency();
        assert_eq!(config.flush_interval_ms, 500);
        assert_eq!(config.max_dirty_tables, Some(10));
    }

    #[test]
    fn test_memory_page_table_insert() {
        let mut pages = MemoryPageTable::new();
        pages.init_table("test");
        
        let id1 = pages.insert("test", vec![Value::Integer(1)]);
        let id2 = pages.insert("test", vec![Value::Integer(2)]);
        
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);
        
        let rows = pages.scan("test");
        assert_eq!(rows.len(), 2);
    }

    #[test]
    fn test_memory_page_table_update() {
        let mut pages = MemoryPageTable::new();
        pages.init_table("test");
        
        let id = pages.insert("test", vec![Value::Integer(1)]);
        assert!(pages.update("test", id, vec![Value::Integer(100)]));
        
        let row = pages.get("test", id).unwrap();
        assert_eq!(row.values[0], Value::Integer(100));
    }

    #[test]
    fn test_memory_page_table_delete() {
        let mut pages = MemoryPageTable::new();
        pages.init_table("test");
        
        let id = pages.insert("test", vec![Value::Integer(1)]);
        assert!(pages.delete("test", id));
        assert!(pages.get("test", id).is_none());
    }

    #[test]
    fn test_memory_page_table_scan() {
        let mut pages = MemoryPageTable::new();
        pages.init_table("test");
        
        for i in 1..=100 {
            pages.insert("test", vec![Value::Integer(i)]);
        }
        
        let rows = pages.scan("test");
        assert_eq!(rows.len(), 100);
    }
}
