//! Shared state between main thread and background worker

use std::collections::{HashMap, HashSet};
use std::sync::{Mutex, RwLock};

use super::page_table::MemoryPageTable;
use super::delta_crdt::TableDeltaState;

/// Shared state between main thread and background worker
pub(crate) struct SandstoneSharedState {
    /// In-memory page table
    pub(crate) pages: RwLock<MemoryPageTable>,
    /// Set of tables with uncommitted changes
    pub(crate) dirty_tables: Mutex<HashSet<String>>,
    /// Delta-CRDT state per table
    pub(crate) crdt_states: RwLock<HashMap<String, TableDeltaState>>,
    /// Shutdown flag
    pub(crate) shutdown: Mutex<bool>,
}

impl SandstoneSharedState {
    pub(crate) fn new() -> Self {
        Self {
            pages: RwLock::new(MemoryPageTable::new()),
            dirty_tables: Mutex::new(HashSet::new()),
            crdt_states: RwLock::new(HashMap::new()),
            shutdown: Mutex::new(false),
        }
    }
}
