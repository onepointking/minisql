//! Background worker for Sandstone engine
//!
//! Handles:
//! - Periodic flushing of dirty tables
//! - Delta-CRDT state management
//! - Conflict-free merge operations

use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crate::storage::StorageEngine;
use super::shared_state::SandstoneSharedState;

/// Background worker loop
pub(crate) fn worker_loop(
    state: Arc<SandstoneSharedState>,
    storage: Arc<StorageEngine>,
    flush_interval: Duration,
) {
    log::info!("Sandstone background worker started (flush interval: {:?})", flush_interval);
    
    let mut last_flush = Instant::now();
    
    loop {
        // Check shutdown
        if *state.shutdown.lock().unwrap() {
            log::info!("Sandstone worker shutting down");
            // Final flush before shutdown
            flush_dirty_tables(&state, &storage);
            break;
        }

        // Sleep until next flush interval
        let elapsed = last_flush.elapsed();
        if elapsed < flush_interval {
            thread::sleep(flush_interval - elapsed);
        }

        // Flush dirty tables
        flush_dirty_tables(&state, &storage);
        last_flush = Instant::now();
    }
}

/// Flush all dirty tables to disk
pub(crate) fn flush_dirty_tables(state: &Arc<SandstoneSharedState>, storage: &Arc<StorageEngine>) {
    // Get and clear dirty set
    let dirty: Vec<String> = {
        let mut dirty_set = state.dirty_tables.lock().unwrap();
        let result: Vec<_> = dirty_set.drain().collect();
        result
    };

    if dirty.is_empty() {
        return;
    }

    log::debug!("Sandstone flushing {} dirty tables", dirty.len());

    for table_name in dirty {
        // Get rows from page table
        let rows = {
            let pages = state.pages.read().unwrap();
            pages.get_all_rows(&table_name)
        };

        // Apply to storage and save
        if let Err(e) = storage.replace_table_rows(&table_name, &rows) {
            log::error!("Sandstone flush failed for table '{}': {}", table_name, e);
            // Re-mark as dirty for retry
            state.dirty_tables.lock().unwrap().insert(table_name);
        }
    }
}
