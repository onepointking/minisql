//! In-memory page table for Sandstone engine
//!
//! Maps table_name -> row_id -> row values for fast in-memory operations.

use std::collections::HashMap;
use crate::types::{Row, Value};

/// In-memory page table for fast writes
/// Maps table_name -> row_id -> row values
#[derive(Debug, Default)]
pub struct MemoryPageTable {
    /// Pages: table_name -> (row_id -> values)
    pages: HashMap<String, HashMap<u64, Vec<Value>>>,
    /// Next row ID per table
    next_row_ids: HashMap<String, u64>,
}

impl MemoryPageTable {
    pub fn new() -> Self {
        Self::default()
    }

    /// Initialize a table's page
    #[allow(dead_code)]
    pub fn init_table(&mut self, table_name: &str) {
        self.pages.entry(table_name.to_string()).or_default();
        self.next_row_ids.entry(table_name.to_string()).or_insert(1);
    }

    /// Insert a row, returns the row ID
    pub fn insert(&mut self, table_name: &str, values: Vec<Value>) -> u64 {
        let row_id = *self.next_row_ids.entry(table_name.to_string()).or_insert(1);
        self.next_row_ids.insert(table_name.to_string(), row_id + 1);
        
        self.pages
            .entry(table_name.to_string())
            .or_default()
            .insert(row_id, values);
        
        row_id
    }

    /// Update a row
    pub fn update(&mut self, table_name: &str, row_id: u64, values: Vec<Value>) -> bool {
        if let Some(table) = self.pages.get_mut(table_name) {
            if table.contains_key(&row_id) {
                table.insert(row_id, values);
                return true;
            }
        }
        false
    }

    /// Delete a row
    pub fn delete(&mut self, table_name: &str, row_id: u64) -> bool {
        if let Some(table) = self.pages.get_mut(table_name) {
            return table.remove(&row_id).is_some();
        }
        false
    }

    /// Scan all rows in a table
    pub fn scan(&self, table_name: &str) -> Vec<Row> {
        if let Some(table) = self.pages.get(table_name) {
            table
                .iter()
                .map(|(&row_id, values)| Row {
                    id: row_id,
                    values: values.clone(),
                })
                .collect()
        } else {
            Vec::new()
        }
    }

    /// Get a single row by ID
    pub fn get(&self, table_name: &str, row_id: u64) -> Option<Row> {
        self.pages
            .get(table_name)?
            .get(&row_id)
            .map(|values| Row {
                id: row_id,
                values: values.clone(),
            })
    }

    /// Get all dirty table names and clear the flag
    #[allow(dead_code)]
    pub fn get_table_names(&self) -> Vec<String> {
        self.pages.keys().cloned().collect()
    }

    /// Get all rows for a table (for flushing to disk)
    pub fn get_all_rows(&self, table_name: &str) -> Vec<(u64, Vec<Value>)> {
        if let Some(table) = self.pages.get(table_name) {
            table.iter().map(|(&id, v)| (id, v.clone())).collect()
        } else {
            Vec::new()
        }
    }

    /// Load rows from storage into page table
    pub fn load_from_storage(&mut self, table_name: &str, rows: &[Row]) {
        let page = self.pages.entry(table_name.to_string()).or_default();
        let mut max_id = 0u64;
        
        for row in rows {
            page.insert(row.id, row.values.clone());
            max_id = max_id.max(row.id);
        }
        
        self.next_row_ids.insert(table_name.to_string(), max_id + 1);
    }

    /// Expose pages for direct access (needed by merge_delta)
    pub(crate) fn pages_mut(&mut self) -> &mut HashMap<String, HashMap<u64, Vec<Value>>> {
        &mut self.pages
    }
}
