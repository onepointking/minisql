//! Storage Engine for MiniSQL
//!
//! ## Storage Format
//!
//! The storage engine uses a simple file-based format:
//!
//! ### Directory Structure
//! ```text
//! data/
//! ├── catalog.json      # Table metadata (schemas)
//! ├── wal.log           # Write-ahead log for durability
//! ├── wal.checkpoint    # Checkpoint marker
//! └── tables/
//!     ├── users.dat     # Row data for 'users' table
//!     ├── orders.dat    # Row data for 'orders' table
//!     └── ...
//! ```
//!
//! ### Table Data Format (.dat files)
//! Each row is stored as a JSON line (JSONL format) for simplicity:
//! ```json
//! {"id":1,"values":[1,"alice","alice@example.com",{"meta":"data"}]}
//! {"id":2,"values":[2,"bob","bob@example.com",null]}
//! ```
//!
//! This format is:
//! - Human readable (good for educational purposes)
//! - Easy to parse and debug
//! - Supports JSON columns naturally
//! - Append-friendly
//!
//! ### Catalog Format (catalog.json)
//! ```json
//! {
//!   "tables": {
//!     "users": {
//!       "name": "users",
//!       "columns": [
//!         {"name": "id", "data_type": "Integer", "nullable": false, "primary_key": true},
//!         {"name": "name", "data_type": {"Varchar": 100}, "nullable": false}
//!       ]
//!     }
//!   },
//!   "next_row_id": 1000
//! }
//! ```

use std::collections::HashMap;
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};
use std::thread;

use serde::{Deserialize, Serialize};

use crate::error::{MiniSqlError, Result};
use crate::types::{IndexMetadata, Row, TableSchema, Value};

/// Catalog file storing table metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
struct Catalog {
    tables: HashMap<String, TableSchema>,
    indexes: HashMap<String, IndexMetadata>,
    next_row_id: u64,
}

impl Catalog {
    fn new() -> Self {
        Self {
            tables: HashMap::new(),
            indexes: HashMap::new(),
            next_row_id: 1,
        }
    }
}

/// Index entry mapping (composite key -> Vec<row_id>)
type IndexData = BTreeMap<String, Vec<u64>>;

/// In-memory table data
#[derive(Debug)]
struct TableData {
    schema: TableSchema,
    /// Rows indexed by row ID for fast lookup
    rows: HashMap<u64, Row>,
    /// Indexes: index_name -> BTreeMap(composite_key -> row_ids)
    indexes: HashMap<String, IndexData>,
}

impl TableData {
    fn new(schema: TableSchema) -> Self {
        Self {
            schema,
            rows: HashMap::new(),
            indexes: HashMap::new(),
        }
    }
}

/// The storage engine manages all table data and persistence
pub struct StorageEngine {
    /// Base directory for data files
    data_dir: PathBuf,
    /// In-memory table data, protected by RwLock for concurrent access
    tables: Arc<RwLock<HashMap<String, TableData>>>,
    /// Catalog data (schemas and metadata)
    catalog: Arc<RwLock<Catalog>>,
}

impl StorageEngine {
    /// Create a new storage engine with the given data directory
    pub fn new(data_dir: PathBuf) -> Result<Self> {
        // Ensure directories exist
        fs::create_dir_all(&data_dir)?;
        fs::create_dir_all(data_dir.join("tables"))?;

        let engine = Self {
            data_dir: data_dir.clone(),
            tables: Arc::new(RwLock::new(HashMap::new())),
            catalog: Arc::new(RwLock::new(Catalog::new())),
        };

        // Load existing catalog
        engine.load_catalog()?;
        
        // Load all table data into memory
        engine.load_tables()?;

        // Rebuild all indexes from catalog metadata
        engine.rebuild_all_indexes()?;

        Ok(engine)
    }

    /// Get path to catalog file
    fn catalog_path(&self) -> PathBuf {
        self.data_dir.join("catalog.json")
    }

    /// Validate a table name to prevent path traversal attacks
    fn validate_table_name(name: &str) -> Result<()> {
        if name.is_empty() {
            return Err(MiniSqlError::Syntax("Table name cannot be empty".into()));
        }
        if name.contains("..") || name.contains('/') || name.contains('\\') || name.contains('\0') {
            return Err(MiniSqlError::Syntax(format!(
                "Invalid table name '{}': contains forbidden characters",
                name
            )));
        }
        // Also reject names that are too long (filesystem limit)
        if name.len() > 255 {
            return Err(MiniSqlError::Syntax("Table name too long".into()));
        }
        Ok(())
    }

    /// Get path to table data file (validates name first)
    fn table_path(&self, table_name: &str) -> PathBuf {
        // Note: Callers should validate before calling this, but we sanitize anyway
        self.data_dir.join("tables").join(format!("{}.dat", table_name))
    }

    /// Load catalog from disk
    fn load_catalog(&self) -> Result<()> {
        let path = self.catalog_path();
        if path.exists() {
            let file = File::open(&path)?;
            let catalog: Catalog = serde_json::from_reader(file).map_err(|e| {
                MiniSqlError::Json(format!(
                    "Failed to parse catalog '{}': {}. Check that the file contains valid JSON (no trailing commas, proper quoting, etc.)",
                    path.display(), e
                ))
            })?;
            *self.catalog.write().unwrap() = catalog;
        }
        Ok(())
    }

    /// Save catalog to disk
    pub fn save_catalog(&self) -> Result<()> {
        let path = self.catalog_path();
        let temp_path = path.with_extension("json.tmp");
        
        let catalog = self.catalog.read().unwrap();
        let file = File::create(&temp_path)?;
        let writer = BufWriter::new(file);
        serde_json::to_writer_pretty(writer, &*catalog)?;
        
        // Atomic rename for crash safety
        fs::rename(temp_path, path)?;
        Ok(())
    }

    /// Load all tables into memory
    fn load_tables(&self) -> Result<()> {
        let catalog = self.catalog.read().unwrap();
        let mut tables = self.tables.write().unwrap();

        for (name, schema) in &catalog.tables {
            let mut table_data = TableData::new(schema.clone());
            
            let path = self.table_path(name);
            if path.exists() {
                let file = File::open(&path)?;
                let reader = BufReader::new(file);
                
                for (idx, line_res) in reader.lines().enumerate() {
                    let line = line_res?;
                    if line.trim().is_empty() {
                        continue;
                    }
                    let row: Row = serde_json::from_str(&line).map_err(|e| {
                        MiniSqlError::Json(format!(
                            "Failed to parse JSON row in '{}', line {}: {}. Content: {}",
                            path.display(), idx + 1, e, &line
                        ))
                    })?;
                    table_data.rows.insert(row.id, row);
                }
            }
            
            tables.insert(name.clone(), table_data);
        }

        Ok(())
    }

    /// Save a table to disk (full rewrite)
    pub fn save_table(&self, table_name: &str) -> Result<()> {
        let tables = self.tables.read().unwrap();
        let table = tables.get(table_name).ok_or_else(|| {
            MiniSqlError::table_not_found(table_name)
        })?;

        let path = self.table_path(table_name);
        let temp_path = path.with_extension("dat.tmp");

        let file = File::create(&temp_path)?;
        let mut writer = BufWriter::new(file);

        // Write rows sorted by ID for consistency
        let mut row_ids: Vec<_> = table.rows.keys().collect();
        row_ids.sort();

        for id in row_ids {
            if let Some(row) = table.rows.get(id) {
                let json = serde_json::to_string(row)?;
                writeln!(writer, "{}", json)?;
            }
        }

        writer.flush()?;
        fs::rename(temp_path, path)?;
        
        Ok(())
    }

    /// Save a table asynchronously: spawn a background thread that performs the
    /// same work as `save_table`. Returns immediately. Errors during the
    /// background write are logged but do not surface to the caller.
    ///
    /// This is intended as an opt-in optimization for workloads that prefer
    /// lower foreground latency for auto-commit statements. To enable, set
    /// `MINISQL_ASYNC_SAVES=1` in the environment and the executor will call
    /// this method for auto-commit flows instead of blocking.
    pub fn save_table_async(&self, table_name: &str) -> Result<()> {
        let tables = Arc::clone(&self.tables);
        let table_name = table_name.to_string();
        let path = self.table_path(&table_name);

    // Clone the data dir/catalog references we might need; catalog isn't
    // needed for writing rows here but keep for possible future use.
    let _catalog = Arc::clone(&self.catalog);

        thread::spawn(move || {
            // Perform the same logic as save_table but in the background.
            if let Err(e) = (|| -> Result<()> {
                let tables_lock = tables.read().unwrap();
                let table = tables_lock.get(&table_name).ok_or_else(|| {
                    MiniSqlError::table_not_found(&table_name)
                })?;

                let temp_path = path.with_extension("dat.tmp");
                let file = File::create(&temp_path)?;
                let mut writer = BufWriter::new(file);

                let mut row_ids: Vec<_> = table.rows.keys().collect();
                row_ids.sort();

                for id in row_ids {
                    if let Some(row) = table.rows.get(id) {
                        let json = serde_json::to_string(row)?;
                        writeln!(writer, "{}", json)?;
                    }
                }

                writer.flush()?;
                fs::rename(temp_path, &path)?;

                Ok(())
            })() {
                log::error!("async save_table('{}') failed: {}", table_name, e);
            }
        });

        Ok(())
    }

    /// Create a new table
    pub fn create_table(&self, schema: TableSchema, if_not_exists: bool) -> Result<()> {
        // Validate table name to prevent path traversal
        Self::validate_table_name(&schema.name)?;
        
        let mut catalog = self.catalog.write().unwrap();
        let mut tables = self.tables.write().unwrap();

        if catalog.tables.contains_key(&schema.name) {
            if if_not_exists {
                return Ok(());
            }
            return Err(MiniSqlError::table_already_exists(&schema.name));
        }

        let name = schema.name.clone();
        catalog.tables.insert(name.clone(), schema.clone());
        tables.insert(name.clone(), TableData::new(schema));

        drop(tables);
        drop(catalog);

        self.save_catalog()?;
        
        // Create empty table file
        File::create(self.table_path(&name))?;

        Ok(())
    }

    /// Create a table only in memory (do not persist to disk). This is useful
    /// for unit tests that want to avoid filesystem interactions.
        #[allow(dead_code)]
    pub(crate) fn create_table_in_memory(&self, schema: TableSchema) -> Result<()> {
        Self::validate_table_name(&schema.name)?;

        let mut catalog = self.catalog.write().unwrap();
        let mut tables = self.tables.write().unwrap();

        if catalog.tables.contains_key(&schema.name) {
            return Err(MiniSqlError::table_already_exists(&schema.name));
        }

        let name = schema.name.clone();
        catalog.tables.insert(name.clone(), schema.clone());
        tables.insert(name.clone(), TableData::new(schema));

        Ok(())
    }

    /// Drop a table
    pub fn drop_table(&self, table_name: &str) -> Result<()> {
        let mut catalog = self.catalog.write().unwrap();
        let mut tables = self.tables.write().unwrap();

        if !catalog.tables.contains_key(table_name) {
            return Err(MiniSqlError::table_not_found(table_name));
        }

        catalog.tables.remove(table_name);
        tables.remove(table_name);

        drop(tables);
        drop(catalog);

        self.save_catalog()?;

        // Remove data file
        let path = self.table_path(table_name);
        if path.exists() {
            fs::remove_file(path)?;
        }

        Ok(())
    }

    /// Truncate a table (remove all rows but keep schema)
    pub fn truncate_table(&self, table_name: &str) -> Result<()> {
        let mut catalog = self.catalog.write().unwrap();
        let mut tables = self.tables.write().unwrap();

        if !catalog.tables.contains_key(table_name) {
            return Err(MiniSqlError::table_not_found(table_name));
        }

        // Reset auto-increment counter
        if let Some(schema) = catalog.tables.get_mut(table_name) {
            schema.auto_increment_counter = 1;
        }

        // Clear all rows and indexes
        if let Some(table) = tables.get_mut(table_name) {
            table.rows.clear();
            table.indexes.clear();
        }

        drop(tables);
        drop(catalog);

        self.save_catalog()?;

        // Truncate data file
        let path = self.table_path(table_name);
        if path.exists() {
            // Create a new empty file (truncate)
            File::create(path)?;
        }

        Ok(())
    }

    /// Get table schema
    pub fn get_schema(&self, table_name: &str) -> Result<TableSchema> {
        let catalog = self.catalog.read().unwrap();
        catalog.tables.get(table_name).cloned().ok_or_else(|| {
            MiniSqlError::table_not_found(table_name)
        })
    }

    /// Update table schema (e.g., for ALTER TABLE ENGINE)
    pub fn update_schema(&self, table_name: &str, new_schema: TableSchema) -> Result<()> {
        {
            let mut catalog = self.catalog.write().unwrap();
            if !catalog.tables.contains_key(table_name) {
                return Err(MiniSqlError::table_not_found(table_name));
            }
            catalog.tables.insert(table_name.to_string(), new_schema);
        }
        
        // Persist the change
        self.save_catalog()?;
        Ok(())
    }

    /// Check if table exists
    pub fn table_exists(&self, table_name: &str) -> bool {
        let catalog = self.catalog.read().unwrap();
        catalog.tables.contains_key(table_name)
    }

    /// List all tables
    pub fn list_tables(&self) -> Vec<String> {
        let catalog = self.catalog.read().unwrap();
        catalog.tables.keys().cloned().collect()
    }

    /// Allocate a new row ID
    pub fn next_row_id(&self) -> u64 {
        let mut catalog = self.catalog.write().unwrap();
        let id = catalog.next_row_id;
        catalog.next_row_id += 1;
        id
    }

    /// Insert a row into a table
    pub fn insert_row(&self, table_name: &str, values: Vec<Value>) -> Result<u64> {
        let row_id = self.next_row_id();
        
        // Get index column indices from catalog
        let index_col_indices: Vec<(String, Vec<usize>)> = {
            let catalog = self.catalog.read().unwrap();
            catalog.indexes.values()
                .filter(|idx| idx.table_name == table_name)
                .filter_map(|idx| {
                    let schema = catalog.tables.get(table_name)?;
                    let indices: Vec<usize> = idx.columns.iter()
                        .filter_map(|col| schema.find_column(col))
                        .collect();
                    if indices.len() == idx.columns.len() {
                        Some((idx.name.clone(), indices))
                    } else {
                        None
                    }
                })
                .collect()
        };
        
        {
            let mut tables = self.tables.write().unwrap();
            let table = tables.get_mut(table_name).ok_or_else(|| {
                MiniSqlError::table_not_found(table_name)
            })?;

            // Validate column count
            if values.len() != table.schema.columns.len() {
                return Err(MiniSqlError::column_count_mismatch(
                    table.schema.columns.len(),
                    values.len()
                ));
            }

            let row = Row::new(row_id, values.clone());
            
            // Update composite indexes
            for (index_name, col_indices) in &index_col_indices {
                if let Some(index) = table.indexes.get_mut(index_name) {
                    let key = build_composite_key_from_row(&row, col_indices);
                    index.entry(key).or_insert_with(Vec::new).push(row_id);
                }
            }
            
            table.rows.insert(row_id, row);
        }

        Ok(row_id)
    }

    /// Get all rows from a table
    pub fn scan_table(&self, table_name: &str) -> Result<Vec<Row>> {
        let tables = self.tables.read().unwrap();
        let table = tables.get(table_name).ok_or_else(|| {
            MiniSqlError::table_not_found(table_name)
        })?;

        let mut rows: Vec<_> = table.rows.values().cloned().collect();
        rows.sort_by_key(|r| r.id);
        Ok(rows)
    }

    /// Get a row by ID
    pub fn get_row(&self, table_name: &str, row_id: u64) -> Result<Option<Row>> {
        let tables = self.tables.read().unwrap();
        let table = tables.get(table_name).ok_or_else(|| {
            MiniSqlError::table_not_found(table_name)
        })?;

        Ok(table.rows.get(&row_id).cloned())
    }

    /// Update a row
    pub fn update_row(&self, table_name: &str, row_id: u64, values: Vec<Value>) -> Result<bool> {
        // Get index column indices from catalog
        let index_col_indices: Vec<(String, Vec<usize>)> = {
            let catalog = self.catalog.read().unwrap();
            catalog.indexes.values()
                .filter(|idx| idx.table_name == table_name)
                .filter_map(|idx| {
                    let schema = catalog.tables.get(table_name)?;
                    let indices: Vec<usize> = idx.columns.iter()
                        .filter_map(|col| schema.find_column(col))
                        .collect();
                    if indices.len() == idx.columns.len() {
                        Some((idx.name.clone(), indices))
                    } else {
                        None
                    }
                })
                .collect()
        };
        
        let mut tables = self.tables.write().unwrap();
        let table = tables.get_mut(table_name).ok_or_else(|| {
            MiniSqlError::table_not_found(table_name)
        })?;

        if let Some(row) = table.rows.get_mut(&row_id) {
            let old_row = Row::new(row_id, row.values.clone());
            let new_row = Row::new(row_id, values.clone());
            row.values = values;
            
            // Update composite indexes: remove old entries and add new ones
            for (index_name, col_indices) in &index_col_indices {
                if let Some(index) = table.indexes.get_mut(index_name) {
                    // Remove old index entry
                    let old_key = build_composite_key_from_row(&old_row, col_indices);
                    if let Some(row_ids) = index.get_mut(&old_key) {
                        row_ids.retain(|&id| id != row_id);
                        if row_ids.is_empty() {
                            index.remove(&old_key);
                        }
                    }
                    
                    // Add new index entry
                    let new_key = build_composite_key_from_row(&new_row, col_indices);
                    index.entry(new_key).or_insert_with(Vec::new).push(row_id);
                }
            }
            
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Delete a row
    pub fn delete_row(&self, table_name: &str, row_id: u64) -> Result<bool> {
        // Get index column indices from catalog
        let index_col_indices: Vec<(String, Vec<usize>)> = {
            let catalog = self.catalog.read().unwrap();
            catalog.indexes.values()
                .filter(|idx| idx.table_name == table_name)
                .filter_map(|idx| {
                    let schema = catalog.tables.get(table_name)?;
                    let indices: Vec<usize> = idx.columns.iter()
                        .filter_map(|col| schema.find_column(col))
                        .collect();
                    if indices.len() == idx.columns.len() {
                        Some((idx.name.clone(), indices))
                    } else {
                        None
                    }
                })
                .collect()
        };
        
        let mut tables = self.tables.write().unwrap();
        let table = tables.get_mut(table_name).ok_or_else(|| {
            MiniSqlError::table_not_found(table_name)
        })?;

        if let Some(row) = table.rows.remove(&row_id) {
            // Remove from composite indexes
            for (index_name, col_indices) in &index_col_indices {
                if let Some(index) = table.indexes.get_mut(index_name) {
                    let key = build_composite_key_from_row(&row, col_indices);
                    if let Some(row_ids) = index.get_mut(&key) {
                        row_ids.retain(|&id| id != row_id);
                        if row_ids.is_empty() {
                            index.remove(&key);
                        }
                    }
                }
            }
            Ok(true)
        } else {
            Ok(false)
        }
    }

    /// Restore a row (used during recovery)
    pub fn restore_row(&self, table_name: &str, row: Row) -> Result<()> {
        let mut tables = self.tables.write().unwrap();
        let table = tables.get_mut(table_name).ok_or_else(|| {
            MiniSqlError::table_not_found(table_name)
        })?;

        table.rows.insert(row.id, row);
        Ok(())
    }

    /// Apply a schema (used during recovery)
    pub fn apply_schema(&self, schema: TableSchema) -> Result<()> {
        let mut catalog = self.catalog.write().unwrap();
        let mut tables = self.tables.write().unwrap();

        let name = schema.name.clone();
        if !catalog.tables.contains_key(&name) {
            catalog.tables.insert(name.clone(), schema.clone());
            tables.insert(name, TableData::new(schema));
        }

        Ok(())
    }

    /// Flush all tables to disk
    pub fn flush_all(&self) -> Result<()> {
        self.save_catalog()?;
        
        let table_names: Vec<String> = {
            let tables = self.tables.read().unwrap();
            tables.keys().cloned().collect()
        };

        for name in table_names {
            self.save_table(&name)?;
        }

        Ok(())
    }

    /// Replace all rows in a table (used by Sandstone engine for flush)
    /// This atomically replaces the table contents and saves to disk.
    pub fn replace_table_rows(&self, table_name: &str, rows: &[(u64, Vec<Value>)]) -> Result<()> {
        {
            let mut tables = self.tables.write().unwrap();
            let table = tables.get_mut(table_name).ok_or_else(|| {
                MiniSqlError::table_not_found(table_name)
            })?;

            // Clear existing rows
            table.rows.clear();
            table.indexes.clear();

            // Insert new rows
            for (row_id, values) in rows {
                table.rows.insert(*row_id, Row::new(*row_id, values.clone()));
            }
        }

        // Save to disk
        self.save_table(table_name)?;
        
        Ok(())
    }

    /// Get the data directory path
    pub fn data_dir(&self) -> &PathBuf {
        &self.data_dir
    }

    /// Create an index on a table column
    pub fn create_index(&self, index: IndexMetadata, if_not_exists: bool) -> Result<()> {
        let mut catalog = self.catalog.write().unwrap();
        
        // Check if index already exists
        if catalog.indexes.contains_key(&index.name) {
            if if_not_exists {
                return Ok(());
            }
            return Err(MiniSqlError::Table(format!(
                "Index '{}' already exists",
                index.name
            )));
        }
        
        // Validate table exists
        if !catalog.tables.contains_key(&index.table_name) {
            return Err(MiniSqlError::table_not_found(&index.table_name));
        }
        
        // Validate all columns exist
        let schema = &catalog.tables[&index.table_name];
        for col_name in &index.columns {
            if schema.find_column(col_name).is_none() {
                return Err(MiniSqlError::unknown_column(col_name, crate::error::ColumnContext::General));
            }
        }
        
        catalog.indexes.insert(index.name.clone(), index.clone());
        drop(catalog);
        
        // Build the index
        self.build_composite_index(&index.name, &index.table_name, &index.columns)?;
        
        // Save catalog with new index metadata
        self.save_catalog()?;
        
        Ok(())
    }
    
    /// Drop an index
    pub fn drop_index(&self, index_name: &str) -> Result<()> {
        let mut catalog = self.catalog.write().unwrap();
        
        let index = catalog.indexes.remove(index_name).ok_or_else(|| {
            MiniSqlError::Table(format!("Index '{}' not found", index_name))
        })?;
        
        drop(catalog);
        
        // Remove index from table data
        let mut tables = self.tables.write().unwrap();
        if let Some(table) = tables.get_mut(&index.table_name) {
            table.indexes.remove(index_name);
        }
        drop(tables);
        
        // Save catalog
        self.save_catalog()?;
        
        Ok(())
    }
    
    /// Rebuild all indexes from catalog metadata on startup
    fn rebuild_all_indexes(&self) -> Result<()> {
        let catalog = self.catalog.read().unwrap();
        let indexes: Vec<IndexMetadata> = catalog.indexes.values().cloned().collect();
        drop(catalog);

        if indexes.is_empty() {
            return Ok(());
        }

        println!("Rebuilding {} index(es) from catalog metadata...", indexes.len());
        
        for (idx, index_meta) in indexes.iter().enumerate() {
            let columns_str = index_meta.columns.join(", ");
            print!("  [{}/{}] Rebuilding index '{}' on {}({})... ", 
                   idx + 1, 
                   indexes.len(), 
                   index_meta.name,
                   index_meta.table_name,
                   columns_str);
            std::io::stdout().flush().ok();
            
            match self.build_composite_index(&index_meta.name, &index_meta.table_name, &index_meta.columns) {
                Ok(_) => {
                    // Count rows in the index to report
                    let tables = self.tables.read().unwrap();
                    if let Some(table) = tables.get(&index_meta.table_name) {
                        if let Some(index_data) = table.indexes.get(&index_meta.name) {
                            let total_entries: usize = index_data.values().map(|v| v.len()).sum();
                            println!("✓ ({} entries)", total_entries);
                        } else {
                            println!("✓");
                        }
                    } else {
                        println!("✓");
                    }
                }
                Err(e) => {
                    println!("✗");
                    eprintln!("    Warning: Failed to rebuild index '{}': {}", index_meta.name, e);
                    // Continue with other indexes even if one fails
                }
            }
        }
        
        println!("Index rebuilding complete.");
        Ok(())
    }

    /// Build a composite index for a table with multiple columns
    fn build_composite_index(&self, index_name: &str, table_name: &str, columns: &[String]) -> Result<()> {
        let mut tables = self.tables.write().unwrap();
        let table = tables.get_mut(table_name).ok_or_else(|| {
            MiniSqlError::table_not_found(table_name)
        })?;
        
        // Get column indices
        let col_indices: Vec<usize> = columns.iter()
            .filter_map(|name| table.schema.find_column(name))
            .collect();
        
        if col_indices.len() != columns.len() {
            return Err(MiniSqlError::Syntax("One or more index columns not found".to_string()));
        }
        
        let mut index_data: IndexData = BTreeMap::new();
        
        // Scan all rows and populate the index
        for (row_id, row) in &table.rows {
            let key = build_composite_key_from_row(row, &col_indices);
            index_data.entry(key).or_insert_with(Vec::new).push(*row_id);
        }
        
        table.indexes.insert(index_name.to_string(), index_data);
        
        Ok(())
    }
    
    /// Get index metadata for a specific index
    pub fn get_index(&self, index_name: &str) -> Result<IndexMetadata> {
        let catalog = self.catalog.read().unwrap();
        catalog.indexes.get(index_name).cloned().ok_or_else(|| {
            MiniSqlError::Table(format!("Index '{}' not found", index_name))
        })
    }
    
    /// List all indexes
    pub fn list_indexes(&self) -> Vec<IndexMetadata> {
        let catalog = self.catalog.read().unwrap();
        catalog.indexes.values().cloned().collect()
    }
    
    /// Get rows using a single-column index (for backward compatibility)
    pub fn get_rows_by_index(&self, table_name: &str, column_name: &str, value: &Value) -> Result<Vec<Row>> {
        self.get_rows_by_composite_index(table_name, &[column_name.to_string()], &[value.clone()])
    }
    
    /// Get rows using a composite index
    /// Supports prefix matching: if you query with fewer columns than the index has,
    /// it will return all rows that match the prefix.
    pub fn get_rows_by_composite_index(&self, table_name: &str, columns: &[String], values: &[Value]) -> Result<Vec<Row>> {
        let catalog = self.catalog.read().unwrap();
        let tables = self.tables.read().unwrap();
        
        let table = tables.get(table_name).ok_or_else(|| {
            MiniSqlError::table_not_found(table_name)
        })?;
        
        // Find an index that matches these columns (exact match or prefix)
        let matching_index = catalog.indexes.values()
            .find(|idx| idx.table_name == table_name && idx.can_use_for_columns(columns));
        
        if let Some(index_meta) = matching_index {
            if let Some(index_data) = table.indexes.get(&index_meta.name) {
                // Build the composite key prefix from the provided values
                let value_refs: Vec<&Value> = values.iter().collect();
                let key_prefix = build_composite_key(&value_refs);
                
                // If we're querying with all index columns, do exact match
                if columns.len() == index_meta.columns.len() {
                    if let Some(row_ids) = index_data.get(&key_prefix) {
                        let rows: Vec<Row> = row_ids
                            .iter()
                            .filter_map(|id| table.rows.get(id).cloned())
                            .collect();
                        return Ok(rows);
                    }
                } else {
                    // Prefix match: scan all keys that start with this prefix
                    // Use BTreeMap range to find all matching keys
                    let prefix_with_separator = format!("{}\x00", key_prefix);
                    let mut rows = Vec::new();
                    
                    for (key, row_ids) in index_data.range(key_prefix.clone()..) {
                        // Check if this key starts with our prefix
                        if key.starts_with(&key_prefix) && (key == &key_prefix || key.starts_with(&prefix_with_separator)) {
                            for row_id in row_ids {
                                if let Some(row) = table.rows.get(row_id) {
                                    rows.push(row.clone());
                                }
                            }
                        } else if key > &prefix_with_separator && !key.starts_with(&key_prefix) {
                            // We've passed all matching keys
                            break;
                        }
                    }
                    
                    return Ok(rows);
                }
            }
        }
        
        Ok(Vec::new())
    }
    
    /// Find the best index for the given columns on a table
    pub fn find_index_for_columns(&self, table_name: &str, columns: &[String]) -> Option<IndexMetadata> {
        let catalog = self.catalog.read().unwrap();
        
        // Find the best matching index (prefer exact matches, then longer prefix matches)
        catalog.indexes.values()
            .filter(|idx| idx.table_name == table_name)
            .filter_map(|idx| {
                idx.matches_columns(columns).map(|matched| (idx.clone(), matched))
            })
            .max_by_key(|(_, matched)| *matched)
            .map(|(idx, _)| idx)
    }

    /// Get the next auto-increment value for a table (atomically increments counter)
    pub fn next_auto_increment(&self, table_name: &str) -> Result<i64> {
        let mut catalog = self.catalog.write().unwrap();
        let schema = catalog.tables.get_mut(table_name).ok_or_else(|| {
            MiniSqlError::table_not_found(table_name)
        })?;
        
        let value = schema.auto_increment_counter as i64;
        schema.auto_increment_counter += 1;
        Ok(value)
    }

    /// Update the auto-increment counter if the provided value is >= current counter
    /// This is used when explicit values are inserted
    pub fn update_auto_increment_if_needed(&self, table_name: &str, value: i64) -> Result<()> {
        if value <= 0 {
            return Ok(());
        }
        
        let mut catalog = self.catalog.write().unwrap();
        let schema = catalog.tables.get_mut(table_name).ok_or_else(|| {
            MiniSqlError::table_not_found(table_name)
        })?;
        
        let next_value = (value + 1) as u64;
        if next_value > schema.auto_increment_counter {
            schema.auto_increment_counter = next_value;
        }
        Ok(())
    }

    /// Get the current auto-increment counter value (without incrementing)
    pub fn get_auto_increment(&self, table_name: &str) -> Result<u64> {
        let catalog = self.catalog.read().unwrap();
        let schema = catalog.tables.get(table_name).ok_or_else(|| {
            MiniSqlError::table_not_found(table_name)
        })?;
        Ok(schema.auto_increment_counter)
    }

    /// Check if a value exists in a unique index (for primary key uniqueness enforcement)
    pub fn check_unique_violation(
        &self,
        table_name: &str,
        column_indices: &[usize],
        values: &[Value],
        exclude_row_id: Option<u64>,
    ) -> Result<bool> {
        let tables = self.tables.read().unwrap();
        let table = tables.get(table_name).ok_or_else(|| {
            MiniSqlError::table_not_found(table_name)
        })?;

        // Build the key from the column values
        let key_values: Vec<&Value> = column_indices.iter()
            .filter_map(|&idx| values.get(idx))
            .collect();

        // If any key value is NULL, no uniqueness violation (NULL != NULL)
        if key_values.iter().any(|v| v.is_null()) {
            return Ok(false);
        }

        // Check for duplicate in existing rows
        for (row_id, row) in &table.rows {
            // Skip the row we're updating
            if let Some(exclude_id) = exclude_row_id {
                if *row_id == exclude_id {
                    continue;
                }
            }

            // Check if all key columns match
            let matches = column_indices.iter().enumerate().all(|(_, &col_idx)| {
                if col_idx < row.values.len() && col_idx < values.len() {
                    row.values[col_idx] == values[col_idx]
                } else {
                    false
                }
            });

            if matches {
                return Ok(true); // Duplicate found
            }
        }

        Ok(false) // No duplicate
    }

    /// Check if index exists for the given column (single column, backward compatible)
    pub fn has_index_for_columns(&self, table_name: &str, column_name: &str) -> bool {
        self.has_index_for_column_list(table_name, &[column_name.to_string()])
    }
    
    /// Check if index exists for the given column list
    pub fn has_index_for_column_list(&self, table_name: &str, columns: &[String]) -> bool {
        let catalog = self.catalog.read().unwrap();
        catalog.indexes.values().any(|idx| {
            idx.table_name == table_name && idx.can_use_for_columns(columns)
        })
    }

    /// Get primary key index for a table
    pub fn get_primary_key_index(&self, table_name: &str) -> Option<IndexMetadata> {
        let catalog = self.catalog.read().unwrap();
        catalog.indexes.values()
            .find(|idx| idx.table_name == table_name && idx.is_primary)
            .cloned()
    }
    
    /// Get column indices for an index by name
    #[allow(dead_code)]
    fn get_index_column_indices(&self, table_name: &str, index_name: &str) -> Option<Vec<usize>> {
        let catalog = self.catalog.read().unwrap();
        let index = catalog.indexes.get(index_name)?;
        let schema = catalog.tables.get(table_name)?;
        
        let indices: Vec<usize> = index.columns.iter()
            .filter_map(|col| schema.find_column(col))
            .collect();
        
        if indices.len() == index.columns.len() {
            Some(indices)
        } else {
            None
        }
    }

    /// Vacuum the database - rebuild all tables and indexes, compacting data and resetting internal row IDs
    /// This is similar to SQLite's VACUUM command. It:
    /// 1. Rebuilds all table data files (removes any fragmentation)
    /// 2. Resets internal row IDs to start from 1 (does NOT affect primary key values)
    /// 3. Rebuilds all indexes
    /// 4. Resets the next_row_id counter
    ///
    /// Note: This does NOT reset auto-increment counters or primary key values.
    /// Foreign key relationships based on primary keys remain intact.
    pub fn vacuum(&self) -> Result<()> {
        println!("VACUUM: Starting database rebuild...");
        
        // Get list of all tables
        let table_names: Vec<String> = {
            let catalog = self.catalog.read().unwrap();
            catalog.tables.keys().cloned().collect()
        };

        if table_names.is_empty() {
            println!("VACUUM: No tables to rebuild.");
            return Ok(());
        }

        println!("VACUUM: Rebuilding {} table(s)...", table_names.len());

        // Track the new row ID mapping (old_id -> new_id)
        let mut total_rows = 0u64;

        // Process each table
        for (table_idx, table_name) in table_names.iter().enumerate() {
            print!("  [{}/{}] Rebuilding table '{}'... ", 
                   table_idx + 1, table_names.len(), table_name);
            std::io::stdout().flush().ok();

            let old_row_count = {
                let tables = self.tables.read().unwrap();
                tables.get(table_name).map(|t| t.rows.len()).unwrap_or(0)
            };

            // Rebuild this table
            {
                let mut tables = self.tables.write().unwrap();
                let table = tables.get_mut(table_name).ok_or_else(|| {
                    MiniSqlError::table_not_found(table_name)
                })?;

                // Collect all rows and sort by current row ID for deterministic ordering
                let mut row_ids: Vec<u64> = table.rows.keys().copied().collect();
                row_ids.sort();

                // Create new rows with sequential IDs starting from the current total
                let mut new_rows = HashMap::new();
                for (seq_idx, old_id) in row_ids.iter().enumerate() {
                    if let Some(row) = table.rows.get(old_id) {
                        let new_id = total_rows + seq_idx as u64 + 1;
                        let new_row = Row::new(new_id, row.values.clone());
                        new_rows.insert(new_id, new_row);
                    }
                }

                // Update row count
                total_rows += new_rows.len() as u64;

                // Replace old rows with new rows
                table.rows = new_rows;

                // Clear all indexes (they will be rebuilt)
                table.indexes.clear();
            }

            // Save the rebuilt table to disk
            self.save_table(table_name)?;

            println!("✓ ({} rows)", old_row_count);
        }

        // Update the next_row_id counter
        {
            let mut catalog = self.catalog.write().unwrap();
            catalog.next_row_id = total_rows + 1;
        }

        // Rebuild all indexes
        println!("VACUUM: Rebuilding indexes...");
        self.rebuild_all_indexes()?;

        // Save the catalog
        self.save_catalog()?;

        println!("VACUUM: Database rebuild complete. Total rows: {}, next row ID: {}", 
                 total_rows, total_rows + 1);

        Ok(())
    }
}

impl Clone for StorageEngine {
    fn clone(&self) -> Self {
        Self {
            data_dir: self.data_dir.clone(),
            tables: Arc::clone(&self.tables),
            catalog: Arc::clone(&self.catalog),
        }
    }
}

/// Convert a Value to an index key (string representation for BTreeMap ordering)
pub fn value_to_index_key(value: &Value) -> String {
    match value {
        Value::Null => "\0NULL".to_string(),
        Value::Integer(n) => format!("I{:020}", n), // Pad integers for proper sorting
        Value::Float(f) => format!("F{:020}", f.to_bits()), // Convert float bits for sorting
        Value::String(s) => format!("S{}", s),
        Value::Boolean(b) => format!("B{}", if *b { "1" } else { "0" }),
        Value::Json(j) => format!("J{}", j.to_string()),
    }
}

/// Build a composite index key from multiple column values
pub fn build_composite_key(values: &[&Value]) -> String {
    values.iter()
        .map(|v| value_to_index_key(v))
        .collect::<Vec<_>>()
        .join("\x00")  // Use null byte as separator
}

/// Build a composite key from row values given column indices
fn build_composite_key_from_row(row: &Row, col_indices: &[usize]) -> String {
    let values: Vec<&Value> = col_indices.iter()
        .filter_map(|&idx| row.values.get(idx))
        .collect();
    build_composite_key(&values)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{ColumnDef, DataType};
    use tempfile::tempdir;

    fn create_test_schema(name: &str) -> TableSchema {
        TableSchema {
            name: name.to_string(),
            columns: vec![
                ColumnDef {
                    name: "id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    default: None,
                    primary_key: true,
                    auto_increment: false,
                },
                ColumnDef {
                    name: "name".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                    default: None,
                    primary_key: false,
                    auto_increment: false,
                },
            ],
            auto_increment_counter: 1, engine_type: crate::engines::EngineType::default(),
        }
    }

    #[test]
    fn test_storage_engine_basic() {
        let dir = tempdir().unwrap();
        let engine = StorageEngine::new(dir.path().to_path_buf()).unwrap();
        
        let schema = create_test_schema("users");
        engine.create_table(schema, false).unwrap();
        
        assert!(engine.table_exists("users"));
        assert_eq!(engine.list_tables(), vec!["users"]);
        
        let row_id = engine.insert_row("users", vec![Value::Integer(1), Value::String("Alice".into())]).unwrap();
        assert_eq!(row_id, 1);
        
        let rows = engine.scan_table("users").unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[0], Value::Integer(1));
        assert_eq!(rows[0].values[1], Value::String("Alice".into()));
    }

    #[test]
    fn test_storage_engine_persistence() {
        let dir = tempdir().unwrap();
        let path = dir.path().to_path_buf();
        
        {
            let engine = StorageEngine::new(path.clone()).unwrap();
            let schema = create_test_schema("users");
            engine.create_table(schema, false).unwrap();
            engine.insert_row("users", vec![Value::Integer(1), Value::String("Alice".into())]).unwrap();
            // Ensure the table is persisted before dropping the engine.
            engine.save_table("users").unwrap();
        }
        
        // Re-open
        let engine = StorageEngine::new(path).unwrap();
        assert!(engine.table_exists("users"));
        let rows = engine.scan_table("users").unwrap();
        assert_eq!(rows.len(), 1);
    }

    #[test]
    fn test_validate_table_name_rejects_path_traversal() {
        // Parent directory traversal
        assert!(StorageEngine::validate_table_name("../etc/passwd").is_err());
        assert!(StorageEngine::validate_table_name("..\\windows\\system32").is_err());
        
        // Forward slashes
        assert!(StorageEngine::validate_table_name("foo/bar").is_err());
        
        // Backslashes
        assert!(StorageEngine::validate_table_name("foo\\bar").is_err());
        
        // Null bytes
        assert!(StorageEngine::validate_table_name("foo\0bar").is_err());
        
        // Empty name
        assert!(StorageEngine::validate_table_name("").is_err());
        
        // Valid names should pass
        assert!(StorageEngine::validate_table_name("users").is_ok());
        assert!(StorageEngine::validate_table_name("my_table_123").is_ok());
        assert!(StorageEngine::validate_table_name("CamelCaseTable").is_ok());
    }

    #[test]
    fn test_create_table_rejects_malicious_names() {
        let dir = tempdir().unwrap();
        let engine = StorageEngine::new(dir.path().to_path_buf()).unwrap();
        
        let malicious_schema = create_test_schema("../etc/passwd");
        let result = engine.create_table(malicious_schema, false);
        assert!(result.is_err());
        
        let malicious_schema2 = create_test_schema("foo\\bar");
        let result2 = engine.create_table(malicious_schema2, false);
        assert!(result2.is_err());
    }

    #[test]
    fn test_index_based_row_retrieval() {
        let dir = tempdir().unwrap();
        let engine = StorageEngine::new(dir.path().to_path_buf()).unwrap();
        
        let schema = create_test_schema("indexed_table");
        engine.create_table(schema, false).unwrap();
        
        // Insert some rows
        engine.insert_row("indexed_table", vec![Value::Integer(1), Value::String("Alice".into())]).unwrap();
        engine.insert_row("indexed_table", vec![Value::Integer(2), Value::String("Bob".into())]).unwrap();
        engine.insert_row("indexed_table", vec![Value::Integer(3), Value::String("Alice".into())]).unwrap();
        
        // Create an index on the name column
        let index_meta = crate::types::IndexMetadata {
            name: "idx_name".to_string(),
            table_name: "indexed_table".to_string(),
            columns: vec!["name".to_string()],
            unique: false,
            is_primary: false,
        };
        engine.create_index(index_meta, false).unwrap();
        
        // Query using index
        let rows = engine.get_rows_by_index("indexed_table", "name", &Value::String("Alice".into())).unwrap();
        assert_eq!(rows.len(), 2); // Should find both Alice rows
        
        // Query for non-existent value
        let rows = engine.get_rows_by_index("indexed_table", "name", &Value::String("Charlie".into())).unwrap();
        assert_eq!(rows.len(), 0);
    }
    
    #[test]
    fn test_composite_index_creation_and_lookup() {
        let dir = tempdir().unwrap();
        let engine = StorageEngine::new(dir.path().to_path_buf()).unwrap();
        
        // Create a table with multiple columns
        let schema = TableSchema {
            name: "tiles".to_string(),
            columns: vec![
                ColumnDef {
                    name: "layer_id".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    default: None,
                    primary_key: false,
                    auto_increment: false,
                },
                ColumnDef {
                    name: "z".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    default: None,
                    primary_key: false,
                    auto_increment: false,
                },
                ColumnDef {
                    name: "x".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    default: None,
                    primary_key: false,
                    auto_increment: false,
                },
                ColumnDef {
                    name: "y".to_string(),
                    data_type: DataType::Integer,
                    nullable: false,
                    default: None,
                    primary_key: false,
                    auto_increment: false,
                },
                ColumnDef {
                    name: "data".to_string(),
                    data_type: DataType::Text,
                    nullable: true,
                    default: None,
                    primary_key: false,
                    auto_increment: false,
                },
            ],
            auto_increment_counter: 1, engine_type: crate::engines::EngineType::default(),
        };
        engine.create_table(schema, false).unwrap();
        
        // Insert some rows
        engine.insert_row("tiles", vec![Value::Integer(1), Value::Integer(5), Value::Integer(10), Value::Integer(20), Value::String("tile1".into())]).unwrap();
        engine.insert_row("tiles", vec![Value::Integer(1), Value::Integer(5), Value::Integer(10), Value::Integer(21), Value::String("tile2".into())]).unwrap();
        engine.insert_row("tiles", vec![Value::Integer(1), Value::Integer(5), Value::Integer(11), Value::Integer(20), Value::String("tile3".into())]).unwrap();
        engine.insert_row("tiles", vec![Value::Integer(2), Value::Integer(5), Value::Integer(10), Value::Integer(20), Value::String("tile4".into())]).unwrap();
        
        // Create a composite index on (layer_id, z, x, y)
        let index_meta = crate::types::IndexMetadata {
            name: "idx_tiles_composite".to_string(),
            table_name: "tiles".to_string(),
            columns: vec!["layer_id".to_string(), "z".to_string(), "x".to_string(), "y".to_string()],
            unique: false,
            is_primary: false,
        };
        engine.create_index(index_meta, false).unwrap();
        
        // Query using all four columns
        let rows = engine.get_rows_by_composite_index(
            "tiles",
            &["layer_id".to_string(), "z".to_string(), "x".to_string(), "y".to_string()],
            &[Value::Integer(1), Value::Integer(5), Value::Integer(10), Value::Integer(20)]
        ).unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].values[4], Value::String("tile1".into()));
        
        // Query for non-existent combination
        let rows = engine.get_rows_by_composite_index(
            "tiles",
            &["layer_id".to_string(), "z".to_string(), "x".to_string(), "y".to_string()],
            &[Value::Integer(1), Value::Integer(5), Value::Integer(10), Value::Integer(99)]
        ).unwrap();
        assert_eq!(rows.len(), 0);
    }
    
    #[test]
    fn test_composite_index_prefix_matching() {
        let dir = tempdir().unwrap();
        let engine = StorageEngine::new(dir.path().to_path_buf()).unwrap();
        
        let schema = create_test_schema("users");
        engine.create_table(schema, false).unwrap();
        
        // Create a composite index on (id, name)
        let index_meta = crate::types::IndexMetadata {
            name: "idx_id_name".to_string(),
            table_name: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
            unique: false,
            is_primary: false,
        };
        engine.create_index(index_meta, false).unwrap();
        
        // Check that the index can be found for the full column list
        assert!(engine.has_index_for_column_list("users", &["id".to_string(), "name".to_string()]));
        
        // Check that the index can be found for a prefix (just id)
        assert!(engine.has_index_for_column_list("users", &["id".to_string()]));
        
        // Check that the index cannot be found for non-prefix columns
        assert!(!engine.has_index_for_column_list("users", &["name".to_string()]));
    }
    
    #[test]
    fn test_composite_index_maintenance_on_dml() {
        let dir = tempdir().unwrap();
        let engine = StorageEngine::new(dir.path().to_path_buf()).unwrap();
        
        let schema = create_test_schema("users");
        engine.create_table(schema, false).unwrap();
        
        // Create a composite index
        let index_meta = crate::types::IndexMetadata {
            name: "idx_id_name".to_string(),
            table_name: "users".to_string(),
            columns: vec!["id".to_string(), "name".to_string()],
            unique: false,
            is_primary: false,
        };
        engine.create_index(index_meta, false).unwrap();
        
        // Insert a row
        let row_id = engine.insert_row("users", vec![Value::Integer(1), Value::String("Alice".into())]).unwrap();
        
        // Verify the index lookup works
        let rows = engine.get_rows_by_composite_index(
            "users",
            &["id".to_string(), "name".to_string()],
            &[Value::Integer(1), Value::String("Alice".into())]
        ).unwrap();
        assert_eq!(rows.len(), 1);
        
        // Update the row
        engine.update_row("users", row_id, vec![Value::Integer(1), Value::String("Bob".into())]).unwrap();
        
        // Old value should not be found
        let rows = engine.get_rows_by_composite_index(
            "users",
            &["id".to_string(), "name".to_string()],
            &[Value::Integer(1), Value::String("Alice".into())]
        ).unwrap();
        assert_eq!(rows.len(), 0);
        
        // New value should be found
        let rows = engine.get_rows_by_composite_index(
            "users",
            &["id".to_string(), "name".to_string()],
            &[Value::Integer(1), Value::String("Bob".into())]
        ).unwrap();
        assert_eq!(rows.len(), 1);
        
        // Delete the row
        engine.delete_row("users", row_id).unwrap();
        
        // Should not be found anymore
        let rows = engine.get_rows_by_composite_index(
            "users",
            &["id".to_string(), "name".to_string()],
            &[Value::Integer(1), Value::String("Bob".into())]
        ).unwrap();
        assert_eq!(rows.len(), 0);
    }
}
