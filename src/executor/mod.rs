//! Query Executor for MiniSQL
//!
//! The executor takes parsed SQL statements and executes them against the storage engine,
//! coordinating with the transaction manager for ACID guarantees.
//!
//! ## Execution Model
//!
//! 1. Parse SQL → AST (done by parser)
//! 2. Validate (check tables exist, columns match, types compatible)
//! 3. Execute (scan tables, evaluate WHERE, apply changes)
//! 4. Return results
//!
//! For transactions:
//! - BEGIN: Start a transaction, return txn_id
//! - DML operations: Log to WAL, apply to storage
//! - COMMIT: Flush changes, sync WAL
//! - ROLLBACK: Undo changes from undo log

use std::collections::{HashMap, HashSet};
use crate::error::Result;
use crate::parser::Statement;
use crate::storage::StorageEngine;
use crate::engines::{TransactionManager, granite::TxnId};
use crate::types::{DataType, QueryResult};

pub mod ddl;
pub mod dml;
pub mod query;
pub mod transaction;
pub mod evaluator;
pub mod schema;
pub mod aggregation;

/// A prepared statement stored in the session
#[derive(Debug, Clone)]
pub struct PreparedStatement {
    /// Unique statement ID within this connection
    pub id: u32,
    /// Original SQL string
    pub sql: String,
    /// Parsed statement AST
    pub statement: Statement,
    /// Number of parameter placeholders (?)
    pub param_count: usize,
    /// Column types for result set (for SELECT statements)
    pub column_types: Vec<DataType>,
    /// Column names for result set
    pub column_names: Vec<String>,
}

/// Session state for a client connection
pub struct Session {
    /// Active transaction ID (None if auto-commit mode)
    pub txn_id: Option<TxnId>,
    /// Engines that have been modified during the current transaction
    pub modified_engines: HashSet<EngineType>,
    /// Prepared statements keyed by statement ID
    pub prepared_statements: HashMap<u32, PreparedStatement>,
    /// Next statement ID to assign
    pub next_stmt_id: u32,
    /// Last insert ID generated in this session
    pub last_insert_id: u64,
}

impl Session {
    pub fn new() -> Self {
        Self { 
            txn_id: None,
            modified_engines: HashSet::new(),
            prepared_statements: HashMap::new(),
            next_stmt_id: 1,
            last_insert_id: 0,
        }
    }
}

impl Default for Session {
    fn default() -> Self {
        Self::new()
    }
}

use std::sync::Arc;
use crate::engines::{EngineType, EngineHandler};

/// The query executor
pub struct Executor {
    pub(crate) storage: std::sync::Arc<StorageEngine>,
    pub(crate) txn_manager: std::sync::Arc<TransactionManager>,
    pub(crate) handlers: HashMap<EngineType, Arc<dyn EngineHandler>>,
}

impl Executor {
    /// Get reference to the underlying storage engine
    pub fn storage(&self) -> &std::sync::Arc<StorageEngine> {
        &self.storage
    }

    /// Create a new executor with only Granite engine (backward compatible)
    pub fn new(storage: StorageEngine, txn_manager: TransactionManager) -> Self {
        let storage = std::sync::Arc::new(storage);
        let txn_manager = std::sync::Arc::new(txn_manager);
        
        let mut handlers: HashMap<EngineType, Arc<dyn EngineHandler>> = HashMap::new();
        handlers.insert(
            EngineType::Granite, 
            Arc::new(crate::engines::granite::GraniteHandler::new(
                Arc::clone(&storage),
                Arc::clone(&txn_manager)
            ))
        );

        Self {
            storage,
            txn_manager,
            handlers,
        }
    }

    /// Create a new executor with both Granite and Sandstone engines
    pub fn with_sandstone(
        storage: StorageEngine,
        txn_manager: TransactionManager,
        sandstone_config: crate::engines::SandstoneConfig,
    ) -> crate::error::Result<Self> {
        let storage = std::sync::Arc::new(storage);
        let txn_manager = std::sync::Arc::new(txn_manager);
        
        let mut handlers: HashMap<EngineType, Arc<dyn EngineHandler>> = HashMap::new();
        
        // Register Granite
        handlers.insert(
            EngineType::Granite, 
            Arc::new(crate::engines::granite::GraniteHandler::new(
                Arc::clone(&storage),
                Arc::clone(&txn_manager)
            ))
        );
        
        // Register Sandstone
        let sandstone = crate::engines::SandstoneEngine::new(
            std::sync::Arc::clone(&storage),
            sandstone_config,
        )?;
        handlers.insert(
            EngineType::Sandstone,
            Arc::new(sandstone)
        );
        
        Ok(Self {
            storage,
            txn_manager,
            handlers,
        })
    }
    
    /// Get the engine handler for a table
    pub(crate) fn get_engine(&self, table_name: &str) -> Result<Arc<dyn EngineHandler>> {
        let schema = self.storage.get_schema(table_name)?;
        let engine_type = schema.engine_type;
        
        self.handlers.get(&engine_type)
            .cloned()
            .ok_or_else(|| crate::error::MiniSqlError::Table(
                format!("Engine '{}' not enabled", engine_type)
            ))
    }

    /// Check if a table's engine supports secondary indexes.
    /// Used to decide whether to attempt index-based lookups.
    pub(crate) fn engine_supports_indexes(&self, table_name: &str) -> Result<bool> {
        let engine = self.get_engine(table_name)?;
        Ok(engine.supports_indexes())
    }

    /// Check if a table's engine supports transactions.
    /// Engines that return false will silently ignore BEGIN/COMMIT/ROLLBACK (MySQL MyISAM behavior).
    #[allow(dead_code)]
    pub(crate) fn engine_supports_transactions(&self, table_name: &str) -> Result<bool> {
        let engine = self.get_engine(table_name)?;
        Ok(engine.supports_transactions())
    }

    /// Scan a table, routing to the correct engine
    pub(crate) fn scan_table(&self, table_name: &str) -> Result<Vec<crate::types::Row>> {
        let engine = self.get_engine(table_name)?;
        engine.scan(table_name)
    }

    /// Initialize a table in its engine (e.g., load from disk into memory for Sandstone)
    pub fn init_engine_table(&self, table_name: &str) -> crate::error::Result<()> {
        let engine = self.get_engine(table_name)?;
        engine.init_table(table_name)
    }

    /// Execute a SQL statement
    pub fn execute(&self, stmt: Statement, session: &mut Session) -> Result<QueryResult> {
        let result = match stmt {
            Statement::Begin => self.execute_begin(session),
            Statement::Commit => self.execute_commit(session),
            Statement::Rollback => self.execute_rollback(session),
            Statement::CreateTable(create) => self.execute_create_table(create, session),
            Statement::CreateIndex(create_idx) => self.execute_create_index(create_idx, session),
            Statement::DropTable(name) => self.execute_drop_table(&name, session),
            Statement::DropIndex(name) => self.execute_drop_index(&name, session),
            Statement::TruncateTable(name) => self.execute_truncate_table(&name, session),
            Statement::AlterTable(alter) => self.execute_alter_table(alter, session),
            Statement::Checkpoint => self.execute_checkpoint(session),
            Statement::Vacuum => self.execute_vacuum(session),
            Statement::Select(select) => self.execute_select(select, session),
            Statement::Insert(insert) => self.execute_insert(insert, session),
            Statement::Update(update) => self.execute_update(update, session),
            Statement::Delete(delete) => self.execute_delete(delete, session),
            Statement::ShowTables => self.execute_show_tables(),
            Statement::Describe(table) => self.execute_describe(&table),
        }?;

        // Update session state from result
        if let QueryResult::Modified { last_insert_id, .. } = &result {
            if *last_insert_id > 0 {
                session.last_insert_id = *last_insert_id;
            }
        }

        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::Parser;
    use crate::storage::StorageEngine;
    use crate::engines::TransactionManager;
    use crate::types::Value;
    use tempfile::{tempdir, TempDir};

    fn setup_executor() -> (Executor, Session, TempDir) {
        let dir = tempdir().unwrap();
        let storage = StorageEngine::new(dir.path().to_path_buf()).unwrap();
        let txn_manager = TransactionManager::new(dir.path().to_path_buf()).unwrap();
        let executor = Executor::new(storage, txn_manager);
        let session = Session::new();
        (executor, session, dir)
    }

    #[test]
    fn test_executor_basic_flow() {
        let (executor, mut session, _temp_dir) = setup_executor();
        
        // Create table
        let stmt = Parser::parse("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").unwrap();
        let res = executor.execute(stmt, &mut session).unwrap();
        assert!(matches!(res, QueryResult::Ok));
        
        // Insert
        let stmt = Parser::parse("INSERT INTO users VALUES (1, 'Alice')").unwrap();
        let res = executor.execute(stmt, &mut session).unwrap();
        match res {
            QueryResult::Modified { rows_affected, .. } => assert_eq!(rows_affected, 1),
            _ => panic!("Expected Modified"),
        }
        
        // Select
        let stmt = Parser::parse("SELECT * FROM users").unwrap();
        let res = executor.execute(stmt, &mut session).unwrap();
        match res {
            QueryResult::Select(rs) => {
                assert_eq!(rs.rows.len(), 1);
                assert_eq!(rs.rows[0][0], Value::Integer(1));
                assert_eq!(rs.rows[0][1], Value::String("Alice".into()));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_count_star() {
        let (executor, mut session, _temp_dir) = setup_executor();
        
        // Create table and insert data
        executor.execute(Parser::parse("CREATE TABLE items (id INTEGER PRIMARY KEY, name TEXT)").unwrap(), &mut session).unwrap();
        executor.execute(Parser::parse("INSERT INTO items VALUES (1, 'A')").unwrap(), &mut session).unwrap();
        executor.execute(Parser::parse("INSERT INTO items VALUES (2, 'B')").unwrap(), &mut session).unwrap();
        executor.execute(Parser::parse("INSERT INTO items VALUES (3, 'C')").unwrap(), &mut session).unwrap();
        
        // COUNT(*)
        let stmt = Parser::parse("SELECT COUNT(*) FROM items").unwrap();
        let res = executor.execute(stmt, &mut session).unwrap();
        match res {
            QueryResult::Select(rs) => {
                assert_eq!(rs.rows.len(), 1);
                assert_eq!(rs.rows[0][0], Value::Integer(3));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_count_with_where() {
        let (executor, mut session, _temp_dir) = setup_executor();
        
        executor.execute(Parser::parse("CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)").unwrap(), &mut session).unwrap();
        executor.execute(Parser::parse("INSERT INTO items VALUES (1, 10)").unwrap(), &mut session).unwrap();
        executor.execute(Parser::parse("INSERT INTO items VALUES (2, 50)").unwrap(), &mut session).unwrap();
        executor.execute(Parser::parse("INSERT INTO items VALUES (3, 100)").unwrap(), &mut session).unwrap();
        
        let stmt = Parser::parse("SELECT COUNT(*) FROM items WHERE value > 20").unwrap();
        let res = executor.execute(stmt, &mut session).unwrap();
        match res {
            QueryResult::Select(rs) => {
                assert_eq!(rs.rows[0][0], Value::Integer(2)); // 50 and 100
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_sum() {
        let (executor, mut session, _temp_dir) = setup_executor();
        
        executor.execute(Parser::parse("CREATE TABLE items (id INTEGER PRIMARY KEY, amount INTEGER)").unwrap(), &mut session).unwrap();
        executor.execute(Parser::parse("INSERT INTO items VALUES (1, 10)").unwrap(), &mut session).unwrap();
        executor.execute(Parser::parse("INSERT INTO items VALUES (2, 20)").unwrap(), &mut session).unwrap();
        executor.execute(Parser::parse("INSERT INTO items VALUES (3, 30)").unwrap(), &mut session).unwrap();
        
        let stmt = Parser::parse("SELECT SUM(amount) FROM items").unwrap();
        let res = executor.execute(stmt, &mut session).unwrap();
        match res {
            QueryResult::Select(rs) => {
                assert_eq!(rs.rows[0][0], Value::Integer(60));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_avg() {
        let (executor, mut session, _temp_dir) = setup_executor();
        
        executor.execute(Parser::parse("CREATE TABLE items (id INTEGER PRIMARY KEY, value INTEGER)").unwrap(), &mut session).unwrap();
        executor.execute(Parser::parse("INSERT INTO items VALUES (1, 10)").unwrap(), &mut session).unwrap();
        executor.execute(Parser::parse("INSERT INTO items VALUES (2, 20)").unwrap(), &mut session).unwrap();
        executor.execute(Parser::parse("INSERT INTO items VALUES (3, 30)").unwrap(), &mut session).unwrap();
        
        let stmt = Parser::parse("SELECT AVG(value) FROM items").unwrap();
        let res = executor.execute(stmt, &mut session).unwrap();
        match res {
            QueryResult::Select(rs) => {
                match &rs.rows[0][0] {
                    Value::Float(f) => assert!((f - 20.0).abs() < 0.01),
                    _ => panic!("Expected Float"),
                }
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_group_by() {
        let (executor, mut session, _temp_dir) = setup_executor();
        
        executor.execute(Parser::parse("CREATE TABLE sales (id INTEGER PRIMARY KEY, category TEXT, amount INTEGER)").unwrap(), &mut session).unwrap();
        executor.execute(Parser::parse("INSERT INTO sales VALUES (1, 'A', 10)").unwrap(), &mut session).unwrap();
        executor.execute(Parser::parse("INSERT INTO sales VALUES (2, 'A', 20)").unwrap(), &mut session).unwrap();
        executor.execute(Parser::parse("INSERT INTO sales VALUES (3, 'B', 30)").unwrap(), &mut session).unwrap();
        executor.execute(Parser::parse("INSERT INTO sales VALUES (4, 'B', 40)").unwrap(), &mut session).unwrap();
        executor.execute(Parser::parse("INSERT INTO sales VALUES (5, 'B', 50)").unwrap(), &mut session).unwrap();
        
        let stmt = Parser::parse("SELECT category, COUNT(*) FROM sales GROUP BY category").unwrap();
        let res = executor.execute(stmt, &mut session).unwrap();
        match res {
            QueryResult::Select(rs) => {
                assert_eq!(rs.rows.len(), 2); // Two groups: A and B
                
                // Find counts for each category
                let mut a_count = 0i64;
                let mut b_count = 0i64;
                for row in &rs.rows {
                    match (&row[0], &row[1]) {
                        (Value::String(cat), Value::Integer(count)) => {
                            if cat == "A" { a_count = *count; }
                            if cat == "B" { b_count = *count; }
                        }
                        _ => {}
                    }
                }
                assert_eq!(a_count, 2);
                assert_eq!(b_count, 3);
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_group_by_with_sum() {
        let (executor, mut session, _temp_dir) = setup_executor();
        
        executor.execute(Parser::parse("CREATE TABLE sales (id INTEGER PRIMARY KEY, category TEXT, amount INTEGER)").unwrap(), &mut session).unwrap();
        executor.execute(Parser::parse("INSERT INTO sales VALUES (1, 'A', 10)").unwrap(), &mut session).unwrap();
        executor.execute(Parser::parse("INSERT INTO sales VALUES (2, 'A', 20)").unwrap(), &mut session).unwrap();
        executor.execute(Parser::parse("INSERT INTO sales VALUES (3, 'B', 30)").unwrap(), &mut session).unwrap();
        
        let stmt = Parser::parse("SELECT category, SUM(amount) FROM sales GROUP BY category").unwrap();
        let res = executor.execute(stmt, &mut session).unwrap();
        match res {
            QueryResult::Select(rs) => {
                assert_eq!(rs.rows.len(), 2);
                
                let mut a_sum = 0i64;
                let mut b_sum = 0i64;
                for row in &rs.rows {
                    match (&row[0], &row[1]) {
                        (Value::String(cat), Value::Integer(sum)) => {
                            if cat == "A" { a_sum = *sum; }
                            if cat == "B" { b_sum = *sum; }
                        }
                        _ => {}
                    }
                }
                assert_eq!(a_sum, 30); // 10 + 20
                assert_eq!(b_sum, 30); // 30
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_select_with_index() {
        let (executor, mut session, _temp_dir) = setup_executor();
        
        // Create table with data
        executor.execute(Parser::parse("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, category TEXT)").unwrap(), &mut session).unwrap();
        executor.execute(Parser::parse("INSERT INTO products VALUES (1, 'Widget', 'Hardware')").unwrap(), &mut session).unwrap();
        executor.execute(Parser::parse("INSERT INTO products VALUES (2, 'Gadget', 'Hardware')").unwrap(), &mut session).unwrap();
        executor.execute(Parser::parse("INSERT INTO products VALUES (3, 'Software', 'Digital')").unwrap(), &mut session).unwrap();
        executor.execute(Parser::parse("INSERT INTO products VALUES (4, 'Ebook', 'Digital')").unwrap(), &mut session).unwrap();
        
        // Create index on category column
        executor.execute(Parser::parse("CREATE INDEX idx_category ON products(category)").unwrap(), &mut session).unwrap();
        
        // Query using indexed column - should use index scan internally
        let stmt = Parser::parse("SELECT * FROM products WHERE category = 'Digital'").unwrap();
        let res = executor.execute(stmt, &mut session).unwrap();
        match res {
            QueryResult::Select(rs) => {
                assert_eq!(rs.rows.len(), 2); // Software and Ebook
            }
            _ => panic!("Expected Select"),
        }
        
        // Query with non-indexed column - should still work via full scan
        let stmt = Parser::parse("SELECT * FROM products WHERE name = 'Widget'").unwrap();
        let res = executor.execute(stmt, &mut session).unwrap();
        match res {
            QueryResult::Select(rs) => {
                assert_eq!(rs.rows.len(), 1);
                assert_eq!(rs.rows[0][1], Value::String("Widget".into()));
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_select_with_reversed_equality() {
        // Test that 'literal = column' is also optimized
        let (executor, mut session, _temp_dir) = setup_executor();
        
        executor.execute(Parser::parse("CREATE TABLE items (id INTEGER PRIMARY KEY, status TEXT)").unwrap(), &mut session).unwrap();
        executor.execute(Parser::parse("INSERT INTO items VALUES (1, 'active')").unwrap(), &mut session).unwrap();
        executor.execute(Parser::parse("INSERT INTO items VALUES (2, 'inactive')").unwrap(), &mut session).unwrap();
        executor.execute(Parser::parse("INSERT INTO items VALUES (3, 'active')").unwrap(), &mut session).unwrap();
        
        executor.execute(Parser::parse("CREATE INDEX idx_status ON items(status)").unwrap(), &mut session).unwrap();
        
        // Query with literal = column (reversed)
        let stmt = Parser::parse("SELECT * FROM items WHERE 'active' = status").unwrap();
        let res = executor.execute(stmt, &mut session).unwrap();
        match res {
            QueryResult::Select(rs) => {
                assert_eq!(rs.rows.len(), 2); // Both active items
            }
            _ => panic!("Expected Select"),
        }
    }

    #[test]
    fn test_truncate_table() {
        let (executor, mut session, _temp_dir) = setup_executor();
        
        // Create table and insert data
        executor.execute(Parser::parse("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)").unwrap(), &mut session).unwrap();
        executor.execute(Parser::parse("INSERT INTO test_table VALUES (1, 'Alice')").unwrap(), &mut session).unwrap();
        executor.execute(Parser::parse("INSERT INTO test_table VALUES (2, 'Bob')").unwrap(), &mut session).unwrap();
        
        // Verify data exists
        let stmt = Parser::parse("SELECT * FROM test_table").unwrap();
        let res = executor.execute(stmt, &mut session).unwrap();
        match res {
            QueryResult::Select(rs) => assert_eq!(rs.rows.len(), 2),
            _ => panic!("Expected Select"),
        }
        
        // Truncate table
        let stmt = Parser::parse("TRUNCATE TABLE test_table").unwrap();
        let res = executor.execute(stmt, &mut session).unwrap();
        assert!(matches!(res, QueryResult::Ok));
        
        // Verify table is empty
        let stmt = Parser::parse("SELECT * FROM test_table").unwrap();
        let res = executor.execute(stmt, &mut session).unwrap();
        match res {
            QueryResult::Select(rs) => assert_eq!(rs.rows.len(), 0),
            _ => panic!("Expected Select"),
        }
        
        // Verify schema still exists
        let stmt = Parser::parse("INSERT INTO test_table VALUES (3, 'Charlie')").unwrap();
        let res = executor.execute(stmt, &mut session).unwrap();
        assert!(matches!(res, QueryResult::Modified { .. }));
    }

    #[test]
    fn test_checkpoint() {
        let (executor, mut session, _temp_dir) = setup_executor();
        
        // Create table and insert data
        executor.execute(Parser::parse("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)").unwrap(), &mut session).unwrap();
        executor.execute(Parser::parse("INSERT INTO test_table VALUES (1, 'Alice')").unwrap(), &mut session).unwrap();
        
        // Execute CHECKPOINT
        let stmt = Parser::parse("CHECKPOINT").unwrap();
        let res = executor.execute(stmt, &mut session).unwrap();
        assert!(matches!(res, QueryResult::Ok));
    }
}
