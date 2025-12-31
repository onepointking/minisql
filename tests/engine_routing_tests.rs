use minisql::parser::Parser;
use minisql::engines::{EngineType, SandstoneConfig};
use minisql::executor::{Executor, Session};
use minisql::storage::StorageEngine;
use minisql::engines::granite::TransactionManager;
use tempfile::TempDir;

fn setup_sandstone_executor() -> (TempDir, Executor) {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageEngine::new(temp_dir.path().to_path_buf()).unwrap();
    let txn_manager = TransactionManager::new(temp_dir.path().to_path_buf()).unwrap();
    let executor = Executor::with_sandstone(
        storage,
        txn_manager,
        SandstoneConfig::default(),
    ).unwrap();
    (temp_dir, executor)
}

#[test]
fn test_insert_routing() {
    let (_temp_dir, executor) = setup_sandstone_executor();
    let mut session = Session::new();

    // 1. Create Granite Table (Implicit default)
    let create_granite = Parser::parse("CREATE TABLE granite_t (id INT PRIMARY KEY, val TEXT)").unwrap();
    executor.execute(create_granite, &mut session).unwrap();

    // 2. Create Sandstone Table (Explicit engine)
    let create_sandstone = Parser::parse("CREATE TABLE sandstone_t (id INT PRIMARY KEY, val TEXT) ENGINE=Sandstone").unwrap();
    executor.execute(create_sandstone, &mut session).unwrap();

    // 3. Verify schemas have correct engine types
    let granite_schema = executor.storage().get_schema("granite_t").unwrap();
    assert_eq!(granite_schema.engine_type, EngineType::Granite);

    let sandstone_schema = executor.storage().get_schema("sandstone_t").unwrap();
    assert_eq!(sandstone_schema.engine_type, EngineType::Sandstone);

    // 4. Insert into Granite table
    let insert_granite = Parser::parse("INSERT INTO granite_t VALUES (1, 'granite_val')").unwrap();
    let res_granite = executor.execute(insert_granite, &mut session).unwrap();
    match res_granite {
        minisql::types::QueryResult::Modified { rows_affected, .. } => assert_eq!(rows_affected, 1),
        _ => panic!("Expected insert result"),
    }

    // 5. Insert into Sandstone table
    let insert_sandstone = Parser::parse("INSERT INTO sandstone_t VALUES (1, 'sandstone_val')").unwrap();
    let res_sandstone = executor.execute(insert_sandstone, &mut session).unwrap();
    match res_sandstone {
        minisql::types::QueryResult::Modified { rows_affected, .. } => assert_eq!(rows_affected, 1),
        _ => panic!("Expected insert result"),
    }

    // 6. Verify data retrieval works for both
    let select_granite = Parser::parse("SELECT * FROM granite_t").unwrap();
    match executor.execute(select_granite, &mut session).unwrap() {
        minisql::types::QueryResult::Select(rs) => {
            assert_eq!(rs.rows.len(), 1);
            assert_eq!(rs.rows[0][1], minisql::types::Value::String("granite_val".to_string()));
        }
        _ => panic!("Expected Select result"),
    }

    let select_sandstone = Parser::parse("SELECT * FROM sandstone_t").unwrap();
    match executor.execute(select_sandstone, &mut session).unwrap() {
        minisql::types::QueryResult::Select(rs) => {
            assert_eq!(rs.rows.len(), 1);
            assert_eq!(rs.rows[0][1], minisql::types::Value::String("sandstone_val".to_string()));
        }
        _ => panic!("Expected Select result"),
    }
}

#[test]
fn test_engine_persistence() {
    let temp_dir = TempDir::new().unwrap();
    let db_path = temp_dir.path().to_path_buf();
    
    // Phase 1: Create tables and close executor
    {
        let storage = StorageEngine::new(db_path.clone()).unwrap();
        let txn_manager = TransactionManager::new(db_path.clone()).unwrap();
        let executor = Executor::with_sandstone(
            storage,
            txn_manager,
            SandstoneConfig::default(),
        ).unwrap();
        let mut session = Session::new();

        executor.execute(Parser::parse("CREATE TABLE s_table (id INT PRIMARY KEY) ENGINE=Sandstone").unwrap(), &mut session).unwrap();
        executor.execute(Parser::parse("CREATE TABLE g_table (id INT PRIMARY KEY) ENGINE=Granite").unwrap(), &mut session).unwrap();
    } // Executor dropped here

    // Phase 2: Re-open executor and verify engine types persisted
    {
        let storage = StorageEngine::new(db_path.clone()).unwrap();
        let txn_manager = TransactionManager::new(db_path.clone()).unwrap();
        let executor = Executor::with_sandstone(
            storage,
            txn_manager,
            SandstoneConfig::default(),
        ).unwrap();

        let s_schema = executor.storage().get_schema("s_table").expect("Sandstone table should exist");
        assert_eq!(s_schema.engine_type, EngineType::Sandstone, "Sandstone engine type should be persisted");

        let g_schema = executor.storage().get_schema("g_table").expect("Granite table should exist");
        assert_eq!(g_schema.engine_type, EngineType::Granite, "Granite engine type should be persisted");
    }
}

#[test]
fn test_alter_table_switching_execution() {
    let (_temp_dir, executor) = setup_sandstone_executor();
    let mut session = Session::new();

    // 1. Create as Granite
    executor.execute(Parser::parse("CREATE TABLE switch_t (id INT PRIMARY KEY, val INT)").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("INSERT INTO switch_t VALUES (1, 100)").unwrap(), &mut session).unwrap();

    // Verify initial state
    let schema_v1 = executor.storage().get_schema("switch_t").unwrap();
    assert_eq!(schema_v1.engine_type, EngineType::Granite);

    // 2. Switch to Sandstone
    executor.execute(Parser::parse("ALTER TABLE switch_t ENGINE=Sandstone").unwrap(), &mut session).unwrap();
    
    // Verify switch
    let schema_v2 = executor.storage().get_schema("switch_t").unwrap();
    assert_eq!(schema_v2.engine_type, EngineType::Sandstone);

    // 3. Insert new data (should go to Sandstone engine)
    executor.execute(Parser::parse("INSERT INTO switch_t VALUES (2, 200)").unwrap(), &mut session).unwrap();

    // 4. Verify all data is accessible
    match executor.execute(Parser::parse("SELECT * FROM switch_t ORDER BY id").unwrap(), &mut session).unwrap() {
        minisql::types::QueryResult::Select(rs) => {
            assert_eq!(rs.rows.len(), 2);
            assert_eq!(rs.rows[0][1], minisql::types::Value::Integer(100)); // From Granite
            assert_eq!(rs.rows[1][1], minisql::types::Value::Integer(200)); // From Sandstone
        }
        _ => panic!("Expected Select result"),
    }
}

// ============== Engine Capability Tests ==============

#[test]
fn test_engine_supports_indexes_granite() {
    let (_temp_dir, executor) = setup_sandstone_executor();
    let mut session = Session::new();
    
    // Create a Granite table
    executor.execute(Parser::parse("CREATE TABLE granite_idx_test (id INT PRIMARY KEY, val TEXT)").unwrap(), &mut session).unwrap();
    
    // Granite should support indexes (used for optimized lookups)
    // We can't directly call engine_supports_indexes since it's pub(crate),
    // but we can verify index creation works
    executor.execute(Parser::parse("CREATE INDEX idx_val ON granite_idx_test(val)").unwrap(), &mut session).unwrap();
    
    // Insert test data
    executor.execute(Parser::parse("INSERT INTO granite_idx_test VALUES (1, 'test')").unwrap(), &mut session).unwrap();
    
    // Query should work (index may or may not be used, but shouldn't error)
    let result = executor.execute(Parser::parse("SELECT * FROM granite_idx_test WHERE val = 'test'").unwrap(), &mut session);
    assert!(result.is_ok());
}

#[test]
fn test_engine_sandstone_full_scan_only() {
    let (_temp_dir, executor) = setup_sandstone_executor();
    let mut session = Session::new();
    
    // Create a Sandstone table
    executor.execute(Parser::parse("CREATE TABLE sand_scan_test (id INT PRIMARY KEY, val TEXT) ENGINE=Sandstone").unwrap(), &mut session).unwrap();
    
    // Insert test data
    executor.execute(Parser::parse("INSERT INTO sand_scan_test VALUES (1, 'apple')").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("INSERT INTO sand_scan_test VALUES (2, 'banana')").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("INSERT INTO sand_scan_test VALUES (3, 'cherry')").unwrap(), &mut session).unwrap();
    
    // Query with WHERE clause should work via full scan (Sandstone doesn't support indexes)
    let result = executor.execute(Parser::parse("SELECT * FROM sand_scan_test WHERE val = 'banana'").unwrap(), &mut session).unwrap();
    match result {
        minisql::types::QueryResult::Select(rs) => {
            assert_eq!(rs.rows.len(), 1);
            assert_eq!(rs.rows[0][1], minisql::types::Value::String("banana".to_string()));
        }
        _ => panic!("Expected Select result"),
    }
}

#[test]
fn test_init_engine_table() {
    let (_temp_dir, executor) = setup_sandstone_executor();
    let mut session = Session::new();
    
    // Create a Sandstone table with data
    executor.execute(Parser::parse("CREATE TABLE init_test (id INT PRIMARY KEY, val TEXT) ENGINE=Sandstone").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("INSERT INTO init_test VALUES (1, 'hello')").unwrap(), &mut session).unwrap();
    
    // Use the generic init_engine_table method (tests the renamed API)
    let init_result = executor.init_engine_table("init_test");
    assert!(init_result.is_ok(), "init_engine_table should succeed for Sandstone tables");
    
    // Data should still be accessible
    let result = executor.execute(Parser::parse("SELECT * FROM init_test").unwrap(), &mut session).unwrap();
    match result {
        minisql::types::QueryResult::Select(rs) => {
            assert_eq!(rs.rows.len(), 1);
        }
        _ => panic!("Expected Select result"),
    }
}

#[test]
fn test_transactions_on_sandstone_mysql_behavior() {
    // MySQL MyISAM behavior: BEGIN succeeds but has no effect (no rollback)
    let (_temp_dir, executor) = setup_sandstone_executor();
    let mut session = Session::new();
    
    // Create a Sandstone table
    executor.execute(Parser::parse("CREATE TABLE txn_test (id INT PRIMARY KEY, val INT) ENGINE=Sandstone").unwrap(), &mut session).unwrap();
    
    // BEGIN should succeed (MySQL compatibility)
    let begin_result = executor.execute(Parser::parse("BEGIN").unwrap(), &mut session);
    assert!(begin_result.is_ok(), "BEGIN should succeed on Sandstone (MySQL MyISAM behavior)");
    
    // Insert data
    executor.execute(Parser::parse("INSERT INTO txn_test VALUES (1, 100)").unwrap(), &mut session).unwrap();
    
    // ROLLBACK - but Sandstone auto-commits, so data should still be there
    let rollback_result = executor.execute(Parser::parse("ROLLBACK").unwrap(), &mut session);
    assert!(rollback_result.is_ok(), "ROLLBACK should succeed on Sandstone");
    
    // Data should still exist (Sandstone ignores transaction semantics)
    let result = executor.execute(Parser::parse("SELECT * FROM txn_test").unwrap(), &mut session).unwrap();
    match result {
        minisql::types::QueryResult::Select(rs) => {
            // Sandstone auto-commits, so the row should be there despite ROLLBACK
            assert_eq!(rs.rows.len(), 1, "Sandstone should have auto-committed the insert");
        }
        _ => panic!("Expected Select result"),
    }
}

#[test]
fn test_sandstone_only_transaction_fast_commit() {
    // Sandstone-only transactions should NOT wait for WAL fsync
    // This verifies the fix for the profiler issue where COMMIT took ~360ms
    let (_temp_dir, executor) = setup_sandstone_executor();
    let mut session = Session::new();
    
    // Create a Sandstone table
    executor.execute(Parser::parse("CREATE TABLE fast_commit_test (id INT PRIMARY KEY, val INT) ENGINE=Sandstone").unwrap(), &mut session).unwrap();
    
    // Start transaction
    executor.execute(Parser::parse("BEGIN").unwrap(), &mut session).unwrap();
    
    // Insert data into Sandstone table
    executor.execute(Parser::parse("INSERT INTO fast_commit_test VALUES (1, 100)").unwrap(), &mut session).unwrap();
    
    // COMMIT should be fast (no WAL fsync needed for Sandstone-only transactions)
    let start = std::time::Instant::now();
    let commit_result = executor.execute(Parser::parse("COMMIT").unwrap(), &mut session);
    let elapsed = start.elapsed();
    
    assert!(commit_result.is_ok(), "COMMIT should succeed");
    
    // Sandstone-only commit should be much faster than 300ms WAL fsync
    // With checkpoint optimization, this should be very fast (<50ms)
    assert!(elapsed.as_millis() < 50, 
        "Sandstone-only COMMIT should be fast (<50ms), took {}ms", elapsed.as_millis());
    
    // Data should be accessible
    let result = executor.execute(Parser::parse("SELECT * FROM fast_commit_test").unwrap(), &mut session).unwrap();
    match result {
        minisql::types::QueryResult::Select(rs) => {
            assert_eq!(rs.rows.len(), 1);
        }
        _ => panic!("Expected Select result"),
    }
}

#[test]
fn test_empty_transaction_fast_commit() {
    // Empty transactions should not wait for WAL fsync
    let (_temp_dir, executor) = setup_sandstone_executor();
    let mut session = Session::new();
    
    // Create a table (doesn't matter which engine)
    executor.execute(Parser::parse("CREATE TABLE empty_txn_test (id INT PRIMARY KEY)").unwrap(), &mut session).unwrap();
    
    // Start transaction
    executor.execute(Parser::parse("BEGIN").unwrap(), &mut session).unwrap();
    
    // COMMIT immediately without any DML operations
    let start = std::time::Instant::now();
    let commit_result = executor.execute(Parser::parse("COMMIT").unwrap(), &mut session);
    let elapsed = start.elapsed();
    
    assert!(commit_result.is_ok(), "Empty COMMIT should succeed");
    
    // Empty commit should be instant (<50ms)
    assert!(elapsed.as_millis() < 50, 
        "Empty COMMIT should be instant (<50ms), took {}ms", elapsed.as_millis());
}

#[test]
fn test_modified_engines_tracking() {
    // Verify that modified_engines is correctly tracked in the session
    let (_temp_dir, executor) = setup_sandstone_executor();
    let mut session = Session::new();
    
    // Create tables with different engines
    executor.execute(Parser::parse("CREATE TABLE granite_track (id INT PRIMARY KEY, val INT)").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("CREATE TABLE sandstone_track (id INT PRIMARY KEY, val INT) ENGINE=Sandstone").unwrap(), &mut session).unwrap();
    
    // Start transaction
    executor.execute(Parser::parse("BEGIN").unwrap(), &mut session).unwrap();
    
    // Initially no engines modified
    assert!(session.modified_engines.is_empty(), "No engines should be modified yet");
    
    // Insert into Sandstone table
    executor.execute(Parser::parse("INSERT INTO sandstone_track VALUES (1, 100)").unwrap(), &mut session).unwrap();
    assert!(session.modified_engines.contains(&minisql::engines::EngineType::Sandstone));
    assert_eq!(session.modified_engines.len(), 1);
    
    // Insert into Granite table
    executor.execute(Parser::parse("INSERT INTO granite_track VALUES (1, 200)").unwrap(), &mut session).unwrap();
    assert!(session.modified_engines.contains(&minisql::engines::EngineType::Granite));
    assert_eq!(session.modified_engines.len(), 2);
    
    // Commit should clear modified_engines
    executor.execute(Parser::parse("COMMIT").unwrap(), &mut session).unwrap();
    assert!(session.modified_engines.is_empty(), "modified_engines should be cleared after COMMIT");
}
