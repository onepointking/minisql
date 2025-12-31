//! Tests for ALTER TABLE ENGINE functionality

use minisql::parser::Parser;
use minisql::engines::{EngineType, SandstoneConfig};
use minisql::executor::{Executor, Session};
use minisql::storage::StorageEngine;
use minisql::engines::granite::TransactionManager;
use tempfile::TempDir;

fn setup_test_env() -> (TempDir, StorageEngine, TransactionManager) {
    let temp_dir = TempDir::new().unwrap();
    let storage = StorageEngine::new(temp_dir.path().to_path_buf()).unwrap();
    let txn_manager = TransactionManager::new(temp_dir.path().to_path_buf()).unwrap();
    (temp_dir, storage, txn_manager)
}

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

// ============== Parser Tests ==============

#[test]
fn test_parse_alter_table_engine_sandstone() {
    let stmt = Parser::parse("ALTER TABLE logs ENGINE=Sandstone").unwrap();
    match stmt {
        minisql::parser::Statement::AlterTable(alter) => {
            assert_eq!(alter.table_name, "logs");
            match alter.action {
                minisql::parser::AlterTableAction::ChangeEngine(engine) => {
                    assert_eq!(engine, EngineType::Sandstone);
                }
            }
        }
        _ => panic!("Expected AlterTable statement"),
    }
}

#[test]
fn test_parse_alter_table_engine_granite() {
    let stmt = Parser::parse("ALTER TABLE users ENGINE=Granite").unwrap();
    match stmt {
        minisql::parser::Statement::AlterTable(alter) => {
            assert_eq!(alter.table_name, "users");
            match alter.action {
                minisql::parser::AlterTableAction::ChangeEngine(engine) => {
                    assert_eq!(engine, EngineType::Granite);
                }
            }
        }
        _ => panic!("Expected AlterTable statement"),
    }
}

#[test]
fn test_parse_alter_table_engine_case_insensitive() {
    // Engine names should be case-insensitive
    let cases = vec![
        ("ALTER TABLE t ENGINE=sandstone", EngineType::Sandstone),
        ("ALTER TABLE t ENGINE=SANDSTONE", EngineType::Sandstone),
        ("ALTER TABLE t ENGINE=SandStone", EngineType::Sandstone),
        ("ALTER TABLE t ENGINE=granite", EngineType::Granite),
        ("ALTER TABLE t ENGINE=GRANITE", EngineType::Granite),
    ];
    
    for (sql, expected_engine) in cases {
        let stmt = Parser::parse(sql).unwrap();
        match stmt {
            minisql::parser::Statement::AlterTable(alter) => {
                match alter.action {
                    minisql::parser::AlterTableAction::ChangeEngine(engine) => {
                        assert_eq!(engine, expected_engine, "Failed for SQL: {}", sql);
                    }
                }
            }
            _ => panic!("Expected AlterTable statement for SQL: {}", sql),
        }
    }
}

#[test]
fn test_parse_alter_table_invalid_engine() {
    // Unknown engine should produce an error
    let result = Parser::parse("ALTER TABLE t ENGINE=Unknown");
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    assert!(err_msg.contains("Unknown engine type"));
}

#[test]
fn test_parse_alter_table_with_semicolon() {
    let stmt = Parser::parse("ALTER TABLE logs ENGINE=Sandstone;").unwrap();
    match stmt {
        minisql::parser::Statement::AlterTable(alter) => {
            assert_eq!(alter.table_name, "logs");
        }
        _ => panic!("Expected AlterTable statement"),
    }
}

// ============== Executor Tests ==============

#[test]
fn test_execute_alter_table_to_sandstone() {
    let (_temp_dir, executor) = setup_sandstone_executor();
    let mut session = Session::new();
    
    // Create a Granite table
    let create = Parser::parse("CREATE TABLE fast_logs (id INT PRIMARY KEY, msg TEXT)").unwrap();
    executor.execute(create, &mut session).unwrap();
    
    // Alter to Sandstone
    let alter = Parser::parse("ALTER TABLE fast_logs ENGINE=Sandstone").unwrap();
    executor.execute(alter, &mut session).unwrap();
    
    // Verify it's now Sandstone - we check via a SELECT that works
    // (can't directly access storage.get_schema since it's behind Arc)
    let result = executor.execute(Parser::parse("SELECT * FROM fast_logs").unwrap(), &mut session);
    assert!(result.is_ok());
}

#[test]
fn test_execute_alter_table_roundtrip() {
    let (_temp_dir, executor) = setup_sandstone_executor();
    let mut session = Session::new();
    
    // Create a table
    let create = Parser::parse("CREATE TABLE test_table (id INT PRIMARY KEY)").unwrap();
    executor.execute(create, &mut session).unwrap();
    
    // Change to Sandstone
    let alter_to_sandstone = Parser::parse("ALTER TABLE test_table ENGINE=Sandstone").unwrap();
    executor.execute(alter_to_sandstone, &mut session).unwrap();
    
    // Change back to Granite
    let alter_to_granite = Parser::parse("ALTER TABLE test_table ENGINE=Granite").unwrap();
    executor.execute(alter_to_granite, &mut session).unwrap();
    
    // Table should still be accessible
    let result = executor.execute(Parser::parse("SELECT * FROM test_table").unwrap(), &mut session);
    assert!(result.is_ok());
}

#[test]
fn test_execute_alter_table_no_change() {
    let (_temp_dir, executor) = setup_sandstone_executor();
    let mut session = Session::new();
    
    // Create a Granite table
    let create = Parser::parse("CREATE TABLE test (id INT PRIMARY KEY)").unwrap();
    executor.execute(create, &mut session).unwrap();
    
    // Alter to Granite (same engine) should be no-op
    let alter = Parser::parse("ALTER TABLE test ENGINE=Granite").unwrap();
    let result = executor.execute(alter, &mut session);
    assert!(result.is_ok());
}

#[test]
fn test_execute_alter_table_nonexistent() {
    let (_temp_dir, executor) = setup_sandstone_executor();
    let mut session = Session::new();
    
    // Try to alter a non-existent table
    let alter = Parser::parse("ALTER TABLE nonexistent ENGINE=Sandstone").unwrap();
    let result = executor.execute(alter, &mut session);
    assert!(result.is_err());
}

#[test]
fn test_alter_table_preserves_data() {
    let (_temp_dir, executor) = setup_sandstone_executor();
    let mut session = Session::new();
    
    // Create table and insert data
    executor.execute(Parser::parse("CREATE TABLE data_test (id INT PRIMARY KEY, name TEXT)").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("INSERT INTO data_test VALUES (1, 'Alice')").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("INSERT INTO data_test VALUES (2, 'Bob')").unwrap(), &mut session).unwrap();
    
    // Migrate to Sandstone
    executor.execute(Parser::parse("ALTER TABLE data_test ENGINE=Sandstone").unwrap(), &mut session).unwrap();
    
    // Data should still be accessible
    let result = executor.execute(Parser::parse("SELECT * FROM data_test").unwrap(), &mut session).unwrap();
    match result {
        minisql::types::QueryResult::Select(resultset) => {
            assert_eq!(resultset.rows.len(), 2, "Expected 2 rows after engine migration");
        }
        _ => panic!("Expected Select result"),
    }
}

#[test]
fn test_alter_sandstone_not_enabled_error() {
    let (_temp_dir, storage, txn_manager) = setup_test_env();
    
    // Create executor WITHOUT Sandstone enabled
    let executor = Executor::new(storage, txn_manager);
    let mut session = Session::new();
    
    // Create a table
    let create = Parser::parse("CREATE TABLE test (id INT PRIMARY KEY)").unwrap();
    executor.execute(create, &mut session).unwrap();
    
    // Try to alter to Sandstone - should fail
    let alter = Parser::parse("ALTER TABLE test ENGINE=Sandstone").unwrap();
    let result = executor.execute(alter, &mut session);
    assert!(result.is_err());
    let err_msg = result.unwrap_err().to_string();
    // Check for the generic engine not enabled error (new format)
    assert!(err_msg.contains("Sandstone") && err_msg.contains("not enabled"), 
            "Expected 'Sandstone' and 'not enabled' in error message, got: {}", err_msg);
}
