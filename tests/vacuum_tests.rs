use minisql::executor::{Executor, Session};
use minisql::parser::Parser;
use minisql::storage::StorageEngine;
use minisql::engines::granite::TransactionManager;
use minisql::types::{QueryResult, Value};
use tempfile::tempdir;

#[test]
fn test_vacuum_basic() {
    let dir = tempdir().unwrap();
    let storage = StorageEngine::new(dir.path().to_path_buf()).unwrap();
    let txn_manager = TransactionManager::new(dir.path().to_path_buf()).unwrap();
    let executor = Executor::new(storage.clone(), txn_manager);
    let mut session = Session::new();

    // Create a table
    executor.execute(
        Parser::parse("CREATE TABLE users (id INTEGER PRIMARY KEY, name TEXT)").unwrap(),
        &mut session,
    ).unwrap();

    // Insert some rows
    executor.execute(
        Parser::parse("INSERT INTO users VALUES (1, 'Alice')").unwrap(),
        &mut session,
    ).unwrap();
    executor.execute(
        Parser::parse("INSERT INTO users VALUES (2, 'Bob')").unwrap(),
        &mut session,
    ).unwrap();
    executor.execute(
        Parser::parse("INSERT INTO users VALUES (3, 'Charlie')").unwrap(),
        &mut session,
    ).unwrap();

    // Get row IDs before vacuum
    let rows_before = storage.scan_table("users").unwrap();
    assert_eq!(rows_before.len(), 3);
    let row_ids_before: Vec<u64> = rows_before.iter().map(|r| r.id).collect();
    println!("Row IDs before VACUUM: {:?}", row_ids_before);

    // Run VACUUM
    let result = executor.execute(Parser::parse("VACUUM").unwrap(), &mut session).unwrap();
    assert!(matches!(result, QueryResult::Ok));

    // Verify data is still intact
    let result = executor.execute(
        Parser::parse("SELECT * FROM users ORDER BY id").unwrap(),
        &mut session,
    ).unwrap();

    if let QueryResult::Select(rs) = result {
        assert_eq!(rs.rows.len(), 3);
        assert_eq!(rs.rows[0][0], Value::Integer(1));
        assert_eq!(rs.rows[0][1], Value::String("Alice".into()));
        assert_eq!(rs.rows[1][0], Value::Integer(2));
        assert_eq!(rs.rows[1][1], Value::String("Bob".into()));
        assert_eq!(rs.rows[2][0], Value::Integer(3));
        assert_eq!(rs.rows[2][1], Value::String("Charlie".into()));
    } else {
        panic!("Expected Select result");
    }

    // Check that internal row IDs have been reset
    let rows_after = storage.scan_table("users").unwrap();
    assert_eq!(rows_after.len(), 3);
    let row_ids_after: Vec<u64> = rows_after.iter().map(|r| r.id).collect();
    println!("Row IDs after VACUUM: {:?}", row_ids_after);

    // Row IDs should now be sequential starting from 1
    let mut sorted_ids = row_ids_after.clone();
    sorted_ids.sort();
    assert_eq!(sorted_ids, vec![1, 2, 3]);
}

#[test]
fn test_vacuum_with_indexes() {
    let dir = tempdir().unwrap();
    let storage = StorageEngine::new(dir.path().to_path_buf()).unwrap();
    let txn_manager = TransactionManager::new(dir.path().to_path_buf()).unwrap();
    let executor = Executor::new(storage.clone(), txn_manager);
    let mut session = Session::new();

    // Create a table
    executor.execute(
        Parser::parse("CREATE TABLE products (id INTEGER PRIMARY KEY, name TEXT, price INTEGER)").unwrap(),
        &mut session,
    ).unwrap();

    // Create an index
    executor.execute(
        Parser::parse("CREATE INDEX idx_name ON products(name)").unwrap(),
        &mut session,
    ).unwrap();

    // Insert some rows
    for i in 1..=10 {
        executor.execute(
            Parser::parse(&format!("INSERT INTO products VALUES ({}, 'Product{}', {})", i, i, i * 100)).unwrap(),
            &mut session,
        ).unwrap();
    }

    // Delete some rows to create gaps
    executor.execute(
        Parser::parse("DELETE FROM products WHERE id IN (3, 5, 7)").unwrap(),
        &mut session,
    ).unwrap();

    // Verify index works before VACUUM
    let result = executor.execute(
        Parser::parse("SELECT * FROM products WHERE name = 'Product2'").unwrap(),
        &mut session,
    ).unwrap();
    if let QueryResult::Select(rs) = result {
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(rs.rows[0][0], Value::Integer(2));
    } else {
        panic!("Expected Select result");
    }

    // Run VACUUM
    executor.execute(Parser::parse("VACUUM").unwrap(), &mut session).unwrap();

    // Verify data is still intact
    let result = executor.execute(
        Parser::parse("SELECT COUNT(*) FROM products").unwrap(),
        &mut session,
    ).unwrap();
    if let QueryResult::Select(rs) = result {
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(rs.rows[0][0], Value::Integer(7)); // 10 - 3 deleted
    } else {
        panic!("Expected Select result");
    }

    // Verify index still works after VACUUM
    let result = executor.execute(
        Parser::parse("SELECT * FROM products WHERE name = 'Product2'").unwrap(),
        &mut session,
    ).unwrap();
    if let QueryResult::Select(rs) = result {
        assert_eq!(rs.rows.len(), 1);
        assert_eq!(rs.rows[0][0], Value::Integer(2));
        assert_eq!(rs.rows[0][1], Value::String("Product2".into()));
    } else {
        panic!("Expected Select result");
    }

    // Verify all expected rows are present
    let result = executor.execute(
        Parser::parse("SELECT id FROM products ORDER BY id").unwrap(),
        &mut session,
    ).unwrap();
    if let QueryResult::Select(rs) = result {
        let ids: Vec<i64> = rs.rows.iter().map(|r| {
            if let Value::Integer(i) = r[0] { i } else { panic!("Expected integer") }
        }).collect();
        assert_eq!(ids, vec![1, 2, 4, 6, 8, 9, 10]);
    } else {
        panic!("Expected Select result");
    }
}

#[test]
fn test_vacuum_multiple_tables() {
    let dir = tempdir().unwrap();
    let storage = StorageEngine::new(dir.path().to_path_buf()).unwrap();
    let txn_manager = TransactionManager::new(dir.path().to_path_buf()).unwrap();
    let executor = Executor::new(storage.clone(), txn_manager);
    let mut session = Session::new();

    // Create multiple tables
    executor.execute(
        Parser::parse("CREATE TABLE table1 (id INTEGER PRIMARY KEY, value TEXT)").unwrap(),
        &mut session,
    ).unwrap();
    executor.execute(
        Parser::parse("CREATE TABLE table2 (id INTEGER PRIMARY KEY, value TEXT)").unwrap(),
        &mut session,
    ).unwrap();
    executor.execute(
        Parser::parse("CREATE TABLE table3 (id INTEGER PRIMARY KEY, value TEXT)").unwrap(),
        &mut session,
    ).unwrap();

    // Insert data into each table
    for i in 1..=5 {
        executor.execute(
            Parser::parse(&format!("INSERT INTO table1 VALUES ({}, 'T1-{}')", i, i)).unwrap(),
            &mut session,
        ).unwrap();
        executor.execute(
            Parser::parse(&format!("INSERT INTO table2 VALUES ({}, 'T2-{}')", i * 10, i)).unwrap(),
            &mut session,
        ).unwrap();
        executor.execute(
            Parser::parse(&format!("INSERT INTO table3 VALUES ({}, 'T3-{}')", i * 100, i)).unwrap(),
            &mut session,
        ).unwrap();
    }

    // Run VACUUM
    executor.execute(Parser::parse("VACUUM").unwrap(), &mut session).unwrap();

    // Verify all tables still have their data
    let result = executor.execute(
        Parser::parse("SELECT COUNT(*) FROM table1").unwrap(),
        &mut session,
    ).unwrap();
    if let QueryResult::Select(rs) = result {
        assert_eq!(rs.rows[0][0], Value::Integer(5));
    } else {
        panic!("Expected Select result");
    }

    let result = executor.execute(
        Parser::parse("SELECT COUNT(*) FROM table2").unwrap(),
        &mut session,
    ).unwrap();
    if let QueryResult::Select(rs) = result {
        assert_eq!(rs.rows[0][0], Value::Integer(5));
    } else {
        panic!("Expected Select result");
    }

    let result = executor.execute(
        Parser::parse("SELECT COUNT(*) FROM table3").unwrap(),
        &mut session,
    ).unwrap();
    if let QueryResult::Select(rs) = result {
        assert_eq!(rs.rows[0][0], Value::Integer(5));
    } else {
        panic!("Expected Select result");
    }

    // Verify primary keys are preserved
    let result = executor.execute(
        Parser::parse("SELECT id FROM table2 ORDER BY id").unwrap(),
        &mut session,
    ).unwrap();
    if let QueryResult::Select(rs) = result {
        let ids: Vec<i64> = rs.rows.iter().map(|r| {
            if let Value::Integer(i) = r[0] { i } else { panic!("Expected integer") }
        }).collect();
        assert_eq!(ids, vec![10, 20, 30, 40, 50]);
    } else {
        panic!("Expected Select result");
    }
}

#[test]
fn test_vacuum_cannot_run_in_transaction() {
    let dir = tempdir().unwrap();
    let storage = StorageEngine::new(dir.path().to_path_buf()).unwrap();
    let txn_manager = TransactionManager::new(dir.path().to_path_buf()).unwrap();
    let executor = Executor::new(storage, txn_manager);
    let mut session = Session::new();

    // Create a table
    executor.execute(
        Parser::parse("CREATE TABLE test (id INTEGER PRIMARY KEY)").unwrap(),
        &mut session,
    ).unwrap();

    // Begin a transaction
    executor.execute(Parser::parse("BEGIN").unwrap(), &mut session).unwrap();

    // Try to run VACUUM (should fail)
    let result = executor.execute(Parser::parse("VACUUM").unwrap(), &mut session);
    assert!(result.is_err());
    assert!(result.unwrap_err().to_string().contains("cannot be run inside a transaction"));

    // Rollback
    executor.execute(Parser::parse("ROLLBACK").unwrap(), &mut session).unwrap();
}

#[test]
fn test_vacuum_empty_database() {
    let dir = tempdir().unwrap();
    let storage = StorageEngine::new(dir.path().to_path_buf()).unwrap();
    let txn_manager = TransactionManager::new(dir.path().to_path_buf()).unwrap();
    let executor = Executor::new(storage, txn_manager);
    let mut session = Session::new();

    // Run VACUUM on empty database (should succeed without error)
    let result = executor.execute(Parser::parse("VACUUM").unwrap(), &mut session);
    assert!(result.is_ok());
}

#[test]
fn test_vacuum_preserves_auto_increment() {
    let dir = tempdir().unwrap();
    let storage = StorageEngine::new(dir.path().to_path_buf()).unwrap();
    let txn_manager = TransactionManager::new(dir.path().to_path_buf()).unwrap();
    let executor = Executor::new(storage.clone(), txn_manager);
    let mut session = Session::new();

    // Create table with auto-increment
    executor.execute(
        Parser::parse("CREATE TABLE posts (id INTEGER PRIMARY KEY AUTO_INCREMENT, title TEXT)").unwrap(),
        &mut session,
    ).unwrap();

    // Insert rows
    executor.execute(
        Parser::parse("INSERT INTO posts (title) VALUES ('Post 1')").unwrap(),
        &mut session,
    ).unwrap();
    executor.execute(
        Parser::parse("INSERT INTO posts (title) VALUES ('Post 2')").unwrap(),
        &mut session,
    ).unwrap();
    executor.execute(
        Parser::parse("INSERT INTO posts (title) VALUES ('Post 3')").unwrap(),
        &mut session,
    ).unwrap();

    // Get current auto-increment value
    let auto_inc_before = storage.get_auto_increment("posts").unwrap();

    // Run VACUUM
    executor.execute(Parser::parse("VACUUM").unwrap(), &mut session).unwrap();

    // Verify auto-increment counter is preserved
    let auto_inc_after = storage.get_auto_increment("posts").unwrap();
    assert_eq!(auto_inc_before, auto_inc_after);

    // Insert another row to verify auto-increment still works
    executor.execute(
        Parser::parse("INSERT INTO posts (title) VALUES ('Post 4')").unwrap(),
        &mut session,
    ).unwrap();

    let result = executor.execute(
        Parser::parse("SELECT id FROM posts ORDER BY id").unwrap(),
        &mut session,
    ).unwrap();
    if let QueryResult::Select(rs) = result {
        let ids: Vec<i64> = rs.rows.iter().map(|r| {
            if let Value::Integer(i) = r[0] { i } else { panic!("Expected integer") }
        }).collect();
        // Should have sequential IDs from auto-increment
        assert_eq!(ids, vec![1, 2, 3, 4]);
    } else {
        panic!("Expected Select result");
    }
}
