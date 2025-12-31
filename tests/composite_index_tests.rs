//! Integration tests for composite (multi-column) index functionality

use tempfile::tempdir;
use minisql::storage::StorageEngine;
use minisql::executor::{Executor, Session};
use minisql::engines::granite::TransactionManager;
use minisql::parser::Parser;
use minisql::types::QueryResult;

/// Helper to create an executor with a temporary data directory
fn create_test_executor() -> (Executor, Session, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let storage = StorageEngine::new(dir.path().to_path_buf()).unwrap();
    let txn_manager = TransactionManager::new(dir.path().to_path_buf()).unwrap();
    let executor = Executor::new(storage, txn_manager);
    let session = Session::new();
    (executor, session, dir)
}

/// Helper to execute SQL and expect success
fn execute_ok(executor: &Executor, session: &mut Session, sql: &str) {
    let stmt = Parser::parse(sql).expect(&format!("Failed to parse: {}", sql));
    let result = executor.execute(stmt, session);
    assert!(result.is_ok(), "SQL '{}' failed: {:?}", sql, result.err());
}

/// Helper to execute SQL and return the result
fn execute(executor: &Executor, session: &mut Session, sql: &str) -> QueryResult {
    let stmt = Parser::parse(sql).expect(&format!("Failed to parse: {}", sql));
    let result = executor.execute(stmt, session);
    assert!(result.is_ok(), "SQL '{}' failed: {:?}", sql, result.err());
    result.unwrap()
}

/// Helper to count rows in a SELECT result
fn count_rows(result: &QueryResult) -> usize {
    match result {
        QueryResult::Select(rs) => rs.rows.len(),
        _ => panic!("Expected SELECT result"),
    }
}

#[cfg(test)]
mod composite_index_tests {
    use super::*;

    #[test]
    fn test_create_composite_index() {
        let (executor, mut session, _dir) = create_test_executor();
        
        // Create a table
        execute_ok(&executor, &mut session, "CREATE TABLE tiles (
            layer_id INTEGER,
            z INTEGER,
            x INTEGER,
            y INTEGER,
            data TEXT
        )");
        
        // Create a composite index
        execute_ok(&executor, &mut session, "CREATE INDEX idx_tiles_composite ON tiles (layer_id, z, x, y)");
        
        // Insert some data and query to verify the index is working
        execute_ok(&executor, &mut session, "INSERT INTO tiles VALUES (1, 5, 10, 20, 'test')");
        let result = execute(&executor, &mut session, "SELECT data FROM tiles WHERE layer_id = 1 AND z = 5 AND x = 10 AND y = 20");
        assert_eq!(count_rows(&result), 1);
    }
    
    #[test]
    fn test_composite_index_query_with_all_columns() {
        let (executor, mut session, _dir) = create_test_executor();
        
        // Create a table and insert data
        execute_ok(&executor, &mut session, "CREATE TABLE tiles (
            layer_id INTEGER,
            z INTEGER,
            x INTEGER,
            y INTEGER,
            data TEXT
        )");
        
        // Insert some test data
        execute_ok(&executor, &mut session, "INSERT INTO tiles VALUES (1, 5, 10, 20, 'tile1')");
        execute_ok(&executor, &mut session, "INSERT INTO tiles VALUES (1, 5, 10, 21, 'tile2')");
        execute_ok(&executor, &mut session, "INSERT INTO tiles VALUES (1, 5, 11, 20, 'tile3')");
        execute_ok(&executor, &mut session, "INSERT INTO tiles VALUES (2, 5, 10, 20, 'tile4')");
        
        // Create the composite index
        execute_ok(&executor, &mut session, "CREATE INDEX idx_tiles_composite ON tiles (layer_id, z, x, y)");
        
        // Query using all four columns - should use the composite index
        let result = execute(&executor, &mut session, "SELECT data FROM tiles WHERE layer_id = 1 AND z = 5 AND x = 10 AND y = 20");
        assert_eq!(count_rows(&result), 1);
        
        // Query using all columns with different values - should find the right tile
        let result = execute(&executor, &mut session, "SELECT data FROM tiles WHERE layer_id = 1 AND z = 5 AND x = 10 AND y = 21");
        assert_eq!(count_rows(&result), 1);
        
        // Query for non-existent combination
        let result = execute(&executor, &mut session, "SELECT data FROM tiles WHERE layer_id = 1 AND z = 5 AND x = 10 AND y = 99");
        assert_eq!(count_rows(&result), 0);
    }
    
    #[test]
    fn test_composite_index_with_partial_columns() {
        let (executor, mut session, _dir) = create_test_executor();
        
        // Create a table and insert data
        execute_ok(&executor, &mut session, "CREATE TABLE orders (
            customer_id INTEGER,
            order_date INTEGER,
            product_id INTEGER,
            quantity INTEGER
        )");
        
        // Insert test data
        execute_ok(&executor, &mut session, "INSERT INTO orders VALUES (1, 20231201, 100, 5)");
        execute_ok(&executor, &mut session, "INSERT INTO orders VALUES (1, 20231201, 101, 3)");
        execute_ok(&executor, &mut session, "INSERT INTO orders VALUES (1, 20231202, 100, 2)");
        execute_ok(&executor, &mut session, "INSERT INTO orders VALUES (2, 20231201, 100, 1)");
        
        // Create a composite index on (customer_id, order_date)
        execute_ok(&executor, &mut session, "CREATE INDEX idx_customer_date ON orders (customer_id, order_date)");
        
        // Query using both columns - should use the full composite index
        let result = execute(&executor, &mut session, "SELECT * FROM orders WHERE customer_id = 1 AND order_date = 20231201");
        assert_eq!(count_rows(&result), 2); // Two orders on that date for customer 1
        
        // Query using just the first column - can still use the index prefix
        let result = execute(&executor, &mut session, "SELECT * FROM orders WHERE customer_id = 1");
        // Without full index match, this will do a full scan but still return correct results
        assert_eq!(count_rows(&result), 3); // All orders for customer 1
    }
    
    #[test]
    fn test_composite_index_if_not_exists() {
        let (executor, mut session, _dir) = create_test_executor();
        
        execute_ok(&executor, &mut session, "CREATE TABLE t1 (a INTEGER, b INTEGER, c TEXT)");
        
        // Create index first time
        execute_ok(&executor, &mut session, "CREATE INDEX IF NOT EXISTS idx_ab ON t1 (a, b)");
        
        // Create same index again - should not fail
        execute_ok(&executor, &mut session, "CREATE INDEX IF NOT EXISTS idx_ab ON t1 (a, b)");
    }
    
    #[test]
    fn test_composite_index_drop() {
        let (executor, mut session, _dir) = create_test_executor();
        
        execute_ok(&executor, &mut session, "CREATE TABLE t1 (a INTEGER, b INTEGER, c TEXT)");
        execute_ok(&executor, &mut session, "CREATE INDEX idx_ab ON t1 (a, b)");
        
        // Insert and verify index works
        execute_ok(&executor, &mut session, "INSERT INTO t1 VALUES (1, 2, 'test')");
        let result = execute(&executor, &mut session, "SELECT c FROM t1 WHERE a = 1 AND b = 2");
        assert_eq!(count_rows(&result), 1);
        
        // Drop the index
        execute_ok(&executor, &mut session, "DROP INDEX idx_ab");
        
        // Query should still work (just do a full scan now)
        let result = execute(&executor, &mut session, "SELECT c FROM t1 WHERE a = 1 AND b = 2");
        assert_eq!(count_rows(&result), 1);
    }
    
    #[test]
    fn test_composite_index_maintained_on_insert() {
        let (executor, mut session, _dir) = create_test_executor();
        
        execute_ok(&executor, &mut session, "CREATE TABLE t1 (a INTEGER, b INTEGER, c TEXT)");
        execute_ok(&executor, &mut session, "CREATE INDEX idx_ab ON t1 (a, b)");
        
        // Insert data after index creation
        execute_ok(&executor, &mut session, "INSERT INTO t1 VALUES (1, 2, 'first')");
        execute_ok(&executor, &mut session, "INSERT INTO t1 VALUES (1, 3, 'second')");
        
        // Query should use the index and return correct results
        let result = execute(&executor, &mut session, "SELECT c FROM t1 WHERE a = 1 AND b = 2");
        assert_eq!(count_rows(&result), 1);
    }
    
    #[test]
    fn test_composite_index_maintained_on_update() {
        let (executor, mut session, _dir) = create_test_executor();
        
        execute_ok(&executor, &mut session, "CREATE TABLE t1 (a INTEGER, b INTEGER, c TEXT)");
        execute_ok(&executor, &mut session, "CREATE INDEX idx_ab ON t1 (a, b)");
        execute_ok(&executor, &mut session, "INSERT INTO t1 VALUES (1, 2, 'original')");
        
        // Update should maintain the index
        execute_ok(&executor, &mut session, "UPDATE t1 SET b = 3 WHERE a = 1");
        
        // Old index entry should not exist
        let result = execute(&executor, &mut session, "SELECT c FROM t1 WHERE a = 1 AND b = 2");
        assert_eq!(count_rows(&result), 0);
        
        // New index entry should exist
        let result = execute(&executor, &mut session, "SELECT c FROM t1 WHERE a = 1 AND b = 3");
        assert_eq!(count_rows(&result), 1);
    }
    
    #[test]
    fn test_composite_index_maintained_on_delete() {
        let (executor, mut session, _dir) = create_test_executor();
        
        execute_ok(&executor, &mut session, "CREATE TABLE t1 (a INTEGER, b INTEGER, c TEXT)");
        execute_ok(&executor, &mut session, "CREATE INDEX idx_ab ON t1 (a, b)");
        execute_ok(&executor, &mut session, "INSERT INTO t1 VALUES (1, 2, 'to_delete')");
        execute_ok(&executor, &mut session, "INSERT INTO t1 VALUES (1, 3, 'to_keep')");
        
        // Delete one row
        execute_ok(&executor, &mut session, "DELETE FROM t1 WHERE b = 2");
        
        // Deleted row should not be found
        let result = execute(&executor, &mut session, "SELECT c FROM t1 WHERE a = 1 AND b = 2");
        assert_eq!(count_rows(&result), 0);
        
        // Other row should still be found
        let result = execute(&executor, &mut session, "SELECT c FROM t1 WHERE a = 1 AND b = 3");
        assert_eq!(count_rows(&result), 1);
    }
    
    #[test]
    fn test_composite_index_with_equality_in_any_order() {
        let (executor, mut session, _dir) = create_test_executor();
        
        execute_ok(&executor, &mut session, "CREATE TABLE t1 (a INTEGER, b INTEGER, c TEXT)");
        execute_ok(&executor, &mut session, "INSERT INTO t1 VALUES (1, 2, 'found')");
        execute_ok(&executor, &mut session, "CREATE INDEX idx_ab ON t1 (a, b)");
        
        // Query with columns in index order
        let result = execute(&executor, &mut session, "SELECT c FROM t1 WHERE a = 1 AND b = 2");
        assert_eq!(count_rows(&result), 1);
        
        // Query with columns in different order in WHERE clause
        // The planner should still be able to match the index
        let result = execute(&executor, &mut session, "SELECT c FROM t1 WHERE b = 2 AND a = 1");
        assert_eq!(count_rows(&result), 1);
    }
    
    #[test]
    fn test_single_column_index_backward_compatible() {
        let (executor, mut session, _dir) = create_test_executor();
        
        execute_ok(&executor, &mut session, "CREATE TABLE users (id INTEGER, name TEXT)");
        execute_ok(&executor, &mut session, "INSERT INTO users VALUES (1, 'Alice')");
        execute_ok(&executor, &mut session, "INSERT INTO users VALUES (2, 'Bob')");
        
        // Create a single-column index using the same syntax
        execute_ok(&executor, &mut session, "CREATE INDEX idx_name ON users (name)");
        
        // Verify it works
        let result = execute(&executor, &mut session, "SELECT id FROM users WHERE name = 'Alice'");
        assert_eq!(count_rows(&result), 1);
    }
    
    #[test]
    fn test_composite_index_performance_improvement() {
        let (executor, mut session, _dir) = create_test_executor();
        
        // Create a table with many rows
        execute_ok(&executor, &mut session, "CREATE TABLE perf_test (a INTEGER, b INTEGER, c INTEGER, data TEXT)");
        
        // Insert 100 rows with more predictable patterns
        // a cycles 0-9, b cycles 0-4, c is unique (row number)
        for i in 0..100 {
            let sql = format!("INSERT INTO perf_test VALUES ({}, {}, {}, 'data{}')", i % 10, i % 5, i, i);
            execute_ok(&executor, &mut session, &sql);
        }
        
        // Create composite index
        execute_ok(&executor, &mut session, "CREATE INDEX idx_abc ON perf_test (a, b, c)");
        
        // Query for values that exist: i=50 gives a=0, b=0, c=50
        let result = execute(&executor, &mut session, "SELECT data FROM perf_test WHERE a = 0 AND b = 0 AND c = 50");
        assert_eq!(count_rows(&result), 1);
        
        // Query for i=27: a=7, b=2, c=27
        let result = execute(&executor, &mut session, "SELECT data FROM perf_test WHERE a = 7 AND b = 2 AND c = 27");
        assert_eq!(count_rows(&result), 1);
        
        // Query for non-existent combination (c=999 doesn't exist)
        let result = execute(&executor, &mut session, "SELECT data FROM perf_test WHERE a = 0 AND b = 0 AND c = 999");
        assert_eq!(count_rows(&result), 0);
    }
}
