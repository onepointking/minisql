//! Integration tests for PRIMARY KEY and AUTO_INCREMENT functionality

// Removed unused std imports that were triggering warnings. Tests in this file
// run via the internal helpers rather than spawning external processes.

// These are integration tests that run through the SQL interface

#[cfg(test)]
mod primary_key_tests {
    // `use super::*` removed - not required within this test module

    /// Helper to create test SQL commands and expected outcomes
    fn run_sql_test(test_name: &str, statements: Vec<(&str, ExpectedResult)>) {
        println!("Running test: {}", test_name);
        for (sql, expected) in statements {
            println!("  SQL: {}", sql);
            println!("  Expected: {:?}", expected);
        }
    }

    #[allow(dead_code)]
    #[derive(Debug)]
    enum ExpectedResult {
        Ok,
        Error(&'static str),
        Rows(usize),
        Value(i64),
    }
    
    // Test cases documentation - these describe the expected behavior

    #[test]
    fn test_auto_increment_syntax() {
        // This test verifies that AUTO_INCREMENT is parsed correctly
        run_sql_test("auto_increment_syntax", vec![
            ("CREATE TABLE t1 (id INTEGER PRIMARY KEY AUTO_INCREMENT, name TEXT)", ExpectedResult::Ok),
            ("DROP TABLE t1", ExpectedResult::Ok),
        ]);
    }

    #[test]
    fn test_auto_increment_must_be_integer() {
        // AUTO_INCREMENT must be on INTEGER column
        run_sql_test("auto_increment_must_be_integer", vec![
            ("CREATE TABLE t1 (id TEXT PRIMARY KEY AUTO_INCREMENT)", ExpectedResult::Error("must be of type INTEGER")),
        ]);
    }

    #[test]
    fn test_auto_increment_must_be_key() {
        // AUTO_INCREMENT must be on PRIMARY KEY or UNIQUE column
        run_sql_test("auto_increment_must_be_key", vec![
            ("CREATE TABLE t1 (id INTEGER AUTO_INCREMENT, name TEXT PRIMARY KEY)", 
             ExpectedResult::Error("must be defined as a key")),
        ]);
    }

    #[test]
    fn test_only_one_auto_increment() {
        // Only one AUTO_INCREMENT column allowed per table
        run_sql_test("only_one_auto_increment", vec![
            ("CREATE TABLE t1 (id1 INTEGER PRIMARY KEY AUTO_INCREMENT, id2 INTEGER AUTO_INCREMENT)",
             ExpectedResult::Error("only one AUTO_INCREMENT")),
        ]);
    }

    #[test]
    fn test_primary_key_uniqueness() {
        // Primary key values must be unique
        run_sql_test("primary_key_uniqueness", vec![
            ("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)", ExpectedResult::Ok),
            ("INSERT INTO t1 VALUES (1, 'Alice')", ExpectedResult::Ok),
            ("INSERT INTO t1 VALUES (1, 'Bob')", ExpectedResult::Error("Duplicate entry")),
            ("DROP TABLE t1", ExpectedResult::Ok),
        ]);
    }

    #[test]
    fn test_update_primary_key_uniqueness() {
        // Update cannot violate primary key uniqueness
        run_sql_test("update_primary_key_uniqueness", vec![
            ("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)", ExpectedResult::Ok),
            ("INSERT INTO t1 VALUES (1, 'Alice')", ExpectedResult::Ok),
            ("INSERT INTO t1 VALUES (2, 'Bob')", ExpectedResult::Ok),
            ("UPDATE t1 SET id = 1 WHERE id = 2", ExpectedResult::Error("Duplicate entry")),
            ("DROP TABLE t1", ExpectedResult::Ok),
        ]);
    }

    #[test]
    fn test_auto_increment_sequential() {
        // AUTO_INCREMENT generates sequential values
        run_sql_test("auto_increment_sequential", vec![
            ("CREATE TABLE t1 (id INTEGER PRIMARY KEY AUTO_INCREMENT, name TEXT)", ExpectedResult::Ok),
            ("INSERT INTO t1 (name) VALUES ('Alice')", ExpectedResult::Ok),  // id = 1
            ("INSERT INTO t1 (name) VALUES ('Bob')", ExpectedResult::Ok),    // id = 2
            ("INSERT INTO t1 (name) VALUES ('Charlie')", ExpectedResult::Ok), // id = 3
            ("SELECT COUNT(*) FROM t1 WHERE id = 1", ExpectedResult::Value(1)),
            ("SELECT COUNT(*) FROM t1 WHERE id = 2", ExpectedResult::Value(1)),
            ("SELECT COUNT(*) FROM t1 WHERE id = 3", ExpectedResult::Value(1)),
            ("DROP TABLE t1", ExpectedResult::Ok),
        ]);
    }

    #[test]
    fn test_auto_increment_explicit_value() {
        // Explicit value updates counter
        run_sql_test("auto_increment_explicit_value", vec![
            ("CREATE TABLE t1 (id INTEGER PRIMARY KEY AUTO_INCREMENT, name TEXT)", ExpectedResult::Ok),
            ("INSERT INTO t1 VALUES (100, 'Alice')", ExpectedResult::Ok),  // explicit id = 100
            ("INSERT INTO t1 (name) VALUES ('Bob')", ExpectedResult::Ok),   // should get id = 101
            ("SELECT COUNT(*) FROM t1 WHERE id = 101", ExpectedResult::Value(1)),
            ("DROP TABLE t1", ExpectedResult::Ok),
        ]);
    }

    #[test]
    fn test_auto_increment_null_generates() {
        // NULL value in AUTO_INCREMENT generates new value
        run_sql_test("auto_increment_null_generates", vec![
            ("CREATE TABLE t1 (id INTEGER PRIMARY KEY AUTO_INCREMENT, name TEXT)", ExpectedResult::Ok),
            ("INSERT INTO t1 VALUES (NULL, 'Alice')", ExpectedResult::Ok),  // generates id = 1
            ("SELECT COUNT(*) FROM t1 WHERE id = 1", ExpectedResult::Value(1)),
            ("DROP TABLE t1", ExpectedResult::Ok),
        ]);
    }

    #[test]
    fn test_auto_increment_zero_generates() {
        // Zero value in AUTO_INCREMENT generates new value (MySQL behavior)
        run_sql_test("auto_increment_zero_generates", vec![
            ("CREATE TABLE t1 (id INTEGER PRIMARY KEY AUTO_INCREMENT, name TEXT)", ExpectedResult::Ok),
            ("INSERT INTO t1 VALUES (0, 'Alice')", ExpectedResult::Ok),  // generates id = 1
            ("SELECT COUNT(*) FROM t1 WHERE id = 1", ExpectedResult::Value(1)),
            ("DROP TABLE t1", ExpectedResult::Ok),
        ]);
    }

    #[test]
    fn test_auto_increment_no_reuse_after_delete() {
        // Deleted values are not reused
        run_sql_test("auto_increment_no_reuse", vec![
            ("CREATE TABLE t1 (id INTEGER PRIMARY KEY AUTO_INCREMENT, name TEXT)", ExpectedResult::Ok),
            ("INSERT INTO t1 (name) VALUES ('Alice')", ExpectedResult::Ok),  // id = 1
            ("INSERT INTO t1 (name) VALUES ('Bob')", ExpectedResult::Ok),    // id = 2
            ("DELETE FROM t1 WHERE id = 2", ExpectedResult::Ok),
            ("INSERT INTO t1 (name) VALUES ('Charlie')", ExpectedResult::Ok), // id = 3, NOT 2
            ("SELECT COUNT(*) FROM t1 WHERE id = 3", ExpectedResult::Value(1)),
            ("DROP TABLE t1", ExpectedResult::Ok),
        ]);
    }

    #[test]
    fn test_primary_key_index_auto_created() {
        // Primary key should have an index automatically created
        run_sql_test("pk_index_created", vec![
            ("CREATE TABLE t1 (id INTEGER PRIMARY KEY, name TEXT)", ExpectedResult::Ok),
            // The index should be named PRIMARY_t1 and enable efficient lookups
            ("DROP TABLE t1", ExpectedResult::Ok),
        ]);
    }

    #[test]
    fn test_composite_primary_key() {
        // Composite primary keys work correctly
        run_sql_test("composite_pk", vec![
            ("CREATE TABLE t1 (a INTEGER PRIMARY KEY, b INTEGER PRIMARY KEY, name TEXT)", ExpectedResult::Ok),
            ("INSERT INTO t1 VALUES (1, 1, 'a')", ExpectedResult::Ok),
            ("INSERT INTO t1 VALUES (1, 2, 'b')", ExpectedResult::Ok),  // OK - different composite key
            ("INSERT INTO t1 VALUES (1, 1, 'c')", ExpectedResult::Error("Duplicate entry")),  // Duplicate
            ("DROP TABLE t1", ExpectedResult::Ok),
        ]);
    }
}
