//! Integration tests for ORDER BY behavior (strings, ints, floats, dates)

use tempfile::tempdir;
use minisql::storage::StorageEngine;
use minisql::executor::{Executor, Session};
use minisql::engines::granite::TransactionManager;
use minisql::parser::Parser;
use minisql::types::{QueryResult, Value};

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

/// Extract values from the first column of a SELECT result
fn first_column_values(result: &QueryResult) -> Vec<Value> {
    match result {
        QueryResult::Select(rs) => rs.rows.iter().map(|r| r[0].clone()).collect(),
        _ => panic!("Expected SELECT result"),
    }
}

/// Extract first two columns from a SELECT result as pairs
fn first_two_columns_values(result: &QueryResult) -> Vec<(Value, Value)> {
    match result {
        QueryResult::Select(rs) => rs.rows.iter().map(|r| (r[0].clone(), r[1].clone())).collect(),
        _ => panic!("Expected SELECT result"),
    }
}

#[cfg(test)]
mod ordering_tests {
    use super::*;

    #[test]
    fn test_order_by_strings_asc_desc() {
        let (executor, mut session, _dir) = create_test_executor();

        execute_ok(&executor, &mut session, "CREATE TABLE t_str (s TEXT)");
        execute_ok(&executor, &mut session, "INSERT INTO t_str VALUES ('banana')");
        execute_ok(&executor, &mut session, "INSERT INTO t_str VALUES ('apple')");
        execute_ok(&executor, &mut session, "INSERT INTO t_str VALUES ('cherry')");

        let res = execute(&executor, &mut session, "SELECT s FROM t_str ORDER BY s ASC");
        let vals = first_column_values(&res);
        assert_eq!(vals, vec![Value::String("apple".into()), Value::String("banana".into()), Value::String("cherry".into())]);

        let res = execute(&executor, &mut session, "SELECT s FROM t_str ORDER BY s DESC");
        let vals = first_column_values(&res);
        assert_eq!(vals, vec![Value::String("cherry".into()), Value::String("banana".into()), Value::String("apple".into())]);
    }

    #[test]
    fn test_order_by_integers_asc_desc() {
        let (executor, mut session, _dir) = create_test_executor();

        execute_ok(&executor, &mut session, "CREATE TABLE t_int (n INTEGER)");
        execute_ok(&executor, &mut session, "INSERT INTO t_int VALUES (10)");
        execute_ok(&executor, &mut session, "INSERT INTO t_int VALUES (1)");
        execute_ok(&executor, &mut session, "INSERT INTO t_int VALUES (5)");

        let res = execute(&executor, &mut session, "SELECT n FROM t_int ORDER BY n ASC");
        let vals = first_column_values(&res);
        assert_eq!(vals, vec![Value::Integer(1), Value::Integer(5), Value::Integer(10)]);

        let res = execute(&executor, &mut session, "SELECT n FROM t_int ORDER BY n DESC");
        let vals = first_column_values(&res);
        assert_eq!(vals, vec![Value::Integer(10), Value::Integer(5), Value::Integer(1)]);
    }

    #[test]
    fn test_order_by_floats_asc_desc() {
        let (executor, mut session, _dir) = create_test_executor();

        execute_ok(&executor, &mut session, "CREATE TABLE t_float (f FLOAT)");
        execute_ok(&executor, &mut session, "INSERT INTO t_float VALUES (2.5)");
        execute_ok(&executor, &mut session, "INSERT INTO t_float VALUES (-1.0)");
        execute_ok(&executor, &mut session, "INSERT INTO t_float VALUES (3.14)");

        let res = execute(&executor, &mut session, "SELECT f FROM t_float ORDER BY f ASC");
        let vals = first_column_values(&res);
        assert_eq!(vals, vec![Value::Float(-1.0), Value::Float(2.5), Value::Float(3.14)]);

        let res = execute(&executor, &mut session, "SELECT f FROM t_float ORDER BY f DESC");
        let vals = first_column_values(&res);
        assert_eq!(vals, vec![Value::Float(3.14), Value::Float(2.5), Value::Float(-1.0)]);
    }

    #[test]
    fn test_order_by_dates_integer_and_iso_strings() {
        let (executor, mut session, _dir) = create_test_executor();

        // Integer date form (yyyymmdd)
        execute_ok(&executor, &mut session, "CREATE TABLE t_date_int (d INTEGER)");
        execute_ok(&executor, &mut session, "INSERT INTO t_date_int VALUES (20200102)");
        execute_ok(&executor, &mut session, "INSERT INTO t_date_int VALUES (20191231)");
        execute_ok(&executor, &mut session, "INSERT INTO t_date_int VALUES (20210501)");

        let res = execute(&executor, &mut session, "SELECT d FROM t_date_int ORDER BY d ASC");
        let vals = first_column_values(&res);
        assert_eq!(vals, vec![Value::Integer(20191231), Value::Integer(20200102), Value::Integer(20210501)]);

        // ISO-8601 string form (lexicographic order matches chronological order)
        execute_ok(&executor, &mut session, "CREATE TABLE t_date_str (d TEXT)");
        execute_ok(&executor, &mut session, "INSERT INTO t_date_str VALUES ('2020-01-02')");
        execute_ok(&executor, &mut session, "INSERT INTO t_date_str VALUES ('2019-12-31')");
        execute_ok(&executor, &mut session, "INSERT INTO t_date_str VALUES ('2021-05-01')");

        let res = execute(&executor, &mut session, "SELECT d FROM t_date_str ORDER BY d ASC");
        let vals = first_column_values(&res);
        assert_eq!(vals, vec![Value::String("2019-12-31".into()), Value::String("2020-01-02".into()), Value::String("2021-05-01".into())]);
    }

    #[test]
    fn test_order_by_multiple_columns_simple() {
        let (executor, mut session, _dir) = create_test_executor();

        execute_ok(&executor, &mut session, "CREATE TABLE people (first TEXT, last TEXT, age INTEGER)");
        execute_ok(&executor, &mut session, "INSERT INTO people VALUES ('John', 'Doe', 30)");
        execute_ok(&executor, &mut session, "INSERT INTO people VALUES ('Alice', 'Smith', 25)");
        execute_ok(&executor, &mut session, "INSERT INTO people VALUES ('Bob', 'Smith', 20)");
        execute_ok(&executor, &mut session, "INSERT INTO people VALUES ('Alice', 'Adams', 40)");

        // Order by last asc, first asc
        let res = execute(&executor, &mut session, "SELECT last, first FROM people ORDER BY last ASC, first ASC");
        let vals = first_two_columns_values(&res);
        let expected = vec![
            (Value::String("Adams".into()), Value::String("Alice".into())),
            (Value::String("Doe".into()), Value::String("John".into())),
            (Value::String("Smith".into()), Value::String("Alice".into())),
            (Value::String("Smith".into()), Value::String("Bob".into())),
        ];
        assert_eq!(vals, expected);
    }

    #[test]
    fn test_order_by_multiple_mixed_directions() {
        let (executor, mut session, _dir) = create_test_executor();

        execute_ok(&executor, &mut session, "CREATE TABLE items (category TEXT, name TEXT, score INTEGER)");
        execute_ok(&executor, &mut session, "INSERT INTO items VALUES ('fruit', 'apple', 10)");
        execute_ok(&executor, &mut session, "INSERT INTO items VALUES ('fruit', 'banana', 5)");
        execute_ok(&executor, &mut session, "INSERT INTO items VALUES ('veg', 'carrot', 8)");
        execute_ok(&executor, &mut session, "INSERT INTO items VALUES ('fruit', 'apple', 20)");

        // Order by category ASC, score DESC (so within same category, higher score first)
        let res = execute(&executor, &mut session, "SELECT category, score FROM items ORDER BY category ASC, score DESC");
        let vals = first_two_columns_values(&res);
        let expected = vec![
            (Value::String("fruit".into()), Value::Integer(20)),
            (Value::String("fruit".into()), Value::Integer(10)),
            (Value::String("fruit".into()), Value::Integer(5)),
            (Value::String("veg".into()), Value::Integer(8)),
        ];
        assert_eq!(vals, expected);
    }
}
