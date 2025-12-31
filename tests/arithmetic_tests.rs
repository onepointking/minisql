//! Tests for arithmetic operators: + - * / with integers, floats, NULLs and columns

use tempfile::tempdir;
use minisql::storage::StorageEngine;
use minisql::executor::{Executor, Session};
use minisql::engines::granite::TransactionManager;
use minisql::parser::Parser;
use minisql::types::{QueryResult, Value};

fn create_test_executor() -> (Executor, Session, tempfile::TempDir) {
    let dir = tempdir().unwrap();
    let storage = StorageEngine::new(dir.path().to_path_buf()).unwrap();
    let txn_manager = TransactionManager::new(dir.path().to_path_buf()).unwrap();
    let executor = Executor::new(storage, txn_manager);
    let session = Session::new();
    (executor, session, dir)
}

fn execute_ok(executor: &Executor, session: &mut Session, sql: &str) {
    let stmt = Parser::parse(sql).expect(&format!("Failed to parse: {}", sql));
    let result = executor.execute(stmt, session);
    assert!(result.is_ok(), "SQL '{}' failed: {:?}", sql, result.err());
}

fn execute(executor: &Executor, session: &mut Session, sql: &str) -> QueryResult {
    let stmt = Parser::parse(sql).expect(&format!("Failed to parse: {}", sql));
    let result = executor.execute(stmt, session);
    assert!(result.is_ok(), "SQL '{}' failed: {:?}", sql, result.err());
    result.unwrap()
}

fn first_row(result: &QueryResult) -> Vec<Value> {
    match result {
        QueryResult::Select(rs) => rs.rows[0].clone(),
        _ => panic!("Expected SELECT result"),
    }
}

fn first_column(result: &QueryResult) -> Vec<Value> {
    match result {
        QueryResult::Select(rs) => rs.rows.iter().map(|r| r[0].clone()).collect(),
        _ => panic!("Expected SELECT result"),
    }
}

#[cfg(test)]
mod arithmetic_tests {
    use super::*;

    #[test]
    fn test_integer_arithmetic_constants() {
        let (executor, mut session, _dir) = create_test_executor();

        let res = execute(&executor, &mut session, "SELECT 1+1, 5-2, 3*4, 10/2");
        let row = first_row(&res);
        let expected = vec![
            Value::Integer(2),
            Value::Integer(3),
            Value::Integer(12),
            Value::Integer(5),
        ];
        assert_eq!(row, expected);
    }

    #[test]
    fn test_mixed_and_float_arithmetic() {
        let (executor, mut session, _dir) = create_test_executor();

        // Mixed integer + float, multiplication with float literal, integer division producing float
        let res = execute(&executor, &mut session, "SELECT 1 + 1.5, 2.0 * 3, 5 / 2");
        let row = first_row(&res);
        // 1 + 1.5 = 2.5 (float)
        // 2.0 * 3 = 6.0 (float)
        // 5 / 2 = 2.5 (float)
        match &row[0] {
            Value::Float(f) => assert_eq!(*f, 2.5),
            other => panic!("Expected Float, got {:?}", other),
        }
        match &row[1] {
            Value::Float(f) => assert_eq!(*f, 6.0),
            other => panic!("Expected Float, got {:?}", other),
        }
        match &row[2] {
            Value::Float(f) => assert_eq!(*f, 2.5),
            other => panic!("Expected Float, got {:?}", other),
        }
    }

    #[test]
    fn test_null_propagation_in_arithmetic() {
        let (executor, mut session, _dir) = create_test_executor();

        let res = execute(&executor, &mut session, "SELECT NULL + 1, 1 + NULL, NULL * 5");
        let row = first_row(&res);
        assert_eq!(row[0], Value::Null);
        assert_eq!(row[1], Value::Null);
        assert_eq!(row[2], Value::Null);
    }

    #[test]
    fn test_division_by_zero_returns_nan() {
        let (executor, mut session, _dir) = create_test_executor();

        let res = execute(&executor, &mut session, "SELECT 1/0");
        let row = first_row(&res);
        match &row[0] {
            Value::Float(f) => assert!(f.is_nan()),
            other => panic!("Expected Float NaN, got {:?}", other),
        }
    }

    #[test]
    fn test_insert_division_by_zero_sets_null_in_integer_column() {
        let (executor, mut session, _dir) = create_test_executor();

        execute_ok(&executor, &mut session, "CREATE TABLE t_div (i INTEGER)");
        // INSERT using VALUES with an expression that divides by zero
        execute_ok(&executor, &mut session, "INSERT INTO t_div VALUES (1/0)");

        let res = execute(&executor, &mut session, "SELECT i FROM t_div");
        let vals = first_column(&res);
        // Expect the inserted value to be NULL (MySQL default behavior)
        assert_eq!(vals, vec![Value::Null]);
    }

    #[test]
    fn test_insert_division_by_zero_into_not_null_errors() {
        let (executor, mut session, _dir) = create_test_executor();

        execute_ok(&executor, &mut session, "CREATE TABLE t_div_notnull (i INTEGER NOT NULL)");
        // Attempt to insert 1/0 into NOT NULL column - coercion should produce NULL and then constraint error
        let stmt = Parser::parse("INSERT INTO t_div_notnull VALUES (1/0)").unwrap();
        let res = executor.execute(stmt, &mut session);
        assert!(res.is_err(), "Expected constraint error but insert succeeded");
        let err = res.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("cannot be NULL") || msg.contains("cannot be null") || msg.contains("NULL"), "Unexpected error message: {}", msg);
    }

    #[test]
    fn test_insert_float_division_by_zero_sets_null_in_float_column() {
        let (executor, mut session, _dir) = create_test_executor();

        execute_ok(&executor, &mut session, "CREATE TABLE t_fdiv (f FLOAT)");
        // Insert float division by zero
        execute_ok(&executor, &mut session, "INSERT INTO t_fdiv VALUES (1.1/0)");

        let res = execute(&executor, &mut session, "SELECT f FROM t_fdiv");
        let vals = first_column(&res);
        // Expect the inserted float value to be NULL (NaN -> NULL)
        assert_eq!(vals, vec![Value::Null]);
    }

    #[test]
    fn test_insert_float_division_by_zero_into_not_null_errors() {
        let (executor, mut session, _dir) = create_test_executor();

        execute_ok(&executor, &mut session, "CREATE TABLE t_fdiv_notnull (f FLOAT NOT NULL)");
        let stmt = Parser::parse("INSERT INTO t_fdiv_notnull VALUES (1.1/0)").unwrap();
        let res = executor.execute(stmt, &mut session);
        assert!(res.is_err(), "Expected constraint error but insert succeeded");
        let err = res.unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("cannot be NULL") || msg.contains("NULL") || msg.contains("cannot be null"), "Unexpected error message: {}", msg);
    }

    #[test]
    fn test_column_arithmetic() {
        let (executor, mut session, _dir) = create_test_executor();

        execute_ok(&executor, &mut session, "CREATE TABLE t_arith (a INTEGER)");
        execute_ok(&executor, &mut session, "INSERT INTO t_arith VALUES (1)");
        execute_ok(&executor, &mut session, "INSERT INTO t_arith VALUES (2)");
        execute_ok(&executor, &mut session, "INSERT INTO t_arith VALUES (3)");

        let res = execute(&executor, &mut session, "SELECT a, a*2, a+1, a/2 FROM t_arith ORDER BY a");
        match res {
            QueryResult::Select(rs) => {
                let vals: Vec<(Value, Value, Value, Value)> = rs.rows.iter().map(|r| (r[0].clone(), r[1].clone(), r[2].clone(), r[3].clone())).collect();
                let expected = vec![
                    (Value::Integer(1), Value::Integer(2), Value::Integer(2), Value::Float(0.5)),
                    (Value::Integer(2), Value::Integer(4), Value::Integer(3), Value::Integer(1)),
                    (Value::Integer(3), Value::Integer(6), Value::Integer(4), Value::Float(1.5)),
                ];
                assert_eq!(vals, expected);
            }
            _ => panic!("Expected SELECT result"),
        }
    }
}
