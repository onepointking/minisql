//! Tests for WHERE clause operators: AND/OR/NOT, NULL semantics, IS NULL, LIKE, BETWEEN-equivalent, comparisons

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

fn rows_as_pairs(result: &QueryResult) -> Vec<(Value, Value)> {
    match result {
        QueryResult::Select(rs) => rs.rows.iter().map(|r| (r[0].clone(), r[1].clone())).collect(),
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
mod where_operator_tests {
    use super::*;

    #[test]
    fn test_and_or_precedence_and_parentheses() {
        let (executor, mut session, _dir) = create_test_executor();

        execute_ok(&executor, &mut session, "CREATE TABLE t1 (a INTEGER, b INTEGER)");
        execute_ok(&executor, &mut session, "INSERT INTO t1 VALUES (1,1)");
        execute_ok(&executor, &mut session, "INSERT INTO t1 VALUES (1,0)");
        execute_ok(&executor, &mut session, "INSERT INTO t1 VALUES (0,1)");
        execute_ok(&executor, &mut session, "INSERT INTO t1 VALUES (0,0)");

        // AND has higher precedence than OR: a=1 OR b=1 AND a=0 -> a=1 OR (b=1 AND a=0)
        let res = execute(&executor, &mut session, "SELECT a, b FROM t1 WHERE a = 1 OR b = 1 AND a = 0 ORDER BY a DESC, b DESC");
        let pairs = rows_as_pairs(&res);
        // Expect rows: (1,1), (1,0), (0,1)
        let expected = vec![
            (Value::Integer(1), Value::Integer(1)),
            (Value::Integer(1), Value::Integer(0)),
            (Value::Integer(0), Value::Integer(1)),
        ];
        assert_eq!(pairs, expected);

        // Parentheses change grouping: (a=1 OR b=1) AND a=0 -> only (0,1)
        let res = execute(&executor, &mut session, "SELECT a, b FROM t1 WHERE (a = 1 OR b = 1) AND a = 0 ORDER BY a, b");
        let pairs = rows_as_pairs(&res);
        let expected = vec![(Value::Integer(0), Value::Integer(1))];
        assert_eq!(pairs, expected);
    }

    #[test]
    fn test_or_with_null_semantics() {
        let (executor, mut session, _dir) = create_test_executor();

        execute_ok(&executor, &mut session, "CREATE TABLE t_nulls (v1 INTEGER, v2 INTEGER)");
        execute_ok(&executor, &mut session, "INSERT INTO t_nulls VALUES (NULL, 0)");
        execute_ok(&executor, &mut session, "INSERT INTO t_nulls VALUES (NULL, 1)");
        execute_ok(&executor, &mut session, "INSERT INTO t_nulls VALUES (1, NULL)");
        execute_ok(&executor, &mut session, "INSERT INTO t_nulls VALUES (0, NULL)");
        execute_ok(&executor, &mut session, "INSERT INTO t_nulls VALUES (NULL, NULL)");

        // v1 = 1 OR v2 = 1 should select rows where either side is TRUE.
        // Rows: (NULL,1) and (1,NULL) should be selected; rows yielding NULL should be excluded.
        let res = execute(&executor, &mut session, "SELECT v1, v2 FROM t_nulls WHERE v1 = 1 OR v2 = 1 ORDER BY v1 ASC, v2 ASC");
        let pairs = rows_as_pairs(&res);
        let expected = vec![
            (Value::Null, Value::Integer(1)),
            (Value::Integer(1), Value::Null),
        ];
        assert_eq!(pairs, expected);

        // NULL OR TRUE => TRUE (so a row with NULL left operand can be selected if right is TRUE)
        let res_all = execute(&executor, &mut session, "SELECT v1 FROM t_nulls WHERE v1 = 1 OR 1 = 1 ORDER BY v1 ASC");
        let vals = first_column(&res_all);
        // 1=1 true means all rows selected; order by v1 (Null first)
        assert_eq!(vals.len(), 5);
    }

    #[test]
    fn test_not_operator_and_null_behavior() {
        let (executor, mut session, _dir) = create_test_executor();

        execute_ok(&executor, &mut session, "CREATE TABLE t_not (n INTEGER)");
        execute_ok(&executor, &mut session, "INSERT INTO t_not VALUES (1)");
        execute_ok(&executor, &mut session, "INSERT INTO t_not VALUES (2)");
        execute_ok(&executor, &mut session, "INSERT INTO t_not VALUES (NULL)");

        // NOT (n = 1) should exclude NULL row (since n = 1 is NULL -> NOT NULL is NULL -> treated as false in WHERE)
        let res = execute(&executor, &mut session, "SELECT n FROM t_not WHERE NOT (n = 1) ORDER BY n ASC");
        let vals = first_column(&res);
        assert_eq!(vals, vec![Value::Integer(2)]);

        // Combined: NOT (n = 1 OR n = 2) should exclude both 1 and 2; NULL row remains excluded
        let res = execute(&executor, &mut session, "SELECT n FROM t_not WHERE NOT (n = 1 OR n = 2)");
        let vals = first_column(&res);
        assert_eq!(vals.len(), 0);
    }

    #[test]
    fn test_is_null_and_is_not_null() {
        let (executor, mut session, _dir) = create_test_executor();

        execute_ok(&executor, &mut session, "CREATE TABLE t_isnull (a INTEGER)");
        execute_ok(&executor, &mut session, "INSERT INTO t_isnull VALUES (1)");
        execute_ok(&executor, &mut session, "INSERT INTO t_isnull VALUES (NULL)");

        let res = execute(&executor, &mut session, "SELECT a FROM t_isnull WHERE a IS NULL");
        let vals = first_column(&res);
        assert_eq!(vals, vec![Value::Null]);

        let res = execute(&executor, &mut session, "SELECT a FROM t_isnull WHERE a IS NOT NULL ORDER BY a");
        let vals = first_column(&res);
        assert_eq!(vals, vec![Value::Integer(1)]);
    }

    #[test]
    fn test_like_patterns() {
        let (executor, mut session, _dir) = create_test_executor();

        execute_ok(&executor, &mut session, "CREATE TABLE t_like (s TEXT)");
        execute_ok(&executor, &mut session, "INSERT INTO t_like VALUES ('apple')");
        execute_ok(&executor, &mut session, "INSERT INTO t_like VALUES ('apricot')");
        execute_ok(&executor, &mut session, "INSERT INTO t_like VALUES ('banana')");

        let res = execute(&executor, &mut session, "SELECT s FROM t_like WHERE s LIKE 'ap%' ORDER BY s");
        let vals = first_column(&res);
        assert_eq!(vals, vec![Value::String("apple".into()), Value::String("apricot".into())]);

        // Single-character wildcard _
        let res = execute(&executor, &mut session, "SELECT s FROM t_like WHERE s LIKE 'b_na%' ORDER BY s");
        let vals = first_column(&res);
        assert_eq!(vals, vec![Value::String("banana".into())]);
    }

    #[test]
    fn test_between_equivalent_and_comparisons() {
        let (executor, mut session, _dir) = create_test_executor();

        execute_ok(&executor, &mut session, "CREATE TABLE t_range (x INTEGER)");
        execute_ok(&executor, &mut session, "INSERT INTO t_range VALUES (1)");
        execute_ok(&executor, &mut session, "INSERT INTO t_range VALUES (2)");
        execute_ok(&executor, &mut session, "INSERT INTO t_range VALUES (5)");
        execute_ok(&executor, &mut session, "INSERT INTO t_range VALUES (9)");
        execute_ok(&executor, &mut session, "INSERT INTO t_range VALUES (10)");

        // BETWEEN 2 AND 9 (inclusive) equivalent
        let res = execute(&executor, &mut session, "SELECT x FROM t_range WHERE x >= 2 AND x <= 9 ORDER BY x");
        let vals = first_column(&res);
        assert_eq!(vals, vec![Value::Integer(2), Value::Integer(5), Value::Integer(9)]);

        // Comparisons: <, >, <=, >=
        let res = execute(&executor, &mut session, "SELECT x FROM t_range WHERE x < 5 ORDER BY x");
        let vals = first_column(&res);
        assert_eq!(vals, vec![Value::Integer(1), Value::Integer(2)]);

        let res = execute(&executor, &mut session, "SELECT x FROM t_range WHERE x > 5 ORDER BY x");
        let vals = first_column(&res);
        assert_eq!(vals, vec![Value::Integer(9), Value::Integer(10)]);

        let res = execute(&executor, &mut session, "SELECT x FROM t_range WHERE x <= 2 ORDER BY x");
        let vals = first_column(&res);
        assert_eq!(vals, vec![Value::Integer(1), Value::Integer(2)]);

        let res = execute(&executor, &mut session, "SELECT x FROM t_range WHERE x >= 9 ORDER BY x");
        let vals = first_column(&res);
        assert_eq!(vals, vec![Value::Integer(9), Value::Integer(10)]);
    }
}
