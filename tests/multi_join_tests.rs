use minisql::parser::Parser;
use minisql::executor::{Executor, Session};
use minisql::storage::StorageEngine;
use minisql::engines::granite::TransactionManager;
use minisql::types::{Value, QueryResult};
use tempfile::{tempdir, TempDir};

fn setup_test() -> (Executor, Session, TempDir) {
    let dir = tempdir().unwrap();
    let storage = StorageEngine::new(dir.path().to_path_buf()).unwrap();
    let txn_manager = TransactionManager::new(dir.path().to_path_buf()).unwrap();
    let executor = Executor::new(storage, txn_manager);
    let session = Session::new();
    (executor, session, dir)
}

#[test]
fn test_triple_join_qualified_columns() {
    let (executor, mut session, _dir) = setup_test();

    // Create tables
    executor.execute(Parser::parse("CREATE TABLE ingest_jobs (id INT PRIMARY KEY, layer_id INT, vintage_id INT)").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("CREATE TABLE layers (id INT PRIMARY KEY, name VARCHAR(50))").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("CREATE TABLE vintages (id INT PRIMARY KEY, version VARCHAR(50))").unwrap(), &mut session).unwrap();

    // Insert data
    executor.execute(Parser::parse("INSERT INTO layers VALUES (10, 'Layer A')").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("INSERT INTO vintages VALUES (20, 'v1.0')").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("INSERT INTO ingest_jobs VALUES (1, 10, 20)").unwrap(), &mut session).unwrap();

    // The query that was failing:
    // SELECT ingest_jobs.*, layers.name as layer_name, vintages.version as vintage_version
    // FROM ingest_jobs
    // LEFT JOIN layers ON layers.id = ingest_jobs.layer_id
    // LEFT JOIN vintages ON vintages.id = ingest_jobs.vintage_id
    
    let sql = "SELECT ingest_jobs.id, layers.name, vintages.version 
               FROM ingest_jobs 
               LEFT JOIN layers ON layers.id = ingest_jobs.layer_id 
               LEFT JOIN vintages ON vintages.id = ingest_jobs.vintage_id";
    
    let result = executor.execute(Parser::parse(sql).unwrap(), &mut session).unwrap();

    if let QueryResult::Select(result_set) = result {
        assert_eq!(result_set.rows.len(), 1);
        assert_eq!(result_set.rows[0][0], Value::Integer(1));
        assert_eq!(result_set.rows[0][1], Value::String("Layer A".to_string()));
        assert_eq!(result_set.rows[0][2], Value::String("v1.0".to_string()));
    } else {
        panic!("Expected Select result");
    }
}

#[test]
fn test_mixed_join_types() {
    let (executor, mut session, _dir) = setup_test();

    executor.execute(Parser::parse("CREATE TABLE t1 (id INT PRIMARY KEY, a INT)").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("CREATE TABLE t2 (id INT PRIMARY KEY, b INT)").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("CREATE TABLE t3 (id INT PRIMARY KEY, c INT)").unwrap(), &mut session).unwrap();

    executor.execute(Parser::parse("INSERT INTO t1 VALUES (1, 100)").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("INSERT INTO t1 VALUES (2, 200)").unwrap(), &mut session).unwrap();
    
    executor.execute(Parser::parse("INSERT INTO t2 VALUES (1, 100)").unwrap(), &mut session).unwrap();
    // t2 has no entry for t1.id=2
    
    executor.execute(Parser::parse("INSERT INTO t3 VALUES (1, 100)").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("INSERT INTO t3 VALUES (2, 200)").unwrap(), &mut session).unwrap();

    // Mixed INNER and LEFT join
    // t1 INNER JOIN t2 (only id=1 matches)
    // LEFT JOIN t3 (id=1 matches)
    let sql = "SELECT t1.id, t2.b, t3.c 
               FROM t1 
               INNER JOIN t2 ON t1.a = t2.b 
               LEFT JOIN t3 ON t1.a = t3.c";
    
    let result = executor.execute(Parser::parse(sql).unwrap(), &mut session).unwrap();

    if let QueryResult::Select(result_set) = result {
        // Only id=1 should remain after INNER JOIN
        assert_eq!(result_set.rows.len(), 1);
        assert_eq!(result_set.rows[0][0], Value::Integer(1));
        assert_eq!(result_set.rows[0][1], Value::Integer(100));
        assert_eq!(result_set.rows[0][2], Value::Integer(100));
    } else {
        panic!("Expected Select result");
    }
}

#[test]
fn test_four_table_chain_join() {
    let (executor, mut session, _dir) = setup_test();

    executor.execute(Parser::parse("CREATE TABLE a (id INT PRIMARY KEY, val INT)").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("CREATE TABLE b (id INT PRIMARY KEY, a_id INT)").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("CREATE TABLE c (id INT PRIMARY KEY, b_id INT)").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("CREATE TABLE d (id INT PRIMARY KEY, c_id INT)").unwrap(), &mut session).unwrap();

    executor.execute(Parser::parse("INSERT INTO a VALUES (1, 10)").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("INSERT INTO b VALUES (1, 1)").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("INSERT INTO c VALUES (1, 1)").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("INSERT INTO d VALUES (1, 1)").unwrap(), &mut session).unwrap();

    let sql = "SELECT a.val 
               FROM a 
               JOIN b ON a.id = b.a_id 
               JOIN c ON b.id = c.b_id 
               JOIN d ON c.id = d.c_id";
    
    let result = executor.execute(Parser::parse(sql).unwrap(), &mut session).unwrap();

    if let QueryResult::Select(result_set) = result {
        assert_eq!(result_set.rows.len(), 1);
        assert_eq!(result_set.rows[0][0], Value::Integer(10));
    } else {
        panic!("Expected Select result");
    }
}

#[test]
fn test_aliases_and_column_selection() {
    let (executor, mut session, _dir) = setup_test();

    executor.execute(Parser::parse("CREATE TABLE users (id INT PRIMARY KEY, name VARCHAR(50))").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("CREATE TABLE posts (id INT PRIMARY KEY, user_id INT, title VARCHAR(100))").unwrap(), &mut session).unwrap();

    executor.execute(Parser::parse("INSERT INTO users VALUES (1, 'Alice')").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("INSERT INTO posts VALUES (1, 1, 'Hello')").unwrap(), &mut session).unwrap();

    // Use aliases and select qualified columns
    let sql = "SELECT u.id, p.title FROM users AS u JOIN posts AS p ON u.id = p.user_id";
    let result = executor.execute(Parser::parse(sql).unwrap(), &mut session).unwrap();
    if let QueryResult::Select(result_set) = result {
        assert_eq!(result_set.rows.len(), 1);
        assert_eq!(result_set.rows[0][0], Value::Integer(1));
        assert_eq!(result_set.rows[0][1], Value::String("Hello".to_string()));
    } else {
        panic!("Expected Select result");
    }
}

#[test]
fn test_same_column_names_ambiguous_and_qualified() {
    let (executor, mut session, _dir) = setup_test();

    executor.execute(Parser::parse("CREATE TABLE t1 (id INT PRIMARY KEY, value INT)").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("CREATE TABLE t2 (id INT PRIMARY KEY, value INT)").unwrap(), &mut session).unwrap();

    executor.execute(Parser::parse("INSERT INTO t1 VALUES (1, 100)").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("INSERT INTO t2 VALUES (1, 200)").unwrap(), &mut session).unwrap();

    // Unqualified 'value' should be ambiguous in SELECT field list
    let err = executor.execute(Parser::parse("SELECT value FROM t1 JOIN t2 ON t1.id = t2.id").unwrap(), &mut session).unwrap_err();
    let msg = err.to_string();
    assert!(msg.contains("ambiguous") || msg.contains("Ambiguous") || msg.contains("is ambiguous"));

    // Qualified selection should succeed
    let res = executor.execute(Parser::parse("SELECT t1.value, t2.value FROM t1 JOIN t2 ON t1.id = t2.id").unwrap(), &mut session).unwrap();
    if let QueryResult::Select(result_set) = res {
        assert_eq!(result_set.rows.len(), 1);
        assert_eq!(result_set.rows[0][0], Value::Integer(100));
        assert_eq!(result_set.rows[0][1], Value::Integer(200));
    } else {
        panic!("Expected Select result");
    }
}

#[test]
fn test_select_qualified_star_returns_table_columns_only() {
    let (executor, mut session, _dir) = setup_test();

    executor.execute(Parser::parse("CREATE TABLE customers (id INT PRIMARY KEY, name VARCHAR(50))").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("CREATE TABLE orders (id INT PRIMARY KEY, customer_id INT, total INT)").unwrap(), &mut session).unwrap();

    executor.execute(Parser::parse("INSERT INTO customers VALUES (1, 'C1')").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("INSERT INTO orders VALUES (10, 1, 999)").unwrap(), &mut session).unwrap();

    // SELECT customers.* should return only customer columns (id, name)
    let sql = "SELECT customers.* FROM customers JOIN orders ON customers.id = orders.customer_id";
    let result = executor.execute(Parser::parse(sql).unwrap(), &mut session).unwrap();
    if let QueryResult::Select(result_set) = result {
        assert_eq!(result_set.rows.len(), 1);
        // customers.* -> two columns
        assert_eq!(result_set.rows[0].len(), 2);
        assert_eq!(result_set.rows[0][0], Value::Integer(1));
        assert_eq!(result_set.rows[0][1], Value::String("C1".to_string()));
    } else { panic!("Expected Select result"); }
}

#[test]
fn test_alias_qualified_star_with_alias() {
    let (executor, mut session, _dir) = setup_test();

    executor.execute(Parser::parse("CREATE TABLE u (id INT PRIMARY KEY, nm VARCHAR(10))").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("CREATE TABLE p (id INT PRIMARY KEY, u_id INT, title VARCHAR(20))").unwrap(), &mut session).unwrap();

    executor.execute(Parser::parse("INSERT INTO u VALUES (1, 'AA')").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("INSERT INTO p VALUES (1, 1, 'T1')").unwrap(), &mut session).unwrap();

    let sql = "SELECT p.* FROM u JOIN p ON u.id = p.u_id";
    let result = executor.execute(Parser::parse(sql).unwrap(), &mut session).unwrap();
    if let QueryResult::Select(result_set) = result {
        assert_eq!(result_set.rows.len(), 1);
        // p.* => 3 columns from p
        assert_eq!(result_set.rows[0].len(), 3);
        assert_eq!(result_set.rows[0][2], Value::String("T1".to_string()));
    } else { panic!("Expected Select result"); }
}

#[test]
fn test_order_by_on_joined_column() {
    let (executor, mut session, _dir) = setup_test();

    executor.execute(Parser::parse("CREATE TABLE a (id INT PRIMARY KEY, v INT)").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("CREATE TABLE b (id INT PRIMARY KEY, a_id INT, score INT)").unwrap(), &mut session).unwrap();

    executor.execute(Parser::parse("INSERT INTO a VALUES (1, 10)").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("INSERT INTO a VALUES (2, 20)").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("INSERT INTO b VALUES (1, 1, 5)").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("INSERT INTO b VALUES (2, 2, 15)").unwrap(), &mut session).unwrap();

    let sql = "SELECT a.id, b.score FROM a JOIN b ON a.id = b.a_id ORDER BY b.score DESC";
    let result = executor.execute(Parser::parse(sql).unwrap(), &mut session).unwrap();
    if let QueryResult::Select(result_set) = result {
        assert_eq!(result_set.rows.len(), 2);
        // First row should have the highest score
        assert_eq!(result_set.rows[0][1], Value::Integer(15));
        assert_eq!(result_set.rows[1][1], Value::Integer(5));
    } else { panic!("Expected Select result"); }
}

#[test]
fn test_unqualified_id_ambiguous_in_select() {
    let (executor, mut session, _dir) = setup_test();

    executor.execute(Parser::parse("CREATE TABLE x (id INT PRIMARY KEY, v INT)").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("CREATE TABLE y (id INT PRIMARY KEY, v INT)").unwrap(), &mut session).unwrap();

    executor.execute(Parser::parse("INSERT INTO x VALUES (1, 1)").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("INSERT INTO y VALUES (1, 2)").unwrap(), &mut session).unwrap();

    let res = executor.execute(Parser::parse("SELECT id FROM x JOIN y ON x.id = y.id").unwrap(), &mut session);
    assert!(res.is_err());
}

#[test]
fn test_alias_collision_rejected() {
    let (executor, mut session, _dir) = setup_test();

    executor.execute(Parser::parse("CREATE TABLE ta (id INT PRIMARY KEY, v INT)").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("CREATE TABLE tb (id INT PRIMARY KEY, v INT)").unwrap(), &mut session).unwrap();

    executor.execute(Parser::parse("INSERT INTO ta VALUES (11, 10)").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("INSERT INTO tb VALUES (22, 20)").unwrap(), &mut session).unwrap();

    // MySQL rejects duplicate aliases (ER_NON_UNIQ_TABLE / 1066). Tests should
    // reflect that behaviour. If the server does not currently enforce this,
    // the test will fail (which documents the discrepancy).
    let res = executor.execute(Parser::parse("SELECT x.v FROM ta AS x JOIN tb AS x ON x.id = x.id").unwrap(), &mut session);
    assert!(res.is_err(), "Expected error for duplicate table alias, but query succeeded");
    
    // Verify the error message contains the expected text
    let err_msg = res.unwrap_err().to_string();
    assert!(err_msg.contains("Not unique table/alias"), "Error message should mention 'Not unique table/alias', got: {}", err_msg);
    assert!(err_msg.contains("'x'"), "Error message should mention the duplicate alias 'x', got: {}", err_msg);
}

#[test]
fn test_mixed_star_and_qualified_star() {
    let (executor, mut session, _dir) = setup_test();

    executor.execute(Parser::parse("CREATE TABLE t1 (id INT PRIMARY KEY, a INT)").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("CREATE TABLE t2 (id INT PRIMARY KEY, b INT)").unwrap(), &mut session).unwrap();

    executor.execute(Parser::parse("INSERT INTO t1 VALUES (1, 100)").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("INSERT INTO t2 VALUES (1, 200)").unwrap(), &mut session).unwrap();

    // SELECT t1.*, t2.id should return t1's columns followed by t2.id
    let sql = "SELECT t1.*, t2.id FROM t1 JOIN t2 ON t1.id = t2.id";
    let result = executor.execute(Parser::parse(sql).unwrap(), &mut session).unwrap();
    if let QueryResult::Select(result_set) = result {
        assert_eq!(result_set.rows.len(), 1);
        // t1.* -> 2 columns (id,a) and then t2.id -> 1 column => total 3
        assert_eq!(result_set.rows[0].len(), 3);
        assert_eq!(result_set.rows[0][0], Value::Integer(1));
        assert_eq!(result_set.rows[0][1], Value::Integer(100));
        assert_eq!(result_set.rows[0][2], Value::Integer(1));
    } else { panic!("Expected Select result"); }
}

#[test]
fn test_non_equi_join_nested_loop() {
    let (executor, mut session, _dir) = setup_test();

    executor.execute(Parser::parse("CREATE TABLE l (id INT PRIMARY KEY, val INT)").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("CREATE TABLE r (id INT PRIMARY KEY, val INT)").unwrap(), &mut session).unwrap();

    executor.execute(Parser::parse("INSERT INTO l VALUES (1, 5)").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("INSERT INTO l VALUES (2, 10)").unwrap(), &mut session).unwrap();

    executor.execute(Parser::parse("INSERT INTO r VALUES (1, 3)").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("INSERT INTO r VALUES (2, 8)").unwrap(), &mut session).unwrap();

    // Non-equi join: l.val > r.val should force nested-loop join and produce 3 matches
    let sql = "SELECT l.id, r.id FROM l JOIN r ON l.val > r.val";
    let result = executor.execute(Parser::parse(sql).unwrap(), &mut session).unwrap();
    if let QueryResult::Select(result_set) = result {
        assert_eq!(result_set.rows.len(), 3);
    } else { panic!("Expected Select result"); }
}

#[test]
fn test_duplicate_alias_case_insensitive() {
    let (executor, mut session, _dir) = setup_test();

    executor.execute(Parser::parse("CREATE TABLE ta (id INT PRIMARY KEY, v INT)").unwrap(), &mut session).unwrap();
    executor.execute(Parser::parse("CREATE TABLE tb (id INT PRIMARY KEY, v INT)").unwrap(), &mut session).unwrap();

    // Test with different cases - should still be rejected
    let res = executor.execute(Parser::parse("SELECT x.v FROM ta AS x JOIN tb AS X ON x.id = X.id").unwrap(), &mut session);
    assert!(res.is_err(), "Expected error for duplicate table alias with different case");
    
    let err_msg = res.unwrap_err().to_string();
    assert!(err_msg.contains("Not unique table/alias"), "Error message should mention 'Not unique table/alias', got: {}", err_msg);
}
