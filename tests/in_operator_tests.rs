// Unit tests for IN operator
// These test the parser and evaluator for IN/NOT IN

#[cfg(test)]
mod in_operator_tests {
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
    fn test_parse_in_single_value() {
        let sql = "SELECT * FROM users WHERE id IN (1)";
        let stmt = Parser::parse(sql).unwrap();
        
        // Check that it parses without error
        assert!(matches!(stmt, minisql::parser::Statement::Select(_)));
    }

    #[test]
    fn test_parse_in_multiple_values() {
        let sql = "SELECT * FROM users WHERE id IN (1, 2, 3)";
        let stmt = Parser::parse(sql).unwrap();
        
        assert!(matches!(stmt, minisql::parser::Statement::Select(_)));
    }

    #[test]
    fn test_parse_not_in() {
        let sql = "SELECT * FROM users WHERE id NOT IN (1, 2, 3)";
        let stmt = Parser::parse(sql).unwrap();
        
        assert!(matches!(stmt, minisql::parser::Statement::Select(_)));
    }

    #[test]
    fn test_parse_in_with_strings() {
        let sql = "SELECT * FROM users WHERE name IN ('Alice', 'Bob')";
        let stmt = Parser::parse(sql).unwrap();
        
        assert!(matches!(stmt, minisql::parser::Statement::Select(_)));
    }

    #[test]
    fn test_in_operator_basic() {
        let (executor, mut session, _dir) = setup_test();

        // Create table
        executor.execute(
            Parser::parse("CREATE TABLE test_in (id INT PRIMARY KEY, name VARCHAR(50))").unwrap(),
            &mut session
        ).unwrap();

        // Insert data
        executor.execute(
            Parser::parse("INSERT INTO test_in VALUES (1, 'Alice')").unwrap(),
            &mut session
        ).unwrap();
        executor.execute(
            Parser::parse("INSERT INTO test_in VALUES (2, 'Bob')").unwrap(),
            &mut session
        ).unwrap();
        executor.execute(
            Parser::parse("INSERT INTO test_in VALUES (3, 'Charlie')").unwrap(),
            &mut session
        ).unwrap();

        // Test IN with single value
        let result = executor.execute(
            Parser::parse("SELECT * FROM test_in WHERE id IN (1)").unwrap(),
            &mut session
        ).unwrap();

        if let QueryResult::Select(result_set) = result {
            let rows = result_set.rows;
            assert_eq!(rows.len(), 1, "Should return 1 row");
            assert_eq!(rows[0][0], Value::Integer(1));
        } else {
            panic!("Expected Select result");
        }
    }

    #[test]
    fn test_in_operator_multiple_values() {
        let (executor, mut session, _dir) = setup_test();

        // Setup
        executor.execute(
            Parser::parse("CREATE TABLE test_in (id INT PRIMARY KEY, name VARCHAR(50))").unwrap(),
            &mut session
        ).unwrap();
        
        for i in 1..=5 {
            executor.execute(
                Parser::parse(&format!("INSERT INTO test_in VALUES ({}, 'Name{}')", i, i)).unwrap(),
                &mut session
            ).unwrap();
        }

        // Test IN with multiple values
        let result = executor.execute(
            Parser::parse("SELECT * FROM test_in WHERE id IN (1, 3, 5)").unwrap(),
            &mut session
        ).unwrap();

        if let QueryResult::Select(result_set) = result {
            let rows = result_set.rows;
            assert_eq!(rows.len(), 3, "Should return 3 rows");
            assert_eq!(rows[0][0], Value::Integer(1));
            assert_eq!(rows[1][0], Value::Integer(3));
            assert_eq!(rows[2][0], Value::Integer(5));
        } else {
            panic!("Expected Select result");
        }
    }

    #[test]
    fn test_not_in_operator() {
        let (executor, mut session, _dir) = setup_test();

        // Setup
        executor.execute(
            Parser::parse("CREATE TABLE test_in (id INT PRIMARY KEY)").unwrap(),
            &mut session
        ).unwrap();
        
        for i in 1..=5 {
            executor.execute(
                Parser::parse(&format!("INSERT INTO test_in VALUES ({})", i)).unwrap(),
                &mut session
            ).unwrap();
        }

        // Test NOT IN
        let result = executor.execute(
            Parser::parse("SELECT * FROM test_in WHERE id NOT IN (2, 4)").unwrap(),
            &mut session
        ).unwrap();

        if let QueryResult::Select(result_set) = result {
            let rows = result_set.rows;
            assert_eq!(rows.len(), 3, "Should return 3 rows (1, 3, 5)");
            assert_eq!(rows[0][0], Value::Integer(1));
            assert_eq!(rows[1][0], Value::Integer(3));
            assert_eq!(rows[2][0], Value::Integer(5));
        } else {
            panic!("Expected Select result");
        }
    }

    #[test]
    fn test_update_with_in_critical() {
        // THIS IS THE CRITICAL BUG TEST!
        // Before fix: Would update ALL rows
        // After fix: Should update only 1 row
        
        let (executor, mut session, _dir) = setup_test();

        // Setup
        executor.execute(
            Parser::parse("CREATE TABLE test_in (id INT PRIMARY KEY, value INT)").unwrap(),
            &mut session
        ).unwrap();
        
        for i in 1..=5 {
            executor.execute(
                Parser::parse(&format!("INSERT INTO test_in VALUES ({}, {})", i, i * 100)).unwrap(),
                &mut session
            ).unwrap();
        }

        // Update using IN
        let result = executor.execute(
            Parser::parse("UPDATE test_in SET value = 999 WHERE id IN (3)").unwrap(),
            &mut session
        ).unwrap();

        // Should affect exactly 1 row
        if let QueryResult::Modified { rows_affected, .. } = result {
            assert_eq!(rows_affected, 1, "Should update exactly 1 row, not all rows!");
        } else {
            panic!("Expected Modified result");
        }

        // Verify only id=3 was updated
        let check = executor.execute(
            Parser::parse("SELECT * FROM test_in WHERE value = 999").unwrap(),
            &mut session
        ).unwrap();

        if let QueryResult::Select(result_set) = check {
            let rows = result_set.rows;
            assert_eq!(rows.len(), 1, "Only one row should have value=999");
            assert_eq!(rows[0][0], Value::Integer(3), "Should be row with id=3");
        } else {
            panic!("Expected Select result");
        }
    }

    #[test]
    fn test_delete_with_in() {
        let (executor, mut session, _dir) = setup_test();

        // Setup
        executor.execute(
            Parser::parse("CREATE TABLE test_in (id INT PRIMARY KEY)").unwrap(),
            &mut session
        ).unwrap();
        
        for i in 1..=5 {
            executor.execute(
                Parser::parse(&format!("INSERT INTO test_in VALUES ({})", i)).unwrap(),
                &mut session
            ).unwrap();
        }

        // Delete using IN
        let result = executor.execute(
            Parser::parse("DELETE FROM test_in WHERE id IN (2, 4)").unwrap(),
            &mut session
        ).unwrap();

        // Should delete exactly 2 rows
        if let QueryResult::Modified { rows_affected, .. } = result {
            assert_eq!(rows_affected, 2, "Should delete exactly 2 rows");
        } else {
            panic!("Expected Modified result");
        }

        // Verify 3 rows remain
        let check = executor.execute(
            Parser::parse("SELECT COUNT(*) FROM test_in").unwrap(),
            &mut session
        ).unwrap();

        if let QueryResult::Select(result_set) = check {
            let rows = result_set.rows;
            assert_eq!(rows[0][0], Value::Integer(3), "Should have 3 rows remaining");
        } else {
            panic!("Expected Select result");
        }
    }
}
