use super::*;
use crate::types::Value;

#[test]
fn test_parse_select() {
    let stmt = Parser::parse("SELECT * FROM users").unwrap();
    match stmt {
        Statement::Select(s) => {
            let from = s.from.as_ref().unwrap();
            assert_eq!(from.name, "users");
            assert!(from.alias.is_none());
            assert!(s.joins.is_empty());
            assert!(matches!(s.columns[0], SelectColumn::Star));
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_create_table() {
    let stmt = Parser::parse(
        "CREATE TABLE users (id INTEGER PRIMARY KEY, name VARCHAR(100), data JSON)"
    ).unwrap();
    match stmt {
        Statement::CreateTable(c) => {
            assert_eq!(c.table_name, "users");
            assert_eq!(c.columns.len(), 3);
        }
        _ => panic!("Expected CREATE TABLE"),
    }
}

#[test]
fn test_parse_json_access() {
    let stmt = Parser::parse("SELECT data->'name' FROM users").unwrap();
    match stmt {
        Statement::Select(s) => {
            assert!(matches!(
                &s.columns[0],
                SelectColumn::Expr { expr: Expr::JsonAccess { .. }, .. }
            ));
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_inner_join() {
    let stmt = Parser::parse(
        "SELECT users.name, orders.product FROM users INNER JOIN orders ON users.id = orders.user_id"
    ).unwrap();
    match stmt {
        Statement::Select(s) => {
            let from = s.from.as_ref().unwrap();
            assert_eq!(from.name, "users");
            assert_eq!(s.joins.len(), 1);
            assert_eq!(s.joins[0].join_type, JoinType::Inner);
            assert_eq!(s.joins[0].table.name, "orders");
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_left_join() {
    let stmt = Parser::parse(
        "SELECT * FROM orders LEFT JOIN users ON orders.user_id = users.id"
    ).unwrap();
    match stmt {
        Statement::Select(s) => {
            let from = s.from.as_ref().unwrap();
            assert_eq!(from.name, "orders");
            assert_eq!(s.joins.len(), 1);
            assert_eq!(s.joins[0].join_type, JoinType::Left);
            assert_eq!(s.joins[0].table.name, "users");
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_left_outer_join() {
    let stmt = Parser::parse(
        "SELECT * FROM orders LEFT OUTER JOIN users ON orders.user_id = users.id"
    ).unwrap();
    match stmt {
        Statement::Select(s) => {
            let _from = s.from.as_ref();
            assert_eq!(s.joins.len(), 1);
            assert_eq!(s.joins[0].join_type, JoinType::Left);
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_table_alias() {
    let stmt = Parser::parse(
        "SELECT u.name, o.product FROM users u JOIN orders o ON u.id = o.user_id"
    ).unwrap();
    match stmt {
        Statement::Select(s) => {
            let from = s.from.as_ref().unwrap();
            assert_eq!(from.name, "users");
            assert_eq!(from.alias, Some("u".to_string()));
            assert_eq!(s.joins[0].table.name, "orders");
            assert_eq!(s.joins[0].table.alias, Some("o".to_string()));
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_table_alias_with_as() {
    let stmt = Parser::parse(
        "SELECT * FROM users AS u JOIN orders AS o ON u.id = o.user_id"
    ).unwrap();
    match stmt {
        Statement::Select(s) => {
            let from = s.from.as_ref().unwrap();
            assert_eq!(from.alias, Some("u".to_string()));
            assert_eq!(s.joins[0].table.alias, Some("o".to_string()));
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_qualified_column() {
    let stmt = Parser::parse("SELECT users.name FROM users").unwrap();
    match stmt {
        Statement::Select(s) => {
            let _from = s.from.as_ref();
            match &s.columns[0] {
                SelectColumn::Expr { expr: Expr::Column { table, name }, .. } => {
                    assert_eq!(table, &Some("users".to_string()));
                    assert_eq!(name, "name");
                }
                _ => panic!("Expected qualified column"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_plain_join_is_inner() {
    let stmt = Parser::parse(
        "SELECT * FROM users JOIN orders ON users.id = orders.user_id"
    ).unwrap();
    match stmt {
        Statement::Select(s) => {
            let _from = s.from.as_ref();
            assert_eq!(s.joins[0].join_type, JoinType::Inner);
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_insert() {
    let stmt = Parser::parse("INSERT INTO users (id, name) VALUES (1, 'Alice'), (2, 'Bob')").unwrap();
    match stmt {
        Statement::Insert(i) => {
            assert_eq!(i.table_name, "users");
            assert_eq!(i.columns.unwrap(), vec!["id", "name"]);
            assert_eq!(i.values.len(), 2);
        }
        _ => panic!("Expected INSERT"),
    }
}

#[test]
fn test_parse_update() {
    let stmt = Parser::parse("UPDATE users SET name = 'Alice' WHERE id = 1").unwrap();
    match stmt {
        Statement::Update(u) => {
            assert_eq!(u.table_name, "users");
            assert_eq!(u.assignments[0].0, "name");
            assert!(u.where_clause.is_some());
        }
        _ => panic!("Expected UPDATE"),
    }
}

#[test]
fn test_parse_delete() {
    let stmt = Parser::parse("DELETE FROM users WHERE id = 1").unwrap();
    match stmt {
        Statement::Delete(d) => {
            assert_eq!(d.table_name, "users");
            assert!(d.where_clause.is_some());
        }
        _ => panic!("Expected DELETE"),
    }
}

#[test]
fn test_parse_ddl() {
    assert!(matches!(Parser::parse("SHOW TABLES").unwrap(), Statement::ShowTables));
    assert!(matches!(Parser::parse("DESCRIBE users").unwrap(), Statement::Describe(t) if t == "users"));
    assert!(matches!(Parser::parse("DROP TABLE users").unwrap(), Statement::DropTable(t) if t == "users"));
}

#[test]
fn test_parse_transactions() {
    assert!(matches!(Parser::parse("BEGIN").unwrap(), Statement::Begin));
    assert!(matches!(Parser::parse("COMMIT").unwrap(), Statement::Commit));
    assert!(matches!(Parser::parse("ROLLBACK").unwrap(), Statement::Rollback));
}

#[test]
fn test_parse_count_star() {
    let stmt = Parser::parse("SELECT COUNT(*) FROM users").unwrap();
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.columns.len(), 1);
            match &s.columns[0] {
                SelectColumn::Expr { expr: Expr::FunctionCall { name, args }, .. } => {
                    assert_eq!(name.to_uppercase(), "COUNT");
                    assert!(args.is_empty(), "COUNT(*) should have empty args");
                }
                _ => panic!("Expected FunctionCall"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_count_column() {
    let stmt = Parser::parse("SELECT COUNT(id) FROM users").unwrap();
    match stmt {
        Statement::Select(s) => {
            match &s.columns[0] {
                SelectColumn::Expr { expr: Expr::FunctionCall { name, args }, .. } => {
                    assert_eq!(name.to_uppercase(), "COUNT");
                    assert_eq!(args.len(), 1);
                }
                _ => panic!("Expected FunctionCall"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_group_by_single() {
    let stmt = Parser::parse("SELECT category, COUNT(*) FROM products GROUP BY category").unwrap();
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.group_by.len(), 1);
            match &s.group_by[0] {
                Expr::Column { table: None, name } => {
                    assert_eq!(name, "category");
                }
                _ => panic!("Expected column in GROUP BY"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_group_by_multiple() {
    let stmt = Parser::parse(
        "SELECT category, subcategory, SUM(amount) FROM sales GROUP BY category, subcategory"
    ).unwrap();
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.group_by.len(), 2);
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_aggregate_functions() {
    // Test SUM
    let stmt = Parser::parse("SELECT SUM(amount) FROM orders").unwrap();
    match stmt {
        Statement::Select(s) => {
            match &s.columns[0] {
                SelectColumn::Expr { expr: Expr::FunctionCall { name, .. }, .. } => {
                    assert_eq!(name.to_uppercase(), "SUM");
                }
                _ => panic!("Expected FunctionCall"),
            }
        }
        _ => panic!("Expected SELECT"),
    }

    // Test AVG
    let stmt = Parser::parse("SELECT AVG(price) FROM products").unwrap();
    match stmt {
        Statement::Select(s) => {
            match &s.columns[0] {
                SelectColumn::Expr { expr: Expr::FunctionCall { name, .. }, .. } => {
                    assert_eq!(name.to_uppercase(), "AVG");
                }
                _ => panic!("Expected FunctionCall"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_group_by_with_where() {
    let stmt = Parser::parse(
        "SELECT category, COUNT(*) FROM products WHERE active = 1 GROUP BY category"
    ).unwrap();
    match stmt {
        Statement::Select(s) => {
            assert!(s.where_clause.is_some());
            assert_eq!(s.group_by.len(), 1);
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_group_by_with_order_by() {
    let stmt = Parser::parse(
        "SELECT category, COUNT(*) FROM products GROUP BY category ORDER BY category"
    ).unwrap();
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.group_by.len(), 1);
            assert_eq!(s.order_by.len(), 1);
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_empty_input() {
    let result = Parser::parse("");
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("end of input") || err.contains("Empty"));
}

#[test]
fn test_incomplete_select() {
    let result = Parser::parse("SELECT");
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("end of input") || err.contains("Expected"));
}

#[test]
fn test_incomplete_select_from() {
    let result = Parser::parse("SELECT * FROM");
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("identifier") || err.contains("end of input"));
}

#[test]
fn test_missing_from_clause() {
    let result = Parser::parse("SELECT * WHERE id = 1");
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("FROM") || err.contains("Expected"));
}

#[test]
fn test_incomplete_insert() {
    let result = Parser::parse("INSERT INTO users");
    assert!(result.is_err());
}

#[test]
fn test_incomplete_insert_values() {
    let result = Parser::parse("INSERT INTO users VALUES");
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("end of input") || err.contains("Expected"));
}

#[test]
fn test_incomplete_update() {
    let result = Parser::parse("UPDATE users SET");
    assert!(result.is_err());
}

#[test]
fn test_incomplete_where_clause() {
    let result = Parser::parse("SELECT * FROM users WHERE");
    assert!(result.is_err());
}

#[test]
fn test_incomplete_create_table() {
    let result = Parser::parse("CREATE TABLE users");
    assert!(result.is_err());
}

#[test]
fn test_incomplete_create_table_columns() {
    let result = Parser::parse("CREATE TABLE users (id");
    assert!(result.is_err());
}

#[test]
fn test_invalid_token() {
    let result = Parser::parse("INVALID STATEMENT");
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    assert!(err.contains("Unexpected") || err.contains("token"));
}

#[test]
fn test_missing_semicolon_multistatement() {
    // Single statement without semicolon should succeed
    let result = Parser::parse("SELECT * FROM users");
    assert!(result.is_ok());
}

#[test]
fn test_unclosed_parenthesis() {
    let result = Parser::parse("SELECT COUNT(id FROM users");
    assert!(result.is_err());
}

#[test]
fn test_incomplete_expression() {
    let result = Parser::parse("SELECT * FROM users WHERE id =");
    assert!(result.is_err());
}

#[test]
fn test_invalid_operator() {
    let result = Parser::parse("SELECT * FROM users WHERE id & 5");
    assert!(result.is_err());
}

#[test]
fn test_complex_nested_expressions() {
    let stmt = Parser::parse(
        "SELECT * FROM users WHERE (age > 18 AND status = 'active') OR (vip = true AND balance > 1000)"
    ).unwrap();
    match stmt {
        Statement::Select(s) => {
            assert!(s.where_clause.is_some());
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_multiple_arithmetic_operations() {
    let stmt = Parser::parse(
        "SELECT price * quantity + tax - discount FROM orders"
    ).unwrap();
    match stmt {
        Statement::Select(_) => { /* Success */ }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_deeply_nested_parentheses() {
    let stmt = Parser::parse(
        "SELECT * FROM users WHERE (((id = 1)))"
    ).unwrap();
    match stmt {
        Statement::Select(_) => { /* Success */ }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_multiple_comparison_operators() {
    let stmt = Parser::parse(
        "SELECT * FROM products WHERE price >= 10 AND price <= 100 AND stock > 0"
    ).unwrap();
    match stmt {
        Statement::Select(_) => { /* Success */ }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_is_null_in_complex_expression() {
    let stmt = Parser::parse(
        "SELECT * FROM users WHERE (email IS NULL OR email = '') AND active = true"
    ).unwrap();
    match stmt {
        Statement::Select(_) => { /* Success */ }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_backtick_quoted_identifier() {
    let stmt = Parser::parse("SELECT * FROM `users`").unwrap();
    match stmt {
        Statement::Select(s) => {
            let from = s.from.as_ref().unwrap();
            assert_eq!(from.name, "users");
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_backtick_quoted_column() {
    let stmt = Parser::parse("SELECT `user id` FROM users").unwrap();
    match stmt {
        Statement::Select(s) => {
            match &s.columns[0] {
                SelectColumn::Expr { expr: Expr::Column { name, .. }, .. } => {
                    assert_eq!(name, "user id");
                }
                _ => panic!("Expected column"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_double_quoted_identifier() {
    let stmt = Parser::parse(r#"SELECT * FROM "users""#).unwrap();
    match stmt {
        Statement::Select(s) => {
            let from = s.from.as_ref().unwrap();
            assert_eq!(from.name, "users");
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_double_quoted_column() {
    let stmt = Parser::parse(r#"SELECT "user name" FROM users"#).unwrap();
    match stmt {
        Statement::Select(s) => {
            match &s.columns[0] {
                SelectColumn::Expr { expr: Expr::Column { name, .. }, .. } => {
                    assert_eq!(name, "user name");
                }
                _ => panic!("Expected column"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_quoted_identifier_with_special_chars() {
    let stmt = Parser::parse(r#"SELECT * FROM `my-table-2024`"#).unwrap();
    match stmt {
        Statement::Select(s) => {
            let from = s.from.as_ref().unwrap();
            assert_eq!(from.name, "my-table-2024");
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_string_with_braces_not_parsed_as_json() {
    let stmt = Parser::parse(r#"SELECT * FROM users WHERE name = '{hello}'"#).unwrap();
    match stmt {
        Statement::Select(s) => {
            // Verify the value is a string, not JSON
            match &s.where_clause {
                Some(Expr::BinaryOp { right, .. }) => {
                    match &**right {
                        Expr::Literal(Value::String(s)) => {
                            assert_eq!(s, "{hello}");
                        }
                        Expr::Literal(Value::Json(_)) => {
                            panic!("String should not be parsed as JSON!");
                        }
                        _ => panic!("Expected string literal"),
                    }
                }
                _ => panic!("Expected WHERE clause"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_string_with_brackets_not_parsed_as_json() {
    let stmt = Parser::parse(r#"SELECT * FROM users WHERE code = '[test]'"#).unwrap();
    match stmt {
        Statement::Select(s) => {
            match &s.where_clause {
                Some(Expr::BinaryOp { right, .. }) => {
                    match &**right {
                        Expr::Literal(Value::String(s)) => {
                            assert_eq!(s, "[test]");
                        }
                        Expr::Literal(Value::Json(_)) => {
                            panic!("String should not be parsed as JSON!");
                        }
                        _ => panic!("Expected string literal"),
                    }
                }
                _ => panic!("Expected WHERE clause"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_error_contains_context_info() {
    let result = Parser::parse("SELECT * FROM users WHERE");
    assert!(result.is_err());
    let err = result.unwrap_err().to_string();
    // Error should contain position information
    assert!(err.contains("position") || err.contains("parsing"));
}

#[test]
fn test_error_in_nested_context() {
    let result = Parser::parse("SELECT COUNT( FROM users");
    assert!(result.is_err());
    // Just verify it produces an error - the context should make it more debuggable
}

#[test]
fn test_parse_create_single_column_index() {
    let stmt = Parser::parse("CREATE INDEX idx_name ON users (name)").unwrap();
    match stmt {
        Statement::CreateIndex(c) => {
            assert_eq!(c.index_name, "idx_name");
            assert_eq!(c.table_name, "users");
            assert_eq!(c.columns, vec!["name"]);
            assert!(!c.if_not_exists);
        }
        _ => panic!("Expected CREATE INDEX"),
    }
}

#[test]
fn test_parse_create_composite_index() {
    let stmt = Parser::parse("CREATE INDEX idx_composite ON tiles (layer_id, z, x, y)").unwrap();
    match stmt {
        Statement::CreateIndex(c) => {
            assert_eq!(c.index_name, "idx_composite");
            assert_eq!(c.table_name, "tiles");
            assert_eq!(c.columns, vec!["layer_id", "z", "x", "y"]);
            assert!(!c.if_not_exists);
        }
        _ => panic!("Expected CREATE INDEX"),
    }
}

#[test]
fn test_parse_create_composite_index_if_not_exists() {
    let stmt = Parser::parse("CREATE INDEX IF NOT EXISTS idx_multi ON orders (customer_id, order_date)").unwrap();
    match stmt {
        Statement::CreateIndex(c) => {
            assert_eq!(c.index_name, "idx_multi");
            assert_eq!(c.table_name, "orders");
            assert_eq!(c.columns, vec!["customer_id", "order_date"]);
            assert!(c.if_not_exists);
        }
        _ => panic!("Expected CREATE INDEX"),
    }
}

#[test]
fn test_parse_qualified_star_simple() {
    let stmt = Parser::parse("SELECT users.* FROM users").unwrap();
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.columns.len(), 1);
            match &s.columns[0] {
                SelectColumn::QualifiedStar { table } => {
                    assert_eq!(table, "users");
                }
                _ => panic!("Expected QualifiedStar, got {:?}", s.columns[0]),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_qualified_star_with_join() {
    let stmt = Parser::parse("SELECT u.*, o.* FROM users u JOIN orders o ON u.id = o.user_id").unwrap();
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.columns.len(), 2);
            match &s.columns[0] {
                SelectColumn::QualifiedStar { table } => {
                    assert_eq!(table, "u");
                }
                _ => panic!("Expected QualifiedStar for first column"),
            }
            match &s.columns[1] {
                SelectColumn::QualifiedStar { table } => {
                    assert_eq!(table, "o");
                }
                _ => panic!("Expected QualifiedStar for second column"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_mixed_columns_with_qualified_star() {
    let stmt = Parser::parse("SELECT id, users.*, name FROM users").unwrap();
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.columns.len(), 3);
            // First column: id
            match &s.columns[0] {
                SelectColumn::Expr { expr: Expr::Column { table: None, name }, .. } => {
                    assert_eq!(name, "id");
                }
                _ => panic!("Expected column 'id'"),
            }
            // Second column: users.*
            match &s.columns[1] {
                SelectColumn::QualifiedStar { table } => {
                    assert_eq!(table, "users");
                }
                _ => panic!("Expected QualifiedStar"),
            }
            // Third column: name
            match &s.columns[2] {
                SelectColumn::Expr { expr: Expr::Column { table: None, name }, .. } => {
                    assert_eq!(name, "name");
                }
                _ => panic!("Expected column 'name'"),
            }
        }
        _ => panic!("Expected SELECT"),
    }
}

#[test]
fn test_parse_qualified_star_with_table_alias() {
    let stmt = Parser::parse("SELECT t.* FROM my_table AS t").unwrap();
    match stmt {
        Statement::Select(s) => {
            assert_eq!(s.columns.len(), 1);
            match &s.columns[0] {
                SelectColumn::QualifiedStar { table } => {
                    assert_eq!(table, "t");
                }
                _ => panic!("Expected QualifiedStar"),
            }
            let from = s.from.as_ref().unwrap();
            assert_eq!(from.alias, Some("t".to_string()));
        }
        _ => panic!("Expected SELECT"),
    }
}
