//! Join Strategy Module for MiniSQL
//!
//! Implements a strategy pattern for executing different join algorithms:
//! - NestedLoopJoin: Simple O(n×m) algorithm, works with any ON condition
//! - HashJoin: O(n+m) average case, optimal for equi-joins
//!
//! The appropriate strategy is selected automatically based on the ON condition.

use std::collections::HashMap;

use crate::error::{ColumnContext, MiniSqlError, Result};
use crate::parser::{BinaryOperator, Expr, JoinType};
use crate::types::{ColumnDef, Row, TableSchema, Value};

/// Result of a join operation: combined rows from left and right tables
#[derive(Debug, Clone)]
pub struct JoinedRow {
    /// Row from the left table
    pub left: Row,
    /// Row from the right table (None for LEFT JOIN unmatched rows)
    pub right: Option<Row>,
}

/// Context containing schemas and column mappings for join evaluation
pub struct JoinContext {
    /// Schema for the left table
    pub left_schema: TableSchema,
    /// Alias/name for the left table
    pub left_alias: String,
    /// Schema for the right table
    pub right_schema: TableSchema,
    /// Alias/name for the right table
    pub right_alias: String,
    /// Combined schema with qualified column names
    pub combined_schema: TableSchema,
    /// Mapping: (table_alias, column_name) -> index in combined row
    pub column_map: HashMap<(String, String), usize>,
}

impl JoinContext {
    /// Create a new join context from left and right table info
    pub fn new(
        left_schema: TableSchema,
        left_alias: String,
        right_schema: TableSchema,
        right_alias: String,
    ) -> Self {
        let mut columns = Vec::new();
        let mut column_map = HashMap::new();

        // Add left table columns
        for col in &left_schema.columns {
            // If the incoming column name is already qualified (e.g. "table.col"),
            // register mappings both for the original source table and for the
            // current left alias so subsequent joins can reference either.
            let col_name = col.name.clone();
            if let Some(pos) = col_name.find('.') {
                let src_table = col_name[..pos].to_string();
                let simple_name = col_name[pos + 1..].to_string();
                column_map.insert(
                    (left_alias.to_lowercase(), simple_name.to_lowercase()),
                    columns.len(),
                );
                column_map.insert(
                    (src_table.to_lowercase(), simple_name.to_lowercase()),
                    columns.len(),
                );

                columns.push(ColumnDef {
                    name: col_name.clone(),
                    data_type: col.data_type.clone(),
                    nullable: col.nullable,
                    default: col.default.clone(),
                    primary_key: col.primary_key,
                    auto_increment: false,
                });
            } else {
                column_map.insert(
                    (left_alias.to_lowercase(), col_name.to_lowercase()),
                    columns.len(),
                );
                columns.push(ColumnDef {
                    name: format!("{}.{}", left_alias, col_name),
                    data_type: col.data_type.clone(),
                    nullable: col.nullable,
                    default: col.default.clone(),
                    primary_key: col.primary_key,
                    auto_increment: false,
                });
            }
        }

        // Add right table columns
        for col in &right_schema.columns {
            let col_name = col.name.clone();
            if let Some(pos) = col_name.find('.') {
                // If the right schema already contains qualified names, register both
                // the source table qualifier and the provided right alias.
                let src_table = col_name[..pos].to_string();
                let simple_name = col_name[pos + 1..].to_string();
                column_map.insert(
                    (right_alias.to_lowercase(), simple_name.to_lowercase()),
                    columns.len(),
                );
                column_map.insert(
                    (src_table.to_lowercase(), simple_name.to_lowercase()),
                    columns.len(),
                );

                columns.push(ColumnDef {
                    name: col_name.clone(),
                    data_type: col.data_type.clone(),
                    nullable: true, // Right side is nullable in LEFT JOIN
                    default: col.default.clone(),
                    primary_key: false,
                    auto_increment: false,
                });
            } else {
                column_map.insert(
                    (right_alias.to_lowercase(), col_name.to_lowercase()),
                    columns.len(),
                );
                columns.push(ColumnDef {
                    name: format!("{}.{}", right_alias, col_name),
                    data_type: col.data_type.clone(),
                    nullable: true, // Right side is nullable in LEFT JOIN
                    default: col.default.clone(),
                    primary_key: false,
                    auto_increment: false,
                });
            }
        }

        let combined_schema = TableSchema {
            name: "joined".to_string(),
            columns,
            auto_increment_counter: 1,
            engine_type: crate::engines::EngineType::default(),
        };

        Self {
            left_schema,
            left_alias,
            right_schema,
            right_alias,
            combined_schema,
            column_map,
        }
    }

    /// Combine a left row with an optional right row into combined values
    pub fn combine_rows(&self, left: &Row, right: Option<&Row>) -> Vec<Value> {
        let mut values = left.values.clone();
        
        match right {
            Some(r) => values.extend(r.values.clone()),
            None => {
                // Fill with NULLs for LEFT JOIN unmatched rows
                for _ in &self.right_schema.columns {
                    values.push(Value::Null);
                }
            }
        }
        
        values
    }

    /// Find a column index by table alias and column name
    pub fn find_column(&self, table: Option<&str>, name: &str) -> Result<usize> {
        let name_lower = name.to_lowercase();
        
        if let Some(tbl) = table {
            // Qualified column reference
            let tbl_lower = tbl.to_lowercase();
            self.column_map
                .get(&(tbl_lower.clone(), name_lower.clone()))
                .copied()
                .ok_or_else(|| {
                    MiniSqlError::unknown_column_qualified(tbl, name, ColumnContext::OnClause)
                })
        } else {
            // Unqualified - search both tables, error if ambiguous
            let left_match = self.column_map.get(&(self.left_alias.to_lowercase(), name_lower.clone()));
            let right_match = self.column_map.get(&(self.right_alias.to_lowercase(), name_lower.clone()));
            
            match (left_match, right_match) {
                (Some(&idx), None) => Ok(idx),
                (None, Some(&idx)) => Ok(idx),
                (Some(_), Some(_)) => Err(MiniSqlError::ambiguous_column(name, ColumnContext::OnClause)),
                (None, None) => Err(MiniSqlError::unknown_column(name, ColumnContext::OnClause)),
            }
        }
    }
}

/// Strategy for executing joins
pub trait JoinStrategy {
    /// Execute a join between left and right rows
    fn execute(
        &self,
        left_rows: &[Row],
        right_rows: &[Row],
        context: &JoinContext,
        on_condition: &Expr,
        join_type: JoinType,
        eval_condition: &dyn Fn(&Expr, &[Value], &JoinContext) -> Result<bool>,
    ) -> Result<Vec<JoinedRow>>;
    
    /// Name of this join strategy (for debugging/logging)
    fn name(&self) -> &'static str;
}

/// Nested Loop Join - O(n×m), simple and always works
pub struct NestedLoopJoin;

impl JoinStrategy for NestedLoopJoin {
    fn execute(
        &self,
        left_rows: &[Row],
        right_rows: &[Row],
        context: &JoinContext,
        on_condition: &Expr,
        join_type: JoinType,
        eval_condition: &dyn Fn(&Expr, &[Value], &JoinContext) -> Result<bool>,
    ) -> Result<Vec<JoinedRow>> {
        let mut results = Vec::new();

        for left in left_rows {
            let mut matched = false;

            for right in right_rows {
                // Combine values and evaluate ON condition
                let combined = context.combine_rows(left, Some(right));
                
                if eval_condition(on_condition, &combined, context)? {
                    results.push(JoinedRow {
                        left: left.clone(),
                        right: Some(right.clone()),
                    });
                    matched = true;
                }
            }

            // LEFT JOIN: emit unmatched left rows with NULL right side
            if !matched && join_type == JoinType::Left {
                results.push(JoinedRow {
                    left: left.clone(),
                    right: None,
                });
            }
        }

        Ok(results)
    }
    
    fn name(&self) -> &'static str {
        "NestedLoopJoin"
    }
}

/// Wrapper for Value that implements Hash + Eq for use in HashMap
#[allow(dead_code)]
#[derive(Hash, PartialEq, Eq, Clone, Debug)]
struct ValueKey(String);

#[allow(dead_code)]
impl ValueKey {
    fn from_value(value: &Value) -> Self {
        // Use string representation for hashing
        ValueKey(match value {
            Value::Null => String::new(),
            Value::Integer(i) => i.to_string(),
            Value::Float(f) => f.to_string(),
            Value::String(s) => s.clone(),
            Value::Boolean(b) => b.to_string(),
            Value::Json(j) => j.to_string(),
        })
    }
}

/// Hash Join - O(n+m) average case, requires equi-join condition
pub struct HashJoin {
    /// Index of the join key column in the left table
    pub left_key_idx: usize,
    /// Index of the join key column in the right table
    pub right_key_idx: usize,
}

impl HashJoin {
    /// Try to create a hash join from an equi-join condition
    /// Returns None if the condition is not a simple equality
    pub fn try_from_condition(
        on_condition: &Expr,
        context: &JoinContext,
    ) -> Option<Self> {
        // Check if ON condition is a simple equality: col1 = col2
        if let Expr::BinaryOp { left, op: BinaryOperator::Equal, right } = on_condition {
            // Both sides must be column references
            if let (
                Expr::Column { table: left_table, name: left_name },
                Expr::Column { table: right_table, name: right_name },
            ) = (left.as_ref(), right.as_ref()) {
                // Try to resolve which column belongs to which table
                let left_result = Self::resolve_join_columns(
                    left_table.as_deref(),
                    left_name,
                    right_table.as_deref(),
                    right_name,
                    context,
                );
                
                if let Some((left_idx, right_idx)) = left_result {
                    return Some(HashJoin {
                        left_key_idx: left_idx,
                        right_key_idx: right_idx,
                    });
                }
            }
        }
        None
    }
    
    /// Resolve which column belongs to left table and which to right
    fn resolve_join_columns(
        table1: Option<&str>,
        name1: &str,
        table2: Option<&str>,
        name2: &str,
        context: &JoinContext,
    ) -> Option<(usize, usize)> {
        let left_alias = context.left_alias.to_lowercase();
        let right_alias = context.right_alias.to_lowercase();
        
        // Check if col1 is from left and col2 is from right
        let col1_is_left = table1.map(|t| t.to_lowercase() == left_alias).unwrap_or(false)
            || context.left_schema.find_column(name1).is_some();
        let col2_is_right = table2.map(|t| t.to_lowercase() == right_alias).unwrap_or(false)
            || context.right_schema.find_column(name2).is_some();
        
        if col1_is_left && col2_is_right {
            let left_idx = context.left_schema.find_column(name1)?;
            let right_idx = context.right_schema.find_column(name2)?;
            return Some((left_idx, right_idx));
        }
        
        // Check if col1 is from right and col2 is from left
        let col1_is_right = table1.map(|t| t.to_lowercase() == right_alias).unwrap_or(false)
            || context.right_schema.find_column(name1).is_some();
        let col2_is_left = table2.map(|t| t.to_lowercase() == left_alias).unwrap_or(false)
            || context.left_schema.find_column(name2).is_some();
        
        if col1_is_right && col2_is_left {
            let left_idx = context.left_schema.find_column(name2)?;
            let right_idx = context.right_schema.find_column(name1)?;
            return Some((left_idx, right_idx));
        }
        
        None
    }
}

impl JoinStrategy for HashJoin {
    fn execute(
        &self,
        left_rows: &[Row],
        right_rows: &[Row],
        _context: &JoinContext,
        _on_condition: &Expr,
        join_type: JoinType,
        _eval_condition: &dyn Fn(&Expr, &[Value], &JoinContext) -> Result<bool>,
    ) -> Result<Vec<JoinedRow>> {
        // Build hash table from right side - now using Value directly (no string conversion)
        let mut hash_table: HashMap<Value, Vec<usize>> = HashMap::new();
        for (idx, right) in right_rows.iter().enumerate() {
            let key = right.values[self.right_key_idx].clone();
            hash_table.entry(key).or_default().push(idx);
        }

        // Probe with left side
        let mut results = Vec::new();
        for left in left_rows {
            let key = &left.values[self.left_key_idx];
            let mut matched = false;

            if let Some(matching_indices) = hash_table.get(key) {
                for &right_idx in matching_indices {
                    results.push(JoinedRow {
                        left: left.clone(),
                        right: Some(right_rows[right_idx].clone()),
                    });
                    matched = true;
                }
            }

            if !matched && join_type == JoinType::Left {
                results.push(JoinedRow {
                    left: left.clone(),
                    right: None,
                });
            }
        }

        Ok(results)
    }
    
    fn name(&self) -> &'static str {
        "HashJoin"
    }
}

/// Select the best join strategy based on the query
pub fn select_join_strategy(
    on_condition: &Expr,
    context: &JoinContext,
) -> Box<dyn JoinStrategy> {
    // Try to use hash join for simple equi-joins
    if let Some(hash_join) = HashJoin::try_from_condition(on_condition, context) {
        return Box::new(hash_join);
    }
    
    // Fall back to nested loop for complex conditions
    Box::new(NestedLoopJoin)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::DataType;

    fn create_test_schema(name: &str, columns: Vec<(&str, DataType)>) -> TableSchema {
        TableSchema {
            name: name.to_string(),
            columns: columns
                .into_iter()
                .map(|(n, dt)| ColumnDef {
                    name: n.to_string(),
                    data_type: dt,
                    nullable: true,
                    default: None,
                    primary_key: false,
                    auto_increment: false,
                })
                .collect(),
            auto_increment_counter: 1,
            engine_type: crate::engines::EngineType::default(),
        }
    }

    fn create_test_row(id: u64, values: Vec<Value>) -> Row {
        Row { id, values }
    }

    #[test]
    fn test_join_context_creation() {
        let left_schema = create_test_schema("users", vec![
            ("id", DataType::Integer),
            ("name", DataType::Text),
        ]);
        let right_schema = create_test_schema("orders", vec![
            ("id", DataType::Integer),
            ("user_id", DataType::Integer),
            ("product", DataType::Text),
        ]);

        let context = JoinContext::new(
            left_schema,
            "users".to_string(),
            right_schema,
            "orders".to_string(),
        );

        // Check combined schema has all columns
        assert_eq!(context.combined_schema.columns.len(), 5);
        
        // Check column map
        assert_eq!(context.find_column(Some("users"), "id").unwrap(), 0);
        assert_eq!(context.find_column(Some("users"), "name").unwrap(), 1);
        assert_eq!(context.find_column(Some("orders"), "id").unwrap(), 2);
        assert_eq!(context.find_column(Some("orders"), "user_id").unwrap(), 3);
        assert_eq!(context.find_column(Some("orders"), "product").unwrap(), 4);
    }

    #[test]
    fn test_join_context_ambiguous_column() {
        let left_schema = create_test_schema("users", vec![
            ("id", DataType::Integer),
            ("name", DataType::Text),
        ]);
        let right_schema = create_test_schema("orders", vec![
            ("id", DataType::Integer),  // Same name as users.id
            ("product", DataType::Text),
        ]);

        let context = JoinContext::new(
            left_schema,
            "users".to_string(),
            right_schema,
            "orders".to_string(),
        );

        // Unqualified "id" should be ambiguous
        let result = context.find_column(None, "id");
        assert!(result.is_err());
        
        // Unqualified "name" should work (only in users)
        assert_eq!(context.find_column(None, "name").unwrap(), 1);
        
        // Unqualified "product" should work (only in orders)
        assert_eq!(context.find_column(None, "product").unwrap(), 3);
    }

    #[test]
    fn test_combine_rows_with_right() {
        let left_schema = create_test_schema("users", vec![
            ("id", DataType::Integer),
            ("name", DataType::Text),
        ]);
        let right_schema = create_test_schema("orders", vec![
            ("product", DataType::Text),
        ]);

        let context = JoinContext::new(
            left_schema,
            "u".to_string(),
            right_schema,
            "o".to_string(),
        );

        let left = create_test_row(1, vec![
            Value::Integer(1),
            Value::String("Alice".to_string()),
        ]);
        let right = create_test_row(1, vec![
            Value::String("Widget".to_string()),
        ]);

        let combined = context.combine_rows(&left, Some(&right));
        assert_eq!(combined.len(), 3);
        assert_eq!(combined[0], Value::Integer(1));
        assert_eq!(combined[1], Value::String("Alice".to_string()));
        assert_eq!(combined[2], Value::String("Widget".to_string()));
    }

    #[test]
    fn test_combine_rows_without_right() {
        let left_schema = create_test_schema("users", vec![
            ("id", DataType::Integer),
        ]);
        let right_schema = create_test_schema("orders", vec![
            ("product", DataType::Text),
            ("amount", DataType::Float),
        ]);

        let context = JoinContext::new(
            left_schema,
            "u".to_string(),
            right_schema,
            "o".to_string(),
        );

        let left = create_test_row(1, vec![Value::Integer(1)]);

        let combined = context.combine_rows(&left, None);
        assert_eq!(combined.len(), 3);
        assert_eq!(combined[0], Value::Integer(1));
        assert!(combined[1].is_null());
        assert!(combined[2].is_null());
    }

    #[test]
    fn test_nested_loop_join_inner() {
        let left_schema = create_test_schema("users", vec![
            ("id", DataType::Integer),
            ("name", DataType::Text),
        ]);
        let right_schema = create_test_schema("orders", vec![
            ("user_id", DataType::Integer),
            ("product", DataType::Text),
        ]);

        let context = JoinContext::new(
            left_schema,
            "users".to_string(),
            right_schema,
            "orders".to_string(),
        );

        let left_rows = vec![
            create_test_row(1, vec![Value::Integer(1), Value::String("Alice".to_string())]),
            create_test_row(2, vec![Value::Integer(2), Value::String("Bob".to_string())]),
        ];
        let right_rows = vec![
            create_test_row(1, vec![Value::Integer(1), Value::String("Widget".to_string())]),
            create_test_row(2, vec![Value::Integer(1), Value::String("Gadget".to_string())]),
            create_test_row(3, vec![Value::Integer(999), Value::String("Orphan".to_string())]),
        ];

        // ON condition: users.id = orders.user_id
        let on_condition = Expr::BinaryOp {
            left: Box::new(Expr::Column { table: Some("users".to_string()), name: "id".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expr::Column { table: Some("orders".to_string()), name: "user_id".to_string() }),
        };

        // Simple evaluator that checks equality of first columns
        let eval_fn = |_expr: &Expr, values: &[Value], _ctx: &JoinContext| -> Result<bool> {
            // values[0] is users.id, values[2] is orders.user_id
            Ok(values[0] == values[2])
        };

        let strategy = NestedLoopJoin;
        let results = strategy.execute(
            &left_rows,
            &right_rows,
            &context,
            &on_condition,
            JoinType::Inner,
            &eval_fn,
        ).unwrap();

        // Alice (id=1) matches Widget and Gadget (user_id=1)
        // Bob (id=2) matches nothing
        // Orphan (user_id=999) matches nothing
        assert_eq!(results.len(), 2);
        assert!(results.iter().all(|r| r.right.is_some()));
    }

    #[test]
    fn test_nested_loop_join_left() {
        let left_schema = create_test_schema("users", vec![
            ("id", DataType::Integer),
        ]);
        let right_schema = create_test_schema("orders", vec![
            ("user_id", DataType::Integer),
        ]);

        let context = JoinContext::new(
            left_schema,
            "users".to_string(),
            right_schema,
            "orders".to_string(),
        );

        let left_rows = vec![
            create_test_row(1, vec![Value::Integer(1)]),
            create_test_row(2, vec![Value::Integer(2)]),  // No matching order
        ];
        let right_rows = vec![
            create_test_row(1, vec![Value::Integer(1)]),
        ];

        let on_condition = Expr::BinaryOp {
            left: Box::new(Expr::Column { table: Some("users".to_string()), name: "id".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expr::Column { table: Some("orders".to_string()), name: "user_id".to_string() }),
        };

        let eval_fn = |_expr: &Expr, values: &[Value], _ctx: &JoinContext| -> Result<bool> {
            Ok(values[0] == values[1])
        };

        let strategy = NestedLoopJoin;
        let results = strategy.execute(
            &left_rows,
            &right_rows,
            &context,
            &on_condition,
            JoinType::Left,
            &eval_fn,
        ).unwrap();

        // User 1 matches order, User 2 has NULL for order
        assert_eq!(results.len(), 2);
        assert!(results[0].right.is_some());  // User 1 matched
        assert!(results[1].right.is_none());  // User 2 unmatched (LEFT JOIN NULL)
    }

    #[test]
    fn test_hash_join_detection() {
        let left_schema = create_test_schema("users", vec![
            ("id", DataType::Integer),
            ("name", DataType::Text),
        ]);
        let right_schema = create_test_schema("orders", vec![
            ("user_id", DataType::Integer),
            ("product", DataType::Text),
        ]);

        let context = JoinContext::new(
            left_schema,
            "users".to_string(),
            right_schema,
            "orders".to_string(),
        );

        // Simple equi-join should be detected
        let equi_join = Expr::BinaryOp {
            left: Box::new(Expr::Column { table: Some("users".to_string()), name: "id".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expr::Column { table: Some("orders".to_string()), name: "user_id".to_string() }),
        };

        let hash_join = HashJoin::try_from_condition(&equi_join, &context);
        assert!(hash_join.is_some());
        let hj = hash_join.unwrap();
        assert_eq!(hj.left_key_idx, 0);   // users.id is column 0
        assert_eq!(hj.right_key_idx, 0);  // orders.user_id is column 0

        // Non-equality should not be detected as hash join
        let non_equi = Expr::BinaryOp {
            left: Box::new(Expr::Column { table: Some("users".to_string()), name: "id".to_string() }),
            op: BinaryOperator::GreaterThan,
            right: Box::new(Expr::Column { table: Some("orders".to_string()), name: "user_id".to_string() }),
        };

        assert!(HashJoin::try_from_condition(&non_equi, &context).is_none());
    }

    #[test]
    fn test_hash_join_execution() {
        let left_schema = create_test_schema("users", vec![
            ("id", DataType::Integer),
            ("name", DataType::Text),
        ]);
        let right_schema = create_test_schema("orders", vec![
            ("user_id", DataType::Integer),
            ("product", DataType::Text),
        ]);

        let context = JoinContext::new(
            left_schema,
            "users".to_string(),
            right_schema,
            "orders".to_string(),
        );

        let left_rows = vec![
            create_test_row(1, vec![Value::Integer(1), Value::String("Alice".to_string())]),
            create_test_row(2, vec![Value::Integer(2), Value::String("Bob".to_string())]),
            create_test_row(3, vec![Value::Integer(3), Value::String("Charlie".to_string())]),
        ];
        let right_rows = vec![
            create_test_row(1, vec![Value::Integer(1), Value::String("Widget".to_string())]),
            create_test_row(2, vec![Value::Integer(1), Value::String("Gadget".to_string())]),
            create_test_row(3, vec![Value::Integer(2), Value::String("Gizmo".to_string())]),
        ];

        let on_condition = Expr::BinaryOp {
            left: Box::new(Expr::Column { table: Some("users".to_string()), name: "id".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expr::Column { table: Some("orders".to_string()), name: "user_id".to_string() }),
        };

        let strategy = HashJoin {
            left_key_idx: 0,  // users.id
            right_key_idx: 0, // orders.user_id
        };

        // Dummy eval function (not used by hash join)
        let eval_fn = |_: &Expr, _: &[Value], _: &JoinContext| -> Result<bool> { Ok(true) };

        let results = strategy.execute(
            &left_rows,
            &right_rows,
            &context,
            &on_condition,
            JoinType::Inner,
            &eval_fn,
        ).unwrap();

        // Alice (1) -> Widget, Gadget (2 matches)
        // Bob (2) -> Gizmo (1 match)
        // Charlie (3) -> nothing (0 matches)
        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_strategy_selection() {
        let left_schema = create_test_schema("t1", vec![("id", DataType::Integer)]);
        let right_schema = create_test_schema("t2", vec![("t1_id", DataType::Integer)]);
        let context = JoinContext::new(left_schema, "t1".to_string(), right_schema, "t2".to_string());

        // Equi-join should select HashJoin
        let equi = Expr::BinaryOp {
            left: Box::new(Expr::Column { table: Some("t1".to_string()), name: "id".to_string() }),
            op: BinaryOperator::Equal,
            right: Box::new(Expr::Column { table: Some("t2".to_string()), name: "t1_id".to_string() }),
        };
        let strategy = select_join_strategy(&equi, &context);
        assert_eq!(strategy.name(), "HashJoin");

        // Non-equi should select NestedLoopJoin
        let non_equi = Expr::BinaryOp {
            left: Box::new(Expr::Column { table: Some("t1".to_string()), name: "id".to_string() }),
            op: BinaryOperator::LessThan,
            right: Box::new(Expr::Column { table: Some("t2".to_string()), name: "t1_id".to_string() }),
        };
        let strategy = select_join_strategy(&non_equi, &context);
        assert_eq!(strategy.name(), "NestedLoopJoin");
    }
}
