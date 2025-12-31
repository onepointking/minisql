use crate::error::Result;
use crate::join::{JoinContext, select_join_strategy};
use crate::parser::{Expr, SelectColumn, SelectStmt};
use crate::types::{DataType, QueryResult, ResultSet, Row, TableSchema, Value};
use crate::executor::{Executor, Session};
use crate::executor::evaluator;
use crate::executor::schema::{self, JoinTableInfo};
use crate::executor::aggregation::{self, is_aggregate_expr};
use crate::error::MiniSqlError;
use std::collections::HashMap;
use std::time::Instant;

impl Executor {
     /// Execute SELECT
    pub(crate) fn execute_select(&self, select: SelectStmt, session: &Session) -> Result<QueryResult> {
        // Check if this is a simple query (no joins) or a join query
        if select.joins.is_empty() {
            self.execute_simple_select(select, session)
        } else {
            self.execute_join_select(select, session)
        }
    }

    /// Execute a simple SELECT (no joins)
    fn execute_simple_select(&self, select: SelectStmt, session: &Session) -> Result<QueryResult> {
        // Check if this is an aggregate query
        let has_aggregates = select.columns.iter().any(|col| {
            matches!(col, SelectColumn::Expr { expr, .. } if is_aggregate_expr(expr))
        });
        let has_group_by = !select.group_by.is_empty();

        if has_aggregates || has_group_by {
            return self.execute_aggregate_select(select, session);
        }

        let (schema, table_alias, rows) = if let Some(ref from) = select.from {
            let schema = self.storage.get_schema(&from.name)?;
            let table_alias = from.effective_name().to_string();
            let table_name = &from.name;

            // Try to use index for WHERE clauses (single or composite)
            // When the environment variable MINISQL_DEBUG_INDEX=1, emit diagnostic information
            let rows = if let Some(ref where_expr) = select.where_clause {
                let debug = std::env::var("MINISQL_DEBUG_INDEX").map(|v| v != "0").unwrap_or(false);
                // First try to extract multi-column equalities for composite index lookup
                let equalities = Self::extract_equality_conjuncts(where_expr, &schema, &table_alias);
                
                if !equalities.is_empty() && self.engine_supports_indexes(table_name).unwrap_or(false) {
                    // Check if we have an index that matches these columns
                    let query_columns: Vec<String> = equalities.iter().map(|(c, _)| c.clone()).collect();
                    
                    if let Some(index_meta) = self.storage.find_index_for_columns(table_name, &query_columns) {
                        if debug {
                            eprintln!("[IDX DEBUG] Table='{}' where_expr='{:?}' equalities={:?} -> candidate_index='{}' columns={:?}",
                                      table_name, where_expr, equalities, index_meta.name, index_meta.columns);
                        }
                        // Get values in the order of the index columns
                        let values: Vec<Value> = index_meta.columns.iter()
                            .filter_map(|col| {
                                equalities.iter()
                                    .find(|(c, _)| c.eq_ignore_ascii_case(col))
                                    .map(|(_, v)| v.clone())
                            })
                            .collect();
                        
                        // Only use index if we have values for all index columns used in query
                        if values.len() == index_meta.columns.len().min(query_columns.len()) {
                            // Use composite index scan
                            let columns: Vec<String> = index_meta.columns[..values.len()].to_vec();
                            if debug {
                                let t0 = Instant::now();
                                let result = self.storage.get_rows_by_composite_index(table_name, &columns, &values)?;
                                let elapsed = t0.elapsed();
                                eprintln!("[IDX DEBUG] Index scan used: index='{}' columns={:?} values={:?} rows_returned={} time_ms={}",
                                          index_meta.name, columns, values, result.len(), elapsed.as_millis());
                                result
                            } else {
                                self.storage.get_rows_by_composite_index(table_name, &columns, &values)?
                            }
                        } else {
                            // Fall back to full scan
                            if debug {
                                eprintln!("[IDX DEBUG] Index candidate '{}' did not have all required values (have {} expected prefix {}) - falling back to full scan",
                                          index_meta.name, values.len(), index_meta.columns.len().min(query_columns.len()));
                                let t0 = Instant::now();
                                let result = self.scan_table(table_name)?;
                                let elapsed = t0.elapsed();
                                eprintln!("[IDX DEBUG] Full scan rows_returned={} time_ms={}", result.len(), elapsed.as_millis());
                                result
                            } else {
                                self.scan_table(table_name)?
                            }
                        }
                    } else if equalities.len() == 1 {
                        // Single column equality without matching index - try simple lookup
                        let (col_name, value) = &equalities[0];
                        if self.storage.has_index_for_columns(table_name, col_name) {
                            if debug {
                                let t0 = Instant::now();
                                let result = self.storage.get_rows_by_index(table_name, col_name, value)?;
                                let elapsed = t0.elapsed();
                                eprintln!("[IDX DEBUG] Single-column index used: table='{}' col='{}' val='{:?}' rows={} time_ms={}",
                                          table_name, col_name, value, result.len(), elapsed.as_millis());
                                result
                            } else {
                                self.storage.get_rows_by_index(table_name, col_name, value)?
                            }
                        } else {
                            if debug {
                                eprintln!("[IDX DEBUG] No index for single-column equality on '{}.{}' -> full scan", table_name, col_name);
                                let t0 = Instant::now();
                                let result = self.scan_table(table_name)?;
                                let elapsed = t0.elapsed();
                                eprintln!("[IDX DEBUG] Full scan rows_returned={} time_ms={}", result.len(), elapsed.as_millis());
                                result
                            } else {
                                self.scan_table(table_name)?
                            }
                        }
                    } else {
                        // No matching index, fall back to full scan
                        if debug {
                            eprintln!("[IDX DEBUG] No matching index for equalities {:?} on table '{}' -> full scan", equalities, table_name);
                            let t0 = Instant::now();
                            let result = self.scan_table(table_name)?;
                            let elapsed = t0.elapsed();
                            eprintln!("[IDX DEBUG] Full scan rows_returned={} time_ms={}", result.len(), elapsed.as_millis());
                            result
                        } else {
                            self.scan_table(table_name)?
                        }
                    }
                } else {
                    // Complex WHERE clause or Sandstone table, use full scan
                    if debug {
                        eprintln!("[IDX DEBUG] Complex WHERE clause {}, using full scan", table_name);
                        let t0 = Instant::now();
                        let result = self.scan_table(table_name)?;
                        let elapsed = t0.elapsed();
                        eprintln!("[IDX DEBUG] Full scan rows_returned={} time_ms={}", result.len(), elapsed.as_millis());
                        result
                    } else {
                        self.scan_table(table_name)?
                    }
                }
            } else {
                // No WHERE clause, full scan required
                let debug = std::env::var("MINISQL_DEBUG_INDEX").map(|v| v != "0").unwrap_or(false);
                if debug {
                    eprintln!("[IDX DEBUG] No WHERE clause on table '{}' -> full scan", table_name);
                    let t0 = Instant::now();
                    let result = self.scan_table(table_name)?;
                    let elapsed = t0.elapsed();
                    eprintln!("[IDX DEBUG] Full scan rows_returned={} time_ms={}", result.len(), elapsed.as_millis());
                    result
                } else {
                    self.scan_table(table_name)?
                }
            };
            (schema, table_alias, rows)
        } else {
            // FROM-less SELECT: use a dummy schema and a single empty row
            (TableSchema { name: "dual".to_string(), columns: Vec::new(), auto_increment_counter: 1, engine_type: crate::engines::EngineType::default() }, "dual".to_string(), vec![Row::new(0, Vec::new())])
        };

        // Build result set columns
        let (result_columns, result_types) = schema::resolve_select_columns_simple(&select.columns, &schema, &table_alias)?;

        let mut result = ResultSet::new(result_columns.clone(), result_types.clone());

        // Filter and project rows (still needed for non-indexed or complex conditions)
        let mut filtered_rows = Vec::new();
        for row in rows {
            // Apply WHERE clause
            if let Some(ref where_expr) = select.where_clause {
                let matches = evaluator::eval_where_simple(where_expr, &row, &schema, &table_alias, session.last_insert_id)?;
                if !matches {
                    continue;
                }
            }
            
            filtered_rows.push(row);
        }

        // Apply ORDER BY sorting
        if !select.order_by.is_empty() {
            filtered_rows.sort_by(|a, b| {
                for order_clause in &select.order_by {
                    let val_a = evaluator::eval_expr_simple(&order_clause.expr, a, &schema, &table_alias, session.last_insert_id).unwrap_or(Value::Null);
                    let val_b = evaluator::eval_expr_simple(&order_clause.expr, b, &schema, &table_alias, session.last_insert_id).unwrap_or(Value::Null);
                    
                    let cmp = match val_a.partial_cmp(&val_b) {
                        Some(ord) => ord,
                        None => std::cmp::Ordering::Equal,
                    };
                    
                    if cmp != std::cmp::Ordering::Equal {
                        return match order_clause.direction {
                            crate::parser::SortOrder::Asc => cmp,
                            crate::parser::SortOrder::Desc => cmp.reverse(),
                        };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // Apply LIMIT and project
        let mut count = 0;
        for row in filtered_rows {
            if let Some(limit) = select.limit {
                if count >= limit {
                    break;
                }
            }

            // Project columns
            let result_row = self.project_row_simple(&select.columns, &row, &schema, &table_alias, session)?;
            result.add_row(result_row);
            count += 1;
        }

        Ok(QueryResult::Select(result))
    }

    /// Execute an aggregate SELECT (with GROUP BY or aggregate functions)
    fn execute_aggregate_select(&self, select: SelectStmt, session: &Session) -> Result<QueryResult> {
        let (schema, table_alias, rows) = if let Some(ref from) = select.from {
            let schema = self.storage.get_schema(&from.name)?;
            let rows = self.scan_table(&from.name)?;
            let table_alias = from.effective_name().to_string();
            (schema, table_alias, rows)
        } else {
            (TableSchema { name: "dual".to_string(), columns: Vec::new(), auto_increment_counter: 1, engine_type: crate::engines::EngineType::default() }, "dual".to_string(), vec![Row::new(0, Vec::new())])
        };

        // Filter rows with WHERE clause first
        let filtered_rows: Vec<Row> = rows
            .into_iter()
            .filter(|row| {
                if let Some(ref where_expr) = select.where_clause {
                    evaluator::eval_where_simple(where_expr, row, &schema, &table_alias, session.last_insert_id)
                        .unwrap_or(false)
                } else {
                    true
                }
            })
            .collect();

        // Group rows by GROUP BY expressions
        // Key is a string representation of the group values
        let mut groups: HashMap<String, Vec<Row>> = HashMap::new();

        for row in filtered_rows {
            let key = if select.group_by.is_empty() {
                // No GROUP BY: all rows in one group
                String::new()
            } else {
                // Build group key from GROUP BY expressions
                select
                    .group_by
                    .iter()
                    .map(|expr| {
                        evaluator::eval_expr_simple(expr, &row, &schema, &table_alias, session.last_insert_id)
                            .map(|v| format!("{:?}", v))
                            .unwrap_or_default()
                    })
                    .collect::<Vec<_>>()
                    .join("|")
            };
            groups.entry(key).or_default().push(row);
        }

        // Build result columns for aggregate query
        let (result_columns, result_types) = 
            self.resolve_aggregate_columns(&select.columns, &schema)?;
        let mut result = ResultSet::new(result_columns, result_types);

        // Process each group and compute aggregates
        for (_key, group_rows) in groups {
            if group_rows.is_empty() {
                continue;
            }

            let mut result_row = Vec::new();

            for col in &select.columns {
                match col {
                    SelectColumn::Expr { expr, .. } => {
                        let value = self.evaluate_aggregate_expr(
                            expr,
                            &group_rows,
                            &schema,
                            &table_alias,
                            session,
                        )?;
                        result_row.push(value);
                    }
                    SelectColumn::Star => {
                        return Err(MiniSqlError::Syntax(
                            "SELECT * cannot be used with GROUP BY".into(),
                        ));
                    }
                    SelectColumn::QualifiedStar { .. } => {
                        return Err(MiniSqlError::Syntax(
                            "Qualified * cannot be used with GROUP BY".into(),
                        ));
                    }
                }
            }

            result.add_row(result_row);
        }

        Ok(QueryResult::Select(result))
    }

    /// Evaluate an expression that may contain aggregate functions
    fn evaluate_aggregate_expr(
        &self,
        expr: &Expr,
        rows: &[Row],
        schema: &TableSchema,
        table_alias: &str,
        session: &Session,
    ) -> Result<Value> {
        match expr {
            Expr::FunctionCall { name, args } if aggregation::is_aggregate_function(name) => {
                let mut acc = aggregation::create_accumulator(name, args)?;

                for row in rows {
                    let value = if args.is_empty() {
                        // COUNT(*) - just count, use a dummy value
                        Value::Integer(1)
                    } else {
                        // Evaluate the argument expression for this row
                        evaluator::eval_expr_simple(&args[0], row, schema, table_alias, session.last_insert_id)?
                    };
                    acc.accumulate(&value)?;
                }

                Ok(acc.finalize())
            }
            // For non-aggregate expressions in GROUP BY context,
            // use the first row's value (should be same for all rows in group)
            _ => {
                if rows.is_empty() {
                    Ok(Value::Null)
                } else {
                    evaluator::eval_expr_simple(expr, &rows[0], schema, table_alias, session.last_insert_id)
                }
            }
        }
    }

    /// Resolve column names and types for aggregate queries
    fn resolve_aggregate_columns(
        &self,
        columns: &[SelectColumn],
        schema: &TableSchema,
    ) -> Result<(Vec<String>, Vec<DataType>)> {
        let mut names = Vec::new();
        let mut types = Vec::new();

        for col in columns {
            match col {
                SelectColumn::Expr { expr, alias } => {
                    let name = if let Some(a) = alias {
                        a.clone()
                    } else {
                        self.expr_to_column_name(expr)
                    };
                    let dtype = self.infer_aggregate_expr_type(expr, schema)?;
                    names.push(name);
                    types.push(dtype);
                }
                _ => {
                    return Err(MiniSqlError::Syntax(
                        "Only expressions allowed in aggregate SELECT".into(),
                    ));
                }
            }
        }

        Ok((names, types))
    }

    /// Generate a column name from an expression
    fn expr_to_column_name(&self, expr: &Expr) -> String {
        match expr {
            Expr::Column { table, name } => {
                if let Some(t) = table {
                    format!("{}.{}", t, name)
                } else {
                    name.clone()
                }
            }
            Expr::FunctionCall { name, args } => {
                if args.is_empty() {
                    format!("{}(*)", name)
                } else {
                    format!("{}(...)", name)
                }
            }
            Expr::Literal(v) => format!("{}", v),
            _ => "?".to_string(),
        }
    }

    /// Infer the type of an aggregate expression
    fn infer_aggregate_expr_type(&self, expr: &Expr, schema: &TableSchema) -> Result<DataType> {
        match expr {
            Expr::FunctionCall { name, args } => {
                match name.to_uppercase().as_str() {
                    "COUNT" => Ok(DataType::Integer),
                    "SUM" => {
                        if args.is_empty() {
                            Ok(DataType::Integer)
                        } else {
                            evaluator::infer_expr_type_simple(&args[0], schema)
                        }
                    }
                    "AVG" => Ok(DataType::Float),
                    "MIN" | "MAX" => {
                        if args.is_empty() {
                            Ok(DataType::Integer)
                        } else {
                            evaluator::infer_expr_type_simple(&args[0], schema)
                        }
                    }
                    _ => evaluator::infer_expr_type_simple(expr, schema),
                }
            }
            _ => evaluator::infer_expr_type_simple(expr, schema),
        }
    }

    /// Execute a SELECT with JOINs
    fn execute_join_select(&self, select: SelectStmt, session: &Session) -> Result<QueryResult> {
        // For simplicity, we'll handle the case of exactly one join
        // (the pattern can be extended for multiple joins)
        
        // Get the left table (FROM clause)
        let from = select.from.as_ref().ok_or_else(|| MiniSqlError::Syntax("JOIN requires a FROM clause".into()))?;
        let left_schema = self.storage.get_schema(&from.name)?;
        let left_rows = self.scan_table(&from.name)?;
        let left_alias = from.effective_name().to_string();

        // Start with left table as the "current" result
        let mut current_schema = left_schema.clone();
        let mut current_alias = left_alias.clone();
        let mut current_rows = left_rows;

        // Process each join sequentially
        for join_clause in &select.joins {
            let right_schema = self.storage.get_schema(&join_clause.table.name)?;
            let right_rows = self.scan_table(&join_clause.table.name)?;
            let right_alias = join_clause.table.effective_name().to_string();

            // Create join context
            let context = JoinContext::new(
                current_schema.clone(),
                current_alias.clone(),
                right_schema.clone(),
                right_alias.clone(),
            );

            // Select and execute join strategy
            let strategy = select_join_strategy(&join_clause.on_condition, &context);
            
            // Create evaluator closure for the join
            let eval_fn = |expr: &Expr, values: &[Value], ctx: &JoinContext| -> Result<bool> {
                evaluator::eval_join_condition(expr, values, ctx, session.last_insert_id)
            };

            let joined_rows = strategy.execute(
                &current_rows,
                &right_rows,
                &context,
                &join_clause.on_condition,
                join_clause.join_type,
                &eval_fn,
            )?;

            // Build combined rows for the next iteration
            current_rows = joined_rows
                .into_iter()
                .enumerate()
                .map(|(idx, jr)| {
                    let values = context.combine_rows(&jr.left, jr.right.as_ref());
                    Row::new(idx as u64, values)
                })
                .collect();

            // Note: Join logic updates schema for next iteration
            // We need to re-fetch/create combined schema logic if `JoinContext::combined_schema` is used
            current_schema = context.combined_schema.clone();
            current_alias = "joined".to_string();
        }

        // Now we have the joined rows; apply WHERE, ORDER BY, LIMIT, and projection
        
        // Build the final join context for column resolution (use last join's combined schema)
        // We need to rebuild context info for column resolution
        let all_tables = self.collect_table_schemas(&select)?;
        
        // Build result set columns
        let (result_columns, result_types) = schema::resolve_select_columns_join(&select.columns, &all_tables)?;
        let mut result = ResultSet::new(result_columns.clone(), result_types.clone());

        // Filter with WHERE
        let mut filtered_rows = Vec::new();
        for row in current_rows {
            if let Some(ref where_expr) = select.where_clause {
                let val = evaluator::eval_expr_join(where_expr, &row, &all_tables, session.last_insert_id)?;
                if !val.is_truthy() {
                    continue;
                }
            }
            filtered_rows.push(row);
        }

        // Apply ORDER BY sorting
        if !select.order_by.is_empty() {
            // We need to clone tables for the closure
            let tables_clone = all_tables.clone();
            filtered_rows.sort_by(|a, b| {
                for order_clause in &select.order_by {
                    let val_a = evaluator::eval_expr_join(&order_clause.expr, a, &tables_clone, session.last_insert_id).unwrap_or(Value::Null);
                    let val_b = evaluator::eval_expr_join(&order_clause.expr, b, &tables_clone, session.last_insert_id).unwrap_or(Value::Null);
                    
                    let cmp = match val_a.partial_cmp(&val_b) {
                        Some(ord) => ord,
                        None => std::cmp::Ordering::Equal,
                    };
                    
                    if cmp != std::cmp::Ordering::Equal {
                        return match order_clause.direction {
                            crate::parser::SortOrder::Asc => cmp,
                            crate::parser::SortOrder::Desc => cmp.reverse(),
                        };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // Apply LIMIT and project
        let mut count = 0;
        for row in filtered_rows {
            if let Some(limit) = select.limit {
                if count >= limit {
                    break;
                }
            }

            let result_row = self.project_row_join(&select.columns, &row, &all_tables, session)?;
            result.add_row(result_row);
            count += 1;
        }

        Ok(QueryResult::Select(result))
    }

    /// Collect schemas and build column mapping for all tables in a join query
    fn collect_table_schemas(&self, select: &SelectStmt) -> Result<JoinTableInfo> {
        let mut info = JoinTableInfo::new();
        
        // Add the FROM table
        let from = select.from.as_ref().ok_or_else(|| MiniSqlError::Syntax("SELECT requires a FROM clause for this operation".into()))?;
        let from_schema = self.storage.get_schema(&from.name)?;
        let from_alias = from.effective_name().to_string();
        info.add_table(&from_alias, from_schema)?;
        
        // Add each joined table
        for join in &select.joins {
            let join_schema = self.storage.get_schema(&join.table.name)?;
            let join_alias = join.table.effective_name().to_string();
            info.add_table(&join_alias, join_schema)?;
        }
        
        Ok(info)
    }

    /// Execute SHOW TABLES
    pub(crate) fn execute_show_tables(&self) -> Result<QueryResult> {
        let tables = self.storage.list_tables();
        let mut result = ResultSet::new(
            vec!["Tables".to_string()],
            vec![DataType::Text],
        );
        for table in tables {
            result.add_row(vec![Value::String(table)]);
        }
        Ok(QueryResult::Select(result))
    }

    /// Execute DESCRIBE table
    pub(crate) fn execute_describe(&self, table_name: &str) -> Result<QueryResult> {
        let schema = self.storage.get_schema(table_name)?;
        let mut result = ResultSet::new(
            vec![
                "Field".to_string(),
                "Type".to_string(),
                "Null".to_string(),
                "Key".to_string(),
            ],
            vec![DataType::Text, DataType::Text, DataType::Text, DataType::Text],
        );

        for col in &schema.columns {
            result.add_row(vec![
                Value::String(col.name.clone()),
                Value::String(col.data_type.to_string()),
                Value::String(if col.nullable { "YES" } else { "NO" }.to_string()),
                Value::String(if col.primary_key { "PRI" } else { "" }.to_string()),
            ]);
        }

        Ok(QueryResult::Select(result))
    }


    /// Project row for simple query
    fn project_row_simple(
        &self,
        columns: &[SelectColumn],
        row: &Row,
        schema: &TableSchema,
        table_alias: &str,
        session: &Session,
    ) -> Result<Vec<Value>> {
        let mut result = Vec::new();

        for col in columns {
            match col {
                SelectColumn::Star => {
                    result.extend(row.values.clone());
                }
                SelectColumn::QualifiedStar { table: _ } => {
                     // For simple queries, simple projection implies single table so qualified star is same as star
                    result.extend(row.values.clone());
                }
                SelectColumn::Expr { expr, .. } => {
                    let value = evaluator::eval_expr_simple(expr, row, schema, table_alias, session.last_insert_id)?;
                    result.push(value);
                }
            }
        }

        Ok(result)
    }

    /// Project row for join query
    fn project_row_join(
        &self,
        columns: &[SelectColumn],
        row: &Row,
        tables: &JoinTableInfo,
        session: &Session,
    ) -> Result<Vec<Value>> {
        let mut result = Vec::new();

        for col in columns {
            match col {
                SelectColumn::Star => {
                    // For star in join, return all columns (already combined)
                    result.extend(row.values.clone());
                }
                SelectColumn::QualifiedStar { table } => {
                    // Return columns from specific table
                    let start = tables.get_table_offset(table)?;
                    let schema = tables.schemas.get(&table.to_lowercase()).ok_or_else(|| {
                        MiniSqlError::unknown_table_in_field_list(table)
                    })?;
                    let end = start + schema.columns.len();
                    result.extend(row.values[start..end].to_vec());
                }
                SelectColumn::Expr { expr, .. } => {
                    let value = evaluator::eval_expr_join(expr, row, tables, session.last_insert_id)?;
                    result.push(value);
                }
            }
        }

        Ok(result)
    }

    /// Extract a simple equality condition (column = literal) for index lookup
    /// Returns Some((column_name, value)) if the expression is a simple equality
    // extract_simple_equality was removed - it was unused and produced
    // dead_code warnings. Keep index-related extraction logic in
    // `extract_equality_conjuncts` which is actively used.
    
    /// Extract all equality conjuncts from a WHERE clause for composite index lookup
    /// Returns a list of (column_name, value) pairs for all equality conditions
    /// connected by AND. The order of columns matches the order they appear in the expression.
    fn extract_equality_conjuncts(
        expr: &Expr,
        schema: &TableSchema,
        table_alias: &str,
    ) -> Vec<(String, Value)> {
        // BinaryOperator import not needed here
        let mut result = Vec::new();
        Self::collect_equality_conjuncts(expr, schema, table_alias, &mut result);
        result
    }
    
    /// Helper to recursively collect equality conjuncts connected by AND
    fn collect_equality_conjuncts(
        expr: &Expr,
        schema: &TableSchema,
        table_alias: &str,
        result: &mut Vec<(String, Value)>,
    ) {
        use crate::parser::BinaryOperator;
        
        match expr {
            // Handle AND: recursively collect from both sides
            Expr::BinaryOp { left, op: BinaryOperator::And, right } => {
                Self::collect_equality_conjuncts(left, schema, table_alias, result);
                Self::collect_equality_conjuncts(right, schema, table_alias, result);
            }
            
            // Handle equality: extract column = literal
            Expr::BinaryOp { left, op: BinaryOperator::Equal, right } => {
                // Try column = literal
                if let (Expr::Column { table, name }, Expr::Literal(value)) = (left.as_ref(), right.as_ref()) {
                    if Self::column_matches_table(table.as_ref(), table_alias, &schema.name) {
                        if schema.find_column(name).is_some() {
                            // Only add if not already present (avoid duplicates)
                            if !result.iter().any(|(c, _)| c.eq_ignore_ascii_case(name)) {
                                result.push((name.clone(), value.clone()));
                            }
                        }
                    }
                }
                
                // Try literal = column (reversed)
                if let (Expr::Literal(value), Expr::Column { table, name }) = (left.as_ref(), right.as_ref()) {
                    if Self::column_matches_table(table.as_ref(), table_alias, &schema.name) {
                        if schema.find_column(name).is_some() {
                            if !result.iter().any(|(c, _)| c.eq_ignore_ascii_case(name)) {
                                result.push((name.clone(), value.clone()));
                            }
                        }
                    }
                }
            }
            
            // Other expressions are not simple conjuncts
            _ => {}
        }
    }
    
    /// Check if a column's table qualifier matches the expected table
    fn column_matches_table(table_qualifier: Option<&String>, table_alias: &str, table_name: &str) -> bool {
        match table_qualifier {
            Some(tbl) => tbl.eq_ignore_ascii_case(table_alias) || tbl.eq_ignore_ascii_case(table_name),
            None => true, // No qualifier means it could match any table
        }
    }
}
