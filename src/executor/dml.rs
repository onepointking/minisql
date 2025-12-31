use crate::error::{ColumnContext, MiniSqlError, Result};
use crate::parser::{DeleteStmt, InsertStmt, UpdateStmt};
use crate::types::{QueryResult, Value};
use crate::executor::{Executor, Session};
use crate::executor::evaluator;
use crate::executor::schema;
use std::time::Instant;

impl Executor {
    /// Execute INSERT
    pub(crate) fn execute_insert(&self, insert: InsertStmt, session: &mut Session) -> Result<QueryResult> {
        let table_name = &insert.table_name;
        let table_schema = self.storage.get_schema(table_name)?;
        let txn_id = self.get_txn_id(session);
        let mut rows_affected = 0;
        let mut last_insert_id = 0;
        
        let engine = self.get_engine(table_name)?;
        let auto_inc_col_idx = table_schema.auto_increment_column();

        for value_list in insert.values {
            // Resolve column order
            let mut values = if let Some(ref columns) = insert.columns {
                // Map provided columns to schema order
                let mut ordered_values = vec![Value::Null; table_schema.columns.len()];
                
                for (i, col_name) in columns.iter().enumerate() {
                    let idx = table_schema.find_column(col_name).ok_or_else(|| {
                        MiniSqlError::unknown_column(col_name, ColumnContext::InsertList)
                    })?;
                    if i < value_list.len() {
                        ordered_values[idx] = evaluator::eval_const_expr(&value_list[i], session.last_insert_id)?;
                    }
                }
                ordered_values
            } else {
                // Values in schema order
                if value_list.len() != table_schema.columns.len() {
                    return Err(MiniSqlError::column_count_mismatch(
                        table_schema.columns.len(),
                        value_list.len()
                    ));
                }
                value_list
                    .iter()
                    .map(|e| evaluator::eval_const_expr(e, session.last_insert_id))
                    .collect::<Result<Vec<_>>>()?
            };

            // Handle AUTO_INCREMENT
            if let Some(col_idx) = auto_inc_col_idx {
                let current_value = &values[col_idx];
                
                if current_value.is_null() || matches!(current_value, Value::Integer(0)) {
                    // Generate auto-increment value
                    let auto_val = self.storage.next_auto_increment(table_name)?;
                    values[col_idx] = Value::Integer(auto_val);

                    // MySQL protocol: last_insert_id is the FIRST generated ID in a multi-row insert
                    if last_insert_id == 0 {
                        last_insert_id = auto_val as u64;
                    }
                } else if let Value::Integer(explicit_val) = current_value {
                    // Explicit value provided - update counter if needed
                    self.storage.update_auto_increment_if_needed(table_name, *explicit_val)?;
                }
            }

            // Validate and coerce types
            let coerced_values = schema::coerce_row_types(&values, &table_schema)?;

            // Delegate insert to engine
            let _row_id = engine.insert(txn_id, table_name, coerced_values)?;
            
            // If we didn't generate an ID but the engine did (e.g. internal row ID), and we have an auto-inc col,
            // we might want to capture it, but currently MINISQL uses storage-generated IDs for auto-inc.
            // The row_id returned is the internal storage ID (often same as PK if integer).
            if last_insert_id == 0 && auto_inc_col_idx.is_some() {
                 // If the engine returned an ID and we considered it auto-generated (or at least relevant)
                 // For now, trust the logic above for auto-inc.
            }

            rows_affected += 1;
        }

        // Track which engine was modified (for transaction commit routing)
        if session.txn_id.is_some() {
            session.modified_engines.insert(table_schema.engine_type);
        }

        // Save auto-increment counter changes
        self.storage.save_catalog()?;

        // Auto-commit if not in transaction
        if session.txn_id.is_none() {
            engine.flush(table_name)?;
        }

        if last_insert_id > 0 {
            session.last_insert_id = last_insert_id;
        }

        Ok(QueryResult::Modified { 
            rows_affected,
            last_insert_id,
        })
    }

    /// Execute UPDATE
    pub(crate) fn execute_update(&self, update: UpdateStmt, session: &mut Session) -> Result<QueryResult> {
        let table_name = &update.table_name;
        let table_schema = self.storage.get_schema(table_name)?;
        
        // Optionally profile update phases
        let profiling = std::env::var("MINISQL_PROFILE").map(|v| v != "0").unwrap_or(false);
        let scan_t0 = if profiling { Some(Instant::now()) } else { None };
        
        let engine = self.get_engine(table_name)?;
        let rows = engine.scan(table_name)?;
        
        let scan_elapsed = scan_t0.map(|t| t.elapsed());
        let txn_id = self.get_txn_id(session);
        let mut rows_affected = 0;

        // Track per-row update time
        let mut per_row_total = std::time::Duration::default();

        for row in rows {
            let row_t0 = if profiling { Some(Instant::now()) } else { None };
            // Apply WHERE clause
            if let Some(ref where_expr) = update.where_clause {
                let matches = evaluator::eval_where(where_expr, &row, &table_schema, session.last_insert_id)?;
                if !matches {
                    continue;
                }
            }

            // Apply updates
            let mut new_values = row.values.clone();
            for (col_name, value_expr) in &update.assignments {
                let idx = table_schema.find_column(col_name).ok_or_else(|| {
                    MiniSqlError::unknown_column(col_name, ColumnContext::UpdateClause)
                })?;
                new_values[idx] = evaluator::eval_expr(value_expr, &row, &table_schema, session.last_insert_id)?;
            }

            // Delegate update to engine
            if engine.update(txn_id, table_name, row.id, &row.values, new_values)? {
                rows_affected += 1;
            }
            
            if let Some(t0) = row_t0 {
                per_row_total += t0.elapsed();
            }
        }

        // Track which engine was modified (for transaction commit routing)
        if session.txn_id.is_some() && rows_affected > 0 {
            session.modified_engines.insert(table_schema.engine_type);
        }

        // Auto-commit if not in transaction
        let save_t0 = if profiling && session.txn_id.is_none() { Some(Instant::now()) } else { None };
        if session.txn_id.is_none() {
            engine.flush(table_name)?;
        }
        let save_elapsed = save_t0.map(|t| t.elapsed());

        if profiling {
            let scan_ms = scan_elapsed.map(|d| d.as_millis()).unwrap_or(0);
            let per_row_ms = per_row_total.as_millis();
            let save_ms = save_elapsed.map(|d| d.as_millis()).unwrap_or(0);
            log::info!("UPDATE profile: table='{}' rows_affected={} scan_ms={} per_row_ms={} save_ms={}",
                update.table_name, rows_affected, scan_ms, per_row_ms, save_ms);
        }

        Ok(QueryResult::Modified { 
            rows_affected,
            last_insert_id: 0,
        })
    }

    /// Execute DELETE
    pub(crate) fn execute_delete(&self, delete: DeleteStmt, session: &mut Session) -> Result<QueryResult> {
        let table_name = &delete.table_name;
        let table_schema = self.storage.get_schema(table_name)?;
        
        let engine = self.get_engine(table_name)?;
        let rows = engine.scan(table_name)?;
        
        let txn_id = self.get_txn_id(session);
        let mut rows_affected = 0;

        // Collect rows to delete first (to avoid modifying while iterating)
        let mut to_delete = Vec::new();
        for row in rows {
            // Apply WHERE clause
            if let Some(ref where_expr) = delete.where_clause {
                let matches = evaluator::eval_where(where_expr, &row, &table_schema, session.last_insert_id)?;
                if !matches {
                    continue;
                }
            }
            to_delete.push(row);
        }

        // Delete rows using engine
        for row in to_delete {
             if engine.delete(txn_id, table_name, row.id, &row.values)? {
                 rows_affected += 1;
             }
        }

        // Track which engine was modified (for transaction commit routing)
        if session.txn_id.is_some() && rows_affected > 0 {
            session.modified_engines.insert(table_schema.engine_type);
        }

        // Auto-commit if not in transaction
        if session.txn_id.is_none() {
            engine.flush(table_name)?;
        }

        Ok(QueryResult::Modified { 
            rows_affected,
            last_insert_id: 0,
        })
    }
}
