use std::collections::HashMap;
use crate::error::{ColumnContext, MiniSqlError, Result};
use crate::parser::SelectColumn;
use crate::types::{DataType, TableSchema, Value};
use crate::executor::evaluator;

/// Information about tables in a join query
#[derive(Clone)]
pub struct JoinTableInfo {
    /// Table alias -> schema mapping (in order of appearance)
    pub schemas: HashMap<String, TableSchema>,
    /// Column map: (table_alias, column_name) -> index in combined row
    pub column_map: HashMap<(String, String), usize>,
    /// Table alias -> starting offset in combined row
    pub table_offsets: HashMap<String, usize>,
    /// Order of tables (for iteration)
    pub table_order: Vec<String>,
}

impl JoinTableInfo {
    pub fn new() -> Self {
        Self {
            schemas: HashMap::new(),
            column_map: HashMap::new(),
            table_offsets: HashMap::new(),
            table_order: Vec::new(),
        }
    }

    pub fn add_table(&mut self, alias: &str, schema: TableSchema) -> Result<()> {
        let alias_lower = alias.to_lowercase();
        
        // Check for duplicate alias
        if self.schemas.contains_key(&alias_lower) {
            return Err(MiniSqlError::duplicate_table_alias(alias));
        }
        
        let offset = self.column_map.len();
        self.table_offsets.insert(alias_lower.clone(), offset);
        
        for (i, col) in schema.columns.iter().enumerate() {
            self.column_map.insert(
                (alias_lower.clone(), col.name.to_lowercase()),
                offset + i,
            );
        }
        
        self.schemas.insert(alias_lower.clone(), schema);
        self.table_order.push(alias_lower);
        
        Ok(())
    }

    pub fn find_column(&self, table: Option<&str>, name: &str) -> Result<(usize, DataType)> {
        let name_lower = name.to_lowercase();
        
        if let Some(tbl) = table {
            let tbl_lower = tbl.to_lowercase();
            if let Some(&idx) = self.column_map.get(&(tbl_lower.clone(), name_lower.clone())) {
                let schema = self.schemas.get(&tbl_lower).ok_or_else(|| {
                    MiniSqlError::unknown_table_in_field_list(tbl)
                })?;
                let col = schema.columns.iter().find(|c| c.name.to_lowercase() == name_lower)
                    .ok_or_else(|| MiniSqlError::unknown_column(name, ColumnContext::FieldList))?;
                return Ok((idx, col.data_type.clone()));
            }
            return Err(MiniSqlError::unknown_column_qualified(tbl, name, ColumnContext::FieldList));
        }
        
        // Unqualified - search all tables
        let mut found: Option<(usize, DataType)> = None;
        for alias in &self.table_order {
            if let Some(&idx) = self.column_map.get(&(alias.clone(), name_lower.clone())) {
                if found.is_some() {
                    return Err(MiniSqlError::ambiguous_column(name, ColumnContext::FieldList));
                }
                let schema = self.schemas.get(alias).unwrap();
                let col = schema.columns.iter().find(|c| c.name.to_lowercase() == name_lower).unwrap();
                found = Some((idx, col.data_type.clone()));
            }
        }
        
        found.ok_or_else(|| MiniSqlError::unknown_column(name, ColumnContext::FieldList))
    }

    pub fn get_table_offset(&self, table: &str) -> Result<usize> {
        let tbl_lower = table.to_lowercase();
        self.table_offsets.get(&tbl_lower).copied()
            .ok_or_else(|| MiniSqlError::unknown_table_in_field_list(table))
    }
}

/// Coerce row values to match schema types
pub fn coerce_row_types(values: &[Value], schema: &TableSchema) -> Result<Vec<Value>> {
    let mut result = Vec::with_capacity(values.len());
    
    for (i, value) in values.iter().enumerate() {
        let col = &schema.columns[i];
        let coerced = coerce_value(value, &col.data_type)?;
        
        // Check NOT NULL constraint
        if !col.nullable && coerced.is_null() {
            return Err(MiniSqlError::Constraint(format!(
                "Column '{}' cannot be NULL",
                col.name
            )));
        }
        
        result.push(coerced);
    }
    
    Ok(result)
}

/// Coerce a value to a specific type
fn coerce_value(value: &Value, target_type: &DataType) -> Result<Value> {
    if value.is_null() {
        return Ok(Value::Null);
    }

    match (value, target_type) {
        // Already correct type
        (Value::Integer(_), DataType::Integer) => Ok(value.clone()),
        // Treat NaN produced by arithmetic (e.g., 1.1/0) as SQL NULL for FLOAT columns
        (Value::Float(f), DataType::Float) => {
            if f.is_nan() {
                Ok(Value::Null)
            } else {
                Ok(Value::Float(*f))
            }
        }
        (Value::String(_), DataType::Varchar(_) | DataType::Text) => Ok(value.clone()),
        (Value::Boolean(_), DataType::Boolean) => Ok(value.clone()),
        (Value::Json(_), DataType::Json) => Ok(value.clone()),

        // Numeric coercion
        (Value::Integer(i), DataType::Float) => Ok(Value::Float(*i as f64)),
        (Value::Float(f), DataType::Integer) => {
            // If the float is NaN (e.g., result of division by zero), treat as NULL
            if f.is_nan() {
                Ok(Value::Null)
            } else {
                Ok(Value::Integer(*f as i64))
            }
        }

        // String to JSON
        (Value::String(s), DataType::Json) => {
            let json: serde_json::Value = serde_json::from_str(s).map_err(|_| {
                MiniSqlError::Type(format!("Cannot parse '{}' as JSON", s))
            })?;
            Ok(Value::Json(json))
        }

        // Numeric string parsing
        (Value::String(s), DataType::Integer) => {
            let i: i64 = s.parse().map_err(|_| {
                MiniSqlError::Type(format!("Cannot parse '{}' as integer", s))
            })?;
            Ok(Value::Integer(i))
        }
        (Value::String(s), DataType::Float) => {
            let f: f64 = s.parse().map_err(|_| {
                MiniSqlError::Type(format!("Cannot parse '{}' as float", s))
            })?;
            Ok(Value::Float(f))
        }

        // Boolean coercion
        (Value::Integer(i), DataType::Boolean) => Ok(Value::Boolean(*i != 0)),
        (Value::String(s), DataType::Boolean) => {
            match s.to_lowercase().as_str() {
                "true" | "1" | "yes" => Ok(Value::Boolean(true)),
                "false" | "0" | "no" => Ok(Value::Boolean(false)),
                _ => Err(MiniSqlError::Type(format!(
                    "Cannot parse '{}' as boolean",
                    s
                ))),
            }
        }

        // Default: allow if types are compatible
        _ => Ok(value.clone()),
    }
}

/// Resolve SELECT columns to names and types (simple query)
pub fn resolve_select_columns_simple(
    columns: &[SelectColumn],
    schema: &TableSchema,
    _table_alias: &str,
) -> Result<(Vec<String>, Vec<DataType>)> {
    let mut names = Vec::new();
    let mut types = Vec::new();

    for col in columns {
        match col {
            SelectColumn::Star => {
                for c in &schema.columns {
                    names.push(c.name.clone());
                    types.push(c.data_type.clone());
                }
            }
            SelectColumn::QualifiedStar { table: _ } => {
                // For simple queries, qualified star is the same as star
                for c in &schema.columns {
                    names.push(c.name.clone());
                    types.push(c.data_type.clone());
                }
            }
            SelectColumn::Expr { expr, alias } => {
                let name = alias.clone().unwrap_or_else(|| evaluator::expr_name(expr));
                let data_type = evaluator::infer_expr_type_simple(expr, schema)?;
                names.push(name);
                types.push(data_type);
            }
        }
    }

    Ok((names, types))
}


/// Resolve SELECT columns for join query
pub fn resolve_select_columns_join(
    columns: &[SelectColumn],
    tables: &JoinTableInfo,
) -> Result<(Vec<String>, Vec<DataType>)> {
    let mut names = Vec::new();
    let mut types = Vec::new();

    for col in columns {
        match col {
            SelectColumn::Star => {
                // Add all columns from all tables
                for alias in &tables.table_order {
                     if let Some(schema) = tables.schemas.get(alias) {
                        for c in &schema.columns {
                            names.push(format!("{}.{}", alias, c.name));
                            types.push(c.data_type.clone());
                        }
                     }
                }
            }
            SelectColumn::QualifiedStar { table } => {
                // Add all columns from specific table
                if let Some(schema) = tables.schemas.get(&table.to_lowercase()) {
                    for c in &schema.columns {
                        names.push(c.name.clone());
                        types.push(c.data_type.clone());
                    }
                } else {
                    return Err(MiniSqlError::unknown_table_in_field_list(table));
                }
            }
            SelectColumn::Expr { expr, alias } => {
                let name = alias.clone().unwrap_or_else(|| evaluator::expr_name(expr));
                let data_type = evaluator::infer_expr_type_join(expr, tables)?;
                names.push(name);
                types.push(data_type);
            }
        }
    }

    Ok((names, types))
}
