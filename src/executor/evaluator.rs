use crate::error::{ColumnContext, MiniSqlError, Result};
use crate::parser::{BinaryOperator, Expr};
use crate::types::{DataType, Row, TableSchema, Value};
use crate::executor::schema::JoinTableInfo;
use crate::join::JoinContext;

/// Get a name for an expression (for column headers)
pub fn expr_name(expr: &Expr) -> String {
    match expr {
        Expr::Column { table: _, name } => name.clone(),
        Expr::Literal(v) => v.to_string(),
        Expr::JsonAccess { expr, key, as_text } => {
            format!(
                "{}{}'{}'",
                expr_name(expr),
                if *as_text { "->>" } else { "->" },
                key
            )
        }
        Expr::FunctionCall { name, args } => {
            if args.is_empty() {
                format!("{}(*)", name)
            } else {
                format!("{}(...)", name)
            }
        }
        _ => "?".to_string(),
    }
}

/// Infer expression type for simple query
pub fn infer_expr_type_simple(expr: &Expr, schema: &TableSchema) -> Result<DataType> {
    match expr {
        Expr::Literal(Value::Integer(_)) => Ok(DataType::Integer),
        Expr::Literal(Value::Float(_)) => Ok(DataType::Float),
        Expr::Literal(Value::String(_)) => Ok(DataType::Text),
        Expr::Literal(Value::Boolean(_)) => Ok(DataType::Boolean),
        Expr::Literal(Value::Json(_)) => Ok(DataType::Json),
        Expr::Literal(Value::Null) => Ok(DataType::Text),
        Expr::Column { table: _, name } => {
            let idx = schema.find_column(name).ok_or_else(|| {
                MiniSqlError::unknown_column(name, ColumnContext::FieldList)
            })?;
            Ok(schema.columns[idx].data_type.clone())
        }
        Expr::JsonAccess { as_text, .. } => {
            if *as_text {
                Ok(DataType::Text)
            } else {
                Ok(DataType::Json)
            }
        }
        Expr::BinaryOp { left, op, right } => {
            match op {
                BinaryOperator::And
                | BinaryOperator::Or
                | BinaryOperator::Equal
                | BinaryOperator::NotEqual
                | BinaryOperator::LessThan
                | BinaryOperator::LessThanOrEqual
                | BinaryOperator::GreaterThan
                | BinaryOperator::GreaterThanOrEqual
                | BinaryOperator::Like => Ok(DataType::Boolean),
                BinaryOperator::Plus
                | BinaryOperator::Minus
                | BinaryOperator::Multiply
                | BinaryOperator::Divide => {
                    let left_type = infer_expr_type_simple(left, schema)?;
                    let right_type = infer_expr_type_simple(right, schema)?;
                    if matches!(left_type, DataType::Float)
                        || matches!(right_type, DataType::Float)
                    {
                        Ok(DataType::Float)
                    } else {
                        Ok(DataType::Integer)
                    }
                }
            }
        }
        Expr::Not(_) | Expr::IsNull(_) | Expr::IsNotNull(_) | Expr::In { .. } | Expr::NotIn { .. } => Ok(DataType::Boolean),
        Expr::FunctionCall { name, args } => {
            match name.to_uppercase().as_str() {
                "JSON_EXTRACT" => Ok(DataType::Json),
                "COUNT" => Ok(DataType::Integer),
                "SUM" => {
                    if args.is_empty() {
                        Ok(DataType::Integer)
                    } else {
                        infer_expr_type_simple(&args[0], schema)
                    }
                }
                "AVG" => Ok(DataType::Float),
                "MIN" | "MAX" => {
                    if args.is_empty() {
                        Ok(DataType::Integer)
                    } else {
                        infer_expr_type_simple(&args[0], schema)
                    }
                }
                _ => Ok(DataType::Text),
            }
        }
        Expr::Placeholder(_) => Ok(DataType::Text), // Placeholders default to text type
    }
}

/// Infer expression type for join query
pub fn infer_expr_type_join(expr: &Expr, tables: &JoinTableInfo) -> Result<DataType> {
    match expr {
        Expr::Literal(Value::Integer(_)) => Ok(DataType::Integer),
        Expr::Literal(Value::Float(_)) => Ok(DataType::Float),
        Expr::Literal(Value::String(_)) => Ok(DataType::Text),
        Expr::Literal(Value::Boolean(_)) => Ok(DataType::Boolean),
        Expr::Literal(Value::Json(_)) => Ok(DataType::Json),
        Expr::Literal(Value::Null) => Ok(DataType::Text),
        Expr::Column { table, name } => {
            let (_, dt) = tables.find_column(table.as_deref(), name)?;
            Ok(dt)
        }
        Expr::JsonAccess { as_text, .. } => {
            if *as_text {
                Ok(DataType::Text)
            } else {
                Ok(DataType::Json)
            }
        }
        Expr::BinaryOp { left, op, right } => {
            match op {
                BinaryOperator::And
                | BinaryOperator::Or
                | BinaryOperator::Equal
                | BinaryOperator::NotEqual
                | BinaryOperator::LessThan
                | BinaryOperator::LessThanOrEqual
                | BinaryOperator::GreaterThan
                | BinaryOperator::GreaterThanOrEqual
                | BinaryOperator::Like => Ok(DataType::Boolean),
                BinaryOperator::Plus
                | BinaryOperator::Minus
                | BinaryOperator::Multiply
                | BinaryOperator::Divide => {
                    let left_type = infer_expr_type_join(left, tables)?;
                    let right_type = infer_expr_type_join(right, tables)?;
                    if matches!(left_type, DataType::Float)
                        || matches!(right_type, DataType::Float)
                    {
                        Ok(DataType::Float)
                    } else {
                        Ok(DataType::Integer)
                    }
                }
            }
        }
        Expr::Not(_) | Expr::IsNull(_) | Expr::IsNotNull(_) | Expr::In { .. } | Expr::NotIn { .. } => Ok(DataType::Boolean),
        Expr::FunctionCall { name, .. } => {
            match name.to_uppercase().as_str() {
                "JSON_EXTRACT" => Ok(DataType::Json),
                _ => Ok(DataType::Text),
            }
        }
        Expr::Placeholder(_) => Ok(DataType::Text), // Placeholders default to text type
    }
}

// =========================================================================
// Evaluation Logic
// =========================================================================

/// Evaluate a constant expression (no row context)
pub fn eval_const_expr(expr: &Expr, last_insert_id: u64) -> Result<Value> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),
        Expr::BinaryOp { left, op, right } => {
            let left_val = eval_const_expr(left, last_insert_id)?;
            let right_val = eval_const_expr(right, last_insert_id)?;
            apply_binary_op(&left_val, *op, &right_val)
        }
        Expr::Not(inner) => {
            let val = eval_const_expr(inner, last_insert_id)?;
            match val {
                Value::Boolean(b) => Ok(Value::Boolean(!b)),
                _ => Err(MiniSqlError::Type("NOT requires boolean".into())),
            }
        }
        Expr::FunctionCall { name, args } => {
            let mut arg_values = Vec::new();
            for arg in args {
                arg_values.push(eval_const_expr(arg, last_insert_id)?);
            }
            eval_function_values(name, &arg_values, last_insert_id)
        }
        _ => Err(MiniSqlError::Syntax(
            "Expression requires row context".into(),
        )),
    }
}

/// Evaluate an expression in the context of a row
pub fn eval_expr(expr: &Expr, row: &Row, schema: &TableSchema, last_insert_id: u64) -> Result<Value> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),
        Expr::Column { table: _, name } => {
             // Treat both qualified and unqualified as simple column access in simple eval
             // Note: The original code used Expr::Column(name) in eval_expr but Expr::Column{table,name} in eval_expr_simple
             // We need to handle both to be safe
            let idx = schema.find_column(name).ok_or_else(|| {
                MiniSqlError::unknown_column(name, ColumnContext::FieldList)
            })?;
            Ok(row.values.get(idx).cloned().unwrap_or(Value::Null))
        }
        Expr::BinaryOp { left, op, right } => {
            let left_val = eval_expr(left, row, schema, last_insert_id)?;
            let right_val = eval_expr(right, row, schema, last_insert_id)?;
            apply_binary_op(&left_val, *op, &right_val)
        }
        Expr::Not(inner) => {
            let val = eval_expr(inner, row, schema, last_insert_id)?;
            match val {
                Value::Boolean(b) => Ok(Value::Boolean(!b)),
                Value::Null => Ok(Value::Null),
                _ => Err(MiniSqlError::Type("NOT requires boolean".into())),
            }
        }
        Expr::IsNull(inner) => {
            let val = eval_expr(inner, row, schema, last_insert_id)?;
            Ok(Value::Boolean(val.is_null()))
        }
        Expr::IsNotNull(inner) => {
            let val = eval_expr(inner, row, schema, last_insert_id)?;
            Ok(Value::Boolean(!val.is_null()))
        }
        Expr::JsonAccess { expr, key, as_text } => {
            let val = eval_expr(expr, row, schema, last_insert_id)?;
            if *as_text {
                Ok(val.json_get_text(key))
            } else {
                Ok(val.json_get(key))
            }
        }
        Expr::In { expr, values } => {
            let expr_val = eval_expr(expr, row, schema, last_insert_id)?;
            
            // NULL IN (...) is always NULL (SQL standard)
            if expr_val.is_null() {
                return Ok(Value::Null);
            }
            
            for val_expr in values {
                let val = eval_expr(val_expr, row, schema, last_insert_id)?;
                // If any value is NULL and no match found yet, result should be NULL
                // But if we find a match, return true immediately
                if !val.is_null() && expr_val == val {
                    return Ok(Value::Boolean(true));
                }
            }
            
            // Check if any value was NULL (SQL three-valued logic)
            for val_expr in values {
                let val = eval_expr(val_expr, row, schema, last_insert_id)?;
                if val.is_null() {
                    return Ok(Value::Null);
                }
            }
            
            Ok(Value::Boolean(false))
        }
        Expr::NotIn { expr, values } => {
            let expr_val = eval_expr(expr, row, schema, last_insert_id)?;
            
            // NULL NOT IN (...) is always NULL (SQL standard)
            if expr_val.is_null() {
                return Ok(Value::Null);
            }
            
            for val_expr in values {
                let val = eval_expr(val_expr, row, schema, last_insert_id)?;
                // If we find a match, return false immediately
                if !val.is_null() && expr_val == val {
                    return Ok(Value::Boolean(false));
                }
            }
            
            // Check if any value was NULL (SQL three-valued logic)
            for val_expr in values {
                let val = eval_expr(val_expr, row, schema, last_insert_id)?;
                if val.is_null() {
                    return Ok(Value::Null);
                }
            }
            
            Ok(Value::Boolean(true))
        }
        Expr::FunctionCall { name, args } => {
            eval_function(name, args, row, schema, last_insert_id)
        }
        Expr::Placeholder(_) => Err(MiniSqlError::Syntax(
            "Unsubstituted placeholder in expression".into(),
        )),
    }
}

/// Evaluate a WHERE expression, returning true if the row matches
pub fn eval_where(expr: &Expr, row: &Row, schema: &TableSchema, last_insert_id: u64) -> Result<bool> {
    let val = eval_expr(expr, row, schema, last_insert_id)?;
    match val {
        Value::Boolean(b) => Ok(b),
        Value::Null => Ok(false),
        _ => Ok(true), // Non-null, non-boolean values are truthy
    }
}

/// Evaluate expression for simple query (with table alias check)
pub fn eval_expr_simple(expr: &Expr, row: &Row, schema: &TableSchema, table_alias: &str, last_insert_id: u64) -> Result<Value> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),
        Expr::Column { table, name } => {
            // For simple queries, we ignore the table qualifier
            // (or check that it matches the single table)
            if let Some(tbl) = table {
                if !tbl.eq_ignore_ascii_case(table_alias) && !tbl.eq_ignore_ascii_case(&schema.name) {
                    return Err(MiniSqlError::unknown_table_in_field_list(tbl));
                }
            }
            let idx = schema.find_column(name).ok_or_else(|| {
                MiniSqlError::unknown_column(name, ColumnContext::FieldList)
            })?;
            Ok(row.values.get(idx).cloned().unwrap_or(Value::Null))
        }
        Expr::BinaryOp { left, op, right } => {
            let left_val = eval_expr_simple(left, row, schema, table_alias, last_insert_id)?;
            let right_val = eval_expr_simple(right, row, schema, table_alias, last_insert_id)?;
            apply_binary_op(&left_val, *op, &right_val)
        }
        Expr::Not(inner) => {
            let val = eval_expr_simple(inner, row, schema, table_alias, last_insert_id)?;
            match val {
                Value::Boolean(b) => Ok(Value::Boolean(!b)),
                Value::Null => Ok(Value::Null),
                _ => Err(MiniSqlError::Type("NOT requires boolean".into())),
            }
        }
        Expr::IsNull(inner) => {
            let val = eval_expr_simple(inner, row, schema, table_alias, last_insert_id)?;
            Ok(Value::Boolean(val.is_null()))
        }
        Expr::IsNotNull(inner) => {
            let val = eval_expr_simple(inner, row, schema, table_alias, last_insert_id)?;
            Ok(Value::Boolean(!val.is_null()))
        }
        Expr::JsonAccess { expr, key, as_text } => {
            let val = eval_expr_simple(expr, row, schema, table_alias, last_insert_id)?;
            if *as_text {
                Ok(val.json_get_text(key))
            } else {
                Ok(val.json_get(key))
            }
        }
        Expr::In { expr, values } => {
            let expr_val = eval_expr_simple(expr, row, schema, table_alias, last_insert_id)?;
            if expr_val.is_null() {
                return Ok(Value::Null);
            }
            for val_expr in values {
                let val = eval_expr_simple(val_expr, row, schema, table_alias, last_insert_id)?;
                if !val.is_null() && expr_val == val {
                    return Ok(Value::Boolean(true));
                }
            }
            for val_expr in values {
                let val = eval_expr_simple(val_expr, row, schema, table_alias, last_insert_id)?;
                if val.is_null() {
                    return Ok(Value::Null);
                }
            }
            Ok(Value::Boolean(false))
        }
        Expr::NotIn { expr, values } => {
            let expr_val = eval_expr_simple(expr, row, schema, table_alias, last_insert_id)?;
            if expr_val.is_null() {
                return Ok(Value::Null);
            }
            for val_expr in values {
                let val = eval_expr_simple(val_expr, row, schema, table_alias, last_insert_id)?;
                if !val.is_null() && expr_val == val {
                    return Ok(Value::Boolean(false));
                }
            }
            for val_expr in values {
                let val = eval_expr_simple(val_expr, row, schema, table_alias, last_insert_id)?;
                if val.is_null() {
                    return Ok(Value::Null);
                }
            }
            Ok(Value::Boolean(true))
        }
        Expr::FunctionCall { name, args } => {
            eval_function_simple(name, args, row, schema, table_alias, last_insert_id)
        }
        Expr::Placeholder(_) => Err(MiniSqlError::Syntax(
            "Unsubstituted placeholder in expression".into(),
        )),
    }
}

/// Evaluate a WHERE expression for simple query, returning true if the row matches
pub fn eval_where_simple(expr: &Expr, row: &Row, schema: &TableSchema, table_alias: &str, last_insert_id: u64) -> Result<bool> {
    let val = eval_expr_simple(expr, row, schema, table_alias, last_insert_id)?;
    match val {
        Value::Boolean(b) => Ok(b),
        Value::Null => Ok(false),
        _ => Ok(true),
    }
}

/// Evaluate expression for join query
pub fn eval_expr_join(expr: &Expr, row: &Row, tables: &JoinTableInfo, last_insert_id: u64) -> Result<Value> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),
        Expr::Column { table, name } => {
            let (idx, _) = tables.find_column(table.as_deref(), name)?;
            Ok(row.values.get(idx).cloned().unwrap_or(Value::Null))
        }
        Expr::BinaryOp { left, op, right } => {
            let left_val = eval_expr_join(left, row, tables, last_insert_id)?;
            let right_val = eval_expr_join(right, row, tables, last_insert_id)?;
            apply_binary_op(&left_val, *op, &right_val)
        }
        Expr::Not(inner) => {
            let val = eval_expr_join(inner, row, tables, last_insert_id)?;
            match val {
                Value::Boolean(b) => Ok(Value::Boolean(!b)),
                Value::Null => Ok(Value::Null),
                _ => Err(MiniSqlError::Type("NOT requires boolean".into())),
            }
        }
        Expr::IsNull(inner) => {
            let val = eval_expr_join(inner, row, tables, last_insert_id)?;
            Ok(Value::Boolean(val.is_null()))
        }
        Expr::IsNotNull(inner) => {
            let val = eval_expr_join(inner, row, tables, last_insert_id)?;
            Ok(Value::Boolean(!val.is_null()))
        }
        Expr::JsonAccess { expr, key, as_text } => {
            let val = eval_expr_join(expr, row, tables, last_insert_id)?;
            if *as_text {
                Ok(val.json_get_text(key))
            } else {
                Ok(val.json_get(key))
            }
        }
        Expr::In { expr, values } => {
            let expr_val = eval_expr_join(expr, row, tables, last_insert_id)?;
            if expr_val.is_null() {
                return Ok(Value::Null);
            }
            for val_expr in values {
                let val = eval_expr_join(val_expr, row, tables, last_insert_id)?;
                if !val.is_null() && expr_val == val {
                    return Ok(Value::Boolean(true));
                }
            }
            for val_expr in values {
                let val = eval_expr_join(val_expr, row, tables, last_insert_id)?;
                if val.is_null() {
                    return Ok(Value::Null);
                }
            }
            Ok(Value::Boolean(false))
        }
        Expr::NotIn { expr, values } => {
            let expr_val = eval_expr_join(expr, row, tables, last_insert_id)?;
            if expr_val.is_null() {
                return Ok(Value::Null);
            }
            for val_expr in values {
                let val = eval_expr_join(val_expr, row, tables, last_insert_id)?;
                if !val.is_null() && expr_val == val {
                    return Ok(Value::Boolean(false));
                }
            }
            for val_expr in values {
                let val = eval_expr_join(val_expr, row, tables, last_insert_id)?;
                if val.is_null() {
                    return Ok(Value::Null);
                }
            }
            Ok(Value::Boolean(true))
        }
        Expr::FunctionCall { name, args } => {
            let arg_values: Result<Vec<Value>> = args
                .iter()
                .map(|a| eval_expr_join(a, row, tables, last_insert_id))
                .collect();
            eval_function_values(name, &arg_values?, last_insert_id)
        }
        Expr::Placeholder(_) => Err(MiniSqlError::Syntax(
            "Unsubstituted placeholder in expression".into(),
        )),
    }
}

/// Evaluate a join condition
pub fn eval_join_condition(expr: &Expr, values: &[Value], ctx: &JoinContext, last_insert_id: u64) -> Result<bool> {
    let val = eval_expr_with_join_context(expr, values, ctx, last_insert_id)?;
    match val {
        Value::Boolean(b) => Ok(b),
        Value::Null => Ok(false),
        _ => Ok(true),
    }
}

/// Evaluate expression with join context (combined row values)
pub fn eval_expr_with_join_context(expr: &Expr, values: &[Value], ctx: &JoinContext, last_insert_id: u64) -> Result<Value> {
    match expr {
        Expr::Literal(v) => Ok(v.clone()),
        Expr::Column { table, name } => {
            let idx = ctx.find_column(table.as_deref(), name)?;
            Ok(values.get(idx).cloned().unwrap_or(Value::Null))
        }
        Expr::BinaryOp { left, op, right } => {
            let left_val = eval_expr_with_join_context(left, values, ctx, last_insert_id)?;
            let right_val = eval_expr_with_join_context(right, values, ctx, last_insert_id)?;
            apply_binary_op(&left_val, *op, &right_val)
        }
        Expr::Not(inner) => {
            let val = eval_expr_with_join_context(inner, values, ctx, last_insert_id)?;
            match val {
                Value::Boolean(b) => Ok(Value::Boolean(!b)),
                Value::Null => Ok(Value::Null),
                _ => Err(MiniSqlError::Type("NOT requires boolean".into())),
            }
        }
        Expr::IsNull(inner) => {
            let val = eval_expr_with_join_context(inner, values, ctx, last_insert_id)?;
            Ok(Value::Boolean(val.is_null()))
        }
        Expr::IsNotNull(inner) => {
            let val = eval_expr_with_join_context(inner, values, ctx, last_insert_id)?;
            Ok(Value::Boolean(!val.is_null()))
        }
        Expr::JsonAccess { expr, key, as_text } => {
            let val = eval_expr_with_join_context(expr, values, ctx, last_insert_id)?;
            if *as_text {
                Ok(val.json_get_text(key))
            } else {
                Ok(val.json_get(key))
            }
        }
        Expr::In { expr, values: in_values } => {
            let expr_val = eval_expr_with_join_context(expr, values, ctx, last_insert_id)?;
            if expr_val.is_null() {
                return Ok(Value::Null);
            }
            for val_expr in in_values {
                let val = eval_expr_with_join_context(val_expr, values, ctx, last_insert_id)?;
                if !val.is_null() && expr_val == val {
                    return Ok(Value::Boolean(true));
                }
            }
            for val_expr in in_values {
                let val = eval_expr_with_join_context(val_expr, values, ctx, last_insert_id)?;
                if val.is_null() {
                    return Ok(Value::Null);
                }
            }
            Ok(Value::Boolean(false))
        }
        Expr::NotIn { expr, values: in_values } => {
            let expr_val = eval_expr_with_join_context(expr, values, ctx, last_insert_id)?;
            if expr_val.is_null() {
                return Ok(Value::Null);
            }
            for val_expr in in_values {
                let val = eval_expr_with_join_context(val_expr, values, ctx, last_insert_id)?;
                if !val.is_null() && expr_val == val {
                    return Ok(Value::Boolean(false));
                }
            }
            for val_expr in in_values {
                let val = eval_expr_with_join_context(val_expr, values, ctx, last_insert_id)?;
                if val.is_null() {
                    return Ok(Value::Null);
                }
            }
            Ok(Value::Boolean(true))
        }
        Expr::FunctionCall { name, args } => {
            let arg_values: Result<Vec<Value>> = args
                .iter()
                .map(|a| eval_expr_with_join_context(a, values, ctx, last_insert_id))
                .collect();
            eval_function_values(name, &arg_values?, last_insert_id)
        }
        Expr::Placeholder(_) => Err(MiniSqlError::Syntax(
            "Unsubstituted placeholder in expression".into(),
        )),
    }
}


/// Apply a binary operator
pub fn apply_binary_op(left: &Value, op: BinaryOperator, right: &Value) -> Result<Value> {
    // Handle NULL propagation
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        match op {
            BinaryOperator::And => {
                // NULL AND FALSE = FALSE, NULL AND TRUE = NULL
                if let (Value::Boolean(false), _) | (_, Value::Boolean(false)) = (left, right) {
                    return Ok(Value::Boolean(false));
                }
                return Ok(Value::Null);
            }
            BinaryOperator::Or => {
                // NULL OR TRUE = TRUE, NULL OR FALSE = NULL
                if let (Value::Boolean(true), _) | (_, Value::Boolean(true)) = (left, right) {
                    return Ok(Value::Boolean(true));
                }
                return Ok(Value::Null);
            }
            _ => return Ok(Value::Null),
        }
    }

    match op {
        BinaryOperator::Equal => Ok(Value::Boolean(left == right)),
        BinaryOperator::NotEqual => Ok(Value::Boolean(left != right)),
        BinaryOperator::LessThan => {
            Ok(Value::Boolean(left.partial_cmp(right) == Some(std::cmp::Ordering::Less)))
        }
        BinaryOperator::LessThanOrEqual => {
            let cmp = left.partial_cmp(right);
            Ok(Value::Boolean(
                cmp == Some(std::cmp::Ordering::Less) || cmp == Some(std::cmp::Ordering::Equal),
            ))
        }
        BinaryOperator::GreaterThan => {
            Ok(Value::Boolean(left.partial_cmp(right) == Some(std::cmp::Ordering::Greater)))
        }
        BinaryOperator::GreaterThanOrEqual => {
            let cmp = left.partial_cmp(right);
            Ok(Value::Boolean(
                cmp == Some(std::cmp::Ordering::Greater)
                    || cmp == Some(std::cmp::Ordering::Equal),
            ))
        }
        BinaryOperator::And => {
            match (left, right) {
                (Value::Boolean(a), Value::Boolean(b)) => Ok(Value::Boolean(*a && *b)),
                _ => Err(MiniSqlError::Type("AND requires boolean operands".into())),
            }
        }
        BinaryOperator::Or => {
            match (left, right) {
                (Value::Boolean(a), Value::Boolean(b)) => Ok(Value::Boolean(*a || *b)),
                _ => Err(MiniSqlError::Type("OR requires boolean operands".into())),
            }
        }
        BinaryOperator::Plus => apply_arithmetic(left, right, |a, b| a + b),
        BinaryOperator::Minus => apply_arithmetic(left, right, |a, b| a - b),
        BinaryOperator::Multiply => apply_arithmetic(left, right, |a, b| a * b),
        BinaryOperator::Divide => apply_arithmetic(left, right, |a, b| {
            if b == 0.0 {
                f64::NAN
            } else {
                a / b
            }
        }),
        BinaryOperator::Like => {
            match (left, right) {
                (Value::String(s), Value::String(pattern)) => {
                    Ok(Value::Boolean(match_like(s, pattern)))
                }
                _ => Err(MiniSqlError::Type("LIKE requires string operands".into())),
            }
        }
    }
}

/// Apply arithmetic operation
fn apply_arithmetic<F>(left: &Value, right: &Value, f: F) -> Result<Value>
where
    F: Fn(f64, f64) -> f64,
{
    let left_num = match left {
        Value::Integer(i) => *i as f64,
        Value::Float(f) => *f,
        _ => return Err(MiniSqlError::Type("Arithmetic requires numeric operands".into())),
    };
    let right_num = match right {
        Value::Integer(i) => *i as f64,
        Value::Float(f) => *f,
        _ => return Err(MiniSqlError::Type("Arithmetic requires numeric operands".into())),
    };

    let result = f(left_num, right_num);

    // Return integer if both inputs were integers and result is whole
    if matches!(left, Value::Integer(_))
        && matches!(right, Value::Integer(_))
        && result.fract() == 0.0
    {
        Ok(Value::Integer(result as i64))
    } else {
        Ok(Value::Float(result))
    }
}

/// Simple LIKE pattern matching (supports % and _)
fn match_like(s: &str, pattern: &str) -> bool {
    let s_chars: Vec<char> = s.chars().collect();
    let p_chars: Vec<char> = pattern.chars().collect();
    
    match_like_recursive(&s_chars, &p_chars)
}

fn match_like_recursive(s: &[char], p: &[char]) -> bool {
    if p.is_empty() {
        return s.is_empty();
    }

    match p[0] {
        '%' => {
            // % matches any sequence
            for i in 0..=s.len() {
                if match_like_recursive(&s[i..], &p[1..]) {
                    return true;
                }
            }
            false
        }
        '_' => {
            // _ matches exactly one character
            !s.is_empty() && match_like_recursive(&s[1..], &p[1..])
        }
        c => {
            // Regular character match (case-insensitive)
            !s.is_empty()
                && s[0].to_ascii_lowercase() == c.to_ascii_lowercase()
                && match_like_recursive(&s[1..], &p[1..])
        }
    }
}

/// Evaluate a function call
pub fn eval_function(
    name: &str,
    args: &[Expr],
    row: &Row,
    schema: &TableSchema,
    last_insert_id: u64,
) -> Result<Value> {
    match name.to_uppercase().as_str() {
        "LAST_INSERT_ID" => {
            Ok(Value::Integer(last_insert_id as i64))
        }
        "JSON_EXTRACT" => {
            if args.len() != 2 {
                return Err(MiniSqlError::Syntax(
                    "JSON_EXTRACT requires 2 arguments".into(),
                ));
            }
            let json_val = eval_expr(&args[0], row, schema, last_insert_id)?;
            let path = eval_expr(&args[1], row, schema, last_insert_id)?;
            match path {
                Value::String(key) => {
                    // Remove leading $. if present
                    let key = key.trim_start_matches("$.").to_string();
                    Ok(json_val.json_get(&key))
                }
                _ => Err(MiniSqlError::Type("JSON path must be string".into())),
            }
        }
        "COALESCE" => {
            for arg in args {
                let val = eval_expr(arg, row, schema, last_insert_id)?;
                if !val.is_null() {
                    return Ok(val);
                }
            }
            Ok(Value::Null)
        }
        "IFNULL" => {
            if args.len() != 2 {
                return Err(MiniSqlError::Syntax("IFNULL requires 2 arguments".into()));
            }
            let val = eval_expr(&args[0], row, schema, last_insert_id)?;
            if val.is_null() {
                eval_expr(&args[1], row, schema, last_insert_id)
            } else {
                Ok(val)
            }
        }
        _ => Err(MiniSqlError::Syntax(format!("Unknown function: {}", name))),
    }
}

/// Evaluate function for simple query
fn eval_function_simple(
    name: &str,
    args: &[Expr],
    row: &Row,
    schema: &TableSchema,
    table_alias: &str,
    last_insert_id: u64,
) -> Result<Value> {
    let arg_values: Result<Vec<Value>> = args
        .iter()
        .map(|a| eval_expr_simple(a, row, schema, table_alias, last_insert_id))
        .collect();
    eval_function_values(name, &arg_values?, last_insert_id)
}

/// Evaluate function from already-evaluated values
pub fn eval_function_values(name: &str, args: &[Value], last_insert_id: u64) -> Result<Value> {
    match name.to_uppercase().as_str() {
        "LAST_INSERT_ID" => {
            Ok(Value::Integer(last_insert_id as i64))
        }
        "JSON_EXTRACT" => {
            if args.len() != 2 {
                return Err(MiniSqlError::Syntax("JSON_EXTRACT requires 2 arguments".into()));
            }
            let json_val = &args[0];
            let path = &args[1];
            match path {
                Value::String(key) => {
                    let key = key.trim_start_matches("$.").to_string();
                    Ok(json_val.json_get(&key))
                }
                _ => Err(MiniSqlError::Type("JSON path must be string".into())),
            }
        }
        "COALESCE" => {
            for val in args {
                if !val.is_null() {
                    return Ok(val.clone());
                }
            }
            Ok(Value::Null)
        }
        "IFNULL" => {
            if args.len() != 2 {
                return Err(MiniSqlError::Syntax("IFNULL requires 2 arguments".into()));
            }
            if args[0].is_null() {
                Ok(args[1].clone())
            } else {
                Ok(args[0].clone())
            }
        }
        _ => Err(MiniSqlError::Syntax(format!("Unknown function: {}", name))),
    }
}

/// Substitute placeholder expressions with actual parameter values
/// This is used during prepared statement execution
pub fn substitute_placeholders(expr: &Expr, params: &[Value]) -> Result<Expr> {
    match expr {
        Expr::Placeholder(idx) => {
            if *idx >= params.len() {
                return Err(MiniSqlError::Syntax(format!(
                    "Parameter index {} out of bounds (only {} parameters provided)",
                    idx, params.len()
                )));
            }
            Ok(Expr::Literal(params[*idx].clone()))
        }
        Expr::Literal(v) => Ok(Expr::Literal(v.clone())),
        Expr::Column { table, name } => Ok(Expr::Column {
            table: table.clone(),
            name: name.clone(),
        }),
        Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
            left: Box::new(substitute_placeholders(left, params)?),
            op: *op,
            right: Box::new(substitute_placeholders(right, params)?),
        }),
        Expr::Not(inner) => Ok(Expr::Not(Box::new(substitute_placeholders(inner, params)?))),
        Expr::IsNull(inner) => Ok(Expr::IsNull(Box::new(substitute_placeholders(inner, params)?))),
        Expr::IsNotNull(inner) => Ok(Expr::IsNotNull(Box::new(substitute_placeholders(inner, params)?))),
        Expr::JsonAccess { expr: inner, key, as_text } => Ok(Expr::JsonAccess {
            expr: Box::new(substitute_placeholders(inner, params)?),
            key: key.clone(),
            as_text: *as_text,
        }),
        Expr::In { expr: inner, values } => {
            let substituted_values: Result<Vec<Expr>> = values
                .iter()
                .map(|v| substitute_placeholders(v, params))
                .collect();
            Ok(Expr::In {
                expr: Box::new(substitute_placeholders(inner, params)?),
                values: substituted_values?,
            })
        }
        Expr::NotIn { expr: inner, values } => {
            let substituted_values: Result<Vec<Expr>> = values
                .iter()
                .map(|v| substitute_placeholders(v, params))
                .collect();
            Ok(Expr::NotIn {
                expr: Box::new(substitute_placeholders(inner, params)?),
                values: substituted_values?,
            })
        }
        Expr::FunctionCall { name, args } => {
            let substituted_args: Result<Vec<Expr>> = args
                .iter()
                .map(|a| substitute_placeholders(a, params))
                .collect();
            Ok(Expr::FunctionCall {
                name: name.clone(),
                args: substituted_args?,
            })
        }
    }
}
