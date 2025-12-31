//! Aggregate function accumulators for GROUP BY support
//!
//! This module provides accumulators for aggregate functions like COUNT, SUM, and AVG.
//! Each accumulator tracks state across multiple rows and produces a final aggregated value.

use crate::error::{MiniSqlError, Result};
use crate::parser::Expr;
use crate::types::Value;

/// Trait for aggregate function accumulators
pub trait AggregateAccumulator: Send {
    /// Add a value to the accumulator
    fn accumulate(&mut self, value: &Value) -> Result<()>;

    /// Return the final aggregated value
    fn finalize(&self) -> Value;

    /// Create a fresh copy for a new group
    fn clone_empty(&self) -> Box<dyn AggregateAccumulator>;
}

/// COUNT accumulator - counts rows or non-null values
pub struct CountAccumulator {
    count: i64,
    count_star: bool, // true = COUNT(*), false = COUNT(column)
}

impl CountAccumulator {
    pub fn new(count_star: bool) -> Self {
        Self {
            count: 0,
            count_star,
        }
    }
}

impl AggregateAccumulator for CountAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if self.count_star || !value.is_null() {
            self.count += 1;
        }
        Ok(())
    }

    fn finalize(&self) -> Value {
        Value::Integer(self.count)
    }

    fn clone_empty(&self) -> Box<dyn AggregateAccumulator> {
        Box::new(CountAccumulator::new(self.count_star))
    }
}

/// SUM accumulator - sums numeric values, ignores NULLs
pub struct SumAccumulator {
    sum: f64,
    has_value: bool,
    is_integer: bool,
}

impl SumAccumulator {
    pub fn new() -> Self {
        Self {
            sum: 0.0,
            has_value: false,
            is_integer: true,
        }
    }
}

impl Default for SumAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateAccumulator for SumAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        match value {
            Value::Null => Ok(()), // Ignore NULLs
            Value::Integer(i) => {
                self.sum += *i as f64;
                self.has_value = true;
                Ok(())
            }
            Value::Float(f) => {
                self.sum += f;
                self.has_value = true;
                self.is_integer = false;
                Ok(())
            }
            _ => Err(MiniSqlError::Type("SUM requires numeric values".into())),
        }
    }

    fn finalize(&self) -> Value {
        if !self.has_value {
            Value::Null
        } else if self.is_integer && self.sum.fract() == 0.0 {
            Value::Integer(self.sum as i64)
        } else {
            Value::Float(self.sum)
        }
    }

    fn clone_empty(&self) -> Box<dyn AggregateAccumulator> {
        Box::new(SumAccumulator::new())
    }
}

/// AVG accumulator - computes average of numeric values
pub struct AvgAccumulator {
    sum: f64,
    count: i64,
}

impl AvgAccumulator {
    pub fn new() -> Self {
        Self { sum: 0.0, count: 0 }
    }
}

impl Default for AvgAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateAccumulator for AvgAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        match value {
            Value::Null => Ok(()), // Ignore NULLs
            Value::Integer(i) => {
                self.sum += *i as f64;
                self.count += 1;
                Ok(())
            }
            Value::Float(f) => {
                self.sum += f;
                self.count += 1;
                Ok(())
            }
            _ => Err(MiniSqlError::Type("AVG requires numeric values".into())),
        }
    }

    fn finalize(&self) -> Value {
        if self.count == 0 {
            Value::Null
        } else {
            Value::Float(self.sum / self.count as f64)
        }
    }

    fn clone_empty(&self) -> Box<dyn AggregateAccumulator> {
        Box::new(AvgAccumulator::new())
    }
}

/// MIN accumulator - finds minimum value
pub struct MinAccumulator {
    min: Option<Value>,
}

impl MinAccumulator {
    pub fn new() -> Self {
        Self { min: None }
    }
}

impl Default for MinAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateAccumulator for MinAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }
        match &self.min {
            None => self.min = Some(value.clone()),
            Some(current) => {
                if value.partial_cmp(current) == Some(std::cmp::Ordering::Less) {
                    self.min = Some(value.clone());
                }
            }
        }
        Ok(())
    }

    fn finalize(&self) -> Value {
        self.min.clone().unwrap_or(Value::Null)
    }

    fn clone_empty(&self) -> Box<dyn AggregateAccumulator> {
        Box::new(MinAccumulator::new())
    }
}

/// MAX accumulator - finds maximum value
pub struct MaxAccumulator {
    max: Option<Value>,
}

impl MaxAccumulator {
    pub fn new() -> Self {
        Self { max: None }
    }
}

impl Default for MaxAccumulator {
    fn default() -> Self {
        Self::new()
    }
}

impl AggregateAccumulator for MaxAccumulator {
    fn accumulate(&mut self, value: &Value) -> Result<()> {
        if value.is_null() {
            return Ok(());
        }
        match &self.max {
            None => self.max = Some(value.clone()),
            Some(current) => {
                if value.partial_cmp(current) == Some(std::cmp::Ordering::Greater) {
                    self.max = Some(value.clone());
                }
            }
        }
        Ok(())
    }

    fn finalize(&self) -> Value {
        self.max.clone().unwrap_or(Value::Null)
    }

    fn clone_empty(&self) -> Box<dyn AggregateAccumulator> {
        Box::new(MaxAccumulator::new())
    }
}

/// Check if a function name is an aggregate function
pub fn is_aggregate_function(name: &str) -> bool {
    matches!(
        name.to_uppercase().as_str(),
        "COUNT" | "SUM" | "AVG" | "MIN" | "MAX"
    )
}

/// Check if an expression contains an aggregate function call
pub fn is_aggregate_expr(expr: &Expr) -> bool {
    match expr {
        Expr::FunctionCall { name, .. } => is_aggregate_function(name),
        Expr::BinaryOp { left, right, .. } => is_aggregate_expr(left) || is_aggregate_expr(right),
        Expr::Not(inner) | Expr::IsNull(inner) | Expr::IsNotNull(inner) => is_aggregate_expr(inner),
        Expr::JsonAccess { expr, .. } => is_aggregate_expr(expr),
        _ => false,
    }
}

/// Create an accumulator for a given aggregate function name
pub fn create_accumulator(name: &str, args: &[Expr]) -> Result<Box<dyn AggregateAccumulator>> {
    match name.to_uppercase().as_str() {
        "COUNT" => {
            let count_star = args.is_empty(); // COUNT(*) has no args
            Ok(Box::new(CountAccumulator::new(count_star)))
        }
        "SUM" => Ok(Box::new(SumAccumulator::new())),
        "AVG" => Ok(Box::new(AvgAccumulator::new())),
        "MIN" => Ok(Box::new(MinAccumulator::new())),
        "MAX" => Ok(Box::new(MaxAccumulator::new())),
        _ => Err(MiniSqlError::Syntax(format!(
            "Unknown aggregate function: {}",
            name
        ))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_count_star() {
        let mut acc = CountAccumulator::new(true);
        acc.accumulate(&Value::Integer(1)).unwrap();
        acc.accumulate(&Value::Null).unwrap();
        acc.accumulate(&Value::Integer(2)).unwrap();
        assert_eq!(acc.finalize(), Value::Integer(3)); // COUNT(*) counts NULLs
    }

    #[test]
    fn test_count_column() {
        let mut acc = CountAccumulator::new(false);
        acc.accumulate(&Value::Integer(1)).unwrap();
        acc.accumulate(&Value::Null).unwrap();
        acc.accumulate(&Value::Integer(2)).unwrap();
        assert_eq!(acc.finalize(), Value::Integer(2)); // COUNT(col) ignores NULLs
    }

    #[test]
    fn test_sum_integers() {
        let mut acc = SumAccumulator::new();
        acc.accumulate(&Value::Integer(10)).unwrap();
        acc.accumulate(&Value::Null).unwrap();
        acc.accumulate(&Value::Integer(20)).unwrap();
        acc.accumulate(&Value::Integer(30)).unwrap();
        assert_eq!(acc.finalize(), Value::Integer(60));
    }

    #[test]
    fn test_sum_floats() {
        let mut acc = SumAccumulator::new();
        acc.accumulate(&Value::Float(1.5)).unwrap();
        acc.accumulate(&Value::Float(2.5)).unwrap();
        assert_eq!(acc.finalize(), Value::Float(4.0));
    }

    #[test]
    fn test_sum_mixed() {
        let mut acc = SumAccumulator::new();
        acc.accumulate(&Value::Integer(10)).unwrap();
        acc.accumulate(&Value::Float(5.5)).unwrap();
        assert_eq!(acc.finalize(), Value::Float(15.5));
    }

    #[test]
    fn test_sum_empty() {
        let acc = SumAccumulator::new();
        assert_eq!(acc.finalize(), Value::Null);
    }

    #[test]
    fn test_avg() {
        let mut acc = AvgAccumulator::new();
        acc.accumulate(&Value::Integer(10)).unwrap();
        acc.accumulate(&Value::Null).unwrap();
        acc.accumulate(&Value::Integer(20)).unwrap();
        assert_eq!(acc.finalize(), Value::Float(15.0));
    }

    #[test]
    fn test_avg_empty() {
        let acc = AvgAccumulator::new();
        assert_eq!(acc.finalize(), Value::Null);
    }

    #[test]
    fn test_min() {
        let mut acc = MinAccumulator::new();
        acc.accumulate(&Value::Integer(30)).unwrap();
        acc.accumulate(&Value::Null).unwrap();
        acc.accumulate(&Value::Integer(10)).unwrap();
        acc.accumulate(&Value::Integer(20)).unwrap();
        assert_eq!(acc.finalize(), Value::Integer(10));
    }

    #[test]
    fn test_max() {
        let mut acc = MaxAccumulator::new();
        acc.accumulate(&Value::Integer(10)).unwrap();
        acc.accumulate(&Value::Null).unwrap();
        acc.accumulate(&Value::Integer(30)).unwrap();
        acc.accumulate(&Value::Integer(20)).unwrap();
        assert_eq!(acc.finalize(), Value::Integer(30));
    }

    #[test]
    fn test_is_aggregate_function() {
        assert!(is_aggregate_function("COUNT"));
        assert!(is_aggregate_function("count"));
        assert!(is_aggregate_function("SUM"));
        assert!(is_aggregate_function("AVG"));
        assert!(is_aggregate_function("MIN"));
        assert!(is_aggregate_function("MAX"));
        assert!(!is_aggregate_function("COALESCE"));
        assert!(!is_aggregate_function("JSON_EXTRACT"));
    }

    #[test]
    fn test_is_aggregate_expr() {
        let count_expr = Expr::FunctionCall {
            name: "COUNT".to_string(),
            args: vec![],
        };
        assert!(is_aggregate_expr(&count_expr));

        let column_expr = Expr::Column {
            table: None,
            name: "id".to_string(),
        };
        assert!(!is_aggregate_expr(&column_expr));
    }
}
