//! Core types for MiniSQL
//!
//! Defines SQL data types, values, table schemas, and rows.

use crate::engines::EngineType;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use std::cmp::Ordering;
use std::fmt;
use std::hash::{Hash, Hasher};

/// SQL data types supported by MiniSQL
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataType {
    /// 64-bit signed integer
    Integer,
    /// 64-bit floating point
    Float,
    /// Variable-length string with optional max length
    Varchar(Option<u32>),
    /// Text (unlimited length string)
    Text,
    /// Boolean
    Boolean,
    /// JSON document
    Json,
}

impl fmt::Display for DataType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DataType::Integer => write!(f, "INTEGER"),
            DataType::Float => write!(f, "FLOAT"),
            DataType::Varchar(Some(len)) => write!(f, "VARCHAR({})", len),
            DataType::Varchar(None) => write!(f, "VARCHAR"),
            DataType::Text => write!(f, "TEXT"),
            DataType::Boolean => write!(f, "BOOLEAN"),
            DataType::Json => write!(f, "JSON"),
        }
    }
}

/// A SQL value
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    /// NULL value
    Null,
    /// Integer value
    Integer(i64),
    /// Float value
    Float(f64),
    /// String value
    String(String),
    /// Boolean value
    Boolean(bool),
    /// JSON value
    Json(JsonValue),
}

// Implement Hash for Value to enable efficient hash-based lookups
impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        // Hash the discriminant first to differentiate types
        std::mem::discriminant(self).hash(state);
        match self {
            Value::Null => {}
            Value::Integer(i) => i.hash(state),
            Value::Float(f) => f.to_bits().hash(state), // Use bits for consistent hashing
            Value::String(s) => s.hash(state),
            Value::Boolean(b) => b.hash(state),
            Value::Json(j) => j.to_string().hash(state), // Fallback to string for JSON
        }
    }
}

// Implement Eq for Value (required for HashMap keys)
// Note: We consider NaN == NaN for hashing purposes
impl Eq for Value {}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a == b,
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::Json(a), Value::Json(b)) => a == b,
            // Cross-type numeric comparison
            (Value::Integer(a), Value::Float(b)) => (*a as f64) == *b,
            (Value::Float(a), Value::Integer(b)) => *a == (*b as f64),
            // Affinity: String to Integer
            (Value::String(s), Value::Integer(i)) | (Value::Integer(i), Value::String(s)) => {
                if let Ok(parsed) = s.parse::<i64>() {
                    parsed == *i
                } else {
                    false
                }
            }
            // Affinity: String to Float
            (Value::String(s), Value::Float(f)) | (Value::Float(f), Value::String(s)) => {
                if let Ok(parsed) = s.parse::<f64>() {
                    parsed == *f
                } else {
                    false
                }
            }
            _ => false,
        }
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        match (self, other) {
            (Value::Null, Value::Null) => Some(Ordering::Equal),
            (Value::Null, _) => Some(Ordering::Less),
            (_, Value::Null) => Some(Ordering::Greater),
            (Value::Integer(a), Value::Integer(b)) => a.partial_cmp(b),
            (Value::Float(a), Value::Float(b)) => a.partial_cmp(b),
            (Value::String(a), Value::String(b)) => a.partial_cmp(b),
            (Value::Boolean(a), Value::Boolean(b)) => a.partial_cmp(b),
            // Cross-type numeric comparison
            (Value::Integer(a), Value::Float(b)) => (*a as f64).partial_cmp(b),
            (Value::Float(a), Value::Integer(b)) => a.partial_cmp(&(*b as f64)),
            // Affinity: String to Integer
            (Value::String(s), Value::Integer(i)) => {
                if let Ok(parsed) = s.parse::<i64>() {
                    parsed.partial_cmp(i)
                } else {
                    // If string cannot be parsed as number, it's greater than any number (SQLite style)
                    Some(Ordering::Greater)
                }
            }
            (Value::Integer(i), Value::String(s)) => {
                if let Ok(parsed) = s.parse::<i64>() {
                    i.partial_cmp(&parsed)
                } else {
                    // Number is less than any non-numeric string
                    Some(Ordering::Less)
                }
            }
            // Affinity: String to Float
            (Value::String(s), Value::Float(f)) => {
                if let Ok(parsed) = s.parse::<f64>() {
                    parsed.partial_cmp(f)
                } else {
                    Some(Ordering::Greater)
                }
            }
            (Value::Float(f), Value::String(s)) => {
                if let Ok(parsed) = s.parse::<f64>() {
                    f.partial_cmp(&parsed)
                } else {
                    Some(Ordering::Less)
                }
            }
            _ => None,
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Value::Null => write!(f, "NULL"),
            Value::Integer(v) => write!(f, "{}", v),
            Value::Float(v) => write!(f, "{}", v),
            Value::String(v) => write!(f, "{}", v),
            Value::Boolean(v) => write!(f, "{}", if *v { "TRUE" } else { "FALSE" }),
            Value::Json(v) => write!(f, "{}", v),
        }
    }
}

impl Value {
    /// Check if the value is NULL
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Check if the value is truthy (for WHERE clauses)
    pub fn is_truthy(&self) -> bool {
        match self {
            Value::Null => false,
            Value::Boolean(b) => *b,
            Value::Integer(i) => *i != 0,
            Value::Float(f) => *f != 0.0,
            Value::String(s) => !s.is_empty(),
            Value::Json(_) => true,
        }
    }

    /// Convert to string for display
    pub fn to_string(&self) -> String {
        match self {
            Value::Null => "NULL".to_string(),
            Value::Integer(v) => v.to_string(),
            Value::Float(v) => v.to_string(),
            Value::String(v) => v.clone(),
            Value::Boolean(v) => if *v { "TRUE".to_string() } else { "FALSE".to_string() },
            Value::Json(v) => v.to_string(),
        }
    }

    /// Get the value as a string for protocol encoding
    pub fn to_string_repr(&self) -> Option<String> {
        match self {
            Value::Null => None,
            Value::Integer(v) => Some(v.to_string()),
            Value::Float(v) => Some(v.to_string()),
            Value::String(v) => Some(v.clone()),
            Value::Boolean(v) => Some(if *v { "1".to_string() } else { "0".to_string() }),
            Value::Json(v) => Some(v.to_string()),
        }
    }

    /// Extract a field from a JSON value using the -> operator
    pub fn json_get(&self, key: &str) -> Value {
        match self {
            Value::Json(json) => {
                if let Some(obj) = json.as_object() {
                    if let Some(val) = obj.get(key) {
                        return Value::Json(val.clone());
                    }
                }
                // Also try array index access
                if let Ok(idx) = key.parse::<usize>() {
                    if let Some(arr) = json.as_array() {
                        if let Some(val) = arr.get(idx) {
                            return Value::Json(val.clone());
                        }
                    }
                }
                Value::Null
            }
            _ => Value::Null,
        }
    }

    /// Extract a field from a JSON value and return as text (->>) operator
    pub fn json_get_text(&self, key: &str) -> Value {
        match self.json_get(key) {
            Value::Json(json) => {
                match json {
                    JsonValue::String(s) => Value::String(s),
                    JsonValue::Null => Value::Null,
                    other => Value::String(other.to_string()),
                }
            }
            other => other,
        }
    }
}

/// A column definition in a table schema
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDef {
    /// Column name
    pub name: String,
    /// Column data type
    pub data_type: DataType,
    /// Whether the column allows NULL values
    pub nullable: bool,
    /// Default value (if any)
    pub default: Option<Value>,
    /// Whether this column is a primary key
    pub primary_key: bool,
    /// Whether this column auto-increments (only valid for INTEGER PRIMARY KEY)
    #[serde(default)]
    pub auto_increment: bool,
}

/// Table schema definition
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    /// Table name
    pub name: String,
    /// Column definitions
    pub columns: Vec<ColumnDef>,
    /// Current auto-increment value (next value to use)
    #[serde(default = "default_auto_increment")]
    pub auto_increment_counter: u64,
    /// Storage engine type for this table
    #[serde(default = "default_engine_type")]
    pub engine_type: EngineType,
}

/// Default value for auto_increment_counter (for backwards compatibility)
fn default_auto_increment() -> u64 {
    1
}

/// Default value for engine_type (for backwards compatibility)
/// Old tables without this field will default to Granite
fn default_engine_type() -> EngineType {
    EngineType::Granite
}

/// Index metadata
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexMetadata {
    /// Index name
    pub name: String,
    /// Table name this index belongs to
    pub table_name: String,
    /// Column names being indexed (for composite indexes, in order)
    pub columns: Vec<String>,
    /// Whether this is a unique index
    #[serde(default)]
    pub unique: bool,
    /// Whether this is a primary key index
    #[serde(default)]
    pub is_primary: bool,
}

impl IndexMetadata {
    /// Check if this index covers the given columns (exact match or prefix match)
    /// Returns Some(n) where n is the number of columns that match as a prefix
    pub fn matches_columns(&self, query_columns: &[String]) -> Option<usize> {
        if query_columns.is_empty() || self.columns.is_empty() {
            return None;
        }
        
        // Check if query_columns match the beginning of the index columns
        let mut matched = 0;
        for (i, idx_col) in self.columns.iter().enumerate() {
            if i < query_columns.len() && idx_col.eq_ignore_ascii_case(&query_columns[i]) {
                matched += 1;
            } else {
                break;
            }
        }
        
        if matched > 0 {
            Some(matched)
        } else {
            None
        }
    }
    
    /// Check if this index exactly covers all the given columns
    pub fn covers_columns_exactly(&self, query_columns: &[String]) -> bool {
        if query_columns.len() != self.columns.len() {
            return false;
        }
        self.columns.iter().zip(query_columns.iter()).all(|(a, b)| {
            a.eq_ignore_ascii_case(b)
        })
    }
    
    /// Check if the given columns can use this index (prefix match)
    /// Returns true if query_columns form a prefix of the index columns
    pub fn can_use_for_columns(&self, query_columns: &[String]) -> bool {
        if query_columns.is_empty() || query_columns.len() > self.columns.len() {
            return false;
        }
        
        query_columns.iter().zip(self.columns.iter()).all(|(q, i)| {
            q.eq_ignore_ascii_case(i)
        })
    }
}

impl TableSchema {
    /// Find a column by name, returning its index
    pub fn find_column(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|c| c.name.eq_ignore_ascii_case(name))
    }

    /// Get column names
    pub fn column_names(&self) -> Vec<&str> {
        self.columns.iter().map(|c| c.name.as_str()).collect()
    }

    /// Get primary key column indices
    pub fn primary_key_columns(&self) -> Vec<usize> {
        self.columns
            .iter()
            .enumerate()
            .filter(|(_, c)| c.primary_key)
            .map(|(i, _)| i)
            .collect()
    }

    /// Get the auto-increment column index, if any
    pub fn auto_increment_column(&self) -> Option<usize> {
        self.columns
            .iter()
            .position(|c| c.auto_increment)
    }

    /// Generate a primary key index name for this table
    pub fn primary_key_index_name(&self) -> String {
        format!("PRIMARY_{}", self.name)
    }
}

/// A row of data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Row {
    /// Internal row ID (for updates/deletes)
    pub id: u64,
    /// Column values in order matching table schema
    pub values: Vec<Value>,
}

impl Row {
    /// Create a new row with the given ID and values
    pub fn new(id: u64, values: Vec<Value>) -> Self {
        Self { id, values }
    }
}

/// Result set from a query
#[derive(Debug, Clone)]
pub struct ResultSet {
    /// Column names
    pub columns: Vec<String>,
    /// Column types
    pub column_types: Vec<DataType>,
    /// Rows of data
    pub rows: Vec<Vec<Value>>,
}

impl ResultSet {
    /// Create an empty result set
    pub fn empty() -> Self {
        Self {
            columns: vec![],
            column_types: vec![],
            rows: vec![],
        }
    }

    /// Create a result set with the given columns
    pub fn new(columns: Vec<String>, column_types: Vec<DataType>) -> Self {
        Self {
            columns,
            column_types,
            rows: vec![],
        }
    }

    /// Add a row to the result set
    pub fn add_row(&mut self, row: Vec<Value>) {
        self.rows.push(row);
    }
}

/// Query execution result
#[derive(Debug)]
pub enum QueryResult {
    /// SELECT query result
    Select(ResultSet),
    /// INSERT/UPDATE/DELETE result with affected row count and last insert ID
    Modified {
        rows_affected: u64,
        last_insert_id: u64,
    },
    /// DDL result (CREATE TABLE, etc.)
    Ok,
    /// Transaction started
    TransactionStarted,
    /// Transaction committed
    TransactionCommitted,
    /// Transaction rolled back
    TransactionRolledBack,
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_value_equality() {
        assert_eq!(Value::Integer(1), Value::Integer(1));
        assert_ne!(Value::Integer(1), Value::Integer(2));
        assert_eq!(Value::Float(1.0), Value::Float(1.0));
        assert_eq!(Value::String("hi".into()), Value::String("hi".into()));
        assert_eq!(Value::Boolean(true), Value::Boolean(true));
        assert_eq!(Value::Null, Value::Null);
        
        // Cross-type comparison
        assert_eq!(Value::Integer(1), Value::Float(1.0));
        assert_eq!(Value::Float(1.5), Value::Float(1.5));
        assert_ne!(Value::Integer(1), Value::Integer(2));
    }

    #[test]
    fn test_value_ordering() {
        assert!(Value::Integer(1) < Value::Integer(2));
        assert!(Value::Float(1.0) < Value::Float(2.0));
        assert!(Value::Integer(1) < Value::Float(1.5));
        assert!(Value::Null < Value::Integer(1));
    }

    #[test]
    fn test_json_get() {
        let val = Value::Json(json!({"a": 1, "b": {"c": 2}, "d": [10, 20]}));
        
        assert_eq!(val.json_get("a"), Value::Json(json!(1)));
        assert_eq!(val.json_get("b").json_get("c"), Value::Json(json!(2)));
        assert_eq!(val.json_get("d").json_get("1"), Value::Json(json!(20)));
        assert_eq!(val.json_get("nonexistent"), Value::Null);
    }

    #[test]
    fn test_json_get_text() {
        let val = Value::Json(json!({"a": "hello", "b": 123}));
        
        assert_eq!(val.json_get_text("a"), Value::String("hello".into()));
        assert_eq!(val.json_get_text("b"), Value::String("123".into()));
    }

    #[test]
    fn test_value_as_hashmap_key() {
        use std::collections::HashMap;
        
        let mut map: HashMap<Value, &str> = HashMap::new();
        
        // Test different value types as keys
        map.insert(Value::Integer(1), "one");
        map.insert(Value::Integer(2), "two");
        map.insert(Value::String("hello".into()), "greeting");
        map.insert(Value::Float(3.14), "pi");
        map.insert(Value::Boolean(true), "yes");
        map.insert(Value::Null, "nothing");
        
        // Verify lookups work
        assert_eq!(map.get(&Value::Integer(1)), Some(&"one"));
        assert_eq!(map.get(&Value::Integer(2)), Some(&"two"));
        assert_eq!(map.get(&Value::String("hello".into())), Some(&"greeting"));
        assert_eq!(map.get(&Value::Float(3.14)), Some(&"pi"));
        assert_eq!(map.get(&Value::Boolean(true)), Some(&"yes"));
        assert_eq!(map.get(&Value::Null), Some(&"nothing"));
        
        // Non-existent keys
        assert_eq!(map.get(&Value::Integer(99)), None);
    }
    
    #[test]
    fn test_type_affinity_integer_strict() {
        // Valid integer strings should match
        assert_eq!(Value::String("1".into()), Value::Integer(1));
        assert_eq!(Value::String("42".into()), Value::Integer(42));
        assert_eq!(Value::String("-5".into()), Value::Integer(-5));
        
        // Decimal strings should NOT match integers (security: prevent 1.5 matching 1)
        assert_ne!(Value::String("1.5".into()), Value::Integer(1));
        assert_ne!(Value::String("1.0".into()), Value::Integer(1));
        assert_ne!(Value::String("2.9".into()), Value::Integer(2));
        
        // Invalid formats should NOT match
        assert_ne!(Value::String(" 1 ".into()), Value::Integer(1)); // whitespace
        assert_ne!(Value::String("1abc".into()), Value::Integer(1)); // trailing chars
        assert_ne!(Value::String("abc1".into()), Value::Integer(1)); // leading chars
        assert_ne!(Value::String("".into()), Value::Integer(0)); // empty string
    }
    
    #[test]
    fn test_type_affinity_float() {
        // Valid float strings should match
        assert_eq!(Value::String("1.5".into()), Value::Float(1.5));
        assert_eq!(Value::String("3.14".into()), Value::Float(3.14));
        assert_eq!(Value::String("-2.5".into()), Value::Float(-2.5));
        
        // Integer strings should match float values
        assert_eq!(Value::String("1".into()), Value::Float(1.0));
        assert_eq!(Value::String("42".into()), Value::Float(42.0));
    }
}
