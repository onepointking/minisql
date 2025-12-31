//! Error types for MiniSQL
//!
//! Defines a unified error type that can represent errors from all components.
//! Error messages are formatted to be MySQL-compatible.

use std::fmt;
use std::io;

/// Context for where a column reference appears in a query
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ColumnContext {
    /// In SELECT field list
    FieldList,
    /// In WHERE clause
    WhereClause,
    /// In ORDER BY clause
    OrderClause,
    /// In GROUP BY clause  
    GroupByClause,
    /// In HAVING clause
    HavingClause,
    /// In ON clause (joins)
    OnClause,
    /// In INSERT column list
    InsertList,
    /// In UPDATE SET clause
    UpdateClause,
    /// Generic/unknown context
    General,
}

impl ColumnContext {
    /// Get the MySQL-style clause name
    pub fn as_mysql_str(&self) -> &'static str {
        match self {
            ColumnContext::FieldList => "field list",
            ColumnContext::WhereClause => "where clause",
            ColumnContext::OrderClause => "order clause",
            ColumnContext::GroupByClause => "group statement",
            ColumnContext::HavingClause => "having clause",
            ColumnContext::OnClause => "on clause",
            ColumnContext::InsertList => "field list",
            ColumnContext::UpdateClause => "field list",
            ColumnContext::General => "field list",
        }
    }
}

/// Unified error type for MiniSQL operations
#[derive(Debug)]
pub enum MiniSqlError {
    /// I/O error (file operations, network)
    Io(io::Error),
    /// SQL syntax error
    Syntax(String),
    /// Table-related error (not found, already exists, etc.)
    Table(String),
    /// Column-related error
    Column(String),
    /// Type error (wrong type for operation)
    Type(String),
    /// Transaction error
    Transaction(String),
    /// Constraint violation
    Constraint(String),
    /// JSON-related error
    Json(String),
    /// Protocol error (MySQL wire protocol)
    Protocol(String),
    /// Authentication error
    Auth(String),
    /// Generic internal error
    Internal(String),
}

/// Default database name for error messages
const DEFAULT_DATABASE: &str = "minisql";

impl MiniSqlError {
    /// Create a MySQL-compatible "table not found" error
    /// MySQL format: Table 'database.table' doesn't exist
    pub fn table_not_found(table_name: &str) -> Self {
        MiniSqlError::Table(format!(
            "Table '{}.{}' doesn't exist",
            DEFAULT_DATABASE, table_name
        ))
    }

    /// Create a MySQL-compatible "table already exists" error
    /// MySQL format: Table 'table' already exists
    pub fn table_already_exists(table_name: &str) -> Self {
        MiniSqlError::Table(format!("Table '{}' already exists", table_name))
    }

    /// Create a MySQL-compatible "unknown column" error
    /// MySQL format: Unknown column 'column' in 'field list'
    pub fn unknown_column(column_name: &str, context: ColumnContext) -> Self {
        MiniSqlError::Column(format!(
            "Unknown column '{}' in '{}'",
            column_name,
            context.as_mysql_str()
        ))
    }

    /// Create a MySQL-compatible "unknown column" error with table qualifier
    /// MySQL format: Unknown column 'table.column' in 'field list'
    pub fn unknown_column_qualified(table: &str, column: &str, context: ColumnContext) -> Self {
        MiniSqlError::Column(format!(
            "Unknown column '{}.{}' in '{}'",
            table,
            column,
            context.as_mysql_str()
        ))
    }

    /// Create a MySQL-compatible "unknown table" error for use in column resolution
    /// MySQL format: Unknown table 'table' in field list
    pub fn unknown_table_in_field_list(table_name: &str) -> Self {
        MiniSqlError::Column(format!("Unknown table '{}' in field list", table_name))
    }

    /// Create a MySQL-compatible "ambiguous column" error
    /// MySQL format: Column 'column' in field list is ambiguous
    pub fn ambiguous_column(column_name: &str, context: ColumnContext) -> Self {
        MiniSqlError::Column(format!(
            "Column '{}' in {} is ambiguous",
            column_name,
            context.as_mysql_str()
        ))
    }

    /// Create a MySQL-compatible "column count mismatch" error
    /// MySQL format: Column count doesn't match value count at row N
    pub fn column_count_mismatch(expected: usize, got: usize) -> Self {
        MiniSqlError::Column(format!(
            "Column count doesn't match value count (expected {}, got {})",
            expected, got
        ))
    }

    /// Create a MySQL-compatible "duplicate table alias" error
    /// MySQL error 1066: Not unique table/alias: '%s'
    pub fn duplicate_table_alias(alias: &str) -> Self {
        MiniSqlError::Table(format!(
            "Not unique table/alias: '{}'",
            alias
        ))
    }
}

impl fmt::Display for MiniSqlError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // MySQL-compatible error messages: just display the message directly
        // without prefixing with error type (clients already know the type from error code)
        match self {
            MiniSqlError::Io(e) => write!(f, "{}", e),
            MiniSqlError::Syntax(msg) => write!(f, "{}", msg),
            MiniSqlError::Table(msg) => write!(f, "{}", msg),
            MiniSqlError::Column(msg) => write!(f, "{}", msg),
            MiniSqlError::Type(msg) => write!(f, "{}", msg),
            MiniSqlError::Transaction(msg) => write!(f, "{}", msg),
            MiniSqlError::Constraint(msg) => write!(f, "{}", msg),
            MiniSqlError::Json(msg) => write!(f, "{}", msg),
            MiniSqlError::Protocol(msg) => write!(f, "{}", msg),
            MiniSqlError::Auth(msg) => write!(f, "{}", msg),
            MiniSqlError::Internal(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for MiniSqlError {}

impl From<io::Error> for MiniSqlError {
    fn from(e: io::Error) -> Self {
        MiniSqlError::Io(e)
    }
}

impl From<serde_json::Error> for MiniSqlError {
    fn from(e: serde_json::Error) -> Self {
        MiniSqlError::Json(e.to_string())
    }
}

/// Result type alias for MiniSQL operations
pub type Result<T> = std::result::Result<T, MiniSqlError>;

/// MySQL error codes (subset for our implementation)
#[allow(dead_code)]
pub mod mysql_error_codes {
    pub const ER_PARSE_ERROR: u16 = 1064;
    pub const ER_NO_SUCH_TABLE: u16 = 1146;
    pub const ER_TABLE_EXISTS_ERROR: u16 = 1050;
    pub const ER_BAD_FIELD_ERROR: u16 = 1054;
    pub const ER_ACCESS_DENIED_ERROR: u16 = 1045;
    pub const ER_UNKNOWN_COM_ERROR: u16 = 1047;
    pub const ER_NON_UNIQ_TABLE: u16 = 1066;
    pub const ER_LOCK_WAIT_TIMEOUT: u16 = 1205;
    pub const ER_LOCK_DEADLOCK: u16 = 1213;
}

impl MiniSqlError {
    /// Get the MySQL error code for this error
    pub fn mysql_error_code(&self) -> u16 {
        match self {
            MiniSqlError::Syntax(_) => mysql_error_codes::ER_PARSE_ERROR,
            MiniSqlError::Table(msg) if msg.contains("doesn't exist") => mysql_error_codes::ER_NO_SUCH_TABLE,
            MiniSqlError::Table(msg) if msg.contains("Not unique table/alias") => mysql_error_codes::ER_NON_UNIQ_TABLE,
            MiniSqlError::Table(_) => mysql_error_codes::ER_TABLE_EXISTS_ERROR,
            MiniSqlError::Column(_) => mysql_error_codes::ER_BAD_FIELD_ERROR,
            MiniSqlError::Auth(_) => mysql_error_codes::ER_ACCESS_DENIED_ERROR,
            MiniSqlError::Transaction(msg) if msg.contains("timeout") => mysql_error_codes::ER_LOCK_WAIT_TIMEOUT,
            MiniSqlError::Transaction(msg) if msg.contains("deadlock") => mysql_error_codes::ER_LOCK_DEADLOCK,
            _ => mysql_error_codes::ER_UNKNOWN_COM_ERROR,
        }
    }

    /// Get the SQL state for this error
    pub fn sql_state(&self) -> &'static str {
        match self {
            MiniSqlError::Syntax(_) => "42000",
            MiniSqlError::Table(_) => "42S02",
            MiniSqlError::Column(_) => "42S22",
            MiniSqlError::Auth(_) => "28000",
            MiniSqlError::Transaction(_) => "40001",
            _ => "HY000",
        }
    }
}
