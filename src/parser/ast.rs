use crate::types::{DataType, Value};
use crate::engines::EngineType;

/// Top-level SQL statement
#[derive(Debug, Clone)]
pub enum Statement {
    /// CREATE TABLE statement
    CreateTable(CreateTableStmt),
    /// CREATE INDEX statement
    CreateIndex(CreateIndexStmt),
    /// DROP INDEX statement
    DropIndex(String),
    /// ALTER TABLE statement
    AlterTable(AlterTableStmt),
    /// SELECT statement
    Select(SelectStmt),
    /// INSERT statement
    Insert(InsertStmt),
    /// UPDATE statement
    Update(UpdateStmt),
    /// DELETE statement
    Delete(DeleteStmt),
    /// BEGIN transaction
    Begin,
    /// COMMIT transaction
    Commit,
    /// ROLLBACK transaction
    Rollback,
    /// SHOW TABLES
    ShowTables,
    /// DESCRIBE table
    Describe(String),
    /// DROP TABLE
    DropTable(String),
    /// TRUNCATE TABLE
    TruncateTable(String),
    /// CHECKPOINT
    Checkpoint,
    /// VACUUM
    Vacuum,
}

/// ALTER TABLE statement
#[derive(Debug, Clone)]
pub struct AlterTableStmt {
    pub table_name: String,
    pub action: AlterTableAction,
}

/// ALTER TABLE actions
#[derive(Debug, Clone)]
pub enum AlterTableAction {
    /// Change the storage engine
    ChangeEngine(EngineType),
}

/// CREATE INDEX statement
#[derive(Debug, Clone)]
pub struct CreateIndexStmt {
    pub index_name: String,
    pub table_name: String,
    /// Column names for the index (supports composite indexes with multiple columns)
    pub columns: Vec<String>,
    pub if_not_exists: bool,
}

/// CREATE TABLE statement
#[derive(Debug, Clone)]
pub struct CreateTableStmt {
    pub table_name: String,
    pub columns: Vec<ColumnDefAst>,
    pub if_not_exists: bool,
    pub engine: Option<EngineType>,
}

/// Column definition in CREATE TABLE
#[derive(Debug, Clone)]
pub struct ColumnDefAst {
    pub name: String,
    pub data_type: DataType,
    pub nullable: bool,
    pub primary_key: bool,
    pub auto_increment: bool,
    pub default: Option<Expr>,
}

/// SELECT statement
#[derive(Debug, Clone)]
pub struct SelectStmt {
    pub columns: Vec<SelectColumn>,
    pub from: Option<TableRef>,
    pub joins: Vec<JoinClause>,
    pub where_clause: Option<Expr>,
    pub group_by: Vec<Expr>,
    pub order_by: Vec<OrderByClause>,
    pub limit: Option<u64>,
}

/// ORDER BY clause
#[derive(Debug, Clone)]
pub struct OrderByClause {
    pub expr: Expr,
    pub direction: SortOrder,
}

/// Sort order (ascending or descending)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SortOrder {
    Asc,
    Desc,
}

/// Type of JOIN operation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinType {
    /// INNER JOIN - only matching rows
    Inner,
    /// LEFT JOIN - all left rows, NULLs for unmatched right
    Left,
}

/// A table reference with optional alias
#[derive(Debug, Clone)]
pub struct TableRef {
    /// Table name
    pub name: String,
    /// Optional alias (e.g., "users u" or "users AS u")
    pub alias: Option<String>,
}

impl TableRef {
    /// Get the effective name to use for this table (alias if present, otherwise name)
    pub fn effective_name(&self) -> &str {
        self.alias.as_deref().unwrap_or(&self.name)
    }
}

/// A JOIN clause
#[derive(Debug, Clone)]
pub struct JoinClause {
    /// Type of join
    pub join_type: JoinType,
    /// Table being joined
    pub table: TableRef,
    /// ON condition (e.g., t1.id = t2.user_id)
    pub on_condition: Expr,
}

/// A column in SELECT clause
#[derive(Debug, Clone)]
pub enum SelectColumn {
    /// All columns (*)
    Star,
    /// Qualified star (e.g., users.*)
    QualifiedStar { table: String },
    /// A specific expression, optionally aliased
    Expr { expr: Expr, alias: Option<String> },
}

/// INSERT statement
#[derive(Debug, Clone)]
pub struct InsertStmt {
    pub table_name: String,
    pub columns: Option<Vec<String>>,
    pub values: Vec<Vec<Expr>>,
}

/// UPDATE statement
#[derive(Debug, Clone)]
pub struct UpdateStmt {
    pub table_name: String,
    pub assignments: Vec<(String, Expr)>,
    pub where_clause: Option<Expr>,
}

/// DELETE statement
#[derive(Debug, Clone)]
pub struct DeleteStmt {
    pub table_name: String,
    pub where_clause: Option<Expr>,
}

/// Expression (for WHERE clauses, values, etc.)
#[derive(Debug, Clone)]
pub enum Expr {
    /// Literal value
    Literal(Value),
    /// Column reference, optionally qualified with table name/alias
    Column { table: Option<String>, name: String },
    /// Binary operation
    BinaryOp {
        left: Box<Expr>,
        op: BinaryOperator,
        right: Box<Expr>,
    },
    /// Unary NOT
    Not(Box<Expr>),
    /// IS NULL check
    IsNull(Box<Expr>),
    /// IS NOT NULL check
    IsNotNull(Box<Expr>),
    /// JSON field access (column->'key')
    JsonAccess {
        expr: Box<Expr>,
        key: String,
        as_text: bool, // true for ->>, false for ->
    },
    /// Function call
    FunctionCall {
        name: String,
        args: Vec<Expr>,
    },
    /// IN operator: expr IN (value1, value2, ...)
    In {
        expr: Box<Expr>,
        values: Vec<Expr>,
    },
    /// NOT IN operator: expr NOT IN (value1, value2, ...)
    NotIn {
        expr: Box<Expr>,
        values: Vec<Expr>,
    },
    /// Placeholder for prepared statement parameter (? in SQL)
    Placeholder(usize),
}

/// Binary operators
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum BinaryOperator {
    // Comparison
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
    // Logical
    And,
    Or,
    // Arithmetic
    Plus,
    Minus,
    Multiply,
    Divide,
    // String
    Like,
}
