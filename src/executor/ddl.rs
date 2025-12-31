use crate::error::{MiniSqlError, Result};
use crate::parser::{ColumnDefAst, CreateIndexStmt, CreateTableStmt};
use crate::types::{ColumnDef, DataType, IndexMetadata, QueryResult, TableSchema};
use crate::executor::{Executor, Session};
use crate::executor::evaluator;

impl Executor {
    /// Execute CREATE TABLE
    pub(crate) fn execute_create_table(
        &self,
        create: CreateTableStmt,
        session: &Session,
    ) -> Result<QueryResult> {
        // Validate AUTO_INCREMENT constraints
        self.validate_auto_increment_constraints(&create)?;

        // Convert AST column definitions to storage schema
        let columns: Vec<ColumnDef> = create
            .columns
            .iter()
            .map(|c| ColumnDef {
                name: c.name.clone(),
                data_type: c.data_type.clone(),
                nullable: c.nullable,
                default: c.default.as_ref().map(|e| evaluator::eval_const_expr(e, session.last_insert_id)).transpose().ok().flatten(),
                primary_key: c.primary_key,
                auto_increment: c.auto_increment,
            })
            .collect();

        let schema = TableSchema {
            name: create.table_name.clone(),
            columns,
            auto_increment_counter: 1,
            engine_type: create.engine.unwrap_or_default(),
        };

        // Log to WAL
        let txn_id = self.get_txn_id(session);
        self.txn_manager.log_create_table(txn_id, &schema)?;

        // Create the table
        self.storage.create_table(schema.clone(), create.if_not_exists)?;

        // Auto-create primary key index if there are primary key columns
        let pk_columns: Vec<&ColumnDef> = schema.columns.iter().filter(|c| c.primary_key).collect();
        if !pk_columns.is_empty() {
            let pk_index = IndexMetadata {
                name: schema.primary_key_index_name(),
                table_name: schema.name.clone(),
                columns: pk_columns.iter().map(|c| c.name.clone()).collect(),
                unique: true,
                is_primary: true,
            };
            // Create the primary key index (ignore if already exists for IF NOT EXISTS tables)
            let _ = self.storage.create_index(pk_index, true);
        }

        Ok(QueryResult::Ok)
    }

    /// Validate AUTO_INCREMENT constraints on a CREATE TABLE statement
    fn validate_auto_increment_constraints(&self, create: &CreateTableStmt) -> Result<()> {
        let auto_inc_cols: Vec<&ColumnDefAst> = create.columns.iter()
            .filter(|c| c.auto_increment)
            .collect();

        // Only one AUTO_INCREMENT column allowed
        if auto_inc_cols.len() > 1 {
            return Err(MiniSqlError::Syntax(
                "There can be only one AUTO_INCREMENT column per table".to_string()
            ));
        }

        // Validate each AUTO_INCREMENT column
        for col in auto_inc_cols {
            // Must be integer type
            if !matches!(col.data_type, DataType::Integer) {
                return Err(MiniSqlError::Syntax(format!(
                    "AUTO_INCREMENT column '{}' must be of type INTEGER",
                    col.name
                )));
            }

            // Must be part of a key (PRIMARY KEY or will need UNIQUE)
            if !col.primary_key {
                return Err(MiniSqlError::Syntax(format!(
                    "AUTO_INCREMENT column '{}' must be defined as a key (PRIMARY KEY or UNIQUE)",
                    col.name
                )));
            }
        }

        Ok(())
    }

    /// Execute DROP TABLE
    pub(crate) fn execute_drop_table(&self, table_name: &str, session: &Session) -> Result<QueryResult> {
        // Log to WAL
        let txn_id = self.get_txn_id(session);
        self.txn_manager.log_drop_table(txn_id, table_name)?;

        // Drop the table (this should also drop associated indexes)
        self.storage.drop_table(table_name)?;

        Ok(QueryResult::Ok)
    }

    /// Execute CREATE INDEX
    pub(crate) fn execute_create_index(
        &self,
        create: CreateIndexStmt,
        _session: &Session,
    ) -> Result<QueryResult> {
        let index_metadata = IndexMetadata {
            name: create.index_name.clone(),
            table_name: create.table_name.clone(),
            columns: create.columns.clone(),
            unique: false,
            is_primary: false,
        };

        // Create the index
        self.storage.create_index(index_metadata, create.if_not_exists)?;

        Ok(QueryResult::Ok)
    }

    /// Execute DROP INDEX
    pub(crate) fn execute_drop_index(&self, index_name: &str, _session: &Session) -> Result<QueryResult> {
        // Drop the index
        self.storage.drop_index(index_name)?;

        Ok(QueryResult::Ok)
    }

    /// Execute TRUNCATE TABLE
    pub(crate) fn execute_truncate_table(&self, table_name: &str, session: &Session) -> Result<QueryResult> {
        // Log to WAL
        let txn_id = self.get_txn_id(session);
        self.txn_manager.log_truncate_table(txn_id, table_name)?;

        // Truncate the table
        self.storage.truncate_table(table_name)?;

        Ok(QueryResult::Ok)
    }

    /// Execute ALTER TABLE
    pub(crate) fn execute_alter_table(
        &self,
        alter: crate::parser::AlterTableStmt,
        _session: &Session,
    ) -> Result<QueryResult> {
        use crate::parser::AlterTableAction;
        
        match alter.action {
            AlterTableAction::ChangeEngine(new_engine) => {
                // Get current schema
                let mut schema = self.storage.get_schema(&alter.table_name)?;
                let old_engine = schema.engine_type;
                
                // No-op if already using the target engine
                if old_engine == new_engine {
                    return Ok(QueryResult::Ok);
                }
                
                // Check that new engine is enabled
                if !self.handlers.contains_key(&new_engine) {
                    return Err(MiniSqlError::Table(
                        format!("Engine '{}' is not enabled", new_engine)
                    ));
                }
                
                // Migrate data: flush old engine, init new engine
                // This generic approach works for any engine combination
                if let Some(old_handler) = self.handlers.get(&old_engine) {
                    old_handler.flush(&alter.table_name)?;
                }
                if let Some(new_handler) = self.handlers.get(&new_engine) {
                    new_handler.init_table(&alter.table_name)?;
                }
                
                log::info!(
                    "Table '{}' migrated from {} to {}",
                    alter.table_name, old_engine, new_engine
                );
                
                // Update schema with new engine type
                schema.engine_type = new_engine;
                
                // Update the catalog
                self.storage.update_schema(&alter.table_name, schema)?;
                
                Ok(QueryResult::Ok)
            }
        }
    }
}
