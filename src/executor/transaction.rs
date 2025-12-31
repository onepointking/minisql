use crate::error::{MiniSqlError, Result};
use crate::types::QueryResult;
use crate::executor::{Executor, Session};
use crate::engines::granite::TxnId;

impl Executor {
    /// Begin a transaction
    pub(crate) fn execute_begin(&self, session: &mut Session) -> Result<QueryResult> {
        if session.txn_id.is_some() {
            return Err(MiniSqlError::Transaction(
                "Transaction already in progress".into(),
            ));
        }

        let txn_id = self.txn_manager.begin()?;
        session.txn_id = Some(txn_id);
        Ok(QueryResult::TransactionStarted)
    }

    /// Commit a transaction
    /// 
    /// Routes the commit to each engine that was modified during the transaction.
    /// Each engine decides whether it has durable work to commit.
    pub(crate) fn execute_commit(&self, session: &mut Session) -> Result<QueryResult> {
        let txn_id = session.txn_id.take().ok_or_else(|| {
            MiniSqlError::Transaction("No transaction in progress".into())
        })?;

        // Take the set of modified engines from the session
        let modified_engines = std::mem::take(&mut session.modified_engines);

        // Ask each modified engine to commit
        // Engines will decide if they have work to do (e.g., WAL records to fsync)
        for engine_type in &modified_engines {
            if let Some(handler) = self.handlers.get(engine_type) {
                handler.commit_transaction(txn_id)?;
            }
        }

        // Clean up transaction state in the transaction manager
        // This is a lightweight operation if no WAL work was done
        self.txn_manager.finalize_commit(txn_id, &self.storage)?;

        Ok(QueryResult::TransactionCommitted)
    }

    /// Rollback a transaction
    pub(crate) fn execute_rollback(&self, session: &mut Session) -> Result<QueryResult> {
        let txn_id = session.txn_id.take().ok_or_else(|| {
            MiniSqlError::Transaction("No transaction in progress".into())
        })?;

        // Take the set of modified engines from the session
        let modified_engines = std::mem::take(&mut session.modified_engines);

        // Ask each modified engine to rollback
        for engine_type in &modified_engines {
            if let Some(handler) = self.handlers.get(engine_type) {
                handler.rollback_transaction(txn_id)?;
            }
        }

        self.txn_manager.rollback(txn_id, &self.storage)?;
        Ok(QueryResult::TransactionRolledBack)
    }

    /// Execute CHECKPOINT command
    pub(crate) fn execute_checkpoint(&self, _session: &Session) -> Result<QueryResult> {
        self.txn_manager.checkpoint(&self.storage)?;
        Ok(QueryResult::Ok)
    }

    /// Execute VACUUM command
    pub(crate) fn execute_vacuum(&self, session: &Session) -> Result<QueryResult> {
        // Vacuum should not be run inside a transaction
        if session.txn_id.is_some() {
            return Err(MiniSqlError::Transaction(
                "VACUUM cannot be run inside a transaction".into(),
            ));
        }

        // Perform the vacuum operation
        self.storage.vacuum()?;

        Ok(QueryResult::Ok)
    }

    /// Get the current transaction ID or use auto-commit
    pub(crate) fn get_txn_id(&self, session: &Session) -> TxnId {
        session.txn_id.unwrap_or_else(|| self.txn_manager.auto_commit_txn())
    }
}
