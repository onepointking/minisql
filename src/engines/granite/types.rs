//! Core transaction types and state management


use super::log::LogRecord;

/// Log Sequence Number - unique identifier for each WAL record
pub type Lsn = u64;

/// Transaction ID
pub type TxnId = u64;

/// Transaction state
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TxnState {
    Active,
    Committed,
    Aborted,
}

/// In-memory transaction context
#[derive(Debug, Clone)]
pub struct Transaction {
    /// Transaction ID
    pub id: TxnId,
    /// Current state
    pub state: TxnState,
    /// LSN of the BEGIN record
    pub begin_lsn: Lsn,
    /// Operations for potential rollback
    pub undo_log: Vec<LogRecord>,
}

impl Transaction {
    pub fn new(id: TxnId, begin_lsn: Lsn) -> Self {
        Self {
            id,
            state: TxnState::Active,
            begin_lsn,
            undo_log: Vec::new(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_transaction_creation() {
        let txn = Transaction::new(1, 100);
        assert_eq!(txn.id, 1);
        assert_eq!(txn.begin_lsn, 100);
        assert_eq!(txn.state, TxnState::Active);
        assert!(txn.undo_log.is_empty());
    }

    #[test]
    fn test_transaction_state_transitions() {
        let mut txn = Transaction::new(1, 100);
        assert_eq!(txn.state, TxnState::Active);
        
        txn.state = TxnState::Committed;
        assert_eq!(txn.state, TxnState::Committed);
        
        txn.state = TxnState::Aborted;
        assert_eq!(txn.state, TxnState::Aborted);
    }
}
