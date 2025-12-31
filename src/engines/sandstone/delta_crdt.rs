//! Delta-CRDT Implementation for Eventual Consistency
//!
//! Implements delta-state CRDTs for conflict-free replication:
//! - Only propagates changes (deltas), not full state
//! - Commutative, associative, idempotent merge operations
//! - Guarantees strong eventual consistency
//! - Last-Write-Wins (LWW) conflict resolution

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::types::Value;

/// Represents a delta (incremental change) to a table
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeltaState {
    /// Table name
    pub table_name: String,
    /// Logical timestamp (Lamport clock)
    pub timestamp: u64,
    /// Row operations in this delta
    pub operations: Vec<DeltaOperation>,
}

/// Individual row operation in a delta
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum DeltaOperation {
    /// Insert or update a row (last-write-wins)
    Upsert {
        row_id: u64,
        values: Vec<Value>,
        timestamp: u64,
    },
    /// Delete a row (tombstone)
    Delete {
        row_id: u64,
        timestamp: u64,
    },
}

/// Delta-CRDT state tracker for a table
pub struct TableDeltaState {
    /// Current logical clock (Lamport timestamp)
    clock: u64,
    /// Pending deltas not yet flushed
    pending_deltas: Vec<DeltaState>,
    /// Last seen timestamps per row (for LWW conflict resolution)
    row_timestamps: HashMap<u64, u64>,
}

impl TableDeltaState {
    pub fn new() -> Self {
        Self {
            clock: 0,
            pending_deltas: Vec::new(),
            row_timestamps: HashMap::new(),
        }
    }
    
    /// Record a new operation and generate delta
    pub fn record_operation(&mut self, table_name: String, op: DeltaOperation) -> DeltaState {
        self.clock += 1;
        
        // Update row timestamp for conflict resolution
        match &op {
            DeltaOperation::Upsert { row_id, timestamp, .. } => {
                self.row_timestamps.insert(*row_id, *timestamp);
            }
            DeltaOperation::Delete { row_id, timestamp } => {
                self.row_timestamps.insert(*row_id, *timestamp);
            }
        }
        
        let delta = DeltaState {
            table_name,
            timestamp: self.clock,
            operations: vec![op],
        };
        
        self.pending_deltas.push(delta.clone());
        delta
    }
    
    /// Merge incoming delta (commutative, associative, idempotent)
    /// Returns operations that should be applied to storage
    pub fn merge_delta(&mut self, delta: DeltaState) -> Vec<DeltaOperation> {
        let mut applied_ops = Vec::new();
        
        for op in delta.operations {
            match &op {
                DeltaOperation::Upsert { row_id, timestamp, .. } => {
                    // Last-Write-Wins: only apply if timestamp is newer
                    if let Some(&existing_ts) = self.row_timestamps.get(row_id) {
                        if *timestamp <= existing_ts {
                            continue; // Skip older update (idempotent)
                        }
                    }
                    self.row_timestamps.insert(*row_id, *timestamp);
                    applied_ops.push(op);
                }
                DeltaOperation::Delete { row_id, timestamp } => {
                    if let Some(&existing_ts) = self.row_timestamps.get(row_id) {
                        if *timestamp <= existing_ts {
                            continue; // Skip older delete
                        }
                    }
                    self.row_timestamps.insert(*row_id, *timestamp);
                    applied_ops.push(op);
                }
            }
        }
        
        // Update clock to maintain causality (Lamport clock rule)
        if delta.timestamp > self.clock {
            self.clock = delta.timestamp;
        }
        
        applied_ops
    }
    
    /// Get all pending deltas and clear the buffer
    pub fn drain_pending_deltas(&mut self) -> Vec<DeltaState> {
        std::mem::take(&mut self.pending_deltas)
    }
    
    /// Get current logical clock value
    pub fn current_clock(&self) -> u64 {
        self.clock
    }
}

impl Default for TableDeltaState {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Value;
    
    #[test]
    fn test_lww_conflict_resolution() {
        let mut state = TableDeltaState::new();
        
        // Simulate concurrent updates to same row
        let delta1 = DeltaState {
            table_name: "test".to_string(),
            timestamp: 10,
            operations: vec![DeltaOperation::Upsert {
                row_id: 1,
                values: vec![Value::Integer(100)],
                timestamp: 10,
            }],
        };
        
        let delta2 = DeltaState {
            table_name: "test".to_string(),
            timestamp: 15,
            operations: vec![DeltaOperation::Upsert {
                row_id: 1,
                values: vec![Value::Integer(200)],
                timestamp: 15,
            }],
        };
        
        // Apply in any order - should converge
        let ops1 = state.merge_delta(delta1.clone());
        let ops2 = state.merge_delta(delta2.clone());
        
        // Later timestamp wins
        assert_eq!(ops1.len(), 1);
        assert_eq!(ops2.len(), 1);
        
        // Try reverse order - should get same result
        let mut state2 = TableDeltaState::new();
        state2.merge_delta(delta2);
        state2.merge_delta(delta1);
        
        assert_eq!(state.row_timestamps, state2.row_timestamps);
    }
    
    #[test]
    fn test_idempotent_merge() {
        let mut state = TableDeltaState::new();
        
        let delta = DeltaState {
            table_name: "test".to_string(),
            timestamp: 10,
            operations: vec![DeltaOperation::Upsert {
                row_id: 1,
                values: vec![Value::Integer(100)],
                timestamp: 10,
            }],
        };
        
        // Apply same delta multiple times
        let ops1 = state.merge_delta(delta.clone());
        let ops2 = state.merge_delta(delta.clone());
        let ops3 = state.merge_delta(delta);
        
        // First application succeeds
        assert_eq!(ops1.len(), 1);
        // Subsequent applications are no-ops (idempotent)
        assert_eq!(ops2.len(), 0);
        assert_eq!(ops3.len(), 0);
    }
    
    #[test]
    fn test_commutative_merge() {
        let mut state1 = TableDeltaState::new();
        let mut state2 = TableDeltaState::new();
        
        let delta_a = DeltaState {
            table_name: "test".to_string(),
            timestamp: 10,
            operations: vec![DeltaOperation::Upsert {
                row_id: 1,
                values: vec![Value::Integer(100)],
                timestamp: 10,
            }],
        };
        
        let delta_b = DeltaState {
            table_name: "test".to_string(),
            timestamp: 20,
            operations: vec![DeltaOperation::Upsert {
                row_id: 2,
                values: vec![Value::Integer(200)],
                timestamp: 20,
            }],
        };
        
        // Apply in different orders
        state1.merge_delta(delta_a.clone());
        state1.merge_delta(delta_b.clone());
        
        state2.merge_delta(delta_b);
        state2.merge_delta(delta_a);
        
        // Should converge to same state (commutative)
        assert_eq!(state1.row_timestamps, state2.row_timestamps);
        assert_eq!(state1.clock, state2.clock);
    }
    
    #[test]
    fn test_delete_tombstone() {
        let mut state = TableDeltaState::new();
        
        // Insert a row
        let insert_delta = DeltaState {
            table_name: "test".to_string(),
            timestamp: 10,
            operations: vec![DeltaOperation::Upsert {
                row_id: 1,
                values: vec![Value::Integer(100)],
                timestamp: 10,
            }],
        };
        
        // Delete the row
        let delete_delta = DeltaState {
            table_name: "test".to_string(),
            timestamp: 20,
            operations: vec![DeltaOperation::Delete {
                row_id: 1,
                timestamp: 20,
            }],
        };
        
        state.merge_delta(insert_delta);
        let delete_ops = state.merge_delta(delete_delta);
        
        assert_eq!(delete_ops.len(), 1);
        assert!(matches!(delete_ops[0], DeltaOperation::Delete { .. }));
    }
    
    #[test]
    fn test_lamport_clock_advancement() {
        let mut state = TableDeltaState::new();
        
        assert_eq!(state.current_clock(), 0);
        
        // Receive delta with higher timestamp
        let delta = DeltaState {
            table_name: "test".to_string(),
            timestamp: 100,
            operations: vec![DeltaOperation::Upsert {
                row_id: 1,
                values: vec![Value::Integer(1)],
                timestamp: 100,
            }],
        };
        
        state.merge_delta(delta);
        
        // Clock should advance to match received timestamp
        assert_eq!(state.current_clock(), 100);
    }
}
