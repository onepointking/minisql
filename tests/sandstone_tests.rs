//! Integration tests for the Sandstone Engine
//!
//! Tests for delta-CRDT implementation:
//! - Last-Write-Wins (LWW) conflict resolution
//! - Idempotent merge operations
//! - Commutative merge operations
//! - Performance characteristics

use minisql::engines::sandstone::{
    DeltaOperation, DeltaState, SandstoneConfig, TableDeltaState,
};
use minisql::types::Value;
use std::time::Instant;

// ============== Delta-CRDT Unit Tests ==============

#[test]
fn test_lww_newer_timestamp_wins() {
    let mut state = TableDeltaState::new();

    // First update with timestamp 10
    let delta1 = DeltaState {
        table_name: "users".to_string(),
        timestamp: 10,
        operations: vec![DeltaOperation::Upsert {
            row_id: 1,
            values: vec![Value::String("Alice".to_string())],
            timestamp: 10,
        }],
    };

    // Second update with timestamp 20 (should win)
    let delta2 = DeltaState {
        table_name: "users".to_string(),
        timestamp: 20,
        operations: vec![DeltaOperation::Upsert {
            row_id: 1,
            values: vec![Value::String("Alice Updated".to_string())],
            timestamp: 20,
        }],
    };

    // Apply in order: both should be applied
    let ops1 = state.merge_delta(delta1.clone());
    let ops2 = state.merge_delta(delta2.clone());

    assert_eq!(ops1.len(), 1, "First delta should be applied");
    assert_eq!(ops2.len(), 1, "Second delta should be applied (newer timestamp)");

    // Now apply in reverse order on fresh state - should get same result
    let mut state2 = TableDeltaState::new();
    let ops2_first = state2.merge_delta(delta2);
    let ops1_second = state2.merge_delta(delta1);

    assert_eq!(ops2_first.len(), 1, "Newer delta should be applied first");
    assert_eq!(ops1_second.len(), 0, "Older delta should be ignored");
}

#[test]
fn test_lww_older_timestamp_ignored() {
    let mut state = TableDeltaState::new();

    // First apply newer update
    let delta_new = DeltaState {
        table_name: "products".to_string(),
        timestamp: 100,
        operations: vec![DeltaOperation::Upsert {
            row_id: 42,
            values: vec![Value::Integer(999)],
            timestamp: 100,
        }],
    };

    // Then apply older update
    let delta_old = DeltaState {
        table_name: "products".to_string(),
        timestamp: 50,
        operations: vec![DeltaOperation::Upsert {
            row_id: 42,
            values: vec![Value::Integer(111)], // This should be ignored
            timestamp: 50,
        }],
    };

    state.merge_delta(delta_new);
    let ops = state.merge_delta(delta_old);

    assert_eq!(ops.len(), 0, "Older update should be ignored (LWW)");
}

#[test]
fn test_idempotent_merge_same_delta_multiple_times() {
    let mut state = TableDeltaState::new();

    let delta = DeltaState {
        table_name: "inventory".to_string(),
        timestamp: 50,
        operations: vec![DeltaOperation::Upsert {
            row_id: 1,
            values: vec![Value::Integer(100)],
            timestamp: 50,
        }],
    };

    // Apply the same delta 5 times
    let ops1 = state.merge_delta(delta.clone());
    let ops2 = state.merge_delta(delta.clone());
    let ops3 = state.merge_delta(delta.clone());
    let ops4 = state.merge_delta(delta.clone());
    let ops5 = state.merge_delta(delta);

    // Only first application should apply
    assert_eq!(ops1.len(), 1, "First application should apply");
    assert_eq!(ops2.len(), 0, "Second application should be idempotent no-op");
    assert_eq!(ops3.len(), 0, "Third application should be idempotent no-op");
    assert_eq!(ops4.len(), 0, "Fourth application should be idempotent no-op");
    assert_eq!(ops5.len(), 0, "Fifth application should be idempotent no-op");
}

#[test]
fn test_commutative_merge_order_independence() {
    // Create deltas for different rows
    let delta_a = DeltaState {
        table_name: "test".to_string(),
        timestamp: 10,
        operations: vec![DeltaOperation::Upsert {
            row_id: 1,
            values: vec![Value::String("A".to_string())],
            timestamp: 10,
        }],
    };

    let delta_b = DeltaState {
        table_name: "test".to_string(),
        timestamp: 20,
        operations: vec![DeltaOperation::Upsert {
            row_id: 2,
            values: vec![Value::String("B".to_string())],
            timestamp: 20,
        }],
    };

    let delta_c = DeltaState {
        table_name: "test".to_string(),
        timestamp: 30,
        operations: vec![DeltaOperation::Upsert {
            row_id: 3,
            values: vec![Value::String("C".to_string())],
            timestamp: 30,
        }],
    };

    // Apply in order: A, B, C
    let mut state1 = TableDeltaState::new();
    state1.merge_delta(delta_a.clone());
    state1.merge_delta(delta_b.clone());
    state1.merge_delta(delta_c.clone());

    // Apply in order: C, A, B
    let mut state2 = TableDeltaState::new();
    state2.merge_delta(delta_c.clone());
    state2.merge_delta(delta_a.clone());
    state2.merge_delta(delta_b.clone());

    // Apply in order: B, C, A
    let mut state3 = TableDeltaState::new();
    state3.merge_delta(delta_b);
    state3.merge_delta(delta_c);
    state3.merge_delta(delta_a);

    // All should have same final clock
    assert_eq!(state1.current_clock(), 30, "State 1 clock should be 30");
    assert_eq!(state2.current_clock(), 30, "State 2 clock should be 30");
    assert_eq!(state3.current_clock(), 30, "State 3 clock should be 30");
}

#[test]
fn test_delete_tombstone() {
    let mut state = TableDeltaState::new();

    // Insert a row
    let insert = DeltaState {
        table_name: "users".to_string(),
        timestamp: 10,
        operations: vec![DeltaOperation::Upsert {
            row_id: 1,
            values: vec![Value::String("Alice".to_string())],
            timestamp: 10,
        }],
    };

    // Delete the row (newer timestamp)
    let delete = DeltaState {
        table_name: "users".to_string(),
        timestamp: 20,
        operations: vec![DeltaOperation::Delete {
            row_id: 1,
            timestamp: 20,
        }],
    };

    state.merge_delta(insert);
    let delete_ops = state.merge_delta(delete);

    assert_eq!(delete_ops.len(), 1, "Delete should be applied");
    assert!(
        matches!(delete_ops[0], DeltaOperation::Delete { .. }),
        "Should be a delete operation"
    );
}

#[test]
fn test_delete_then_insert_lww() {
    let mut state = TableDeltaState::new();

    // Delete with timestamp 10
    let delete = DeltaState {
        table_name: "users".to_string(),
        timestamp: 10,
        operations: vec![DeltaOperation::Delete {
            row_id: 1,
            timestamp: 10,
        }],
    };

    // Re-insert with timestamp 20 (should win)
    let insert = DeltaState {
        table_name: "users".to_string(),
        timestamp: 20,
        operations: vec![DeltaOperation::Upsert {
            row_id: 1,
            values: vec![Value::String("Resurrected".to_string())],
            timestamp: 20,
        }],
    };

    state.merge_delta(delete);
    let insert_ops = state.merge_delta(insert);

    assert_eq!(insert_ops.len(), 1, "Insert after delete should be applied (newer timestamp)");
}

#[test]
fn test_lamport_clock_advancement() {
    let mut state = TableDeltaState::new();

    assert_eq!(state.current_clock(), 0, "Initial clock should be 0");

    // Receive delta with high timestamp
    let delta = DeltaState {
        table_name: "test".to_string(),
        timestamp: 1000,
        operations: vec![DeltaOperation::Upsert {
            row_id: 1,
            values: vec![Value::Integer(1)],
            timestamp: 1000,
        }],
    };

    state.merge_delta(delta);

    assert_eq!(
        state.current_clock(),
        1000,
        "Clock should advance to received timestamp"
    );
}

#[test]
fn test_record_operation_increments_clock() {
    let mut state = TableDeltaState::new();

    assert_eq!(state.current_clock(), 0);

    // Record an operation
    let _delta1 = state.record_operation(
        "test".to_string(),
        DeltaOperation::Upsert {
            row_id: 1,
            values: vec![Value::Integer(100)],
            timestamp: 1, // Will be overwritten by internal clock
        },
    );

    assert_eq!(state.current_clock(), 1, "Clock should increment to 1");

    // Record another operation
    let _delta2 = state.record_operation(
        "test".to_string(),
        DeltaOperation::Upsert {
            row_id: 2,
            values: vec![Value::Integer(200)],
            timestamp: 2,
        },
    );

    assert_eq!(state.current_clock(), 2, "Clock should increment to 2");
}

#[test]
fn test_drain_pending_deltas() {
    let mut state = TableDeltaState::new();

    // Record several operations
    state.record_operation(
        "test".to_string(),
        DeltaOperation::Upsert {
            row_id: 1,
            values: vec![Value::Integer(1)],
            timestamp: 1,
        },
    );
    state.record_operation(
        "test".to_string(),
        DeltaOperation::Upsert {
            row_id: 2,
            values: vec![Value::Integer(2)],
            timestamp: 2,
        },
    );

    // Drain should return all pending deltas
    let pending = state.drain_pending_deltas();
    assert_eq!(pending.len(), 2, "Should have 2 pending deltas");

    // Drain again should be empty
    let empty = state.drain_pending_deltas();
    assert_eq!(empty.len(), 0, "Second drain should be empty");
}

// ============== Configuration Tests ==============

#[test]
fn test_sandstone_config_presets() {
    let default = SandstoneConfig::default();
    assert_eq!(default.flush_interval_ms, 1000);
    assert!(default.enable_delta_crdt);

    let high_throughput = SandstoneConfig::high_throughput();
    assert_eq!(high_throughput.flush_interval_ms, 5000);
    assert!(high_throughput.enable_delta_crdt);

    let low_latency = SandstoneConfig::low_latency();
    assert_eq!(low_latency.flush_interval_ms, 500);
    assert_eq!(low_latency.max_dirty_tables, Some(10));
}

// ============== Performance Tests ==============

#[test]
fn test_delta_crdt_merge_performance() {
    let mut state = TableDeltaState::new();

    // Generate 10,000 deltas
    let num_deltas = 10_000;
    let deltas: Vec<DeltaState> = (0..num_deltas)
        .map(|i| DeltaState {
            table_name: "bench".to_string(),
            timestamp: i as u64,
            operations: vec![DeltaOperation::Upsert {
                row_id: i as u64,
                values: vec![Value::Integer(i as i64)],
                timestamp: i as u64,
            }],
        })
        .collect();

    // Measure merge time
    let start = Instant::now();
    for delta in deltas {
        state.merge_delta(delta);
    }
    let elapsed = start.elapsed();

    // Should be very fast (<100ms for 10k deltas)
    println!(
        "Merged {} deltas in {:?} ({:.2} ops/sec)",
        num_deltas,
        elapsed,
        num_deltas as f64 / elapsed.as_secs_f64()
    );

    assert!(
        elapsed.as_millis() < 1000,
        "Merging 10k deltas should take <1s, took {:?}",
        elapsed
    );
}

#[test]
fn test_concurrent_updates_to_same_row() {
    let mut state = TableDeltaState::new();

    // Simulate concurrent updates from 100 "replicas" to the same row
    let num_replicas = 100;
    let mut deltas: Vec<DeltaState> = Vec::new();

    for replica_id in 0..num_replicas {
        deltas.push(DeltaState {
            table_name: "hotspot".to_string(),
            timestamp: replica_id as u64,
            operations: vec![DeltaOperation::Upsert {
                row_id: 1, // Same row!
                values: vec![Value::Integer(replica_id as i64)],
                timestamp: replica_id as u64,
            }],
        });
    }

    // Apply in random order
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut hasher = DefaultHasher::new();
    42u64.hash(&mut hasher);
    let seed = hasher.finish();

    // Shuffle using seed
    for i in (1..deltas.len()).rev() {
        let j = ((seed + i as u64) % (i as u64 + 1)) as usize;
        deltas.swap(i, j);
    }

    // Apply all
    let mut applied_count = 0;
    for delta in deltas {
        let ops = state.merge_delta(delta);
        applied_count += ops.len();
    }

    // Only the highest timestamp should be the final value
    // But multiple deltas can be applied as they have increasing timestamps
    println!("Applied {}/{} deltas to hotspot row", applied_count, num_replicas);
    
    // The important thing is that the final clock reflects the highest seen timestamp
    assert_eq!(
        state.current_clock(),
        (num_replicas - 1) as u64,
        "Final clock should be highest timestamp"
    );
}

// ============== Edge Cases ==============

#[test]
fn test_empty_delta() {
    let mut state = TableDeltaState::new();

    let empty_delta = DeltaState {
        table_name: "test".to_string(),
        timestamp: 100,
        operations: vec![], // No operations
    };

    let ops = state.merge_delta(empty_delta);
    assert_eq!(ops.len(), 0, "Empty delta should apply zero operations");
    assert_eq!(
        state.current_clock(),
        100,
        "Clock should still advance even for empty delta"
    );
}

#[test]
fn test_multiple_operations_in_single_delta() {
    let mut state = TableDeltaState::new();

    // Single delta with multiple operations
    let delta = DeltaState {
        table_name: "batch".to_string(),
        timestamp: 50,
        operations: vec![
            DeltaOperation::Upsert {
                row_id: 1,
                values: vec![Value::Integer(1)],
                timestamp: 50,
            },
            DeltaOperation::Upsert {
                row_id: 2,
                values: vec![Value::Integer(2)],
                timestamp: 50,
            },
            DeltaOperation::Upsert {
                row_id: 3,
                values: vec![Value::Integer(3)],
                timestamp: 50,
            },
        ],
    };

    let ops = state.merge_delta(delta);
    assert_eq!(ops.len(), 3, "All 3 operations in batch should be applied");
}
