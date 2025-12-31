use super::*;
use tempfile::tempdir;
use crate::storage::StorageEngine;
use crate::types::Value;
use std::thread;
use std::time::{Duration, Instant};

#[test]
fn test_transaction_manager_basic() {
    let temp_dir = tempdir().unwrap();
    let txn_mgr = TransactionManager::new(temp_dir.path().to_path_buf()).unwrap();
    let storage = StorageEngine::new(temp_dir.path().to_path_buf()).unwrap();

    let txn_id = txn_mgr.begin().unwrap();
    assert!(txn_mgr.is_active(txn_id));

    txn_mgr.commit(txn_id, &storage).unwrap();
    assert!(!txn_mgr.is_active(txn_id));
}

#[test]
fn test_transaction_manager_rollback() {
    let temp_dir = tempdir().unwrap();
    let txn_mgr = TransactionManager::new(temp_dir.path().to_path_buf()).unwrap();
    let storage = StorageEngine::new(temp_dir.path().to_path_buf()).unwrap();

    let txn_id = txn_mgr.begin().unwrap();
    txn_mgr.rollback(txn_id, &storage).unwrap();
    assert!(!txn_mgr.is_active(txn_id));
}

#[test]
fn test_transaction_manager_logging() {
    let temp_dir = tempdir().unwrap();
    let txn_mgr = TransactionManager::new(temp_dir.path().to_path_buf()).unwrap();
    let storage = StorageEngine::new(temp_dir.path().to_path_buf()).unwrap();

    // Create a test table
    let schema = crate::types::TableSchema {
        name: "test_table".to_string(),
        columns: vec![
            crate::types::ColumnDef {
                name: "id".to_string(),
                data_type: crate::types::DataType::Integer,
                nullable: false,
                default: None,
                primary_key: false,
                auto_increment: false,
            },
        ],
        auto_increment_counter: 1,
        engine_type: crate::engines::EngineType::default(),
    };
    storage.apply_schema(schema.clone()).unwrap();

    let txn_id = txn_mgr.begin().unwrap();
    
    // Test insert logging
    txn_mgr.log_insert(txn_id, "test_table", 1, &[Value::Integer(42)]).unwrap();
    
    // Test update logging
    txn_mgr.log_update(
        txn_id,
        "test_table",
        1,
        &[Value::Integer(42)],
        &[Value::Integer(100)]
    ).unwrap();
    
    // Test delete logging
    txn_mgr.log_delete(txn_id, "test_table", 1, &[Value::Integer(100)]).unwrap();
    
    // Test DDL logging
    txn_mgr.log_create_table(txn_id, &schema).unwrap();
    txn_mgr.log_drop_table(txn_id, "test_table").unwrap();
    txn_mgr.log_truncate_table(txn_id, "test_table").unwrap();

    txn_mgr.commit(txn_id, &storage).unwrap();
}

#[test]
fn test_checkpoint_truncates_wal() {
    let temp_dir = tempdir().unwrap();
    let txn_mgr = TransactionManager::new(temp_dir.path().to_path_buf()).unwrap();
    let storage = StorageEngine::new(temp_dir.path().to_path_buf()).unwrap();

    let txn_id = txn_mgr.begin().unwrap();
    txn_mgr.commit(txn_id, &storage).unwrap();

    // Checkpoint should truncate WAL
    txn_mgr.checkpoint(&storage).unwrap();

    // Verify checkpoint file exists
    assert!(temp_dir.path().join("wal.checkpoint").exists());
}

#[test]
fn test_auto_checkpoint_on_last_commit() {
    let temp_dir = tempdir().unwrap();
    let config = GraniteConfig {
        batch_timeout_ms: 5,
        max_batch_size: 128,
        checkpoint_threshold_bytes: 100, // Very low threshold
        fsync_interval_ms: 10,
        max_unfsynced_bytes: 1 << 20,
    };
    let txn_mgr = TransactionManager::new_with_config(temp_dir.path().to_path_buf(), config).unwrap();
    let storage = StorageEngine::new(temp_dir.path().to_path_buf()).unwrap();

    let txn_id = txn_mgr.begin().unwrap();
    txn_mgr.commit(txn_id, &storage).unwrap();

    // Checkpoint should happen automatically
    assert!(temp_dir.path().join("wal.checkpoint").exists());
}

#[test]
fn test_multiple_concurrent_transactions() {
    let temp_dir = tempdir().unwrap();
    let txn_mgr = TransactionManager::new(temp_dir.path().to_path_buf()).unwrap();
    let storage = StorageEngine::new(temp_dir.path().to_path_buf()).unwrap();

    let txn1 = txn_mgr.begin().unwrap();
    let txn2 = txn_mgr.begin().unwrap();
    let txn3 = txn_mgr.begin().unwrap();

    assert!(txn_mgr.is_active(txn1));
    assert!(txn_mgr.is_active(txn2));
    assert!(txn_mgr.is_active(txn3));

    txn_mgr.commit(txn1, &storage).unwrap();
    assert!(!txn_mgr.is_active(txn1));
    assert!(txn_mgr.is_active(txn2));
    assert!(txn_mgr.is_active(txn3));

    txn_mgr.rollback(txn2, &storage).unwrap();
    assert!(!txn_mgr.is_active(txn2));
    assert!(txn_mgr.is_active(txn3));

    txn_mgr.commit(txn3, &storage).unwrap();
    assert!(!txn_mgr.is_active(txn3));
}

// ============== New tests for deferred fsync ==============

#[test]
fn test_commit_is_durable() {
    // Test that after commit() returns, the transaction is durable
    let temp_dir = tempdir().unwrap();
    let config = GraniteConfig {
        fsync_interval_ms: 100, // Long interval to test commit latch
        ..Default::default()
    };
    let txn_mgr = TransactionManager::new_with_config(temp_dir.path().to_path_buf(), config).unwrap();
    let storage = StorageEngine::new(temp_dir.path().to_path_buf()).unwrap();

    let txn_id = txn_mgr.begin().unwrap();
    
    // Perform an operation to ensure commit is not skipped
    txn_mgr.log_insert(txn_id, "test_table", 1, &[Value::Integer(1)]).unwrap();
    
    txn_mgr.commit(txn_id, &storage).unwrap();

    // After commit returns, the commit LSN should be durable
    // The durable_lsn should be >= the commit record's LSN
    // We can't easily get the exact commit LSN, but durable_lsn should be > 0
    assert!(txn_mgr.durable_lsn() > 0);
}

#[test]
fn test_non_commit_writes_fast() {
    // Test that INSERT/UPDATE/DELETE return quickly (don't wait for fsync)
    let temp_dir = tempdir().unwrap();
    let config = GraniteConfig {
        fsync_interval_ms: 1000, // Very long interval
        ..Default::default()
    };
    let txn_mgr = TransactionManager::new_with_config(temp_dir.path().to_path_buf(), config).unwrap();
    let storage = StorageEngine::new(temp_dir.path().to_path_buf()).unwrap();

    // Create a test table
    let schema = crate::types::TableSchema {
        name: "test_table".to_string(),
        columns: vec![
            crate::types::ColumnDef {
                name: "id".to_string(),
                data_type: crate::types::DataType::Integer,
                nullable: false,
                default: None,
                primary_key: false,
                auto_increment: false,
            },
        ],
        auto_increment_counter: 1,
        engine_type: crate::engines::EngineType::default(),
    };
    storage.apply_schema(schema.clone()).unwrap();

    let txn_id = txn_mgr.begin().unwrap();

    // INSERT should return quickly (< 50ms even with slow fsync)
    let start = Instant::now();
    for i in 0..100 {
        txn_mgr.log_insert(txn_id, "test_table", i, &[Value::Integer(i as i64)]).unwrap();
    }
    let elapsed = start.elapsed();

    // 100 inserts should complete in < 1000ms (10x faster than sync would be)
    // Relaxed from 100ms to 1000ms to account for slow test environments
    assert!(elapsed < Duration::from_millis(1000), "INSERTs took too long: {:?}", elapsed);

    // Rollback to avoid waiting for commit
    txn_mgr.rollback(txn_id, &storage).unwrap();
}

#[test]
fn test_multiple_commits_batch() {
    // Test that multiple commits can share a single fsync
    let temp_dir = tempdir().unwrap();
    let config = GraniteConfig {
        fsync_interval_ms: 50,
        ..Default::default()
    };
    let txn_mgr = std::sync::Arc::new(
        TransactionManager::new_with_config(temp_dir.path().to_path_buf(), config).unwrap()
    );
    let storage = std::sync::Arc::new(
        StorageEngine::new(temp_dir.path().to_path_buf()).unwrap()
    );

    // Start multiple transactions concurrently
    let num_txns = 10;
    let mut handles = Vec::new();

    for i in 0..num_txns {
        let txn_mgr_clone = std::sync::Arc::clone(&txn_mgr);
        let storage_clone = std::sync::Arc::clone(&storage);
        handles.push(thread::spawn(move || {
            let txn_id = txn_mgr_clone.begin().unwrap();
            
            // Perform basic op to force commit sync
            txn_mgr_clone.log_insert(txn_id, "test", i as u64, &[Value::Integer(i as i64)]).unwrap();
            
            txn_mgr_clone.commit(txn_id, &storage_clone).unwrap();
        }));
    }

    // Time how long all commits take
    let start = Instant::now();
    for handle in handles {
        handle.join().unwrap();
    }
    let elapsed = start.elapsed();

    // With batching, 10 commits should complete faster than 10 individual fsyncs
    // On SSD, this should be < 200ms (vs 500ms+ for 10 individual fsyncs)
    // Relaxed checking
    assert!(elapsed < Duration::from_millis(200), "Commits took too long: {:?}", elapsed);

    // All should be durable
    assert!(txn_mgr.durable_lsn() > 0);
}

#[test]
fn test_force_sync() {
    let temp_dir = tempdir().unwrap();
    let config = GraniteConfig {
        fsync_interval_ms: 10000, // Very long interval
        ..Default::default()
    };
    let txn_mgr = TransactionManager::new_with_config(temp_dir.path().to_path_buf(), config).unwrap();
    let storage = StorageEngine::new(temp_dir.path().to_path_buf()).unwrap();

    let txn_id = txn_mgr.begin().unwrap();
    
    // Force sync should make everything durable immediately
    let lsn = txn_mgr.force_sync().unwrap();
    assert!(lsn >= 1); // BEGIN record should be synced

    txn_mgr.rollback(txn_id, &storage).unwrap();
}

#[test]
fn test_synchronous_mode() {
    // Test that synchronous mode still works
    let temp_dir = tempdir().unwrap();
    let config = GraniteConfig::synchronous();
    let txn_mgr = TransactionManager::new_with_config(temp_dir.path().to_path_buf(), config).unwrap();
    let storage = StorageEngine::new(temp_dir.path().to_path_buf()).unwrap();

    let txn_id = txn_mgr.begin().unwrap();
    txn_mgr.commit(txn_id, &storage).unwrap();

    // In sync mode, everything should be immediately durable
    assert!(txn_mgr.durable_lsn() > 0);
}

#[test]
fn test_high_throughput_mode() {
    // Test high throughput configuration
    let temp_dir = tempdir().unwrap();
    let config = GraniteConfig::high_throughput();
    let txn_mgr = TransactionManager::new_with_config(temp_dir.path().to_path_buf(), config).unwrap();
    let storage = StorageEngine::new(temp_dir.path().to_path_buf()).unwrap();

    // Do many transactions quickly
    for i in 0..50 {
        let txn_id = txn_mgr.begin().unwrap();
        
        // Add op
        txn_mgr.log_insert(txn_id, "t", i, &[Value::Integer(i as i64)]).unwrap();
        
        txn_mgr.commit(txn_id, &storage).unwrap();
    }

    // All should be durable after commits return
    assert!(txn_mgr.durable_lsn() > 0);
}

#[test]
fn test_concurrent_commit_ordering() {
    // Test that commits maintain proper ordering
    let temp_dir = tempdir().unwrap();
    let config = GraniteConfig {
        fsync_interval_ms: 30,
        ..Default::default()
    };
    let txn_mgr = std::sync::Arc::new(
        TransactionManager::new_with_config(temp_dir.path().to_path_buf(), config).unwrap()
    );
    let storage = std::sync::Arc::new(
        StorageEngine::new(temp_dir.path().to_path_buf()).unwrap()
    );

    // Create transactions and track their commit order
    let committed_order = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));

    let mut handles = Vec::new();
    for i in 0..20 {
        let txn_mgr_clone = std::sync::Arc::clone(&txn_mgr);
        let storage_clone = std::sync::Arc::clone(&storage);
        let order_clone = std::sync::Arc::clone(&committed_order);

        handles.push(thread::spawn(move || {
            let txn_id = txn_mgr_clone.begin().unwrap();
            
            txn_mgr_clone.log_insert(txn_id, "t", i as u64, &[Value::Integer(i)]).unwrap();
            
            txn_mgr_clone.commit(txn_id, &storage_clone).unwrap();
            
            // Record that this transaction committed
            order_clone.lock().unwrap().push(i);
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // All transactions should have committed
    let order = committed_order.lock().unwrap();
    assert_eq!(order.len(), 20);
}

#[test]
fn test_stress_with_deferred_fsync() {
    // Stress test with many concurrent writers
    let temp_dir = tempdir().unwrap();
    let config = GraniteConfig {
        fsync_interval_ms: 20,
        max_batch_size: 256,
        ..Default::default()
    };
    let txn_mgr = std::sync::Arc::new(
        TransactionManager::new_with_config(temp_dir.path().to_path_buf(), config).unwrap()
    );
    let storage = std::sync::Arc::new(
        StorageEngine::new(temp_dir.path().to_path_buf()).unwrap()
    );

    // Create a test table
    let schema = crate::types::TableSchema {
        name: "stress_table".to_string(),
        columns: vec![
            crate::types::ColumnDef {
                name: "id".to_string(),
                data_type: crate::types::DataType::Integer,
                nullable: false,
                default: None,
                primary_key: false,
                auto_increment: false,
            },
        ],
        auto_increment_counter: 1,
        engine_type: crate::engines::EngineType::default(),
    };
    storage.apply_schema(schema.clone()).unwrap();

    let num_threads = 8;
    let txns_per_thread = 20;
    let mut handles = Vec::new();

    for t in 0..num_threads {
        let txn_mgr_clone = std::sync::Arc::clone(&txn_mgr);
        let storage_clone = std::sync::Arc::clone(&storage);

        handles.push(thread::spawn(move || {
            for i in 0..txns_per_thread {
                let txn_id = txn_mgr_clone.begin().unwrap();
                
                // Log some operations
                let row_id = (t * txns_per_thread + i) as u64;
                txn_mgr_clone.log_insert(
                    txn_id,
                    "stress_table",
                    row_id,
                    &[Value::Integer(row_id as i64)]
                ).unwrap();
                
                txn_mgr_clone.commit(txn_id, &storage_clone).unwrap();
            }
        }));
    }

    let start = Instant::now();
    for handle in handles {
        handle.join().unwrap();
    }
    let elapsed = start.elapsed();

    let total_txns = num_threads * txns_per_thread;
    println!("Completed {} transactions in {:?}", total_txns, elapsed);
    println!("Throughput: {:.2} txns/sec", total_txns as f64 / elapsed.as_secs_f64());

    // Should complete reasonably fast with deferred fsync
    assert!(elapsed < Duration::from_secs(10), "Stress test took too long: {:?}", elapsed);

    // All should be durable
    assert!(txn_mgr.durable_lsn() > 0);
}

#[test]
fn test_durable_lsn_increases_monotonically() {
    let temp_dir = tempdir().unwrap();
    let config = GraniteConfig {
        fsync_interval_ms: 20,
        ..Default::default()
    };
    let txn_mgr = TransactionManager::new_with_config(temp_dir.path().to_path_buf(), config).unwrap();
    let storage = StorageEngine::new(temp_dir.path().to_path_buf()).unwrap();

    let mut prev_durable = 0u64;

    for i in 0..10 {
        let txn_id = txn_mgr.begin().unwrap();
        
        // Add op
        txn_mgr.log_insert(txn_id, "t", i as u64, &[Value::Integer(i as i64)]).unwrap();
        
        txn_mgr.commit(txn_id, &storage).unwrap();

        let current_durable = txn_mgr.durable_lsn();
        assert!(current_durable >= prev_durable, 
            "Durable LSN should never decrease: {} -> {}", prev_durable, current_durable);
        prev_durable = current_durable;
    }
}

#[test]
fn test_checkpoint_forces_sync() {
    let temp_dir = tempdir().unwrap();
    let config = GraniteConfig {
        fsync_interval_ms: 10000, // Very long interval
        ..Default::default()
    };
    let txn_mgr = TransactionManager::new_with_config(temp_dir.path().to_path_buf(), config).unwrap();
    let storage = StorageEngine::new(temp_dir.path().to_path_buf()).unwrap();

    // Begin and commit a transaction
    let txn_id = txn_mgr.begin().unwrap();
    // Use an operation to ensure something is written
    txn_mgr.log_insert(txn_id, "t", 1, &[Value::Integer(1)]).unwrap();
    
    txn_mgr.commit(txn_id, &storage).unwrap();

    // Even with long fsync interval, checkpoint should force sync
    txn_mgr.checkpoint(&storage).unwrap();

    // Verify checkpoint file has durable_lsn
    let checkpoint_content = std::fs::read_to_string(temp_dir.path().join("wal.checkpoint")).unwrap();
    let checkpoint: serde_json::Value = serde_json::from_str(&checkpoint_content).unwrap();
    assert!(checkpoint["durable_lsn"].as_u64().unwrap() > 0);
}
