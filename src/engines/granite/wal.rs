//! Granite Engine - Write-Ahead Logging (WAL) worker and I/O operations
//!
//! The Granite Engine is MiniSQL's default transaction engine, providing full ACID
//! guarantees with high throughput via deferred fsync and commit latches.
//!
//! ## Why "Granite"?
//!
//! A play on "WAL" (wall → stone wall → granite). Like granite, this engine is:
//! - **Solid**: Full ACID guarantees with crash recovery
//! - **Durable**: Write-ahead logging ensures data survives crashes
//! - **Strong**: High throughput under concurrent load
//!
//! ## Deferred Fsync with Commit Latches
//!
//! This implementation uses PostgreSQL-style deferred fsync to improve throughput:
//! - Writes are immediately written to the OS buffer (no fsync)
//! - Fsyncs happen periodically (every `fsync_interval_ms`) or when buffer is full
//! - Transactions waiting for durability use "commit latches" to block until fsync
//! - This allows many transactions to share a single fsync, dramatically improving throughput
//!
//! ### ACID Guarantees
//! - **Durability**: COMMIT waits for fsync via commit latch before returning
//! - **Ordering**: LSNs are sequential; earlier commits are always durable before later ones
//! - **Recovery**: On crash, only fsynced records are guaranteed; uncommitted = rollback

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{mpsc, Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

use crate::error::{MiniSqlError, Result};
use super::log::LogRecord;

/// Configuration for the Granite Engine's group-commit and deferred fsync behavior.
///
/// This controls how the Granite engine batches writes and when it performs fsync
/// operations. The default configuration provides a good balance between latency
/// and throughput for most workloads.
///
/// # Presets
///
/// - [`GraniteConfig::default()`] - Balanced (50ms fsync interval)
/// - [`GraniteConfig::synchronous()`] - Low latency, lower throughput
/// - [`GraniteConfig::high_throughput()`] - High throughput, higher latency
#[derive(Debug, Clone)]
pub struct GraniteConfig {
    /// Maximum time to wait before flushing a batch (milliseconds)
    pub batch_timeout_ms: u64,
    /// Maximum number of records in a batch before forcing flush
    pub max_batch_size: usize,
    /// Maximum WAL file size before triggering automatic checkpoint (bytes)
    pub checkpoint_threshold_bytes: u64,
    /// How often to fsync the WAL file (milliseconds). Set to 0 for sync per batch.
    /// Higher values = better throughput but higher commit latency.
    /// PostgreSQL default is ~200ms, we default to 50ms for lower latency.
    pub fsync_interval_ms: u64,
    /// Maximum bytes to buffer before forcing an immediate fsync.
    /// This prevents unbounded memory growth during high-throughput periods.
    pub max_unfsynced_bytes: usize,
}

impl Default for GraniteConfig {
    fn default() -> Self {
        Self {
            batch_timeout_ms: 5,
            max_batch_size: 128,
            checkpoint_threshold_bytes: 10 * 1024 * 1024, // 10 MB
            fsync_interval_ms: 50,                        // 50ms fsync interval
            max_unfsynced_bytes: 1 << 20,                 // 1 MB max unfsynced buffer
        }
    }
}

impl GraniteConfig {
    /// Create a config with synchronous fsync (legacy behavior)
    pub fn synchronous() -> Self {
        Self {
            fsync_interval_ms: 0, // Sync per batch
            ..Default::default()
        }
    }

    /// Create a config optimized for high throughput (higher latency)
    pub fn high_throughput() -> Self {
        Self {
            batch_timeout_ms: 10,
            max_batch_size: 512,
            fsync_interval_ms: 100,
            max_unfsynced_bytes: 4 << 20, // 4 MB
            ..Default::default()
        }
    }
}

/// Deprecated: Use `GraniteConfig` instead.
/// This type alias is provided for backward compatibility.
#[deprecated(since = "0.2.0", note = "Use GraniteConfig instead")]
pub type WalConfig = GraniteConfig;

/// Shared state for commit latches - allows transactions to wait for durability
pub struct FsyncState {
    /// The highest LSN that has been durably fsynced to disk.
    /// Transactions can check this to know if their commit is durable.
    durable_lsn: AtomicU64,
    /// The highest LSN that has been written (but not necessarily fsynced).
    written_lsn: AtomicU64,
    /// Condition variable for threads waiting on fsync completion.
    condvar: Condvar,
    /// Mutex paired with the condvar (condvar requires a mutex).
    mutex: Mutex<()>,
    /// Flag to indicate shutdown in progress
    shutdown: AtomicU64,
}

impl FsyncState {
    fn new() -> Self {
        Self {
            durable_lsn: AtomicU64::new(0),
            written_lsn: AtomicU64::new(0),
            condvar: Condvar::new(),
            mutex: Mutex::new(()),
            shutdown: AtomicU64::new(0),
        }
    }

    /// Get the current durable LSN
    pub fn durable_lsn(&self) -> u64 {
        self.durable_lsn.load(Ordering::Acquire)
    }

    /// Get the current written (but not necessarily durable) LSN
    #[cfg(test)]
    pub fn written_lsn(&self) -> u64 {
        self.written_lsn.load(Ordering::Acquire)
    }



    /// Wait until the given LSN is durable (fsynced).
    /// Returns Ok(()) when durable, or Err if shutdown/timeout.
    pub fn wait_for_durable(&self, target_lsn: u64, timeout: Duration) -> Result<()> {
        // Fast path: already durable
        if self.durable_lsn.load(Ordering::Acquire) >= target_lsn {
            return Ok(());
        }

        let deadline = Instant::now() + timeout;
        let mut guard = self.mutex.lock().map_err(|_| {
            MiniSqlError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                "FsyncState mutex poisoned",
            ))
        })?;

        loop {
            // Check if we're durable
            if self.durable_lsn.load(Ordering::Acquire) >= target_lsn {
                return Ok(());
            }

            // Check for shutdown
            if self.shutdown.load(Ordering::Acquire) != 0 {
                return Err(MiniSqlError::Io(std::io::Error::new(
                    std::io::ErrorKind::Other,
                    "WAL worker shutdown during wait",
                )));
            }

            // Calculate remaining time
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(MiniSqlError::Io(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!(
                        "Timeout waiting for LSN {} to become durable (current: {})",
                        target_lsn,
                        self.durable_lsn.load(Ordering::Acquire)
                    ),
                )));
            }

            // Wait for condvar signal (fsync complete)
            let result = self.condvar.wait_timeout(guard, remaining);
            guard = result
                .map_err(|_| {
                    MiniSqlError::Io(std::io::Error::new(
                        std::io::ErrorKind::Other,
                        "FsyncState condvar wait failed",
                    ))
                })?
                .0;
        }
    }

    /// Signal that fsync has completed up to the given LSN.
    /// Wakes all waiting threads.
    fn signal_durable(&self, lsn: u64) {
        // Update durable LSN (only if higher than current)
        let mut current = self.durable_lsn.load(Ordering::Acquire);
        while lsn > current {
            match self.durable_lsn.compare_exchange_weak(
                current,
                lsn,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(c) => current = c,
            }
        }

        // Wake all waiters
        self.condvar.notify_all();
    }

    /// Update the written LSN (called after write, before fsync)
    fn update_written(&self, lsn: u64) {
        let mut current = self.written_lsn.load(Ordering::Acquire);
        while lsn > current {
            match self.written_lsn.compare_exchange_weak(
                current,
                lsn,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(_) => break,
                Err(c) => current = c,
            }
        }
    }

    /// Signal shutdown
    fn signal_shutdown(&self) {
        self.shutdown.store(1, Ordering::Release);
        self.condvar.notify_all();
    }
}

/// A request to write a log record (used by Granite engine's group-commit worker)
pub struct GraniteWriteRequest {
    pub record: LogRecord,
    pub responder: mpsc::SyncSender<Result<()>>,
}

/// Control messages for the Granite engine's worker thread.
#[allow(dead_code)]
pub enum GraniteMessage {
    /// Normal write request
    Write(GraniteWriteRequest),
    /// Request to truncate the WAL file. The responder will receive the Result
    /// when truncate completes (or an error).
    Truncate(mpsc::SyncSender<Result<()>>),
    /// Force an immediate fsync (used for checkpoint)
    ForceSync(mpsc::SyncSender<Result<u64>>),
    /// Shutdown the worker
    Shutdown,
}

/// Handle to the Granite engine's worker task
pub struct GraniteWorkerHandle {
    pub sender: mpsc::SyncSender<GraniteMessage>,
    /// Shared state for commit latches - allows waiting for durability
    pub fsync_state: Arc<FsyncState>,
    /// Handle to terminate worker on drop
    _shutdown_handle: Arc<AtomicU64>,
}

impl GraniteWorkerHandle {
    /// Create a new Granite engine worker and spawn the worker thread
    pub fn new(wal_path: PathBuf, config: GraniteConfig) -> Result<Self> {
        // Open WAL file in append mode
        let wal_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)?;

        // Create a bounded sync channel for the Granite engine worker
        let (tx, rx) = mpsc::sync_channel::<GraniteMessage>(10000);
        let shutdown_flag = Arc::new(AtomicU64::new(0));
        let fsync_state = Arc::new(FsyncState::new());

        // Spawn the Granite engine worker in a dedicated std::thread
        let config_clone = config.clone();
        let wal_path_clone = wal_path.clone();
        let fsync_state_clone = Arc::clone(&fsync_state);
        std::thread::spawn(move || {
            granite_worker_thread(wal_file, rx, wal_path_clone, config_clone, fsync_state_clone);
        });

        Ok(Self {
            sender: tx,
            fsync_state,
            _shutdown_handle: shutdown_flag,
        })
    }

    /// Wait for a specific LSN to become durable
    pub fn wait_for_durable(&self, lsn: u64) -> Result<()> {
        // Use a generous timeout (30 seconds) - if we hit this, something is very wrong
        self.fsync_state
            .wait_for_durable(lsn, Duration::from_secs(30))
    }

    /// Force an immediate fsync and return the durable LSN
    pub fn force_sync(&self) -> Result<u64> {
        let (tx, rx) = mpsc::sync_channel(1);
        self.sender.send(GraniteMessage::ForceSync(tx)).map_err(|e| {
            MiniSqlError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Granite worker channel closed: {}", e),
            ))
        })?;

        rx.recv().map_err(|e| {
            MiniSqlError::Io(std::io::Error::new(
                std::io::ErrorKind::Other,
                format!("Granite force sync response lost: {}", e),
            ))
        })?
    }

    /// Get the current durable LSN
    pub fn durable_lsn(&self) -> u64 {
        self.fsync_state.durable_lsn()
    }
}

/// Granite engine worker thread - batches writes and performs deferred fsync
fn granite_worker_thread(
    mut file: File,
    rx: mpsc::Receiver<GraniteMessage>,
    wal_path: PathBuf,
    config: GraniteConfig,
    fsync_state: Arc<FsyncState>,
) {
    let batch_timeout = Duration::from_millis(config.batch_timeout_ms);
    let fsync_interval = Duration::from_millis(config.fsync_interval_ms);
    let max_batch_size = config.max_batch_size;
    let max_unfsynced_bytes = config.max_unfsynced_bytes;

    // Is deferred fsync enabled?
    let deferred_fsync = config.fsync_interval_ms > 0;

    // Track state for deferred fsync
    let mut last_fsync = Instant::now();
    let mut unfsynced_bytes: usize = 0;
    let mut max_written_lsn: u64 = 0;

    loop {
        let mut batch = Vec::new();
        let mut pending_truncates: Vec<mpsc::SyncSender<Result<()>>> = Vec::new();
        let mut pending_force_syncs: Vec<mpsc::SyncSender<Result<u64>>> = Vec::new();
        let mut should_shutdown = false;

        // Calculate timeout: use shorter of batch timeout and remaining fsync interval
        let time_until_fsync = if deferred_fsync && unfsynced_bytes > 0 {
            fsync_interval.saturating_sub(last_fsync.elapsed())
        } else {
            fsync_interval
        };
        let recv_timeout = batch_timeout.min(time_until_fsync);

        // Collect first request (with timeout to check fsync interval)
        match rx.recv_timeout(recv_timeout) {
            Ok(GraniteMessage::Write(req)) => batch.push(req),
            Ok(GraniteMessage::Truncate(responder)) => {
                pending_truncates.push(responder);
            }
            Ok(GraniteMessage::ForceSync(responder)) => {
                pending_force_syncs.push(responder);
            }
            Ok(GraniteMessage::Shutdown) => {
                should_shutdown = true;
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                // Timeout - check if we need to fsync
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                log::info!("Granite worker shutting down (channel closed)");
                break;
            }
        }

        // Collect additional requests up to timeout or batch size
        let deadline = Instant::now() + batch_timeout;
        while batch.len() < max_batch_size && !should_shutdown {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }

            match rx.recv_timeout(remaining) {
                Ok(GraniteMessage::Write(req)) => batch.push(req),
                Ok(GraniteMessage::Truncate(responder)) => {
                    pending_truncates.push(responder);
                }
                Ok(GraniteMessage::ForceSync(responder)) => {
                    pending_force_syncs.push(responder);
                }
                Ok(GraniteMessage::Shutdown) => {
                    should_shutdown = true;
                    break;
                }
                Err(mpsc::RecvTimeoutError::Timeout) => break,
                Err(mpsc::RecvTimeoutError::Disconnected) => break,
            }
        }

        // Process accumulated writes
        if !batch.is_empty() {
            let batch_size = batch.len();
            let mut records: Vec<LogRecord> = Vec::with_capacity(batch_size);
            let mut responders: Vec<mpsc::SyncSender<Result<()>>> = Vec::with_capacity(batch_size);
            for req in batch.drain(..) {
                records.push(req.record);
                responders.push(req.responder);
            }

            // Write records (without fsync for deferred mode)
            let write_result = if deferred_fsync {
                write_records_no_sync(&mut file, &records)
            } else {
                write_records_with_sync(&mut file, &records).map(|()| {
                    // Estimate bytes for sync mode (we don't track exactly)
                    records.len() * 64 // rough estimate
                })
            };

            match write_result {
                Ok(bytes_written) => {
                    // Track the highest written LSN (find actual max in case of out-of-order LSNs)
                    if let Some(max_lsn) = records.iter().map(|r| r.lsn).max() {
                        max_written_lsn = max_written_lsn.max(max_lsn);
                        fsync_state.update_written(max_written_lsn);
                    }

                    unfsynced_bytes += bytes_written;

                    // Notify all requesters that write is complete
                    // (For sync mode, this means fsync is done too)
                    for responder in responders {
                        let _ = responder.send(Ok(()));
                    }

                    // If sync mode, update durable LSN immediately
                    if !deferred_fsync {
                        fsync_state.signal_durable(max_written_lsn);
                        unfsynced_bytes = 0;
                        last_fsync = Instant::now();
                    }

                    if batch_size > 1 {
                        log::debug!("WAL batch written: {} records, {} bytes", batch_size, bytes_written);
                    }
                }
                Err(e) => {
                    // Use Arc to avoid cloning error message for each responder
                    let error_msg = Arc::new(e.to_string());
                    for responder in responders {
                        let _ = responder.send(Err(MiniSqlError::Io(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            error_msg.as_ref().clone(),
                        ))));
                    }
                }
            }
        }

        // Check if we need to fsync (deferred mode only)
        let force_sync_requested = !pending_force_syncs.is_empty();
        let truncate_requested = !pending_truncates.is_empty();
        let time_triggered = deferred_fsync && last_fsync.elapsed() >= fsync_interval;
        let size_triggered = unfsynced_bytes >= max_unfsynced_bytes;

        if unfsynced_bytes > 0
            && (time_triggered || size_triggered || force_sync_requested || truncate_requested || should_shutdown)
        {
            match file.sync_data() {
                Ok(()) => {
                    let synced_lsn = max_written_lsn;
                    fsync_state.signal_durable(synced_lsn);

                    if unfsynced_bytes > 0 {
                        log::debug!(
                            "Granite fsync complete: LSN {}, {} bytes (trigger: {})",
                            synced_lsn,
                            unfsynced_bytes,
                            if force_sync_requested {
                                "force"
                            } else if truncate_requested {
                                "truncate"
                            } else if size_triggered {
                                "size"
                            } else {
                                "time"
                            }
                        );
                    }

                    unfsynced_bytes = 0;
                    last_fsync = Instant::now();

                    // Respond to force sync requests
                    for responder in pending_force_syncs {
                        let _ = responder.send(Ok(synced_lsn));
                    }
                }
                Err(e) => {
                    log::error!("Granite fsync failed: {}", e);
                    for responder in pending_force_syncs {
                        let _ = responder.send(Err(MiniSqlError::Io(std::io::Error::new(
                            std::io::ErrorKind::Other,
                            e.to_string(),
                        ))));
                    }
                }
            }
        } else {
            // No fsync needed, but still respond to force sync with current durable LSN
            for responder in pending_force_syncs {
                let _ = responder.send(Ok(fsync_state.durable_lsn()));
            }
        }

        // Perform truncates if requested (after any pending fsync)
        for responder in pending_truncates {
            match perform_truncate(&mut file, &wal_path) {
                Ok(()) => {
                    max_written_lsn = 0;
                    unfsynced_bytes = 0;
                    fsync_state.signal_durable(0);
                    let _ = responder.send(Ok(()));
                }
                Err(e) => {
                    let _ = responder.send(Err(e));
                }
            }
        }

        // Handle shutdown
        if should_shutdown {
            // Final fsync before shutdown
            if unfsynced_bytes > 0 {
                if let Ok(()) = file.sync_data() {
                    fsync_state.signal_durable(max_written_lsn);
                }
            }
            fsync_state.signal_shutdown();
            log::info!("Granite worker shutting down (shutdown message)");
            break;
        }
    }
}

/// Helper to perform WAL truncation on the worker thread.
fn perform_truncate(file: &mut File, wal_path: &PathBuf) -> Result<()> {
    // Flush and sync current writer
    file.flush()?;
    file.sync_all()?;

    // Reopen the file in truncate mode (this clears the file)
    let new_file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&wal_path)?;

    // Replace the file handle
    *file = new_file;

    Ok(())
}

/// Write a batch of log records WITHOUT fsync (for deferred fsync mode).
/// Returns the number of bytes written.
pub(super) fn write_records_no_sync(file: &mut File, records: &[LogRecord]) -> Result<usize> {
    let mut total_bytes = 0;

    for record in records {
        let encoded = bincode::serialize(record)
            .map_err(|e| MiniSqlError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
        let len = encoded.len() as u32;
        file.write_all(&len.to_le_bytes())?;
        file.write_all(&encoded)?;
        total_bytes += 4 + encoded.len();
    }

    // Flush to kernel buffer (but don't fsync to disk)
    file.flush()?;

    Ok(total_bytes)
}

/// Write a batch of log records WITH fsync (synchronous mode).
/// This is the legacy behavior.
pub(super) fn write_records_with_sync(file: &mut File, records: &[LogRecord]) -> Result<()> {
    for record in records {
        let encoded = bincode::serialize(record)
            .map_err(|e| MiniSqlError::Io(std::io::Error::new(std::io::ErrorKind::Other, e)))?;
        let len = encoded.len() as u32;
        file.write_all(&len.to_le_bytes())?;
        file.write_all(&encoded)?;
    }

    file.flush()?;
    file.sync_data()?; // fsync

    Ok(())
}

/// Legacy function for backward compatibility
#[allow(dead_code)]
pub(super) fn write_records(file: &mut File, records: &[LogRecord]) -> Result<()> {
    write_records_with_sync(file, records)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::engines::granite::log::LogOperation;
    use std::thread;
    use std::time::Duration;

    #[test]
    fn test_granite_config_default() {
        let config = GraniteConfig::default();
        assert_eq!(config.batch_timeout_ms, 5);
        assert_eq!(config.max_batch_size, 128);
        assert_eq!(config.checkpoint_threshold_bytes, 10 * 1024 * 1024);
        assert_eq!(config.fsync_interval_ms, 50);
        assert_eq!(config.max_unfsynced_bytes, 1 << 20);
    }

    #[test]
    fn test_granite_config_synchronous() {
        let config = GraniteConfig::synchronous();
        assert_eq!(config.fsync_interval_ms, 0);
    }

    #[test]
    fn test_granite_config_high_throughput() {
        let config = GraniteConfig::high_throughput();
        assert_eq!(config.fsync_interval_ms, 100);
        assert_eq!(config.max_batch_size, 512);
    }

    #[test]
    fn test_granite_config_custom() {
        let config = GraniteConfig {
            batch_timeout_ms: 10,
            max_batch_size: 256,
            checkpoint_threshold_bytes: 20 * 1024 * 1024,
            fsync_interval_ms: 100,
            max_unfsynced_bytes: 2 << 20,
        };
        assert_eq!(config.batch_timeout_ms, 10);
        assert_eq!(config.max_batch_size, 256);
        assert_eq!(config.fsync_interval_ms, 100);
        assert_eq!(config.max_unfsynced_bytes, 2 << 20);
    }

    #[test]
    fn test_fsync_state_initial_values() {
        let state = FsyncState::new();
        assert_eq!(state.durable_lsn(), 0);
        assert_eq!(state.written_lsn(), 0);
    }

    #[test]
    fn test_fsync_state_update_written() {
        let state = FsyncState::new();
        state.update_written(5);
        assert_eq!(state.written_lsn(), 5);
        
        // Should only increase
        state.update_written(3);
        assert_eq!(state.written_lsn(), 5);
        
        state.update_written(10);
        assert_eq!(state.written_lsn(), 10);
    }

    #[test]
    fn test_fsync_state_signal_durable() {
        let state = FsyncState::new();
        state.signal_durable(5);
        assert_eq!(state.durable_lsn(), 5);
        
        // Should only increase
        state.signal_durable(3);
        assert_eq!(state.durable_lsn(), 5);
        
        state.signal_durable(10);
        assert_eq!(state.durable_lsn(), 10);
    }

    #[test]
    fn test_fsync_state_wait_already_durable() {
        let state = FsyncState::new();
        state.signal_durable(10);
        
        // Should return immediately since LSN 5 is already durable
        let result = state.wait_for_durable(5, Duration::from_millis(100));
        assert!(result.is_ok());
    }

    #[test]
    fn test_fsync_state_wait_timeout() {
        let state = FsyncState::new();
        
        // LSN 10 is not durable, should timeout
        let result = state.wait_for_durable(10, Duration::from_millis(50));
        assert!(result.is_err());
        
        let err = result.unwrap_err();
        assert!(err.to_string().contains("Timeout"));
    }

    #[test]
    fn test_fsync_state_wait_signaled() {
        let state = Arc::new(FsyncState::new());
        let state_clone = Arc::clone(&state);
        
        // Spawn thread to signal durable after delay
        let handle = thread::spawn(move || {
            thread::sleep(Duration::from_millis(50));
            state_clone.signal_durable(10);
        });
        
        // Wait for durability - should succeed after signal
        let result = state.wait_for_durable(10, Duration::from_millis(200));
        handle.join().unwrap();
        
        assert!(result.is_ok());
    }

    #[test]
    fn test_fsync_state_concurrent_waiters() {
        let state = Arc::new(FsyncState::new());
        
        // Spawn multiple threads waiting on different LSNs
        let mut handles = Vec::new();
        for i in 1..=5 {
            let state_clone = Arc::clone(&state);
            let lsn = i as u64;
            handles.push(thread::spawn(move || {
                state_clone.wait_for_durable(lsn, Duration::from_millis(500))
            }));
        }
        
        // Signal durability for all
        thread::sleep(Duration::from_millis(50));
        state.signal_durable(5);
        
        // All should succeed
        for handle in handles {
            let result = handle.join().unwrap();
            assert!(result.is_ok());
        }
    }

    #[test]
    fn test_write_records_no_sync_empty() {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_path = temp_dir.path().join("test.log");
        let mut file = File::create(&temp_path).unwrap();

        let records: Vec<LogRecord> = vec![];
        let result = write_records_no_sync(&mut file, &records);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), 0);
    }

    #[test]
    fn test_write_records_no_sync_single() {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_path = temp_dir.path().join("test.log");
        let mut file = File::create(&temp_path).unwrap();

        let record = LogRecord {
            lsn: 1,
            txn_id: 1,
            op: LogOperation::Begin,
            timestamp: 12345,
        };

        let result = write_records_no_sync(&mut file, &[record]);
        assert!(result.is_ok());

        let bytes_written = result.unwrap();
        assert!(bytes_written > 0);

        // Verify file has content
        let metadata = std::fs::metadata(&temp_path).unwrap();
        assert_eq!(metadata.len() as usize, bytes_written);
    }

    #[test]
    fn test_write_records_no_sync_multiple() {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_path = temp_dir.path().join("test.log");
        let mut file = File::create(&temp_path).unwrap();

        let records: Vec<LogRecord> = (1..=10)
            .map(|i| LogRecord {
                lsn: i,
                txn_id: i,
                op: LogOperation::Begin,
                timestamp: 12345,
            })
            .collect();

        let result = write_records_no_sync(&mut file, &records);
        assert!(result.is_ok());

        let bytes_written = result.unwrap();
        assert!(bytes_written > 0);

        // Verify file has content
        let metadata = std::fs::metadata(&temp_path).unwrap();
        assert_eq!(metadata.len() as usize, bytes_written);
    }

    #[test]
    fn test_write_records_with_sync_single() {
        let temp_dir = tempfile::tempdir().unwrap();
        let temp_path = temp_dir.path().join("test.log");
        let mut file = File::create(&temp_path).unwrap();

        let record = LogRecord {
            lsn: 1,
            txn_id: 1,
            op: LogOperation::Begin,
            timestamp: 12345,
        };

        let result = write_records_with_sync(&mut file, &[record]);
        assert!(result.is_ok());

        // Verify file is not empty
        let metadata = std::fs::metadata(&temp_path).unwrap();
        assert!(metadata.len() > 0);
    }

    #[test]
    fn test_granite_worker_synchronous_mode() {
        let temp_dir = tempfile::tempdir().unwrap();
        let wal_path = temp_dir.path().join("wal.log");

        // Use synchronous config
        let config = GraniteConfig::synchronous();
        let handle = GraniteWorkerHandle::new(wal_path.clone(), config).unwrap();

        // Write a record
        let (tx, rx) = mpsc::sync_channel(1);
        let record = LogRecord {
            lsn: 1,
            txn_id: 1,
            op: LogOperation::Begin,
            timestamp: 12345,
        };
        let req = GraniteWriteRequest {
            record,
            responder: tx,
        };

        handle.sender.send(GraniteMessage::Write(req)).unwrap();
        let result = rx.recv().unwrap();
        assert!(result.is_ok());

        // In sync mode, should be immediately durable
        // Give worker a moment to update state
        thread::sleep(Duration::from_millis(20));
        assert!(handle.durable_lsn() >= 1);

        // Shutdown
        handle.sender.send(GraniteMessage::Shutdown).unwrap();
    }

    #[test]
    fn test_granite_worker_deferred_mode() {
        let temp_dir = tempfile::tempdir().unwrap();
        let wal_path = temp_dir.path().join("wal.log");

        // Use deferred config with short interval
        let config = GraniteConfig {
            fsync_interval_ms: 30,
            ..Default::default()
        };
        let handle = GraniteWorkerHandle::new(wal_path.clone(), config).unwrap();

        // Write a record
        let (tx, rx) = mpsc::sync_channel(1);
        let record = LogRecord {
            lsn: 1,
            txn_id: 1,
            op: LogOperation::Begin,
            timestamp: 12345,
        };
        let req = GraniteWriteRequest {
            record,
            responder: tx,
        };

        handle.sender.send(GraniteMessage::Write(req)).unwrap();
        let result = rx.recv().unwrap();
        assert!(result.is_ok());

        // In deferred mode, might not be immediately durable
        // Wait for fsync interval + buffer
        thread::sleep(Duration::from_millis(100));
        assert!(handle.durable_lsn() >= 1);

        // Shutdown
        handle.sender.send(GraniteMessage::Shutdown).unwrap();
    }

    #[test]
    fn test_granite_worker_force_sync() {
        let temp_dir = tempfile::tempdir().unwrap();
        let wal_path = temp_dir.path().join("wal.log");

        let config = GraniteConfig {
            fsync_interval_ms: 1000, // Long interval
            ..Default::default()
        };
        let handle = GraniteWorkerHandle::new(wal_path.clone(), config).unwrap();

        // Write a record
        let (tx, rx) = mpsc::sync_channel(1);
        let record = LogRecord {
            lsn: 1,
            txn_id: 1,
            op: LogOperation::Begin,
            timestamp: 12345,
        };
        let req = GraniteWriteRequest {
            record,
            responder: tx,
        };

        handle.sender.send(GraniteMessage::Write(req)).unwrap();
        rx.recv().unwrap().unwrap();

        // Force sync
        let lsn = handle.force_sync().unwrap();
        assert!(lsn >= 1);
        assert!(handle.durable_lsn() >= 1);

        // Shutdown
        handle.sender.send(GraniteMessage::Shutdown).unwrap();
    }

    #[test]
    fn test_granite_worker_wait_for_durable() {
        let temp_dir = tempfile::tempdir().unwrap();
        let wal_path = temp_dir.path().join("wal.log");

        let config = GraniteConfig {
            fsync_interval_ms: 30,
            ..Default::default()
        };
        let handle = GraniteWorkerHandle::new(wal_path.clone(), config).unwrap();

        // Write a record
        let (tx, rx) = mpsc::sync_channel(1);
        let record = LogRecord {
            lsn: 1,
            txn_id: 1,
            op: LogOperation::Begin,
            timestamp: 12345,
        };
        let req = GraniteWriteRequest {
            record,
            responder: tx,
        };

        handle.sender.send(GraniteMessage::Write(req)).unwrap();
        rx.recv().unwrap().unwrap();

        // Wait for durability
        let result = handle.wait_for_durable(1);
        assert!(result.is_ok());
        assert!(handle.durable_lsn() >= 1);

        // Shutdown
        handle.sender.send(GraniteMessage::Shutdown).unwrap();
    }

    #[test]
    fn test_granite_worker_multiple_writes_share_fsync() {
        let temp_dir = tempfile::tempdir().unwrap();
        let wal_path = temp_dir.path().join("wal.log");

        let config = GraniteConfig {
            fsync_interval_ms: 100, // Longer interval to ensure batching
            batch_timeout_ms: 20,
            ..Default::default()
        };
        let handle = GraniteWorkerHandle::new(wal_path.clone(), config).unwrap();

        // Write multiple records quickly
        let mut receivers = Vec::new();
        for i in 1..=5 {
            let (tx, rx) = mpsc::sync_channel(1);
            let record = LogRecord {
                lsn: i,
                txn_id: i,
                op: LogOperation::Begin,
                timestamp: 12345,
            };
            let req = GraniteWriteRequest {
                record,
                responder: tx,
            };
            handle.sender.send(GraniteMessage::Write(req)).unwrap();
            receivers.push(rx);
        }

        // All writes should complete
        for rx in receivers {
            rx.recv().unwrap().unwrap();
        }

        // Wait for fsync
        thread::sleep(Duration::from_millis(150));
        assert!(handle.durable_lsn() >= 5);

        // Shutdown
        handle.sender.send(GraniteMessage::Shutdown).unwrap();
    }

    #[test]
    fn test_granite_worker_truncate() {
        let temp_dir = tempfile::tempdir().unwrap();
        let wal_path = temp_dir.path().join("wal.log");

        let config = GraniteConfig::synchronous();
        let handle = GraniteWorkerHandle::new(wal_path.clone(), config).unwrap();

        // Write a record
        let (tx, rx) = mpsc::sync_channel(1);
        let record = LogRecord {
            lsn: 1,
            txn_id: 1,
            op: LogOperation::Begin,
            timestamp: 12345,
        };
        let req = GraniteWriteRequest {
            record,
            responder: tx,
        };

        handle.sender.send(GraniteMessage::Write(req)).unwrap();
        rx.recv().unwrap().unwrap();

        // File should have content
        assert!(std::fs::metadata(&wal_path).unwrap().len() > 0);

        // Truncate
        let (tx, rx) = mpsc::sync_channel(1);
        handle.sender.send(GraniteMessage::Truncate(tx)).unwrap();
        rx.recv().unwrap().unwrap();

        // File should be empty
        assert_eq!(std::fs::metadata(&wal_path).unwrap().len(), 0);

        // Shutdown
        handle.sender.send(GraniteMessage::Shutdown).unwrap();
    }

    #[test]
    fn test_granite_worker_size_triggered_fsync() {
        let temp_dir = tempfile::tempdir().unwrap();
        let wal_path = temp_dir.path().join("wal.log");

        let config = GraniteConfig {
            fsync_interval_ms: 10000, // Very long interval
            max_unfsynced_bytes: 100, // Very small buffer
            ..Default::default()
        };
        let handle = GraniteWorkerHandle::new(wal_path.clone(), config).unwrap();

        // Write enough records to trigger size-based fsync
        for i in 1..=10 {
            let (tx, rx) = mpsc::sync_channel(1);
            let record = LogRecord {
                lsn: i,
                txn_id: i,
                op: LogOperation::Insert {
                    table: "big_table".to_string(),
                    row_id: i,
                    values: vec![],
                },
                timestamp: 12345,
            };
            let req = GraniteWriteRequest {
                record,
                responder: tx,
            };
            handle.sender.send(GraniteMessage::Write(req)).unwrap();
            rx.recv().unwrap().unwrap();
        }

        // Should have triggered fsync due to size
        thread::sleep(Duration::from_millis(50)); // Let worker process
        assert!(handle.durable_lsn() > 0);

        // Shutdown
        handle.sender.send(GraniteMessage::Shutdown).unwrap();
    }

    #[test]
    fn test_granite_worker_lsn_ordering() {
        let temp_dir = tempfile::tempdir().unwrap();
        let wal_path = temp_dir.path().join("wal.log");

        let config = GraniteConfig {
            fsync_interval_ms: 20,
            ..Default::default()
        };
        let handle = GraniteWorkerHandle::new(wal_path.clone(), config).unwrap();

        // Write records with increasing LSNs
        for i in 1..=10 {
            let (tx, rx) = mpsc::sync_channel(1);
            let record = LogRecord {
                lsn: i,
                txn_id: i,
                op: LogOperation::Begin,
                timestamp: 12345,
            };
            let req = GraniteWriteRequest {
                record,
                responder: tx,
            };
            handle.sender.send(GraniteMessage::Write(req)).unwrap();
            rx.recv().unwrap().unwrap();
        }

        // Wait for all to be durable
        handle.wait_for_durable(10).unwrap();

        // Durable LSN should be >= 10
        assert!(handle.durable_lsn() >= 10);

        // Shutdown
        handle.sender.send(GraniteMessage::Shutdown).unwrap();
    }

    #[test]
    fn test_granite_worker_concurrent_writers() {
        let temp_dir = tempfile::tempdir().unwrap();
        let wal_path = temp_dir.path().join("wal.log");

        let config = GraniteConfig {
            fsync_interval_ms: 30,
            ..Default::default()
        };
        let handle = Arc::new(GraniteWorkerHandle::new(wal_path.clone(), config).unwrap());

        // Spawn multiple writer threads
        let mut handles = Vec::new();
        for t in 0..5 {
            let worker = Arc::clone(&handle);
            let h = thread::spawn(move || {
                for i in 0..10 {
                    let lsn = (t * 10 + i + 1) as u64;
                    let (tx, rx) = mpsc::sync_channel(1);
                    let record = LogRecord {
                        lsn,
                        txn_id: lsn,
                        op: LogOperation::Begin,
                        timestamp: 12345,
                    };
                    let req = GraniteWriteRequest {
                        record,
                        responder: tx,
                    };
                    worker.sender.send(GraniteMessage::Write(req)).unwrap();
                    rx.recv().unwrap().unwrap();
                }
            });
            handles.push(h);
        }

        // ⏱️ Measure how long it takes to complete all writers
        let start = std::time::Instant::now();

        // Wait for all writers
        for h in handles {
            h.join().unwrap();
        }

        let duration = start.elapsed();

        // ✅ Assert duration — not just side effects after sleeping
        assert!(
            duration <= Duration::from_millis(100),
            "Expected all 50 writes to complete within 100ms, but took {:?}", 
            duration
        );

        // Also validate correctness: 50 records ⇒ durable LSN ≥ 50
        assert!(handle.durable_lsn() >= 50, "Expected at least 50 durable records");

        // Shutdown
        handle.sender.send(GraniteMessage::Shutdown).unwrap();
    }

    #[test]
    fn test_granite_worker_stress_test() {
        let temp_dir = tempfile::tempdir().unwrap();
        let wal_path = temp_dir.path().join("wal.log");

        let config = GraniteConfig {
            fsync_interval_ms: 20,
            max_batch_size: 256,
            ..Default::default()
        };
        let handle = Arc::new(GraniteWorkerHandle::new(wal_path.clone(), config).unwrap());

        // Spawn many writer threads with rapid writes
        let num_threads = 10;
        let writes_per_thread = 100;
        let mut handles = Vec::new();

        for t in 0..num_threads {
            let worker = Arc::clone(&handle);
            let h = thread::spawn(move || {
                for i in 0..writes_per_thread {
                    let lsn = (t * writes_per_thread + i + 1) as u64;
                    let (tx, rx) = mpsc::sync_channel(1);
                    let record = LogRecord {
                        lsn,
                        txn_id: lsn,
                        op: LogOperation::Insert {
                            table: format!("table_{}", t),
                            row_id: i as u64,
                            values: vec![],
                        },
                        timestamp: 12345,
                    };
                    let req = GraniteWriteRequest {
                        record,
                        responder: tx,
                    };
                    worker.sender.send(GraniteMessage::Write(req)).unwrap();
                    rx.recv().unwrap().unwrap();
                }
            });
            handles.push(h);
        }

        // Wait for all writers
        for h in handles {
            h.join().unwrap();
        }

        // Force sync and verify
        handle.force_sync().unwrap();
        let total_writes = num_threads * writes_per_thread;
        assert!(handle.durable_lsn() >= total_writes as u64);

        // Verify file exists and has content
        let metadata = std::fs::metadata(&wal_path).unwrap();
        assert!(metadata.len() > 0);

        // Shutdown
        handle.sender.send(GraniteMessage::Shutdown).unwrap();
    }
}
