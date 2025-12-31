//! Configuration for Sandstone engine

/// Configuration for Sandstone engine
#[derive(Debug, Clone)]
pub struct SandstoneConfig {
    /// How often to flush dirty tables to disk (milliseconds)
    pub flush_interval_ms: u64,
    /// Maximum number of dirty tables before forcing a flush (None = unlimited)
    pub max_dirty_tables: Option<usize>,
    /// Enable delta-CRDT tracking for eventual consistency
    pub enable_delta_crdt: bool,
}

impl Default for SandstoneConfig {
    fn default() -> Self {
        Self {
            flush_interval_ms: 1000,  // 1 second
            max_dirty_tables: None,
            enable_delta_crdt: true,
        }
    }
}

impl SandstoneConfig {
    /// Create a config optimized for high throughput (longer flush interval)
    pub fn high_throughput() -> Self {
        Self {
            flush_interval_ms: 5000,  // 5 seconds
            max_dirty_tables: None,
            enable_delta_crdt: true,
        }
    }
    
    /// Create a config with more frequent flushes (lower data loss window)
    pub fn low_latency() -> Self {
        Self {
            flush_interval_ms: 500,  // 500ms
            max_dirty_tables: Some(10),
            enable_delta_crdt: true,
        }
    }
}
