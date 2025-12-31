//! Database Engines for MiniSQL
//!
//! MiniSQL supports multiple storage engines with different performance characteristics:
//! - **Granite**: Full ACID transactions with WAL-based durability
//! - **Sandstone**: Eventual consistency with delta-CRDTs and high throughput

pub mod granite;
pub mod sandstone;
pub mod handler;

pub use handler::EngineHandler;

use serde::{Deserialize, Serialize};

/// Engine type selection
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum EngineType {
    /// Granite Engine: Full WAL-based ACID with deferred fsync.
    /// This is the default and recommended engine for production use.
    /// 
    /// Features:
    /// - Write-Ahead Logging for crash recovery
    /// - Deferred fsync with commit latches for high throughput
    /// - Full ACID guarantees
    Granite,
    
    /// Sandstone Engine: Eventual consistency with delta-CRDTs.
    /// Optimized for high-speed writes with relaxed durability.
    ///
    /// Features:
    /// - Delta-CRDT conflict-free replication
    /// - No transactions (no BEGIN/COMMIT/ROLLBACK)
    /// - Periodic background flushes
    /// - Strong eventual consistency
    Sandstone,
}

impl Default for EngineType {
    fn default() -> Self {
        EngineType::Granite
    }
}

impl std::fmt::Display for EngineType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineType::Granite => write!(f, "Granite"),
            EngineType::Sandstone => write!(f, "Sandstone"),
        }
    }
}

impl EngineType {
    /// Get a human-readable description of the engine
    pub fn description(&self) -> &'static str {
        match self {
            EngineType::Granite => "WAL-based ACID engine with deferred fsync",
            EngineType::Sandstone => "Eventual consistency engine with delta-CRDTs",
        }
    }

    /// Check if this engine provides full ACID guarantees
    pub fn is_acid_compliant(&self) -> bool {
        match self {
            EngineType::Granite => true,
            EngineType::Sandstone => false,
        }
    }

    /// Check if this engine persists data to disk
    pub fn is_persistent(&self) -> bool {
        match self {
            EngineType::Granite => true,
            EngineType::Sandstone => true,  // Eventually persists via background flush
        }
    }
    
    /// Check if this engine supports transactions
    pub fn supports_transactions(&self) -> bool {
        match self {
            EngineType::Granite => true,
            EngineType::Sandstone => false,
        }
    }

    /// Parse engine type from string name (case-insensitive)
    pub fn from_name(name: &str) -> Option<Self> {
        match name.to_uppercase().as_str() {
            "GRANITE" => Some(EngineType::Granite),
            "SANDSTONE" => Some(EngineType::Sandstone),
            _ => None,
        }
    }
}

// Re-exports for convenience
pub use granite::{GraniteConfig, TransactionManager};
pub use sandstone::{SandstoneConfig, SandstoneEngine};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_engine_type_default_is_granite() {
        assert_eq!(EngineType::default(), EngineType::Granite);
    }

    #[test]
    fn test_engine_type_display() {
        assert_eq!(EngineType::Granite.to_string(), "Granite");
        assert_eq!(EngineType::Sandstone.to_string(), "Sandstone");
    }

    #[test]
    fn test_engine_type_description() {
        assert!(EngineType::Granite.description().contains("WAL"));
        assert!(EngineType::Sandstone.description().contains("delta-CRDT"));
    }

    #[test]
    fn test_engine_type_acid_compliant() {
        assert!(EngineType::Granite.is_acid_compliant());
        assert!(!EngineType::Sandstone.is_acid_compliant());
    }

    #[test]
    fn test_engine_type_persistent() {
        assert!(EngineType::Granite.is_persistent());
        assert!(EngineType::Sandstone.is_persistent());
    }
    
    #[test]
    fn test_engine_type_transactions() {
        assert!(EngineType::Granite.supports_transactions());
        assert!(!EngineType::Sandstone.supports_transactions());
    }
}
