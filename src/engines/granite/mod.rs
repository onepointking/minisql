//! Granite Engine - Full ACID Transactions with WAL
//!
//! Named as a play on "WAL" (wall → stone wall → granite)

pub mod manager;
pub mod wal;
pub mod recovery;
pub mod log;
pub mod types;
pub mod handler;

// Public API
pub use manager::TransactionManager;
pub use wal::{GraniteConfig, GraniteWorkerHandle};
pub use recovery::*;
pub use types::*;
pub use handler::GraniteHandler;
