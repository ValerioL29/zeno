//! Zeno KV Store - Rust Implementation
//! 
//! Port of the Zig implementation with:
//! - Value enum for type-safe storage
//! - ART (Adaptive Radix Tree) index
//! - 256-way sharding
//! - Async API via tokio

pub mod art;
pub mod database;
pub mod shard;
pub mod value;

pub use database::Database;
pub use value::Value;
