//! Database with 256-way sharding.
//!
//! The database shards keys across 256 shards based on hash for parallelism.
//! Each shard operates independently with its own lock.
//!
//! # Examples
//!
//! ```no_run
//! use zeno::{Database, Value};
//!
//! # async fn example() {
//! let db = Database::new();
//! db.put(b"key".to_vec(), Value::string("value")).await;
//! assert_eq!(db.get(b"key").await, Some(Value::string("value")));
//! # }
//! ```

use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

use crate::shard::Shard;
use crate::Value;

/// Number of shards for parallel access.
pub const NUM_SHARDS: usize = 256;

/// Main database managing 256 shards.
///
/// Provides async key-value operations with automatic sharding.
#[derive(Debug)]
pub struct Database {
    shards: Vec<Shard>,
}

impl Database {
    /// Create a new database with 256 empty shards.
    pub fn new() -> Self {
        let shards = (0..NUM_SHARDS)
            .map(|_| Shard::new())
            .collect();
        
        Self { shards }
    }

    /// Get the shard index for a key.
    fn get_shard_index(key: &[u8]) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % NUM_SHARDS
    }

    fn get_shard(&self, key: &[u8]) -> &Shard {
        &self.shards[Self::get_shard_index(key)]
    }

    /// Get value by key.
    ///
    /// Returns `None` if the key doesn't exist.
    pub async fn get(&self, key: &[u8]) -> Option<Value> {
        let shard = self.get_shard(key);
        shard.get(key).await
    }

    /// Insert or update a key-value pair.
    pub async fn put(&self, key: Vec<u8>, value: Value) {
        let shard_index = Self::get_shard_index(&key);
        self.shards[shard_index].put(key, value).await;
    }

    /// Delete a key.
    ///
    /// Returns `true` if the key existed and was deleted.
    pub async fn delete(&self, key: &[u8]) -> bool {
        let shard = self.get_shard(key);
        shard.delete(key).await
    }

    /// Check if a key exists.
    pub async fn exists(&self, key: &[u8]) -> bool {
        let shard = self.get_shard(key);
        shard.exists(key).await
    }
}

impl Default for Database {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use super::*;

    #[tokio::test]
    async fn test_basic_operations() {
        let db = Database::new();
        
        // Test put and get
        db.put(b"key".to_vec(), Value::string("value")).await;
        assert_eq!(db.get(b"key").await, Some(Value::string("value")));
        
        // Test overwrite
        db.put(b"key".to_vec(), Value::int(42)).await;
        assert_eq!(db.get(b"key").await, Some(Value::int(42)));
        
        // Test delete
        assert!(db.delete(b"key").await);
        assert!(!db.exists(b"key").await);
        
        // Test delete non-existent
        assert!(!db.delete(b"nonexistent").await);
        
        // Test get non-existent
        assert_eq!(db.get(b"nonexistent").await, None);
    }

    #[tokio::test]
    async fn test_multiple_keys() {
        let db = Database::new();
        
        for i in 0..100 {
            db.put(format!("key_{}", i).into_bytes(), Value::int(i)).await;
        }
        
        for i in 0..100 {
            let result = db.get(format!("key_{}", i).as_bytes()).await;
            assert_eq!(result, Some(Value::int(i)));
        }
    }

    #[tokio::test]
    async fn test_binary_keys() {
        let db = Database::new();
        
        // Test with binary keys (including null bytes)
        let key1 = vec![0u8, 1, 2, 3];
        let key2 = vec![255u8, 254, 253];
        
        db.put(key1.clone(), Value::string("binary1")).await;
        db.put(key2.clone(), Value::string("binary2")).await;
        
        assert_eq!(db.get(&key1).await, Some(Value::string("binary1")));
        assert_eq!(db.get(&key2).await, Some(Value::string("binary2")));
    }

    #[tokio::test]
    async fn test_exists() {
        let db = Database::new();
        
        assert!(!db.exists(b"key").await);
        
        db.put(b"key".to_vec(), Value::null()).await;
        assert!(db.exists(b"key").await);
        
        db.delete(b"key").await;
        assert!(!db.exists(b"key").await);
    }

    #[tokio::test]
    async fn test_empty_key() {
        let db = Database::new();
        
        db.put(vec![], Value::string("empty")).await;
        assert_eq!(db.get(&[]).await, Some(Value::string("empty")));
    }

    #[tokio::test]
    async fn test_complex_values() {
        let db = Database::new();
        
        // Test array value
        let arr = Value::array(vec![
            Value::int(1),
            Value::int(2),
            Value::int(3),
        ]);
        db.put(b"array".to_vec(), arr.clone()).await;
        assert_eq!(db.get(b"array").await, Some(arr));
        
        // Test object value
        let mut map = HashMap::new();
        map.insert("name".to_string(), Value::string("test"));
        map.insert("value".to_string(), Value::int(42));
        let obj = Value::object(map);
        db.put(b"object".to_vec(), obj.clone()).await;
        assert_eq!(db.get(b"object").await, Some(obj));
    }

    #[tokio::test]
    async fn test_concurrent_access() {
        use tokio::task::JoinSet;
        
        let db = std::sync::Arc::new(Database::new());
        let mut set = JoinSet::new();
        
        // Spawn multiple concurrent writes
        for i in 0..10 {
            let db = db.clone();
            set.spawn(async move {
                db.put(format!("key_{}", i).into_bytes(), Value::int(i)).await;
            });
        }
        
        // Wait for all writes to complete
        while let Some(result) = set.join_next().await {
            result.unwrap();
        }
        
        // Verify all values
        for i in 0..10 {
            let result = db.get(format!("key_{}", i).as_bytes()).await;
            assert_eq!(result, Some(Value::int(i)));
        }
    }

    #[test]
    fn test_shard_index_distribution() {
        // Verify that keys are distributed across shards
        let mut indices = std::collections::HashSet::new();
        
        for i in 0..1000 {
            let key = format!("key_{}", i);
            let idx = Database::get_shard_index(key.as_bytes());
            indices.insert(idx);
        }
        
        // Should use multiple shards (statistically very likely)
        assert!(indices.len() > 10, "Keys should be distributed across multiple shards");
        
        // All indices should be valid
        for idx in &indices {
            assert!(*idx < NUM_SHARDS);
        }
    }
}
