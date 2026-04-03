//! Database with 256-way sharding.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::shard::Shard;
use crate::Value;

const NUM_SHARDS: usize = 256;

/// Main database managing 256 shards.
pub struct Database {
    shards: Vec<Shard>,
}

impl Database {
    /// Create a new database.
    pub fn new() -> Self {
        let shards = (0..NUM_SHARDS).map(|_| Shard::new()).collect();

        Self { shards }
    }

    fn get_shard_index(key: &[u8]) -> usize {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % NUM_SHARDS
    }

    fn get_shard(&self, key: &[u8]) -> &Shard {
        &self.shards[Self::get_shard_index(key)]
    }

    /// Get value by key.
    pub async fn get(&self, key: &[u8]) -> Option<Value> {
        let shard = self.get_shard(key);
        shard.get(key).await
    }

    /// Insert or update key-value pair.
    pub async fn put(&self, key: Vec<u8>, value: Value) {
        let shard_index = Self::get_shard_index(&key);
        self.shards[shard_index].put(key, value).await;
    }

    /// Delete a key.
    pub async fn delete(&self, key: &[u8]) -> bool {
        let shard = self.get_shard(key);
        shard.delete(key).await
    }

    /// Check if key exists.
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
    }

    #[tokio::test]
    async fn test_multiple_keys() {
        let db = Database::new();

        for i in 0..100 {
            db.put(format!("key_{}", i).into_bytes(), Value::int(i))
                .await;
        }

        for i in 0..100 {
            let result = db.get(format!("key_{}", i).as_bytes()).await;
            assert_eq!(result, Some(Value::int(i)));
        }
    }
}
