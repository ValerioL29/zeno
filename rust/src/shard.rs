//! Simplified in-memory store for initial Rust implementation.
//! 
//! This is a simplified version that will be replaced by full ART implementation.
//! For now, it uses HashMap for storage to demonstrate the API.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::Value;

/// A single shard of the database.
pub struct Shard {
    data: Arc<RwLock<HashMap<Vec<u8>, Value>>>,
}

impl Shard {
    /// Create a new empty shard.
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get a value by key.
    pub async fn get(&self, key: &[u8]) -> Option<Value> {
        let data = self.data.read().await;
        data.get(key).cloned()
    }

    /// Insert or update a key-value pair.
    pub async fn put(&self, key: Vec<u8>, value: Value) {
        let mut data = self.data.write().await;
        data.insert(key, value);
    }

    /// Delete a key.
    pub async fn delete(&self, key: &[u8]) -> bool {
        let mut data = self.data.write().await;
        data.remove(key).is_some()
    }

    /// Check if key exists.
    pub async fn exists(&self, key: &[u8]) -> bool {
        let data = self.data.read().await;
        data.contains_key(key)
    }
}

impl Default for Shard {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_put_and_get() {
        let shard = Shard::new();
        shard.put(b"key".to_vec(), Value::string("value")).await;
        
        let result = shard.get(b"key").await;
        assert_eq!(result, Some(Value::string("value")));
    }

    #[tokio::test]
    async fn test_delete() {
        let shard = Shard::new();
        shard.put(b"key".to_vec(), Value::int(42)).await;
        
        assert!(shard.delete(b"key").await);
        assert!(!shard.exists(b"key").await);
    }
}
