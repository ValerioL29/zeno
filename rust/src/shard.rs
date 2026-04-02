//! Single shard of the database.
//!
//! Each shard contains an in-memory HashMap for storage.
//! This is a simplified implementation that will be replaced
//! by a full ART index in the future.
//!
//! Uses tokio's RwLock for concurrent access:
//! - Multiple readers can access simultaneously
//! - Writers have exclusive access

use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;

use crate::Value;

/// A single shard of the database.
///
/// Wraps a HashMap with async read-write locking.
#[derive(Debug)]
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
    ///
    /// Returns `None` if the key doesn't exist.
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
    ///
    /// Returns `true` if the key existed and was deleted.
    pub async fn delete(&self, key: &[u8]) -> bool {
        let mut data = self.data.write().await;
        data.remove(key).is_some()
    }

    /// Check if a key exists.
    pub async fn exists(&self, key: &[u8]) -> bool {
        let data = self.data.read().await;
        data.contains_key(key)
    }

    /// Get the number of keys in the shard.
    pub async fn len(&self) -> usize {
        let data = self.data.read().await;
        data.len()
    }

    /// Check if the shard is empty.
    pub async fn is_empty(&self) -> bool {
        let data = self.data.read().await;
        data.is_empty()
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
    async fn test_get_nonexistent() {
        let shard = Shard::new();
        
        let result = shard.get(b"nonexistent").await;
        assert_eq!(result, None);
    }

    #[tokio::test]
    async fn test_put_overwrite() {
        let shard = Shard::new();
        shard.put(b"key".to_vec(), Value::string("first")).await;
        shard.put(b"key".to_vec(), Value::string("second")).await;
        
        let result = shard.get(b"key").await;
        assert_eq!(result, Some(Value::string("second")));
    }

    #[tokio::test]
    async fn test_delete() {
        let shard = Shard::new();
        shard.put(b"key".to_vec(), Value::int(42)).await;
        
        assert!(shard.delete(b"key").await);
        assert!(!shard.exists(b"key").await);
        assert_eq!(shard.get(b"key").await, None);
    }

    #[tokio::test]
    async fn test_delete_nonexistent() {
        let shard = Shard::new();
        
        assert!(!shard.delete(b"nonexistent").await);
    }

    #[tokio::test]
    async fn test_exists() {
        let shard = Shard::new();
        
        assert!(!shard.exists(b"key").await);
        
        shard.put(b"key".to_vec(), Value::null()).await;
        assert!(shard.exists(b"key").await);
        
        shard.delete(b"key").await;
        assert!(!shard.exists(b"key").await);
    }

    #[tokio::test]
    async fn test_len_and_is_empty() {
        let shard = Shard::new();
        
        assert!(shard.is_empty().await);
        assert_eq!(shard.len().await, 0);
        
        shard.put(b"key1".to_vec(), Value::int(1)).await;
        assert!(!shard.is_empty().await);
        assert_eq!(shard.len().await, 1);
        
        shard.put(b"key2".to_vec(), Value::int(2)).await;
        assert_eq!(shard.len().await, 2);
        
        shard.delete(b"key1").await;
        assert_eq!(shard.len().await, 1);
        
        shard.delete(b"key2").await;
        assert!(shard.is_empty().await);
    }

    #[tokio::test]
    async fn test_binary_keys() {
        let shard = Shard::new();
        
        let key = vec![0u8, 255, 128, 64];
        shard.put(key.clone(), Value::string("binary")).await;
        
        assert_eq!(shard.get(&key).await, Some(Value::string("binary")));
    }

    #[tokio::test]
    async fn test_concurrent_reads() {
        use tokio::task::JoinSet;
        
        let shard = Arc::new(Shard::new());
        shard.put(b"key".to_vec(), Value::string("value")).await;
        
        let mut set = JoinSet::new();
        
        // Spawn multiple concurrent reads
        for _ in 0..10 {
            let shard = shard.clone();
            set.spawn(async move {
                shard.get(b"key").await
            });
        }
        
        // All reads should succeed
        while let Some(result) = set.join_next().await {
            assert_eq!(result.unwrap(), Some(Value::string("value")));
        }
    }

    #[tokio::test]
    async fn test_concurrent_writes() {
        use tokio::task::JoinSet;
        
        let shard = Arc::new(Shard::new());
        let mut set = JoinSet::new();
        
        // Spawn multiple concurrent writes
        for i in 0..10 {
            let shard = shard.clone();
            set.spawn(async move {
                shard.put(format!("key_{}", i).into_bytes(), Value::int(i)).await;
            });
        }
        
        // Wait for all writes
        while let Some(result) = set.join_next().await {
            result.unwrap();
        }
        
        // Verify all writes succeeded
        for i in 0..10 {
            let result = shard.get(format!("key_{}", i).as_bytes()).await;
            assert_eq!(result, Some(Value::int(i)));
        }
    }

    #[tokio::test]
    async fn test_complex_values() {
        let shard = Shard::new();
        
        // Test array
        let arr = Value::array(vec![
            Value::int(1),
            Value::int(2),
            Value::string("three"),
        ]);
        shard.put(b"array".to_vec(), arr.clone()).await;
        assert_eq!(shard.get(b"array").await, Some(arr));
        
        // Test nested object
        let mut inner = HashMap::new();
        inner.insert("x".to_string(), Value::int(10));
        inner.insert("y".to_string(), Value::int(20));
        
        let mut outer = HashMap::new();
        outer.insert("point".to_string(), Value::object(inner));
        outer.insert("name".to_string(), Value::string("test"));
        
        let obj = Value::object(outer);
        shard.put(b"object".to_vec(), obj.clone()).await;
        assert_eq!(shard.get(b"object").await, Some(obj));
    }

    #[test]
    fn test_default() {
        let shard: Shard = Default::default();
        // Just verify it compiles and creates
        drop(shard);
    }
}
