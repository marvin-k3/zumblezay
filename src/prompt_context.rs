use std::collections::HashMap;
use std::sync::Arc;
use std::time;
use tokio::sync::RwLock;

use chrono;
use tokio_util::bytes;

pub type Key = String;

// An entry may have multiple objects in it.
pub struct Entry {
    pub born: chrono::DateTime<chrono::Utc>,
    pub expires: time::Instant,
    pub objects: Vec<bytes::Bytes>,
}

#[derive(Debug)]
pub enum PromptContextError {
    NotFound,
    OffsetOutOfRange,
    Other(String),
}

impl std::fmt::Display for PromptContextError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            PromptContextError::NotFound => write!(f, "Not found"),
            PromptContextError::OffsetOutOfRange => {
                write!(f, "Offset out of range")
            }
            PromptContextError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for PromptContextError {}

pub struct Store {
    entries: RwLock<HashMap<Key, Arc<Entry>>>,
}

impl Store {
    pub fn new() -> Self {
        Self {
            entries: RwLock::new(HashMap::new()),
        }
    }

    pub async fn insert(
        &self,
        key: Key,
        objects: Vec<bytes::Bytes>,
        duration: time::Duration,
    ) {
        let entry = Entry {
            born: chrono::Utc::now(),
            expires: time::Instant::now() + duration,
            objects,
        };
        self.entries.write().await.insert(key, Arc::new(entry));
    }

    pub async fn get_entry(
        &self,
        key: Key,
    ) -> Result<Arc<Entry>, PromptContextError> {
        let store = self.entries.read().await;
        let entry = store.get(&key).ok_or(PromptContextError::NotFound)?;
        Ok(entry.clone())
    }

    /// Get the number of objects in the store.
    pub async fn len(&self) -> usize {
        self.entries.read().await.len()
    }

    /// Get an object from the store.
    /// Returns the object if it exists and is not expired.
    pub async fn get(
        &self,
        key: Key,
        offset: usize,
    ) -> Result<bytes::Bytes, PromptContextError> {
        let store = self.entries.read().await;
        let entry = store.get(&key).ok_or(PromptContextError::NotFound)?;
        let object = entry
            .objects
            .get(offset)
            .ok_or(PromptContextError::OffsetOutOfRange)?;
        Ok(object.clone())
    }

    /// Garbage collect expired entries.
    /// Returns the number of entries removed.
    pub async fn garbage_collect(&self) -> usize {
        let now = time::Instant::now();
        let mut entries = self.entries.write().await;
        let initial_count = entries.len();
        entries.retain(|_, entry| entry.expires > now);
        initial_count - entries.len()
    }

    /// Check if a key exists in the store.
    pub async fn contains_key(&self, key: &Key) -> bool {
        self.entries.read().await.contains_key(key)
    }

    /// Remove an entry from the store.
    /// Returns true if the entry was removed, false if it didn't exist.
    pub async fn remove(&self, key: &Key) -> bool {
        self.entries.write().await.remove(key).is_some()
    }

    /// Get all keys in the store.
    pub async fn keys(&self) -> Vec<Key> {
        self.entries.read().await.keys().cloned().collect()
    }

    /// Get the number of objects for a specific key.
    pub async fn object_count(
        &self,
        key: &Key,
    ) -> Result<usize, PromptContextError> {
        let store = self.entries.read().await;
        let entry = store.get(key).ok_or(PromptContextError::NotFound)?;
        Ok(entry.objects.len())
    }

    /// Clear all entries from the store.
    pub async fn clear(&self) {
        self.entries.write().await.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    #[tokio::test]
    async fn test_new_store_is_empty() {
        let store = Store::new();
        assert_eq!(store.len().await, 0);
    }

    #[tokio::test]
    async fn test_insert_and_get_single_object() {
        let store = Store::new();
        let key = "test_key".to_string();
        let data = bytes::Bytes::from("test data");

        store
            .insert(
                key.clone(),
                vec![data.clone()],
                time::Duration::from_secs(60),
            )
            .await;

        let result = store.get(key, 0).await;
        assert!(result.is_ok());
        assert_eq!(result.unwrap(), data);
    }

    #[tokio::test]
    async fn test_get_nonexistent_key() {
        let store = Store::new();
        let result = store.get("nonexistent".to_string(), 0).await;
        assert!(matches!(result, Err(PromptContextError::NotFound)));
    }

    #[tokio::test]
    async fn test_get_out_of_range_offset() {
        let store = Store::new();
        let key = "test_key".to_string();
        let data = bytes::Bytes::from("test data");

        store
            .insert(key.clone(), vec![data], time::Duration::from_secs(60))
            .await;

        let result = store.get(key, 1).await;
        assert!(matches!(result, Err(PromptContextError::OffsetOutOfRange)));
    }

    #[tokio::test]
    async fn test_contains_key() {
        let store = Store::new();
        let key = "test_key".to_string();

        assert!(!store.contains_key(&key).await);

        store
            .insert(key.clone(), vec![], time::Duration::from_secs(60))
            .await;

        assert!(store.contains_key(&key).await);
    }

    #[tokio::test]
    async fn test_remove_key() {
        let store = Store::new();
        let key = "test_key".to_string();

        store
            .insert(key.clone(), vec![], time::Duration::from_secs(60))
            .await;

        assert!(store.remove(&key).await);
        assert!(!store.contains_key(&key).await);
    }

    #[tokio::test]
    async fn test_remove_nonexistent_key() {
        let store = Store::new();
        assert!(!store.remove(&"nonexistent".to_string()).await);
    }

    #[tokio::test]
    async fn test_object_count() {
        let store = Store::new();
        let key = "test_key".to_string();
        let data1 = bytes::Bytes::from("data1");
        let data2 = bytes::Bytes::from("data2");

        store
            .insert(
                key.clone(),
                vec![data1, data2],
                time::Duration::from_secs(60),
            )
            .await;

        let count = store.object_count(&key).await;
        assert!(count.is_ok());
        assert_eq!(count.unwrap(), 2);
    }

    #[tokio::test]
    async fn test_object_count_nonexistent_key() {
        let store = Store::new();
        let result = store.object_count(&"nonexistent".to_string()).await;
        assert!(matches!(result, Err(PromptContextError::NotFound)));
    }

    #[tokio::test]
    async fn test_clear_store() {
        let store = Store::new();
        let key = "test_key".to_string();

        store
            .insert(key.clone(), vec![], time::Duration::from_secs(60))
            .await;
        assert_eq!(store.len().await, 1);

        store.clear().await;
        assert_eq!(store.len().await, 0);
    }

    #[tokio::test]
    async fn test_garbage_collect() {
        let store = Store::new();
        let key = "test_key".to_string();

        // Insert with very short duration
        store
            .insert(key.clone(), vec![], time::Duration::from_millis(100))
            .await;

        // Wait for expiration
        tokio::time::sleep(time::Duration::from_millis(150)).await;

        let removed = store.garbage_collect().await;
        assert_eq!(removed, 1);
        assert_eq!(store.len().await, 0);
    }
}
