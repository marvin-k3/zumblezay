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
    Expired,
    InvalidSignature,
    Other(String),
}

impl std::fmt::Display for PromptContextError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            PromptContextError::NotFound => write!(f, "Not found"),
            PromptContextError::OffsetOutOfRange => {
                write!(f, "Offset out of range")
            }
            PromptContextError::Expired => write!(f, "Expired"),
            PromptContextError::InvalidSignature => {
                write!(f, "Invalid signature")
            }
            PromptContextError::Other(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for PromptContextError {}

pub struct Store {
    entries: RwLock<HashMap<Key, Arc<Entry>>>,
}

impl Default for Store {
    fn default() -> Self {
        Self::new()
    }
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
        let now = time::Instant::now();

        let (entry, expired) = {
            let store = self.entries.read().await;
            match store.get(&key) {
                Some(entry) if entry.expires > now => {
                    (Some(entry.clone()), false)
                }
                Some(_) => (None, true),
                None => (None, false),
            }
        };

        if let Some(entry) = entry {
            return Ok(entry);
        }

        if expired {
            let mut store = self.entries.write().await;
            if let Some(entry) = store.get(&key) {
                if entry.expires <= now {
                    store.remove(&key);
                }
            }
            return Err(PromptContextError::Expired);
        }

        Err(PromptContextError::NotFound)
    }

    /// Get the number of objects in the store.
    pub async fn len(&self) -> usize {
        self.entries.read().await.len()
    }

    /// Check if the store is empty.
    pub async fn is_empty(&self) -> bool {
        self.entries.read().await.is_empty()
    }

    /// Get an object from the store.
    /// Returns the object if it exists and is not expired.
    pub async fn get(
        &self,
        key: Key,
        offset: usize,
    ) -> Result<bytes::Bytes, PromptContextError> {
        let now = time::Instant::now();

        let (entry, expired) = {
            let store = self.entries.read().await;
            match store.get(&key) {
                Some(entry) if entry.expires > now => {
                    (Some(entry.clone()), false)
                }
                Some(_) => (None, true),
                None => (None, false),
            }
        };

        if let Some(entry) = entry {
            let object = entry
                .objects
                .get(offset)
                .ok_or(PromptContextError::OffsetOutOfRange)?;
            return Ok(object.clone());
        }

        if expired {
            let mut store = self.entries.write().await;
            if let Some(entry) = store.get(&key) {
                if entry.expires <= now {
                    store.remove(&key);
                }
            }
            return Err(PromptContextError::Expired);
        }

        Err(PromptContextError::NotFound)
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

pub mod sign {
    use super::Key;
    use super::PromptContextError;
    use base64::Engine as _;
    use hmac::{Hmac, Mac};
    use sha2::Sha256;
    type HmacSha256 = Hmac<Sha256>;
    use serde::{Deserialize, Serialize};
    use std::time::{Duration, SystemTime, UNIX_EPOCH};

    pub fn sign_request(
        secret: &str,
        expires: u64,
        entry_key: &Key,
        offset: usize,
    ) -> String {
        let mut mac = HmacSha256::new_from_slice(secret.as_bytes())
            .expect("HMAC can handle any key length");
        mac.update(entry_key.as_bytes());
        mac.update(offset.to_string().as_bytes());
        mac.update(expires.to_string().as_bytes());
        let result = mac.finalize().into_bytes().to_vec();

        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(&result)
    }

    #[derive(Debug, Serialize, Deserialize)]
    pub struct SignedRequestParams {
        pub hmac: String,
        pub expires: u64,
    }

    impl SignedRequestParams {
        pub fn validate(&self) -> Result<(), PromptContextError> {
            if self.hmac.is_empty() || self.expires == 0 {
                return Err(PromptContextError::InvalidSignature);
            }
            Ok(())
        }
    }

    pub fn sign_request_with_duration(
        secret: &str,
        duration: Duration,
        entry_key: &Key,
        offset: usize,
    ) -> Result<SignedRequestParams, PromptContextError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| PromptContextError::Expired)?;
        let expires = now + duration;
        let hmac =
            sign_request(secret, expires.as_millis() as u64, entry_key, offset);
        Ok(SignedRequestParams {
            hmac,
            expires: expires.as_millis() as u64,
        })
    }

    pub fn verify_request(
        secret: &str,
        expires: u64,
        entry_key: &Key,
        offset: usize,
        signature: &str,
    ) -> Result<(), PromptContextError> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| PromptContextError::Expired)?;
        let expiry = Duration::from_millis(expires);
        if now > expiry {
            return Err(PromptContextError::Expired);
        }
        let expected = sign_request(secret, expires, entry_key, offset);
        if expected != signature {
            return Err(PromptContextError::InvalidSignature);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio;

    #[tokio::test]
    async fn test_new_store_is_empty() {
        let store = Store::new();
        assert!(store.is_empty().await);
        assert_eq!(store.len().await, 0);
    }

    #[tokio::test]
    async fn test_store_not_empty_after_insert() {
        let store = Store::new();
        let key = "test_key".to_string();
        let data = bytes::Bytes::from("test data");

        assert!(store.is_empty().await);
        store
            .insert(
                key.clone(),
                vec![data.clone()],
                time::Duration::from_secs(60),
            )
            .await;
        assert!(!store.is_empty().await);
    }

    #[tokio::test]
    async fn test_store_empty_after_clear() {
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
        assert!(!store.is_empty().await);

        store.clear().await;
        assert!(store.is_empty().await);
    }

    #[tokio::test]
    async fn test_store_empty_after_remove() {
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
        assert!(!store.is_empty().await);

        store.remove(&key).await;
        assert!(store.is_empty().await);
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
    async fn test_get_expired_entry() {
        let store = Store::new();
        let key = "expired_key".to_string();
        let data = bytes::Bytes::from("stale data");

        store
            .insert(
                key.clone(),
                vec![data.clone()],
                time::Duration::from_millis(10),
            )
            .await;

        tokio::time::sleep(time::Duration::from_millis(20)).await;

        let result = store.get(key.clone(), 0).await;
        assert!(matches!(result, Err(PromptContextError::Expired)));
        assert!(!store.contains_key(&key).await);
    }

    #[tokio::test]
    async fn test_get_entry_expired() {
        let store = Store::new();
        let key = "expired_entry".to_string();
        store
            .insert(key.clone(), vec![], time::Duration::from_millis(10))
            .await;

        tokio::time::sleep(time::Duration::from_millis(20)).await;

        let result = store.get_entry(key.clone()).await;
        assert!(matches!(result, Err(PromptContextError::Expired)));
        assert!(!store.contains_key(&key).await);
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

    #[test]
    fn test_sign_request_basic() {
        let secret = "test_secret";
        let expires = 1234567890;
        let entry_key = "test_key".to_string();
        let offset = 0;

        let signature = sign::sign_request(secret, expires, &entry_key, offset);
        assert!(!signature.is_empty());
        assert!(signature
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_'));
    }

    #[test]
    fn test_sign_request_different_inputs() {
        let secret = "test_secret";
        let expires = 1234567890;
        let entry_key = "test_key".to_string();
        let offset = 0;

        let sig1 = sign::sign_request(secret, expires, &entry_key, offset);
        let sig2 = sign::sign_request(secret, expires + 1, &entry_key, offset);
        let sig3 = sign::sign_request(secret, expires, &entry_key, offset + 1);

        assert_ne!(sig1, sig2);
        assert_ne!(sig1, sig3);
    }

    #[test]
    fn test_verify_request_valid() {
        let secret = "test_secret";
        let expires = (std::time::SystemTime::now()
            + std::time::Duration::from_secs(60))
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
        let entry_key = "test_key".to_string();
        let offset = 0;

        let signature = sign::sign_request(secret, expires, &entry_key, offset);
        let result = sign::verify_request(
            secret, expires, &entry_key, offset, &signature,
        );
        assert!(result.is_ok());
    }

    #[test]
    fn test_verify_request_expired() {
        let secret = "test_secret";
        let expires = (std::time::SystemTime::now()
            - std::time::Duration::from_secs(60))
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
        let entry_key = "test_key".to_string();
        let offset = 0;

        let signature = sign::sign_request(secret, expires, &entry_key, offset);
        let result = sign::verify_request(
            secret, expires, &entry_key, offset, &signature,
        );
        assert!(matches!(result, Err(PromptContextError::Expired)));
    }

    #[test]
    fn test_verify_request_invalid_signature() {
        let secret = "test_secret";
        let expires = (std::time::SystemTime::now()
            + std::time::Duration::from_secs(60))
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
        let entry_key = "test_key".to_string();
        let offset = 0;

        let result = sign::verify_request(
            secret,
            expires,
            &entry_key,
            offset,
            "invalid_signature",
        );
        assert!(matches!(result, Err(PromptContextError::InvalidSignature)));
    }

    #[test]
    fn test_sign_request_with_duration() {
        let secret = "test_secret";
        let entry_key = "test_key".to_string();
        let offset = 0;
        let duration = std::time::Duration::from_secs(60);

        // Calculate expiration time once
        let expires = (std::time::SystemTime::now() + duration)
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;

        // Generate signature with the calculated expiration time
        let signature = sign::sign_request(secret, expires, &entry_key, offset);

        // Verify with the same expiration time
        let verify_result = sign::verify_request(
            secret, expires, &entry_key, offset, &signature,
        );
        assert!(verify_result.is_ok());
    }
}
