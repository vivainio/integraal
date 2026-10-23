//! Tenant script loading: a `ScriptStore` backend (S3 in production) wrapped by
//! a `CachedLoader` that keeps scripts in memory and revalidates by ETag.

mod cache;
mod key;
mod s3;

pub use cache::CachedLoader;
pub use key::ScriptKey;
pub use s3::S3Store;

use std::{collections::HashMap, sync::Arc};

use async_trait::async_trait;

#[derive(Debug, thiserror::Error)]
pub enum LoadError {
    #[error("invalid {0}: {1:?}")]
    InvalidName(&'static str, String),
    #[error("script not found: {0}")]
    NotFound(String),
    #[error("script is not valid UTF-8")]
    NotUtf8,
    #[error("storage error: {0}")]
    Storage(String),
}

#[derive(Debug, Clone)]
pub struct Script {
    pub source: String,
    /// Opaque validator (S3 ETag) used for conditional re-fetches.
    pub etag: Option<String>,
}

pub enum Fetched {
    Fresh(Script),
    NotModified,
}

#[async_trait]
pub trait ScriptStore: Send + Sync {
    /// Fetch the object at `key`. If `etag` matches the stored object, return `NotModified`.
    async fn fetch(&self, key: &ScriptKey, etag: Option<&str>) -> Result<Fetched, LoadError>;
}

/// In-memory store, used in tests and local development.
#[derive(Default)]
pub struct MemoryStore(pub std::sync::Mutex<HashMap<String, String>>);

impl MemoryStore {
    pub fn put(&self, key: &ScriptKey, source: &str) {
        self.0
            .lock()
            .unwrap()
            .insert(key.object_key(), source.to_string());
    }
}

#[async_trait]
impl ScriptStore for MemoryStore {
    async fn fetch(&self, key: &ScriptKey, etag: Option<&str>) -> Result<Fetched, LoadError> {
        let map = self.0.lock().unwrap();
        let src = map
            .get(&key.object_key())
            .ok_or_else(|| LoadError::NotFound(key.object_key()))?;
        let tag = format!("{:x}", src.len());
        if etag == Some(tag.as_str()) {
            return Ok(Fetched::NotModified);
        }
        Ok(Fetched::Fresh(Script {
            source: src.clone(),
            etag: Some(tag),
        }))
    }
}

pub type SharedLoader = Arc<CachedLoader>;
