use std::{io::Read, sync::Arc};

pub const MAX_OBJECT_BYTES: usize = 5 * 1024 * 1024;
pub const MAX_LIST_KEYS: usize = 1000;

/// Storage backend working on full object keys (S3 in production).
/// Implementations may block; scripts run on blocking threads.
pub trait ObjectBackend: Send + Sync {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String>;
    fn put(&self, key: &str, body: Vec<u8>) -> Result<(), String>;
    fn delete(&self, key: &str) -> Result<(), String>;
    /// Streams the object without materializing it. Not subject to `MAX_OBJECT_BYTES`.
    fn open(&self, key: &str) -> Result<Option<Box<dyn Read + Send>>, String>;
    /// Keys starting with `prefix`, at most `limit`.
    fn list(&self, prefix: &str, limit: usize) -> Result<Vec<String>, String>;
}

/// A backend confined to one key prefix (`<base>/<tenant>/`). Scripts only ever
/// see keys relative to it and cannot name anything outside.
#[derive(Clone)]
pub struct DataScope {
    backend: Arc<dyn ObjectBackend>,
    prefix: String,
    max_object_bytes: usize,
}

impl DataScope {
    /// `prefix` must be empty or end with `/`.
    pub fn new(backend: Arc<dyn ObjectBackend>, prefix: impl Into<String>) -> Self {
        let prefix = prefix.into();
        debug_assert!(prefix.is_empty() || prefix.ends_with('/'));
        Self { backend, prefix, max_object_bytes: MAX_OBJECT_BYTES }
    }

    pub fn with_max_object_bytes(mut self, n: usize) -> Self {
        self.max_object_bytes = n;
        self
    }

    fn full(&self, key: &str) -> Result<String, String> {
        let bad = key.is_empty()
            || key.len() > 512
            || key.starts_with('/')
            || key.chars().any(|c| c.is_control() || c == '\\')
            || key.split('/').any(|seg| seg == ".." || seg == ".");
        if bad {
            return Err(format!("invalid key {key:?}"));
        }
        Ok(format!("{}{}", self.prefix, key))
    }

    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        self.backend.get(&self.full(key)?)
    }

    pub fn put(&self, key: &str, body: Vec<u8>) -> Result<(), String> {
        if body.len() > self.max_object_bytes {
            return Err(format!("object exceeds {} bytes", self.max_object_bytes));
        }
        self.backend.put(&self.full(key)?, body)
    }

    pub fn open(&self, key: &str) -> Result<Option<Box<dyn Read + Send>>, String> {
        self.backend.open(&self.full(key)?)
    }

    pub fn delete(&self, key: &str) -> Result<(), String> {
        self.backend.delete(&self.full(key)?)
    }

    /// Lists keys under the relative `prefix` (may be empty), returned relative to the scope.
    pub fn list(&self, prefix: &str) -> Result<Vec<String>, String> {
        if prefix.starts_with('/') || prefix.split('/').any(|s| s == "..") {
            return Err(format!("invalid prefix {prefix:?}"));
        }
        let keys = self.backend.list(&format!("{}{}", self.prefix, prefix), MAX_LIST_KEYS)?;
        Ok(keys.into_iter().filter_map(|k| k.strip_prefix(&self.prefix).map(str::to_string)).collect())
    }
}
