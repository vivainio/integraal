use std::{collections::HashMap, io::Read, sync::Arc};

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
    /// A time-limited URL that lets a third party GET or PUT exactly this key.
    fn presign(&self, _method: PresignMethod, _key: &str, _ttl: std::time::Duration) -> Result<String, String> {
        Err("presigned URLs are not supported by this backend".into())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PresignMethod {
    Get,
    Put,
}

/// Longest a presigned URL may live.
pub const MAX_PRESIGN_TTL: std::time::Duration = std::time::Duration::from_secs(60 * 60);

/// What a tenant may do in one named bucket, granted by the `data` block of its `config.json`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Access {
    /// `get`, `open`, `list`.
    Read,
    /// `put`, `delete`.
    Write,
    ReadWrite,
}

impl Access {
    fn reads(self) -> bool {
        matches!(self, Access::Read | Access::ReadWrite)
    }

    fn writes(self) -> bool {
        matches!(self, Access::Write | Access::ReadWrite)
    }
}

/// The buckets one invocation may use, by name.
pub type DataScopes = HashMap<String, DataScope>;

/// A backend confined to one key prefix (`<base>/<tenant>/`). Scripts only ever
/// see keys relative to it and cannot name anything outside.
#[derive(Clone)]
pub struct DataScope {
    backend: Arc<dyn ObjectBackend>,
    prefix: String,
    max_object_bytes: usize,
    access: Access,
}

impl DataScope {
    /// `prefix` must be empty or end with `/`. Starts with full access; narrow it with `with_access`.
    pub fn new(backend: Arc<dyn ObjectBackend>, prefix: impl Into<String>) -> Self {
        let prefix = prefix.into();
        debug_assert!(prefix.is_empty() || prefix.ends_with('/'));
        Self { backend, prefix, max_object_bytes: MAX_OBJECT_BYTES, access: Access::ReadWrite }
    }

    pub fn with_access(mut self, access: Access) -> Self {
        self.access = access;
        self
    }

    fn need_read(&self) -> Result<(), String> {
        if self.access.reads() { Ok(()) } else { Err("this bucket is write-only for this tenant".into()) }
    }

    fn need_write(&self) -> Result<(), String> {
        if self.access.writes() { Ok(()) } else { Err("this bucket is read-only for this tenant".into()) }
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
        self.need_read()?;
        self.backend.get(&self.full(key)?)
    }

    pub fn put(&self, key: &str, body: Vec<u8>) -> Result<(), String> {
        self.need_write()?;
        if body.len() > self.max_object_bytes {
            return Err(format!("object exceeds {} bytes", self.max_object_bytes));
        }
        self.backend.put(&self.full(key)?, body)
    }

    pub fn open(&self, key: &str) -> Result<Option<Box<dyn Read + Send>>, String> {
        self.need_read()?;
        self.backend.open(&self.full(key)?)
    }

    /// A presigned URL for one key of this scope. GET needs read access, PUT needs write access.
    pub fn presign(&self, method: PresignMethod, key: &str, ttl: Option<std::time::Duration>) -> Result<String, String> {
        match method {
            PresignMethod::Get => self.need_read()?,
            PresignMethod::Put => self.need_write()?,
        }
        let ttl = ttl.unwrap_or(std::time::Duration::from_secs(15 * 60)).clamp(std::time::Duration::from_secs(1), MAX_PRESIGN_TTL);
        self.backend.presign(method, &self.full(key)?, ttl)
    }

    pub fn delete(&self, key: &str) -> Result<(), String> {
        self.need_write()?;
        self.backend.delete(&self.full(key)?)
    }

    /// Lists keys under the relative `prefix` (may be empty), returned relative to the scope.
    pub fn list(&self, prefix: &str) -> Result<Vec<String>, String> {
        self.need_read()?;
        if prefix.starts_with('/') || prefix.split('/').any(|s| s == "..") {
            return Err(format!("invalid prefix {prefix:?}"));
        }
        let keys = self.backend.list(&format!("{}{}", self.prefix, prefix), MAX_LIST_KEYS)?;
        Ok(keys.into_iter().filter_map(|k| k.strip_prefix(&self.prefix).map(str::to_string)).collect())
    }
}
