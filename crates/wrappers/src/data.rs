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
///
/// Presigning is a capability handed to a third party, so it is granted separately from the
/// script's own access: `upload` allows `presign_put`, `download` allows `presign_get`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Access {
    /// `get`, `open`, `list`.
    pub read: bool,
    /// `put`, `delete`.
    pub write: bool,
    /// `presign_put`.
    pub upload: bool,
    /// `presign_get`.
    pub download: bool,
}

impl Access {
    pub const READ_WRITE: Access = Access { read: true, write: true, upload: false, download: false };

    fn grant(&mut self, name: &str) -> Result<(), String> {
        match name {
            "read" => self.read = true,
            "write" => self.write = true,
            "readwrite" => (self.read, self.write) = (true, true),
            "upload" => self.upload = true,
            "download" => self.download = true,
            other => return Err(format!("unknown data grant {other:?} (expected read, write, readwrite, upload or download)")),
        }
        Ok(())
    }
}

/// A grant is one name (`"read"`) or a list (`["read", "upload"]`).
impl<'de> serde::Deserialize<'de> for Access {
    fn deserialize<D: serde::Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        #[derive(serde::Deserialize)]
        #[serde(untagged)]
        enum OneOrMany {
            One(String),
            Many(Vec<String>),
        }
        let names = match OneOrMany::deserialize(d)? {
            OneOrMany::One(n) => vec![n],
            OneOrMany::Many(v) => v,
        };
        if names.is_empty() {
            return Err(serde::de::Error::custom("empty data grant"));
        }
        let mut access = Access::default();
        for n in &names {
            access.grant(n).map_err(serde::de::Error::custom)?;
        }
        Ok(access)
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
    /// `prefix` must be empty or end with `/`. Starts with read and write access; narrow it with `with_access`.
    pub fn new(backend: Arc<dyn ObjectBackend>, prefix: impl Into<String>) -> Self {
        let prefix = prefix.into();
        debug_assert!(prefix.is_empty() || prefix.ends_with('/'));
        Self { backend, prefix, max_object_bytes: MAX_OBJECT_BYTES, access: Access::READ_WRITE }
    }

    pub fn with_access(mut self, access: Access) -> Self {
        self.access = access;
        self
    }

    fn need_read(&self) -> Result<(), String> {
        if self.access.read { Ok(()) } else { Err("this bucket is not readable for this tenant".into()) }
    }

    fn need_write(&self) -> Result<(), String> {
        if self.access.write { Ok(()) } else { Err("this bucket is not writable for this tenant".into()) }
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

    /// A presigned URL for one key of this scope. GET needs the `download` grant, PUT needs `upload`.
    pub fn presign(&self, method: PresignMethod, key: &str, ttl: Option<std::time::Duration>) -> Result<String, String> {
        match method {
            PresignMethod::Get if !self.access.download => return Err("presigned downloads are not granted for this bucket".into()),
            PresignMethod::Put if !self.access.upload => return Err("presigned uploads are not granted for this bucket".into()),
            _ => {}
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
