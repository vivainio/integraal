use std::{
    collections::HashMap,
    io::Read,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
};

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
    /// Starts a streaming upload of `key`. The object only appears once the writer is committed;
    /// a writer dropped without `commit` leaves nothing behind. Not subject to `MAX_OBJECT_BYTES`.
    fn create(&self, _key: &str) -> Result<Box<dyn ObjectWriter>, String> {
        Err("streaming writes are not supported by this backend".into())
    }
    /// Keys starting with `prefix`, at most `limit`.
    fn list(&self, prefix: &str, limit: usize) -> Result<Vec<String>, String>;
    /// A time-limited URL that lets a third party GET or PUT exactly this key.
    fn presign(
        &self,
        _method: PresignMethod,
        _key: &str,
        _ttl: std::time::Duration,
    ) -> Result<String, String> {
        Err("presigned URLs are not supported by this backend".into())
    }
}

/// A streaming upload in progress (S3 multipart in production).
pub trait ObjectWriter: Send {
    fn write(&mut self, data: &[u8]) -> Result<(), String>;
    /// Makes the object visible.
    fn commit(self: Box<Self>) -> Result<(), String>;
    /// Discards everything written so far.
    fn abort(self: Box<Self>);
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
    /// `put`.
    pub write: bool,
    /// `delete`.
    pub delete: bool,
    /// `presign_put`.
    pub upload: bool,
    /// `presign_get`.
    pub download: bool,
}

impl Access {
    pub const READ_WRITE: Access = Access {
        read: true,
        write: true,
        delete: true,
        upload: false,
        download: false,
    };

    fn grant(&mut self, name: &str) -> Result<(), String> {
        match name {
            "read" => self.read = true,
            "write" => self.write = true,
            "delete" => self.delete = true,
            "readwrite" => (self.read, self.write, self.delete) = (true, true, true),
            "upload" => self.upload = true,
            "download" => self.download = true,
            other => return Err(format!("unknown data grant {other:?} (expected read, write, delete, readwrite, upload or download)")),
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

/// Byte limits on one bucket for a single invocation; `None` is unlimited.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct IoLimits {
    pub max_read_bytes: Option<u64>,
    pub max_write_bytes: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IoDir {
    Read,
    Write,
}

/// Called with every chunk of I/O that actually happened, for accounting.
pub type IoObserver = Arc<dyn Fn(IoDir, u64) + Send + Sync>;

/// Bytes moved through one bucket during one invocation, shared by every clone of its scope
/// and by the streams it handed out.
struct IoMeter {
    limits: IoLimits,
    read: AtomicU64,
    written: AtomicU64,
    observer: Option<IoObserver>,
}

impl IoMeter {
    fn counter(&self, dir: IoDir) -> (&AtomicU64, Option<u64>, &'static str) {
        match dir {
            IoDir::Read => (&self.read, self.limits.max_read_bytes, "read"),
            IoDir::Write => (&self.written, self.limits.max_write_bytes, "write"),
        }
    }

    /// Fails if the limit is already used up (before an operation starts).
    fn check(&self, dir: IoDir) -> Result<(), String> {
        let (n, max, what) = self.counter(dir);
        match max {
            Some(max) if n.load(Ordering::Relaxed) >= max => Err(format!(
                "file {what} limit of {max} bytes exceeded for this invocation"
            )),
            _ => Ok(()),
        }
    }

    /// Records `n` bytes; errors once the total passes the limit.
    fn add(&self, dir: IoDir, n: u64) -> Result<(), String> {
        let (c, max, what) = self.counter(dir);
        let total = c.fetch_add(n, Ordering::Relaxed) + n;
        if let Some(f) = &self.observer {
            f(dir, n);
        }
        match max {
            Some(max) if total > max => Err(format!(
                "file {what} limit of {max} bytes exceeded for this invocation"
            )),
            _ => Ok(()),
        }
    }
}

struct MeteredReader {
    inner: Box<dyn Read + Send>,
    meter: Arc<IoMeter>,
}

impl Read for MeteredReader {
    fn read(&mut self, out: &mut [u8]) -> std::io::Result<usize> {
        self.meter
            .check(IoDir::Read)
            .map_err(std::io::Error::other)?;
        let n = self.inner.read(out)?;
        self.meter
            .add(IoDir::Read, n as u64)
            .map_err(std::io::Error::other)?;
        Ok(n)
    }
}

struct MeteredWriter {
    inner: Box<dyn ObjectWriter>,
    meter: Arc<IoMeter>,
}

impl ObjectWriter for MeteredWriter {
    fn write(&mut self, data: &[u8]) -> Result<(), String> {
        self.meter.add(IoDir::Write, data.len() as u64)?;
        self.inner.write(data)
    }

    fn commit(self: Box<Self>) -> Result<(), String> {
        self.inner.commit()
    }

    fn abort(self: Box<Self>) {
        self.inner.abort()
    }
}

/// The buckets one invocation may use, by name.
pub type BucketScopes = HashMap<String, BucketScope>;

/// A backend confined to one key prefix (`<base>/<tenant>/`). Scripts only ever
/// see keys relative to it and cannot name anything outside.
#[derive(Clone)]
pub struct BucketScope {
    backend: Arc<dyn ObjectBackend>,
    prefix: String,
    max_object_bytes: usize,
    access: Access,
    meter: Option<Arc<IoMeter>>,
}

impl BucketScope {
    /// `prefix` must be empty or end with `/`. Starts with read and write access; narrow it with `with_access`.
    pub fn new(backend: Arc<dyn ObjectBackend>, prefix: impl Into<String>) -> Self {
        let prefix = prefix.into();
        debug_assert!(prefix.is_empty() || prefix.ends_with('/'));
        Self {
            backend,
            prefix,
            max_object_bytes: MAX_OBJECT_BYTES,
            access: Access::READ_WRITE,
            meter: None,
        }
    }

    /// Accounts the bytes read and written through this scope (shared by its clones) and
    /// enforces `limits` for the rest of the invocation. Call once per invocation.
    pub fn with_io(mut self, limits: IoLimits, observer: Option<IoObserver>) -> Self {
        self.meter = Some(Arc::new(IoMeter {
            limits,
            read: AtomicU64::new(0),
            written: AtomicU64::new(0),
            observer,
        }));
        self
    }

    /// `(read, written)` bytes so far, when metered.
    pub fn io_bytes(&self) -> Option<(u64, u64)> {
        self.meter.as_ref().map(|m| {
            (
                m.read.load(Ordering::Relaxed),
                m.written.load(Ordering::Relaxed),
            )
        })
    }

    pub fn with_access(mut self, access: Access) -> Self {
        self.access = access;
        self
    }

    fn need_read(&self) -> Result<(), String> {
        if self.access.read {
            Ok(())
        } else {
            Err("this bucket is not readable for this tenant".into())
        }
    }

    fn need_write(&self) -> Result<(), String> {
        if self.access.write {
            Ok(())
        } else {
            Err("this bucket is not writable for this tenant".into())
        }
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
        if let Some(m) = &self.meter {
            m.check(IoDir::Read)?;
        }
        let body = self.backend.get(&self.full(key)?)?;
        if let (Some(m), Some(b)) = (&self.meter, &body) {
            m.add(IoDir::Read, b.len() as u64)?;
        }
        Ok(body)
    }

    pub fn put(&self, key: &str, body: Vec<u8>) -> Result<(), String> {
        self.need_write()?;
        if body.len() > self.max_object_bytes {
            return Err(format!("object exceeds {} bytes", self.max_object_bytes));
        }
        if let Some(m) = &self.meter {
            m.check(IoDir::Write)?;
            m.add(IoDir::Write, body.len() as u64)?;
        }
        self.backend.put(&self.full(key)?, body)
    }

    /// Streaming upload; requires the `write` grant.
    pub fn create(&self, key: &str) -> Result<Box<dyn ObjectWriter>, String> {
        self.need_write()?;
        if let Some(m) = &self.meter {
            m.check(IoDir::Write)?;
        }
        let w = self.backend.create(&self.full(key)?)?;
        Ok(match &self.meter {
            Some(m) => Box::new(MeteredWriter {
                inner: w,
                meter: m.clone(),
            }),
            None => w,
        })
    }

    pub fn open(&self, key: &str) -> Result<Option<Box<dyn Read + Send>>, String> {
        self.need_read()?;
        if let Some(m) = &self.meter {
            m.check(IoDir::Read)?;
        }
        let r = self.backend.open(&self.full(key)?)?;
        Ok(match (&self.meter, r) {
            (Some(m), Some(inner)) => Some(Box::new(MeteredReader {
                inner,
                meter: m.clone(),
            })),
            (_, r) => r,
        })
    }

    /// A presigned URL for one key of this scope. GET needs the `download` grant, PUT needs `upload`.
    pub fn presign(
        &self,
        method: PresignMethod,
        key: &str,
        ttl: Option<std::time::Duration>,
    ) -> Result<String, String> {
        match method {
            PresignMethod::Get if !self.access.download => {
                return Err("presigned downloads are not granted for this bucket".into())
            }
            PresignMethod::Put if !self.access.upload => {
                return Err("presigned uploads are not granted for this bucket".into())
            }
            _ => {}
        }
        let ttl = ttl
            .unwrap_or(std::time::Duration::from_secs(15 * 60))
            .clamp(std::time::Duration::from_secs(1), MAX_PRESIGN_TTL);
        self.backend.presign(method, &self.full(key)?, ttl)
    }

    pub fn delete(&self, key: &str) -> Result<(), String> {
        if !self.access.delete {
            return Err("deleting is not granted for this bucket".into());
        }
        self.backend.delete(&self.full(key)?)
    }

    /// Lists keys under the relative `prefix` (may be empty), returned relative to the scope.
    pub fn list(&self, prefix: &str) -> Result<Vec<String>, String> {
        self.need_read()?;
        if prefix.starts_with('/') || prefix.split('/').any(|s| s == "..") {
            return Err(format!("invalid prefix {prefix:?}"));
        }
        let keys = self
            .backend
            .list(&format!("{}{}", self.prefix, prefix), MAX_LIST_KEYS)?;
        Ok(keys
            .into_iter()
            .filter_map(|k| k.strip_prefix(&self.prefix).map(str::to_string))
            .collect())
    }
}
