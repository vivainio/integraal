//! Deployment-wide settings, read from one JSON file instead of environment variables.
//!
//! The file is `--config <path>` if given, else `integraal.json` in the working directory (for
//! Lambda, bundle it next to `bootstrap`). Only the standard AWS SDK variables (credentials,
//! region, `AWS_ENDPOINT_URL`) are still read from the environment.
//!
//! ```json
//! {
//!   "scripts":   { "bucket": "scripts", "prefix": "" },
//!   "buckets":   { "stores": { "In": { "type": "s3", "bucket": "data-in", "prefix": "tenants/" } } },
//!   "batch":     { "bucket": "partner-batch", "prefix": "tenants/" },
//!   "flow_logs": { "bucket": "integraal-flow-logs", "prefix": "" },
//!   "debug":     { "dir": "/var/lib/integraal/debug", "bucket": "integraal-debug", "prefix": "", "retention_hours": 48 },
//!   "db":        { "table": "integraal" },
//!   "callbacks": { "base_url": "https://hooks.example.com" },
//!   "crypto":    { "keys": { "1": "INTEGRAAL_KEY_1" }, "active": 1 },
//!   "admin":     { "port": 9090, "token_env": "INTEGRAAL_ADMIN_TOKEN" },
//!   "local_api": { "port": 9091 },
//!   "queue":     { "url": "https://sqs.eu-west-1.amazonaws.com/123/integraal" },
//!   "ingress":   { "queues": { "small": "https://sqs.../jobs-small", "large": "https://sqs.../jobs-large" }, "default": "small" },
//!   "schedule":  { "spread_s": { "minute": 20, "hour": 300, "day": 900 } },
//!   "server":    { "port": 8080 },
//!   "throttle":  [{ "url": "https://api.partner.com/v1/", "max_concurrent": 2, "per_second": 5 }],
//!   "ceiling":   { "timeout_ms": 60000, "memory_mb": 256 },
//!   "log":       "info"
//! }
//! ```

use std::{collections::BTreeMap, sync::Arc, time::Duration};

use integraal_loader::{CachedLoader, S3Store};
use integraal_wrappers::{MasterKey, MasterKeys, Throttle, ThrottleRule};
use serde::Deserialize;

use crate::{Engine, Limits};

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GlobalConfig {
    /// Lets editors attach `types/integraal.schema.json`; ignored.
    #[serde(default, rename = "$schema")]
    _schema: Option<String>,
    /// Where scripts and each tenant's `config.json` live.
    pub scripts: ScriptsConfig,
    /// Named tenant buckets (`native.bucket.<name>`).
    #[serde(default)]
    pub buckets: BucketsConfig,
    /// Dedicated bucket for `native.batch` jobs, kept apart from the data buckets.
    #[serde(default)]
    pub batch: Option<BatchConfig>,
    /// Bucket for the invocation logs of tenants with `"flow_logs": true`, kept apart from the
    /// tenant data buckets (scripts cannot reach it).
    #[serde(default)]
    pub flow_logs: Option<FlowLogsConfig>,
    /// Where the captures of tenants with a `debug` section go: a directory, a bucket, or both.
    /// Kept apart from every other bucket, because captures may hold tenant data.
    #[serde(default)]
    pub debug: Option<DebugSinkConfig>,
    /// DynamoDB table behind `native.db` and `native.callback`.
    #[serde(default)]
    pub db: Option<DbConfig>,
    /// Public base URL of the webhook Lambda, for `native.callback.create`.
    #[serde(default)]
    pub callbacks: Option<CallbacksConfig>,
    /// Master key for `native.crypto`.
    #[serde(default)]
    pub crypto: Option<CryptoConfig>,
    /// Operator API on its own port (server only); see the server crate.
    #[serde(default)]
    pub admin: Option<AdminConfig>,
    /// Read-only query API for flow logs, bound to loopback only (server only).
    #[serde(default)]
    pub local_api: Option<LocalApiConfig>,
    /// SQS queue: the server polls it, the webhook Lambda sends to it.
    #[serde(default)]
    pub queue: Option<QueueConfig>,
    /// Intake Lambda only: the job queue of each worker size class.
    #[serde(default)]
    pub ingress: Option<IngressConfig>,
    /// Operator ticks (the server handles `{"type":"system.tick"}` messages from EventBridge Scheduler).
    #[serde(default)]
    pub schedule: crate::ScheduleConfig,
    #[serde(default)]
    pub server: ServerConfig,
    /// Outbound HTTP limits; applied by the Fargate server only.
    #[serde(default)]
    pub throttle: Vec<ThrottleRule>,
    /// Most any tenant config may request.
    #[serde(default)]
    pub ceiling: CeilingConfig,
    /// `tracing` filter, such as `info` or `integraal_core=debug`. Default `info`.
    #[serde(default)]
    pub log: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScriptsConfig {
    pub bucket: String,
    /// Key prefix concatenated as-is; end it with `/`.
    #[serde(default)]
    pub prefix: String,
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BucketsConfig {
    /// Logical name (letters, digits, `_`; by convention `In`, `Out`, `Backup`, `Temp`) to the store
    /// behind `native.bucket.<name>`. Tenant data is `<prefix><tenant>/` in each store.
    #[serde(default)]
    pub stores: BTreeMap<String, StoreConfig>,
}

/// One data store. `type` picks the kind and with it the fields allowed; the grants and the
/// `native.bucket.<name>` API are the same for all of them.
#[derive(Debug, Clone, Deserialize)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum StoreConfig {
    /// An S3 bucket.
    S3(S3Bucket),
    /// A directory on the host that the operator whitelists for scripts.
    Dir(DirStore),
    /// An FTP or FTPS server, reached through the `curl` binary.
    Ftp(FtpStore),
    /// An SFTP server, reached through the `curl` binary.
    Sftp(SftpStore),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct S3Bucket {
    pub bucket: String,
    /// Base prefix; empty or ending with `/`. Tenant data is `<prefix><tenant>/`.
    #[serde(default)]
    pub prefix: String,
    /// Most megabytes one invocation may read from this store; unlimited when absent.
    pub max_read_mb: Option<u64>,
    /// Most megabytes one invocation may write to this store; unlimited when absent.
    pub max_write_mb: Option<u64>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirStore {
    /// Absolute path of the directory.
    pub path: String,
    /// Base prefix; empty or ending with `/`. Tenant data is `<path>/<prefix><tenant>/`.
    #[serde(default)]
    pub prefix: String,
    pub max_read_mb: Option<u64>,
    pub max_write_mb: Option<u64>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum FtpTls {
    /// Plain FTP.
    #[default]
    None,
    /// Plain connection, upgraded with `AUTH TLS`; refused if the server will not.
    Explicit,
    /// TLS from the first byte (`ftps://`, usually port 990).
    Implicit,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FtpStore {
    pub host: String,
    pub port: Option<u16>,
    /// Directory the keys are relative to; starts and ends with `/`. Relative to the login
    /// directory, unless it starts with `//`, which is absolute on the server.
    pub path: String,
    /// Anonymous login when absent.
    pub user: Option<String>,
    /// Name of the environment variable that holds the password.
    pub password_env: Option<String>,
    #[serde(default)]
    pub tls: FtpTls,
    #[serde(default)]
    pub prefix: String,
    pub max_read_mb: Option<u64>,
    pub max_write_mb: Option<u64>,
    /// Longest one operation may take; default 60.
    pub timeout_s: Option<u64>,
    /// The `curl` binary; default `curl` from `PATH`.
    pub curl: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SftpStore {
    pub host: String,
    pub port: Option<u16>,
    /// Absolute directory the keys are relative to; starts and ends with `/`.
    pub path: String,
    pub user: String,
    /// Name of the environment variable that holds the password. Give this or `key_file`.
    pub password_env: Option<String>,
    /// Private key file. Give this or `password_env`.
    pub key_file: Option<String>,
    /// SHA-256 of the server's host key, base64 as `curl --hostpubsha256` takes it.
    pub host_key_sha256: String,
    #[serde(default)]
    pub prefix: String,
    pub max_read_mb: Option<u64>,
    pub max_write_mb: Option<u64>,
    pub timeout_s: Option<u64>,
    pub curl: Option<String>,
}

impl StoreConfig {
    pub fn prefix(&self) -> &str {
        match self {
            Self::S3(s) => &s.prefix,
            Self::Dir(s) => &s.prefix,
            Self::Ftp(s) => &s.prefix,
            Self::Sftp(s) => &s.prefix,
        }
    }

    /// The S3 bucket behind this store, if it is one.
    pub fn s3_bucket(&self) -> Option<&str> {
        match self {
            Self::S3(s) => Some(&s.bucket),
            _ => None,
        }
    }

    pub fn io_limits(&self) -> integraal_wrappers::IoLimits {
        let (r, w) = match self {
            Self::S3(s) => (s.max_read_mb, s.max_write_mb),
            Self::Dir(s) => (s.max_read_mb, s.max_write_mb),
            Self::Ftp(s) => (s.max_read_mb, s.max_write_mb),
            Self::Sftp(s) => (s.max_read_mb, s.max_write_mb),
        };
        integraal_wrappers::IoLimits {
            max_read_bytes: r.map(|m| m * 1024 * 1024),
            max_write_bytes: w.map(|m| m * 1024 * 1024),
        }
    }

    fn validate(&self) -> Result<(), String> {
        if !(self.prefix().is_empty() || self.prefix().ends_with('/')) {
            return Err("prefix must be empty or end with '/'".into());
        }
        match self {
            Self::S3(s) if s.bucket.is_empty() => Err("bucket is empty".into()),
            Self::S3(_) => Ok(()),
            Self::Dir(d) if !std::path::Path::new(&d.path).is_absolute() => {
                Err("path must be an absolute directory".into())
            }
            Self::Dir(_) => Ok(()),
            Self::Ftp(f) => {
                if f.password_env.is_some() && f.user.is_none() {
                    return Err("password_env needs a user".into());
                }
                f.remote(None)?;
                Ok(())
            }
            Self::Sftp(f) => {
                if f.user.is_empty() {
                    return Err("user is empty".into());
                }
                if f.password_env.is_some() == f.key_file.is_some() {
                    return Err("give exactly one of password_env and key_file".into());
                }
                f.remote(None)?;
                Ok(())
            }
        }
    }
}

fn remote_backend(
    cfg: crate::remote::RemoteConfig,
) -> Result<Arc<dyn integraal_wrappers::ObjectBackend>, String> {
    crate::remote::CurlBackend::check_curl(&cfg.curl, cfg.protocol)?;
    Ok(Arc::new(crate::remote::CurlBackend::new(cfg)))
}

fn remote_common(
    protocol: crate::remote::Protocol,
    host: &str,
    port: Option<u16>,
    path: &str,
    timeout_s: Option<u64>,
) -> Result<(String, Duration), String> {
    let origin = crate::remote::RemoteConfig::origin(protocol, host, port)?;
    crate::remote::RemoteConfig::check_dir(path)?;
    let timeout = timeout_s.unwrap_or(60);
    if timeout == 0 {
        return Err("timeout_s must be at least 1".into());
    }
    Ok((origin, Duration::from_secs(timeout)))
}

fn secret_env(name: &Option<String>) -> Result<Option<String>, String> {
    match name {
        Some(n) => std::env::var(n)
            .map(Some)
            .map_err(|_| format!("environment variable {n} is not set")),
        None => Ok(None),
    }
}

impl FtpStore {
    /// `password` is the already-resolved secret; `None` validates without reading the environment.
    fn remote(&self, password: Option<String>) -> Result<crate::remote::RemoteConfig, String> {
        use crate::remote::Protocol;
        let protocol = match self.tls {
            FtpTls::Implicit => Protocol::Ftps,
            _ => Protocol::Ftp,
        };
        let (origin, timeout) =
            remote_common(protocol, &self.host, self.port, &self.path, self.timeout_s)?;
        Ok(crate::remote::RemoteConfig {
            protocol,
            origin,
            dir: self.path.clone(),
            user: self.user.clone(),
            password,
            key_file: None,
            host_key_sha256: None,
            require_tls: self.tls == FtpTls::Explicit,
            timeout,
            curl: self.curl.clone().unwrap_or_else(|| "curl".into()),
        })
    }
}

impl SftpStore {
    fn remote(&self, password: Option<String>) -> Result<crate::remote::RemoteConfig, String> {
        let protocol = crate::remote::Protocol::Sftp;
        let (origin, timeout) =
            remote_common(protocol, &self.host, self.port, &self.path, self.timeout_s)?;
        let h = &self.host_key_sha256;
        let b64 = h.trim_end_matches('=');
        if h.len() != 44
            || b64.len() < 43
            || !b64
                .chars()
                .all(|c| c.is_ascii_alphanumeric() || c == '+' || c == '/')
        {
            return Err(
                "host_key_sha256 must be the 44 character base64 SHA-256 of the host key".into(),
            );
        }
        Ok(crate::remote::RemoteConfig {
            protocol,
            origin,
            dir: self.path.clone(),
            user: Some(self.user.clone()),
            password,
            key_file: self.key_file.clone(),
            host_key_sha256: Some(h.clone()),
            require_tls: false,
            timeout,
            curl: self.curl.clone().unwrap_or_else(|| "curl".into()),
        })
    }
}

/// The bucket partners exchange batch jobs through. Tenants only reach it when their
/// `config.json` sets `"batch": true`, and only their own `<prefix><tenant>/` folder of it.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct BatchConfig {
    pub bucket: String,
    /// Base prefix; empty or ending with `/`. A tenant's jobs are `<prefix><tenant>/jobs/<job>/`.
    #[serde(default)]
    pub prefix: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FlowLogsConfig {
    pub bucket: String,
    /// Base prefix; empty or ending with `/`. A tenant's logs are `<prefix><tenant>/flows/...`.
    #[serde(default)]
    pub prefix: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DebugSinkConfig {
    /// Directory on the host (server only; Lambda's disk is gone after the invocation).
    #[serde(default)]
    pub dir: Option<String>,
    #[serde(default)]
    pub bucket: Option<String>,
    /// Base prefix in the bucket and in the directory; empty or ending with `/`.
    #[serde(default)]
    pub prefix: String,
    /// Captures in `dir` older than this are deleted (hourly sweep). Bucket captures expire with a
    /// lifecycle rule on `<prefix><tenant>/captures/`. Default 48.
    #[serde(default = "default_retention_hours")]
    pub retention_hours: u64,
}

fn default_retention_hours() -> u64 {
    48
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DbConfig {
    pub table: String,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CallbacksConfig {
    pub base_url: String,
}

/// The credential vault's operator API. It listens on its own port so the load balancer need
/// not route to it; clients authenticate with `Authorization: Bearer <token>`, the token being
/// read from the environment variable named here. Needs `db` and `crypto`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdminConfig {
    pub port: u16,
    pub token_env: String,
}

/// Read-only flow log queries on `127.0.0.1:<port>`; no token, because only processes on the same
/// host can connect. Needs `flow_logs`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalApiConfig {
    pub port: u16,
}

/// Master keys are secrets, so the file only names the environment variables that hold them
/// (each 32 bytes as 64 hex characters or base64); inject those from your secret store.
/// To rotate: add the new key under the next number, set `active` to it, and keep the old
/// one until nothing sealed with it remains.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CryptoConfig {
    /// Key number (1-255) to the environment variable holding that key.
    pub keys: BTreeMap<u8, String>,
    /// The key number used for new encryption.
    pub active: u8,
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct QueueConfig {
    pub url: String,
}

/// The intake Lambda's routing table. It consumes `queue` (the intake queue) and forwards each
/// message, unchanged, to the job queue of the class its tenant is placed in.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IngressConfig {
    /// Class name to job queue URL. Each class is served by its own Fargate service.
    pub queues: BTreeMap<String, String>,
    /// Class for tenants that name none (and for anything unplaceable); must be in `queues`.
    pub default: String,
    /// Time one placement script run may take. Default 500.
    #[serde(default)]
    pub place_timeout_ms: Option<u64>,
    /// Heap limit of a tenant's placement VM. Default 16.
    #[serde(default)]
    pub place_memory_mb: Option<u64>,
}

impl IngressConfig {
    pub fn place_options(&self, budget: Duration) -> crate::PlaceOptions {
        let d = crate::PlaceOptions::default();
        crate::PlaceOptions {
            timeout: self
                .place_timeout_ms
                .map_or(d.timeout, Duration::from_millis),
            memory_bytes: self
                .place_memory_mb
                .map_or(d.memory_bytes, |mb| (mb.max(1) * 1024 * 1024) as usize),
            budget,
        }
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    #[serde(default = "default_port")]
    pub port: u16,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            port: default_port(),
        }
    }
}

fn default_port() -> u16 {
    8080
}

#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CeilingConfig {
    pub timeout_ms: Option<u64>,
    pub memory_mb: Option<u64>,
}

impl GlobalConfig {
    pub fn parse(json: &str) -> Result<Self, String> {
        let cfg: GlobalConfig =
            serde_json::from_str(json).map_err(|e| format!("invalid config: {e}"))?;
        cfg.validate()?;
        Ok(cfg)
    }

    /// Reads `--config <path>` / `--config=<path>` from the command line, else `./integraal.json`.
    pub fn load() -> Result<Self, String> {
        let mut args = std::env::args().skip(1);
        let mut path = "integraal.json".to_string();
        while let Some(a) = args.next() {
            if a == "--config" {
                path = args.next().ok_or("--config needs a path")?;
            } else if let Some(p) = a.strip_prefix("--config=") {
                path = p.to_string();
            }
        }
        let json = std::fs::read_to_string(&path)
            .map_err(|e| format!("cannot read config {path}: {e}"))?;
        Self::parse(&json).map_err(|e| format!("{path}: {e}"))
    }

    /// The S3 buckets behind the `s3` stores.
    pub fn s3_buckets(&self) -> impl Iterator<Item = &str> {
        self.buckets
            .stores
            .values()
            .filter_map(StoreConfig::s3_bucket)
    }

    fn validate(&self) -> Result<(), String> {
        if self.scripts.bucket.is_empty() {
            return Err("scripts.bucket is empty".into());
        }
        for (name, store) in &self.buckets.stores {
            let ok =
                !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
            if !ok {
                return Err(format!(
                    "buckets.stores entry {name:?} is invalid (names are letters, digits and _)"
                ));
            }
            store
                .validate()
                .map_err(|e| format!("buckets.stores.{name}: {e}"))?;
        }
        if let Some(b) = &self.batch {
            if b.bucket.is_empty() || !(b.prefix.is_empty() || b.prefix.ends_with('/')) {
                return Err(
                    "batch needs a bucket, and its prefix must be empty or end with '/'".into(),
                );
            }
            if self.s3_buckets().any(|d| d == b.bucket) {
                return Err("batch.bucket must not also be an s3 store".into());
            }
        }
        if let Some(f) = &self.flow_logs {
            if f.bucket.is_empty() || !(f.prefix.is_empty() || f.prefix.ends_with('/')) {
                return Err(
                    "flow_logs needs a bucket, and its prefix must be empty or end with '/'".into(),
                );
            }
            if self.s3_buckets().any(|d| d == f.bucket)
                || self.batch.as_ref().is_some_and(|b| b.bucket == f.bucket)
            {
                return Err(
                    "flow_logs.bucket must not also be a data bucket or the batch bucket".into(),
                );
            }
        }
        if let Some(d) = &self.debug {
            if d.dir.is_none() && d.bucket.is_none() {
                return Err("debug needs a dir, a bucket or both".into());
            }
            if d.dir.as_deref() == Some("") || d.bucket.as_deref() == Some("") {
                return Err("debug.dir and debug.bucket must not be empty".into());
            }
            if !(d.prefix.is_empty() || d.prefix.ends_with('/')) {
                return Err("debug.prefix must be empty or end with '/'".into());
            }
            if let Some(b) = &d.bucket {
                if self.s3_buckets().any(|x| x == b)
                    || self.batch.as_ref().is_some_and(|x| &x.bucket == b)
                    || self.flow_logs.as_ref().is_some_and(|x| &x.bucket == b)
                    || &self.scripts.bucket == b
                {
                    return Err(
                        "debug.bucket must not also be a data, batch, flow log or script bucket"
                            .into(),
                    );
                }
            }
        }
        if self
            .callbacks
            .as_ref()
            .is_some_and(|c| c.base_url.is_empty())
        {
            return Err("callbacks.base_url is empty".into());
        }
        if self.callbacks.is_some() && self.db.is_none() {
            return Err("callbacks need db (callbacks are stored in the table)".into());
        }
        if let Some(c) = &self.crypto {
            if c.keys.contains_key(&0) || c.keys.values().any(String::is_empty) {
                return Err("crypto.keys: numbers are 1-255 and each needs a variable name".into());
            }
            if !c.keys.contains_key(&c.active) {
                return Err(format!("crypto.active {} is not in crypto.keys", c.active));
            }
        }
        if let Some(a) = &self.admin {
            if a.token_env.is_empty() || a.port == self.server.port {
                return Err("admin needs token_env and a port different from server.port".into());
            }
            if self.db.is_none() || self.crypto.is_none() {
                return Err("admin needs db and crypto (the vault lives in the table, sealed with the master keys)".into());
            }
        }
        if let Some(l) = &self.local_api {
            if self.flow_logs.is_none() {
                return Err("local_api needs flow_logs".into());
            }
            if l.port == self.server.port || self.admin.as_ref().is_some_and(|a| a.port == l.port) {
                return Err("local_api.port must differ from server.port and admin.port".into());
            }
        }
        if self.db.as_ref().is_some_and(|d| d.table.is_empty())
            || self.queue.as_ref().is_some_and(|q| q.url.is_empty())
        {
            return Err("db.table and queue.url must not be empty".into());
        }
        if let Some(i) = &self.ingress {
            if !i.queues.contains_key(&i.default) {
                return Err(format!(
                    "ingress.default {:?} is not in ingress.queues",
                    i.default
                ));
            }
            if i.queues.values().any(String::is_empty) {
                return Err("ingress.queues: every class needs a queue URL".into());
            }
        }
        self.schedule.validate()?;
        Throttle::new(self.throttle.clone())?;
        Ok(())
    }

    /// The `tracing` filter to install.
    pub fn log_filter(&self) -> &str {
        self.log.as_deref().unwrap_or("info")
    }

    /// The admin API's port and bearer token, or `None` when it is not configured.
    pub fn admin(&self) -> Result<Option<(u16, String)>, String> {
        let Some(a) = &self.admin else {
            return Ok(None);
        };
        let token = std::env::var(&a.token_env)
            .map_err(|_| format!("admin.token_env: ${} is not set", a.token_env))?;
        if token.len() < 24 {
            return Err(format!("${} must be at least 24 characters", a.token_env));
        }
        Ok(Some((a.port, token)))
    }

    /// The throttle for the Fargate server, or `None` when no rules are configured.
    pub fn throttle(&self) -> Result<Option<Throttle>, String> {
        if self.throttle.is_empty() {
            return Ok(None);
        }
        Throttle::new(self.throttle.clone()).map(Some)
    }

    /// Builds the engine: script loader, data buckets, table and callback URL as configured.
    /// Does not apply `throttle`; only the server does that. Must be called within a Tokio runtime.
    pub async fn engine(&self) -> Result<Engine, String> {
        let store = S3Store::from_env(&self.scripts.bucket, &self.scripts.prefix).await;
        let loader = Arc::new(CachedLoader::new(store, Duration::from_secs(30)));
        let mut engine = Engine::new(loader, Limits::default()).with_ceiling(Limits::ceiling_with(
            self.ceiling.timeout_ms,
            self.ceiling.memory_mb,
        ));
        for (name, store) in &self.buckets.stores {
            let backend: Arc<dyn integraal_wrappers::ObjectBackend> = match store {
                StoreConfig::S3(s) => Arc::new(crate::S3Backend::from_env(&s.bucket).await),
                StoreConfig::Dir(d) => Arc::new(crate::debug::FsBackend::new(&d.path)),
                StoreConfig::Ftp(f) => {
                    let cfg = f.remote(secret_env(&f.password_env)?)?;
                    remote_backend(cfg)?
                }
                StoreConfig::Sftp(f) => {
                    let cfg = f.remote(secret_env(&f.password_env)?)?;
                    remote_backend(cfg)?
                }
            };
            engine = engine
                .with_bucket(name, backend, store.prefix().to_string())
                .with_bucket_io(name, store.io_limits());
        }
        if let Some(b) = &self.batch {
            let backend = crate::S3Backend::from_env(&b.bucket).await;
            engine = engine.with_batch_bucket(Arc::new(backend), b.prefix.clone());
        }
        if let Some(f) = &self.flow_logs {
            let backend = crate::S3Backend::from_env(&f.bucket).await;
            engine = engine.with_flow_logs(Arc::new(backend), f.prefix.clone());
        }
        if let Some(d) = &self.debug {
            let mut backends: Vec<Arc<dyn integraal_wrappers::ObjectBackend>> = Vec::new();
            if let Some(dir) = &d.dir {
                if std::env::var_os("AWS_LAMBDA_FUNCTION_NAME").is_some() {
                    return Err("debug.dir cannot be used on Lambda (its disk does not outlive the invocation); use debug.bucket".into());
                }
                backends.push(Arc::new(crate::debug::FsBackend::new(dir)));
            }
            if let Some(b) = &d.bucket {
                backends.push(Arc::new(crate::S3Backend::from_env(b).await));
            }
            engine =
                engine.with_debug_sink(crate::debug::DebugSink::new(backends, d.prefix.clone()));
        }
        if let Some(db) = &self.db {
            engine = engine.with_db(Arc::new(crate::DynamoBackend::from_env(&db.table).await));
        }
        if let Some(q) = &self.queue {
            engine = engine.with_queue(Arc::new(crate::SqsBackend::from_env(&q.url).await));
        }
        if let Some(cb) = &self.callbacks {
            engine = engine.with_callback_base_url(&cb.base_url);
        }
        if let Some(c) = &self.crypto {
            let mut keys = Vec::new();
            for (id, var) in &c.keys {
                let raw = std::env::var(var)
                    .map_err(|_| format!("crypto.keys.{id}: ${var} is not set"))?;
                keys.push((
                    *id,
                    MasterKey::parse(&raw).map_err(|e| format!("${var}: {e}"))?,
                ));
            }
            engine = engine.with_master_keys(MasterKeys::new(keys, c.active)?);
        }
        Ok(engine)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn minimal_and_full_configs_parse() {
        let c = GlobalConfig::parse(r#"{"scripts":{"bucket":"s"}}"#).unwrap();
        assert_eq!(
            (c.server.port, c.log_filter(), c.buckets.stores.len()),
            (8080, "info", 0)
        );
        assert!(c.throttle().unwrap().is_none());
        assert!(
            GlobalConfig::parse(r#"{"$schema":"x.json","scripts":{"bucket":"s"}}"#).is_ok(),
            "a $schema key is allowed"
        );

        let full = r#"{
            "scripts": {"bucket": "s", "prefix": "p/"},
            "buckets": {"stores": {
                "In": {"type": "s3", "bucket": "data-in", "prefix": "tenants/"},
                "Local": {"type": "dir", "path": "/srv/data", "max_read_mb": 5},
                "Partner": {"type": "ftp", "host": "ftp.partner.com", "path": "/out/", "user": "acme", "password_env": "P", "tls": "explicit"},
                "Sftp": {"type": "sftp", "host": "sftp.partner.com", "port": 2222, "path": "/home/acme/", "user": "acme",
                         "key_file": "/k", "host_key_sha256": "q3r1nO5pYtYVfKFXhVqZxqJmKkQTVhP0pQmK3mG7yCc="}
            }},
            "batch": {"bucket": "partner-batch", "prefix": "tenants/"},
            "flow_logs": {"bucket": "flow-logs", "prefix": "logs/"},
            "db": {"table": "t"}, "callbacks": {"base_url": "https://h.example.com"},
            "queue": {"url": "https://sqs/q"}, "admin": {"port": 9090, "token_env": "T"}, "crypto": {"keys": {"1": "K1", "2": "K2"}, "active": 2}, "server": {"port": 9000}, "schedule": {"spread_s": {"hour": 120}},
            "throttle": [{"url": "https://api.partner.com/", "max_concurrent": 2}],
            "ingress": {"queues": {"small": "https://sqs/s", "large": "https://sqs/l"}, "default": "small", "place_timeout_ms": 200},
            "ceiling": {"timeout_ms": 5000, "memory_mb": 32}, "log": "debug"
        }"#;
        let c = GlobalConfig::parse(full).unwrap();
        let opts = c
            .ingress
            .as_ref()
            .unwrap()
            .place_options(Duration::from_secs(9));
        assert_eq!(
            (opts.timeout, opts.budget),
            (Duration::from_millis(200), Duration::from_secs(9))
        );
        assert_eq!((c.server.port, c.log_filter()), (9000, "debug"));
        assert!(c.throttle().unwrap().is_some());
    }

    #[test]
    fn mistakes_are_rejected() {
        let bad = |extra: &str| {
            GlobalConfig::parse(&format!(r#"{{"scripts":{{"bucket":"s"}},{extra}}}"#)).is_err()
        };
        assert!(GlobalConfig::parse("{}").is_err(), "scripts is required");
        assert!(bad(r#""bucket": "x""#), "unknown top-level key");
        let store = |body: &str| format!(r#""buckets": {{"stores": {{"X": {body}}}}}"#);
        let host_key = "q3r1nO5pYtYVfKFXhVqZxqJmKkQTVhP0pQmK3mG7yCc=";
        assert!(!bad(&store(
            r#"{"type": "dir", "path": "/d", "max_read_mb": 5, "max_write_mb": 1}"#
        )));
        assert!(bad(&store(r#"{"path": "/d"}"#)), "store without a type");
        assert!(
            bad(&store(r#"{"type": "gopher", "path": "/d"}"#)),
            "unknown type"
        );
        assert!(
            bad(&store(r#"{"type": "dir", "path": "/d", "max_rd": 5}"#)),
            "unknown dir key"
        );
        assert!(
            bad(&store(r#"{"type": "dir", "path": "/d", "bucket": "b"}"#)),
            "a key from another type"
        );
        assert!(
            bad(&store(r#"{"type": "dir", "path": "data"}"#)),
            "relative dir"
        );
        assert!(
            bad(&store(
                r#"{"type": "s3", "bucket": "b", "prefix": "tenants"}"#
            )),
            "prefix without slash"
        );
        assert!(
            bad(&store(r#"{"type": "s3", "bucket": ""}"#)),
            "empty bucket"
        );
        assert!(
            bad(r#""buckets": {"stores": {"in-box": {"type": "s3", "bucket": "b"}}}"#),
            "bad store name"
        );
        let ftp = |extra: &str| {
            store(&format!(
                r#"{{"type": "ftp", "host": "h.example", "path": "/out/"{extra}}}"#
            ))
        };
        assert!(!bad(&ftp("")));
        assert!(!bad(&ftp(
            r#", "user": "u", "password_env": "P", "tls": "implicit""#
        )));
        assert!(bad(&ftp(r#", "tls": "sometimes""#)), "unknown tls mode");
        assert!(
            bad(&ftp(r#", "password_env": "P""#)),
            "password without a user"
        );
        assert!(bad(&ftp(r#", "timeout_s": 0"#)), "zero timeout");
        assert!(
            bad(&store(r#"{"type": "ftp", "host": "h", "path": "/out"}"#)),
            "ftp path without trailing slash"
        );
        assert!(
            bad(&store(r#"{"type": "ftp", "host": "u@h", "path": "/o/"}"#)),
            "credentials in the host"
        );
        let sftp = |extra: &str| {
            store(&format!(
                r#"{{"type": "sftp", "host": "h", "path": "/home/a/", "user": "a"{extra}}}"#
            ))
        };
        assert!(!bad(&sftp(&format!(
            r#", "key_file": "/k", "host_key_sha256": "{host_key}""#
        ))));
        assert!(
            bad(&sftp(r#", "key_file": "/k""#)),
            "sftp without a host key to check"
        );
        assert!(
            bad(&sftp(&format!(r#", "host_key_sha256": "{host_key}""#))),
            "sftp without a password or key"
        );
        assert!(
            bad(&sftp(&format!(
                r#", "key_file": "/k", "password_env": "P", "host_key_sha256": "{host_key}""#
            ))),
            "sftp with both a password and a key"
        );
        assert!(
            bad(&sftp(r#", "key_file": "/k", "host_key_sha256": "short""#)),
            "malformed host key"
        );
        assert!(
            bad(r#""callbacks": {"base_url": "https://h"}"#),
            "callbacks without db"
        );
        assert!(
            bad(r#""throttle": [{"url": "https://a.com/"}]"#),
            "rule without a limit"
        );
        assert!(bad(r#""server": {"port": 99999}"#), "port out of range");
        assert!(
            bad(r#""schedule": {"spread_s": {"minute": 60}}"#),
            "spread as long as the period"
        );
        assert!(
            bad(r#""ingress": {"queues": {"a": "u"}, "default": "b"}"#),
            "default class without a queue"
        );
        assert!(
            bad(r#""ingress": {"queues": {"a": ""}, "default": "a"}"#),
            "class without a queue URL"
        );
        assert!(bad(r#""batch": {"bucket": ""}"#), "batch without a bucket");
        assert!(
            bad(r#""batch": {"bucket": "b", "prefix": "x"}"#),
            "batch prefix without slash"
        );
        assert!(
            bad(
                r#""buckets": {"stores": {"In": {"type": "s3", "bucket": "same"}}}, "batch": {"bucket": "same"}"#
            ),
            "batch bucket shared with a data bucket"
        );
        assert!(
            bad(r#""flow_logs": {"bucket": ""}"#),
            "flow logs without a bucket"
        );
        assert!(
            bad(
                r#""buckets": {"stores": {"In": {"type": "s3", "bucket": "same"}}}, "flow_logs": {"bucket": "same"}"#
            ),
            "flow log bucket shared with a data bucket"
        );
        assert!(bad(r#""debug": {}"#), "debug without a place to write");
        assert!(
            bad(r#""debug": {"bucket": "same"}, "flow_logs": {"bucket": "same"}"#),
            "debug bucket shared with the flow log bucket"
        );
        assert!(
            !bad(r#""debug": {"dir": "/tmp/d", "bucket": "dbg", "prefix": "c/"}"#),
            "a valid debug section"
        );
        assert!(
            bad(r#""crypto": {"keys": {"1": "K"}, "active": 2}"#),
            "active not configured"
        );
    }
}
