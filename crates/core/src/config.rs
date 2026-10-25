//! Deployment-wide settings, read from one JSON file instead of environment variables.
//!
//! The file is `--config <path>` if given, else `integraal.json` in the working directory (for
//! Lambda, bundle it next to `bootstrap`). Only the standard AWS SDK variables (credentials,
//! region, `AWS_ENDPOINT_URL`) are still read from the environment.
//!
//! ```json
//! {
//!   "scripts":   { "bucket": "scripts", "prefix": "" },
//!   "buckets":   { "prefix": "tenants/", "map": { "In": "data-in", "Out": "data-out" } },
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
    /// Base prefix in every bucket; empty or ending with `/`. Tenant data is `<prefix><tenant>/`.
    #[serde(default)]
    pub prefix: String,
    /// Logical name (letters, digits, `_`; by convention `In`, `Out`, `Backup`, `Temp`) to S3 bucket.
    #[serde(default)]
    pub map: BTreeMap<String, String>,
    /// Logical name to an absolute directory the operator whitelists for scripts. Tenant data is
    /// `<dir>/<prefix><tenant>/`; the grants and the `native.bucket.<name>` API are the same as for S3 buckets.
    #[serde(default)]
    pub dirs: BTreeMap<String, DirConfig>,
}

/// A whitelisted directory: just the path, or the path with per-invocation I/O limits.
#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum DirConfig {
    Path(String),
    Limited(DirLimits),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DirLimits {
    pub path: String,
    /// Most megabytes one invocation may read from this directory; unlimited when absent.
    pub max_read_mb: Option<u64>,
    /// Most megabytes one invocation may write to this directory; unlimited when absent.
    pub max_write_mb: Option<u64>,
}

impl DirConfig {
    pub fn path(&self) -> &str {
        match self {
            Self::Path(p) => p,
            Self::Limited(l) => &l.path,
        }
    }

    pub fn io_limits(&self) -> integraal_wrappers::IoLimits {
        match self {
            Self::Path(_) => Default::default(),
            Self::Limited(l) => integraal_wrappers::IoLimits {
                max_read_bytes: l.max_read_mb.map(|m| m * 1024 * 1024),
                max_write_bytes: l.max_write_mb.map(|m| m * 1024 * 1024),
            },
        }
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

    fn validate(&self) -> Result<(), String> {
        if self.scripts.bucket.is_empty() {
            return Err("scripts.bucket is empty".into());
        }
        if !(self.buckets.prefix.is_empty() || self.buckets.prefix.ends_with('/')) {
            return Err("buckets.prefix must be empty or end with '/'".into());
        }
        for (name, dir) in &self.buckets.dirs {
            let ok =
                !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
            if !ok || !std::path::Path::new(dir.path()).is_absolute() {
                return Err(format!(
                    "buckets.dirs entry {name:?} is invalid (letters, digits and _; the directory must be an absolute path)"
                ));
            }
            if self.buckets.map.contains_key(name) {
                return Err(format!("buckets.dirs {name:?} is also in buckets.map"));
            }
        }
        for (name, bucket) in &self.buckets.map {
            let ok =
                !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
            if !ok || bucket.is_empty() {
                return Err(format!(
                    "buckets.map entry {name:?} is invalid (names are letters, digits and _)"
                ));
            }
        }
        if let Some(b) = &self.batch {
            if b.bucket.is_empty() || !(b.prefix.is_empty() || b.prefix.ends_with('/')) {
                return Err(
                    "batch needs a bucket, and its prefix must be empty or end with '/'".into(),
                );
            }
            if self.buckets.map.values().any(|d| *d == b.bucket) {
                return Err("batch.bucket must not also be one of buckets.map".into());
            }
        }
        if let Some(f) = &self.flow_logs {
            if f.bucket.is_empty() || !(f.prefix.is_empty() || f.prefix.ends_with('/')) {
                return Err(
                    "flow_logs needs a bucket, and its prefix must be empty or end with '/'".into(),
                );
            }
            if self.buckets.map.values().any(|d| *d == f.bucket)
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
                if self.buckets.map.values().any(|x| x == b)
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
        for (name, bucket) in &self.buckets.map {
            let backend = crate::S3Backend::from_env(bucket).await;
            engine = engine.with_bucket(name, Arc::new(backend), self.buckets.prefix.clone());
        }
        for (name, dir) in &self.buckets.dirs {
            let backend = crate::debug::FsBackend::new(dir.path());
            engine = engine
                .with_bucket(name, Arc::new(backend), self.buckets.prefix.clone())
                .with_bucket_io(name, dir.io_limits());
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
            (c.server.port, c.log_filter(), c.buckets.map.len()),
            (8080, "info", 0)
        );
        assert!(c.throttle().unwrap().is_none());

        let full = r#"{
            "scripts": {"bucket": "s", "prefix": "p/"},
            "buckets": {"prefix": "tenants/", "map": {"In": "data-in", "Out": "data-out"}},
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
        assert!(!bad(r#""buckets": {"dirs": {"Local": "/srv/data"}}"#));
        assert!(!bad(
            r#""buckets": {"dirs": {"Local": {"path": "/d", "max_read_mb": 5, "max_write_mb": 1}}}"#
        ));
        assert!(
            bad(r#""buckets": {"dirs": {"Local": {"path": "/d", "max_rd": 5}}}"#),
            "unknown dir key"
        );
        assert!(
            bad(r#""buckets": {"dirs": {"Local": "data"}}"#),
            "relative dir"
        );
        assert!(
            bad(r#""buckets": {"dirs": {"In": "/d"}, "map": {"In": "b"}}"#),
            "name in both map and dirs"
        );
        assert!(
            bad(r#""buckets": {"prefix": "tenants"}"#),
            "prefix without slash"
        );
        assert!(
            bad(r#""buckets": {"map": {"in-box": "b"}}"#),
            "bad bucket name"
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
            bad(r#""buckets": {"map": {"In": "same"}}, "batch": {"bucket": "same"}"#),
            "batch bucket shared with a data bucket"
        );
        assert!(
            bad(r#""flow_logs": {"bucket": ""}"#),
            "flow logs without a bucket"
        );
        assert!(
            bad(r#""buckets": {"map": {"In": "same"}}, "flow_logs": {"bucket": "same"}"#),
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
