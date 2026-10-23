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
//!   "db":        { "table": "integraal" },
//!   "callbacks": { "base_url": "https://hooks.example.com" },
//!   "crypto":    { "keys": { "1": "INTEGRAAL_KEY_1" }, "active": 1 },
//!   "queue":     { "url": "https://sqs.eu-west-1.amazonaws.com/123/integraal" },
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
    /// DynamoDB table behind `native.db` and `native.callback`.
    #[serde(default)]
    pub db: Option<DbConfig>,
    /// Public base URL of the webhook Lambda, for `native.callback.create`.
    #[serde(default)]
    pub callbacks: Option<CallbacksConfig>,
    /// Master key for `native.crypto`.
    #[serde(default)]
    pub crypto: Option<CryptoConfig>,
    /// SQS queue: the server polls it, the webhook Lambda sends to it.
    #[serde(default)]
    pub queue: Option<QueueConfig>,
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
        for (name, bucket) in &self.buckets.map {
            let ok =
                !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
            if !ok || bucket.is_empty() {
                return Err(format!(
                    "buckets.map entry {name:?} is invalid (names are letters, digits and _)"
                ));
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
        if self.db.as_ref().is_some_and(|d| d.table.is_empty())
            || self.queue.as_ref().is_some_and(|q| q.url.is_empty())
        {
            return Err("db.table and queue.url must not be empty".into());
        }
        Throttle::new(self.throttle.clone())?;
        Ok(())
    }

    /// The `tracing` filter to install.
    pub fn log_filter(&self) -> &str {
        self.log.as_deref().unwrap_or("info")
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
        if let Some(db) = &self.db {
            engine = engine.with_db(Arc::new(crate::DynamoBackend::from_env(&db.table).await));
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
            "db": {"table": "t"}, "callbacks": {"base_url": "https://h.example.com"},
            "queue": {"url": "https://sqs/q"}, "crypto": {"keys": {"1": "K1", "2": "K2"}, "active": 2}, "server": {"port": 9000},
            "throttle": [{"url": "https://api.partner.com/", "max_concurrent": 2}],
            "ceiling": {"timeout_ms": 5000, "memory_mb": 32}, "log": "debug"
        }"#;
        let c = GlobalConfig::parse(full).unwrap();
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
            bad(r#""crypto": {"keys": {"1": "K"}, "active": 2}"#),
            "active not configured"
        );
    }
}
