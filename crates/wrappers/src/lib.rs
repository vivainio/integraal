//! Versioned native API exposed to Luau scripts as the global `native`.
//!
//! A script pins its API version with a header line: `--!runtime 1`.
//! Each version lives in its own module and is only ever extended, so
//! existing tenant scripts keep working while newer versions ship.
//!
//! v1: `native.log`, `native.json`, `native.time`, and `native.data.<bucket>`
//! (`get`/`put`/`delete`/`list`/`open` on the tenant's own prefix of each bucket its config grants), and
//! `native.http` (`request`/`get`, restricted to the tenant's allowlist), and
//! `native.config` (the tenant's `vars`), `native.callback` (single-use callback URLs), and `native.db` (small key-value items
//! under the tenant's own partition keys, each expiring after 24 hours), and `native.crypto`
//! (authenticated encryption with tenant-scoped named keys, hashing, random bytes, hex/base64).

mod crypto;
mod data;
mod db;
mod http;
mod throttle;
mod v1;
mod xml;

pub use crypto::{MasterKey, MasterKeys};
pub use data::{
    Access, DataScope, DataScopes, ObjectBackend, PresignMethod, MAX_LIST_KEYS, MAX_OBJECT_BYTES,
    MAX_PRESIGN_TTL,
};
pub use db::{
    Callback, CallbackSigner, DbScope, Item, ItemBackend, Take, Taken, DB_TTL, MAX_ITEM_BYTES,
    MAX_QUERY_ITEMS,
};
pub use http::{AllowRule, HttpPolicy};
pub use throttle::{Slot, Throttle, ThrottleRule};
pub use xml::{ByteStream, MAX_STREAM_BYTES};

use std::{
    sync::atomic::{AtomicU32, Ordering},
    time::{Duration, Instant},
};

use mlua::Lua;

pub const DEFAULT_VERSION: u32 = 1;
pub const SUPPORTED_VERSIONS: &[u32] = &[1];

#[derive(Debug, thiserror::Error)]
pub enum WrapperError {
    #[error("unsupported runtime version {0} (supported: {SUPPORTED_VERSIONS:?})")]
    UnsupportedVersion(u32),
    #[error("malformed runtime directive: {0:?}")]
    BadDirective(String),
    #[error(transparent)]
    Lua(#[from] mlua::Error),
}

/// Resolved per-invocation limits enforced by the wrappers.
#[derive(Debug, Clone)]
pub struct HostLimits {
    pub deadline: Instant,
    pub max_http_response_bytes: u64,
    pub max_object_bytes: usize,
    pub max_stream_bytes: u64,
    pub max_http_requests: u32,
    pub max_data_ops: u32,
    pub max_http_timeout: Duration,
}

/// Call counters and the deadline, shared by every native function of one invocation.
pub(crate) struct Budget {
    pub deadline: Instant,
    http_left: AtomicU32,
    data_left: AtomicU32,
}

impl Budget {
    fn new(l: &HostLimits) -> Self {
        Self {
            deadline: l.deadline,
            http_left: l.max_http_requests.into(),
            data_left: l.max_data_ops.into(),
        }
    }

    fn take(counter: &AtomicU32, what: &str, deadline: Instant) -> Result<(), String> {
        if Instant::now() >= deadline {
            return Err("__timeout__".into());
        }
        counter
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
            .map(|_| ())
            .map_err(|_| format!("{what} call limit exceeded for this invocation"))
    }

    pub fn take_http(&self) -> Result<(), String> {
        Self::take(&self.http_left, "http", self.deadline)
    }

    pub fn take_data(&self) -> Result<(), String> {
        Self::take(&self.data_left, "data", self.deadline)
    }
}

/// Name of the script currently running in the VM; shared so logs stay accurate when
/// one VM runs a router and then its target.
pub type ScriptName = std::sync::Arc<std::sync::Mutex<String>>;

/// Per-invocation data wrappers may use to scope native calls to the tenant.
#[derive(Clone)]
pub struct HostContext {
    pub tenant: String,
    pub script: ScriptName,
    /// Tenant-scoped object storage by bucket name, as granted by the tenant config; exposed as `native.data.<name>`.
    pub data: DataScopes,
    /// Tenant-scoped small item store with 24h expiry; `native.db` errors when absent.
    pub db: Option<DbScope>,
    /// Public base URL of the webhook ingress; `native.callback` errors when absent.
    pub callback_base_url: Option<String>,
    /// Operator-set limits on outbound HTTP, shared by everything in this process (Fargate only).
    pub throttle: Option<std::sync::Arc<Throttle>>,
    /// Outbound HTTP allowlist from the tenant config (empty denies all).
    pub http: HttpPolicy,
    /// Operator master keys behind `native.crypto.encrypt`/`decrypt`/`hmac_sha256`; those error when absent.
    pub master_keys: Option<MasterKeys>,
    /// Tenant `vars` from config.json, exposed read-only as `native.config`.
    pub vars: serde_json::Map<String, serde_json::Value>,
    pub limits: HostLimits,
}

/// Reads `--!runtime N` from the leading comment lines of a script.
pub fn parse_runtime_version(source: &str) -> Result<u32, WrapperError> {
    for line in source.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Some(rest) = line.strip_prefix("--!") else {
            break;
        };
        if let Some(v) = rest.trim().strip_prefix("runtime") {
            let v = v.trim();
            return v
                .parse()
                .map_err(|_| WrapperError::BadDirective(line.to_string()));
        }
    }
    Ok(DEFAULT_VERSION)
}

/// Installs the `native` global for `version`. Call before sandboxing the state.
pub fn install(lua: &Lua, version: u32, host: &HostContext) -> Result<(), WrapperError> {
    match version {
        1 => v1::install(lua, host)?,
        v => return Err(WrapperError::UnsupportedVersion(v)),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_directive() {
        assert_eq!(parse_runtime_version("--!runtime 1\nreturn {}").unwrap(), 1);
        assert_eq!(
            parse_runtime_version("--!strict\n--!runtime 2\n").unwrap(),
            2
        );
        assert_eq!(parse_runtime_version("return {}").unwrap(), DEFAULT_VERSION);
        assert!(parse_runtime_version("--!runtime x").is_err());
    }
}
