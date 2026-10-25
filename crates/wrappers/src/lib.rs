//! Versioned native API exposed to Luau scripts as the global `native`.
//!
//! A script pins its API version with a header line: `--!runtime 1`.
//! Each version lives in its own module and is only ever extended, so
//! existing tenant scripts keep working while newer versions ship.
//!
//! v1: `native.log`, `native.json`, `native.time`, and `native.store.<name>`
//! (`get`/`put`/`delete`/`list`/`open` on the tenant's own prefix of each store its config grants), and
//! `native.http` (`request`/`get`, restricted to the tenant's allowlist), and
//! `native.config` (the tenant's `vars`), `native.callback` (single-use callback URLs), and `native.db` (small key-value items
//! under the tenant's own partition keys, each expiring after a week), and `native.crypto`
//! (authenticated encryption with tenant-scoped named keys, hashing, random bytes, hex/base64), and `native.zip` (streaming zip reader and writer), `native.gzip` (streaming and string gzip), `native.deadline` (time-boxed blocks), and `native.sql` (scratch SQLite databases that can be saved to and loaded from stores), and `native.queue` (send new work to the job queue), and `native.barrier` (wait for a known set of acknowledgements, then continue once), and `native.task` (attach a title, metrics, attributes and details to the task's execution summary).

mod barrier;
mod batch;
mod crypto;
mod db;
mod deadline;
mod flow_log;
mod gzip;
mod http;
mod http_job;
mod lock;
mod ndjson;
mod queue;
mod sink;
mod sql;
mod store;
mod summary;
mod telemetry;
mod throttle;
mod timer;
pub mod trace;
pub mod trace_index;
mod v1;
mod vault;
mod xml;
mod xml_dom;
mod xml_writer;
mod zip;
mod zip_writer;

pub use barrier::{AckOutcome, Barriers, CreateSpec, MAX_BARRIER_MEMBERS};
pub use batch::{fail_inflight_http, BATCH_STORE};
pub use crypto::{MasterKey, MasterKeys};
pub use db::{
    Callback, CallbackSigner, DbScope, Item, ItemBackend, Take, Taken, DB_TTL, MAX_ITEM_BYTES,
    MAX_LOCK_LEASE, MAX_QUERY_ITEMS,
};
pub use deadline::{Deadline, TIMEOUT as TIMEOUT_MARKER};
pub use flow_log::{now_ms, FlowLog};
pub use http::{AllowRule, HttpPolicy};
pub use queue::{
    group_id, message_group_id, OutMessage, QueueBackend, MAX_DELAY_SECS, MAX_MESSAGE_BYTES,
    MAX_SENT_PER_INVOCATION, SEND_CHUNK,
};
pub use store::{
    Access, IoDir, IoLimits, IoObserver, ObjectBackend, ObjectWriter, PresignMethod, StoreScope,
    StoreScopes, MAX_LIST_KEYS, MAX_OBJECT_BYTES, MAX_PRESIGN_TTL,
};
pub use summary::{Summary, MAX_DETAIL_BYTES};
pub use throttle::{Slot, Throttle, ThrottleRule};
pub use timer::{
    cancel as cancel_timer, list as list_timers, register as register_timer, sweep as sweep_timers,
    Payload as TimerPayload, Pending as PendingTimer, SweepReport, MAX_TIMER_AHEAD_SECS,
};
pub use trace_index::{
    default_ttl as trace_index_ttl, describe_barrier, describe_callback, list as list_trace_index,
};
pub use vault::{Vault, VaultEntry, MAX_SECRET_BYTES, MIN_SECRET_BYTES};
pub use xml::{ByteStream, MAX_STREAM_BYTES};

use std::{
    sync::atomic::{AtomicU32, Ordering},
    time::Duration,
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
    pub deadline: Deadline,
    pub max_http_response_bytes: u64,
    pub max_object_bytes: usize,
    pub max_stream_bytes: u64,
    pub max_sql_bytes: u64,
    pub max_http_requests: u32,
    pub max_data_ops: u32,
    pub max_http_timeout: Duration,
}

/// Call counters and the deadline, shared by every native function of one invocation.
pub(crate) struct Budget {
    pub deadline: Deadline,
    http_left: AtomicU32,
    data_left: AtomicU32,
}

impl Budget {
    fn new(l: &HostLimits) -> Self {
        Self {
            deadline: l.deadline.clone(),
            http_left: l.max_http_requests.into(),
            data_left: l.max_data_ops.into(),
        }
    }

    fn take(counter: &AtomicU32, what: &str, deadline: &Deadline) -> Result<(), String> {
        deadline.check().map_err(str::to_string)?;
        counter
            .try_update(Ordering::Relaxed, Ordering::Relaxed, |n| n.checked_sub(1))
            .map(|_| ())
            .map_err(|_| format!("{what} call limit exceeded for this invocation"))
    }

    pub fn take_http(&self) -> Result<(), String> {
        Self::take(&self.http_left, "http", &self.deadline)
    }

    pub fn take_data(&self) -> Result<(), String> {
        Self::take(&self.data_left, "data", &self.deadline)
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
    /// Tenant-scoped object storage by store name, as granted by the tenant config; exposed as `native.store.<name>`.
    pub stores: StoreScopes,
    /// The tenant's folder in the deployment's dedicated batch bucket; `native.batch` errors when absent.
    pub batch: Option<StoreScope>,
    /// Tenant-scoped small item store with one-week expiry; `native.db` errors when absent.
    pub db: Option<DbScope>,
    /// The job queue behind `native.queue.send`; `native.queue` errors when absent.
    pub queue: Option<std::sync::Arc<dyn QueueBackend>>,
    /// Public base URL of the webhook ingress; `native.callback` errors when absent.
    pub callback_base_url: Option<String>,
    /// Operator-set limits on outbound HTTP, shared by everything in this process (Fargate only).
    pub throttle: Option<std::sync::Arc<Throttle>>,
    /// Whether this process outlives invocations, so `native.batch` jobs may carry `job:http`
    /// requests (Fargate only).
    pub background_http: bool,
    /// Outbound HTTP allowlist from the tenant config (empty denies all).
    pub http: HttpPolicy,
    /// Credential vault for `{{vault:NAME}}` in `native.http`; such requests error when absent.
    pub vault: Option<std::sync::Arc<Vault>>,
    /// Operator master keys behind `native.crypto.encrypt`/`decrypt`/`hmac_sha256`; those error when absent.
    pub master_keys: Option<MasterKeys>,
    /// Tenant `vars` from config.json, exposed read-only as `native.config`.
    pub vars: serde_json::Map<String, serde_json::Value>,
    pub limits: HostLimits,
    /// Collects this invocation's log for the flow it belongs to; absent when the tenant has no flow logs.
    pub flow_log: Option<FlowLog>,
    /// Collects this invocation's task summary (`native.task`); scripts may call it without one,
    /// and what they set is discarded.
    pub summary: Option<Summary>,
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
