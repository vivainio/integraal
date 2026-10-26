use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use integraal_loader::{CachedLoader, LoadError, ScriptKey};
use integraal_wrappers::{
    install, parse_runtime_version, Access, DbScope, Deadline, HostContext, HostLimits, HttpPolicy,
    ItemBackend, MasterKeys, ObjectBackend, QueueBackend, ScriptName, StoreScope, StoreScopes,
    Throttle, Vault,
};
use mlua::{Function, Lua, LuaSerdeExt, Table, VmState};

use crate::{
    debug::{Capture, DebugRun},
    failure::{CaptureSlot, Failure, Kind},
    Error, Invocation, Trigger,
};
use integraal_wrappers::{now_ms, FlowLog, Summary};

#[derive(Debug, Clone)]
pub struct Limits {
    pub timeout: Duration,
    pub memory_bytes: usize,
    pub max_http_response_bytes: u64,
    pub max_object_bytes: usize,
    pub max_stream_bytes: u64,
    pub max_sql_bytes: u64,
    pub max_http_requests: u32,
    pub max_data_ops: u32,
    pub max_http_timeout: Duration,
}

const MB: u64 = 1024 * 1024;

impl Default for Limits {
    /// What a tenant gets when its config says nothing.
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(10),
            memory_bytes: (64 * MB) as usize,
            max_http_response_bytes: 5 * MB,
            max_object_bytes: (5 * MB) as usize,
            max_stream_bytes: 4096 * MB,
            max_sql_bytes: 64 * MB,
            max_http_requests: 100,
            max_data_ops: 1000,
            max_http_timeout: Duration::from_secs(10),
        }
    }
}

impl Limits {
    /// Operator ceiling: the most any tenant config may request.
    pub fn ceiling() -> Self {
        Self::ceiling_with(None, None)
    }

    /// The ceiling with the timeout and heap limit replaced where given (the global config's `ceiling`).
    pub fn ceiling_with(timeout_ms: Option<u64>, memory_mb: Option<u64>) -> Self {
        let mut c = Self {
            timeout: Duration::from_secs(60),
            memory_bytes: (256 * MB) as usize,
            max_http_response_bytes: 100 * MB,
            max_object_bytes: (100 * MB) as usize,
            max_stream_bytes: 4096 * MB,
            max_sql_bytes: 512 * MB,
            max_http_requests: 1000,
            max_data_ops: 10_000,
            max_http_timeout: Duration::from_secs(30),
        };
        if let Some(ms) = timeout_ms {
            c.timeout = Duration::from_millis(ms);
        }
        if let Some(mb) = memory_mb {
            c.memory_bytes = (mb * MB) as usize;
        }
        c
    }

    /// Tenant request over defaults, clamped to the ceiling.
    fn resolve(&self, ceiling: &Limits, t: &TenantLimits) -> Limits {
        let mb = |v: Option<u64>, d: u64| v.map(|v| v.max(1) * MB).unwrap_or(d);
        let ms =
            |v: Option<u64>, d: Duration| v.map(|v| Duration::from_millis(v.max(1))).unwrap_or(d);
        Limits {
            timeout: ms(t.timeout_ms, self.timeout).min(ceiling.timeout),
            memory_bytes: (mb(t.memory_mb, self.memory_bytes as u64) as usize)
                .min(ceiling.memory_bytes),
            max_http_response_bytes: mb(t.max_http_response_mb, self.max_http_response_bytes)
                .min(ceiling.max_http_response_bytes),
            max_object_bytes: (mb(t.max_object_mb, self.max_object_bytes as u64) as usize)
                .min(ceiling.max_object_bytes),
            max_stream_bytes: mb(t.max_stream_mb, self.max_stream_bytes)
                .min(ceiling.max_stream_bytes),
            max_sql_bytes: mb(t.max_sql_mb, self.max_sql_bytes).min(ceiling.max_sql_bytes),
            max_http_requests: t
                .max_http_requests
                .unwrap_or(self.max_http_requests)
                .min(ceiling.max_http_requests),
            max_data_ops: t
                .max_data_ops
                .unwrap_or(self.max_data_ops)
                .min(ceiling.max_data_ops),
            max_http_timeout: ms(t.max_http_timeout_ms, self.max_http_timeout)
                .min(ceiling.max_http_timeout),
        }
    }
}

/// The `limits` block of a tenant's `config.json`. Every field is optional.
#[derive(Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TenantLimits {
    pub timeout_ms: Option<u64>,
    pub memory_mb: Option<u64>,
    pub max_http_response_mb: Option<u64>,
    pub max_object_mb: Option<u64>,
    pub max_stream_mb: Option<u64>,
    pub max_sql_mb: Option<u64>,
    pub max_http_requests: Option<u32>,
    pub max_data_ops: Option<u32>,
    pub max_http_timeout_ms: Option<u64>,
}

/// Per-tenant policy, read from `<tenant>/config.json` in the script bucket.
/// Tenants cannot write there (scripts only reach the data stores), so it is
/// the operator's lever over what a tenant may touch. Absent file = no access.
#[derive(Debug, Default, serde::Deserialize)]
pub struct TenantConfig {
    #[serde(default)]
    pub http: HttpPolicy,
    /// Arbitrary JSON available to scripts as `ig.config`; string values can
    /// be interpolated into `http.allow` as `${name}`. Not a place for secrets.
    #[serde(default)]
    pub vars: serde_json::Map<String, serde_json::Value>,
    #[serde(default)]
    pub limits: TenantLimits,
    /// Stores this tenant may use as `ig.store.<name>`, and how: a grant or list of grants from
    /// `read`, `write` (put), `delete`, `readwrite` (all three), `upload` (presigned PUT) and `download` (presigned GET).
    /// Absent = no access. Names must match the deployment's `stores`.
    #[serde(default)]
    pub stores: HashMap<String, Access>,
    /// Allows `ig.queue.send` (new work for this tenant on the deployment's job queue).
    #[serde(default)]
    pub queue: bool,
    /// Allows `igp.batch` (jobs handed to a partner through the deployment's batch bucket).
    #[serde(default)]
    pub batch: bool,
    /// Keeps this tenant's invocation logs in the deployment's flow log bucket (`flow_logs` in
    /// `integraal.json`), grouped by trace id.
    #[serde(default)]
    pub flow_logs: bool,
    /// Deep diagnostics for watched scripts (or the whole tenant): coverage, a sampling profile,
    /// the input event and the structured failure, written to the deployment's debug sink.
    #[serde(default)]
    pub debug: Option<crate::debug::DebugConfig>,
    /// Script whose `on_route`/`route` handler picks the script for messages that don't name one.
    #[serde(default)]
    pub router: Option<String>,
    /// Which worker size class this tenant's messages are forwarded to by the intake Lambda.
    #[serde(default)]
    pub placement: Placement,
    /// Operator ticks this tenant receives (`"minute"`, `"hour"`, `"day"`), as events with
    /// `type = "schedule"`. The tenant must also be listed in `tenants.json`.
    #[serde(default)]
    pub schedule: Vec<crate::schedule::Every>,
}

/// The `placement` block of a tenant's `config.json`; see `docs/ingress.md`.
#[derive(Debug, Default, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Placement {
    /// The class used when there is no `script`, or the script fails or picks nothing valid.
    /// Absent: the deployment's default class.
    #[serde(default)]
    pub class: Option<String>,
    /// Other classes the placement script may pick; `class` is always allowed.
    #[serde(default)]
    pub classes: Vec<String>,
    /// Script whose `on_place`/`place` handler picks a class per message.
    #[serde(default)]
    pub script: Option<String>,
}

/// A fully resolved unit of work: which script runs, with which event.
struct Call {
    tenant: String,
    script: String,
    trigger: Trigger,
    event: serde_json::Value,
    attempt: u32,
}

#[derive(Clone)]
pub struct Engine {
    loader: Arc<CachedLoader>,
    limits: Limits,
    ceiling: Limits,
    /// Named stores with the base key prefix each tenant's directory sits under.
    stores: HashMap<String, (Arc<dyn ObjectBackend>, String)>,
    /// Per-invocation byte limits of stores that are metered (host directories).
    store_io: HashMap<String, integraal_wrappers::IoLimits>,
    /// The one bucket behind `igp.batch`, kept apart from the data stores.
    batch: Option<(Arc<dyn ObjectBackend>, String)>,
    db: Option<(
        Arc<dyn ItemBackend>,
        Arc<integraal_wrappers::CallbackSigner>,
    )>,
    callback_base_url: Option<String>,
    queue: Option<Arc<dyn QueueBackend>>,
    throttle: Option<Arc<Throttle>>,
    background_http: bool,
    master_keys: Option<MasterKeys>,
    flow_logs: Option<crate::flow_log::FlowLogSink>,
    tasks: Option<Arc<crate::tasks::TaskLog>>,
    debug_sink: Option<crate::debug::DebugSink>,
    /// Invocations in flight in this process, by span id; see `Engine::running`.
    running: Arc<std::sync::Mutex<HashMap<String, serde_json::Value>>>,
}

/// Removes an invocation from `Engine::running` when it ends, however it ends (including being dropped).
struct RunningGuard {
    map: Arc<std::sync::Mutex<HashMap<String, serde_json::Value>>>,
    span_id: String,
}

impl Drop for RunningGuard {
    fn drop(&mut self) {
        if let Ok(mut m) = self.map.lock() {
            m.remove(&self.span_id);
        }
    }
}

/// Everything besides limits and config that one invocation's native API is wired to.
#[derive(Clone)]
pub(crate) struct Services {
    stores: StoreScopes,
    batch: Option<StoreScope>,
    db: Option<DbScope>,
    cb_url: Option<String>,
    queue: Option<Arc<dyn QueueBackend>>,
    throttle: Option<Arc<Throttle>>,
    background_http: bool,
    master_keys: Option<MasterKeys>,
    flow_log: Option<FlowLog>,
    summary: Option<Summary>,
    debug: Option<DebugRun>,
    vault: Option<Arc<Vault>>,
    /// Source for `require`; absent in VMs that run no tenant code of their own.
    modules: Option<crate::modules::ModuleHost>,
}

impl Engine {
    pub fn new(loader: Arc<CachedLoader>, limits: Limits) -> Self {
        Self {
            loader,
            limits,
            ceiling: Limits::ceiling(),
            stores: HashMap::new(),
            store_io: HashMap::new(),
            batch: None,
            db: None,
            callback_base_url: None,
            queue: None,
            throttle: None,
            background_http: false,
            master_keys: None,
            flow_logs: None,
            tasks: None,
            debug_sink: None,
            running: Arc::default(),
        }
    }

    /// Overrides the operator ceiling (default: `Limits::ceiling()`).
    pub fn with_ceiling(mut self, ceiling: Limits) -> Self {
        self.ceiling = ceiling;
        self
    }

    /// Registers a named store for `ig.store.<name>`. Each tenant is confined to
    /// `<base_prefix><tenant>/` in it, and only reaches it if its config grants access.
    pub fn with_store(
        mut self,
        name: impl Into<String>,
        backend: Arc<dyn ObjectBackend>,
        base_prefix: impl Into<String>,
    ) -> Self {
        self.stores
            .insert(name.into(), (backend, base_prefix.into()));
        self
    }

    /// Meters the named store: its I/O is counted in telemetry and capped per invocation.
    pub fn with_store_io(
        mut self,
        name: impl Into<String>,
        limits: integraal_wrappers::IoLimits,
    ) -> Self {
        self.store_io.insert(name.into(), limits);
        self
    }

    /// Registers the dedicated bucket behind `igp.batch`. Each tenant is confined to
    /// `<base_prefix><tenant>/` in it, and only reaches it if its config sets `batch`.
    pub fn with_batch_bucket(
        mut self,
        backend: Arc<dyn ObjectBackend>,
        base_prefix: impl Into<String>,
    ) -> Self {
        self.batch = Some((backend, base_prefix.into()));
        self
    }

    /// Enables `ig.db`. Each tenant is confined to partition keys starting `TEN#<tenant>#`.
    pub fn with_db(mut self, backend: Arc<dyn ItemBackend>) -> Self {
        let signer = Arc::new(integraal_wrappers::CallbackSigner::new(backend.clone()));
        self.db = Some((backend, signer));
        self
    }

    /// Enables `igp.callback` (requires `with_db`): callback URLs are `<base_url>/cb/<tenant>/<id>`.
    pub fn with_callback_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.callback_base_url = Some(base_url.into());
        self
    }

    /// Enables `ig.queue` for tenants whose config sets `"queue": true`: `send` delivers to this queue.
    pub fn with_queue(mut self, backend: Arc<dyn QueueBackend>) -> Self {
        self.queue = Some(backend);
        self
    }

    /// Where the logs of tenants with `"flow_logs": true` go: `<base_prefix><tenant>/flows/...` in
    /// this bucket, which tenants cannot reach.
    pub fn with_flow_logs(
        mut self,
        backend: Arc<dyn ObjectBackend>,
        base_prefix: impl Into<String>,
    ) -> Self {
        self.flow_logs = Some(crate::flow_log::FlowLogSink::new(backend, base_prefix));
        self
    }

    /// Keeps a record of every task (invocation) of every tenant, written as
    /// `<base_prefix>tasks/...` in this bucket. See `crate::tasks`. Call `flush_tasks` to write the
    /// buffered records: a server on a timer, a Lambda after each invocation.
    pub fn with_tasks(
        mut self,
        backend: Arc<dyn ObjectBackend>,
        base_prefix: impl Into<String>,
    ) -> Self {
        self.tasks = Some(Arc::new(crate::tasks::TaskLog::new(backend, base_prefix)));
        self
    }

    /// Writes the buffered task records. A failure is logged and nothing more.
    pub async fn flush_tasks(&self) {
        let Some(tasks) = self.tasks.clone() else {
            return;
        };
        match tokio::task::spawn_blocking(move || tasks.flush()).await {
            Ok(Ok(_)) => {}
            Ok(Err(e)) => tracing::warn!(error = %e, "could not write the task records"),
            Err(e) => tracing::warn!(error = %e, "task record flush failed"),
        }
    }

    /// Where the captures of watched scripts go (tenants with a `debug` section). Not the flow log
    /// bucket: captures may hold tenant data.
    pub fn with_debug_sink(mut self, sink: crate::debug::DebugSink) -> Self {
        self.debug_sink = Some(sink);
        self
    }

    pub fn debug_sink(&self) -> Option<&crate::debug::DebugSink> {
        self.debug_sink.as_ref()
    }

    /// The invocations in flight in this process, longest-running first: tenant, script, trigger,
    /// attempt, trace and span id, start time and how long they have run. Another server or Lambda
    /// has its own list.
    pub fn running(&self) -> Vec<serde_json::Value> {
        let now = now_ms();
        let mut rows: Vec<_> = self
            .running
            .lock()
            .map(|m| m.values().cloned().collect())
            .unwrap_or_default();
        for r in &mut rows {
            r["running_ms"] = now
                .saturating_sub(r["start_ms"].as_u64().unwrap_or(now))
                .into();
        }
        rows.sort_by_key(|r| r["start_ms"].as_u64().unwrap_or(0));
        rows
    }

    /// What `tenant`'s flow `trace_id` has pending that is indexed by trace: its timers (including
    /// barrier deadlines), soonest first. Barriers, callbacks and batch jobs are keyed by their own
    /// ids and are not listed here.
    pub fn pending_timers(
        &self,
        tenant: &str,
        trace_id: &str,
    ) -> Result<Vec<integraal_wrappers::PendingTimer>, String> {
        let Some(db) = self.item_backend() else {
            return Err("the config has no db".into());
        };
        let trace_id = trace_id.to_ascii_lowercase();
        Ok(integraal_wrappers::list_timers(&*db, tenant, 10_000)?
            .into_iter()
            .filter(|t| {
                t.payload
                    .trace
                    .as_ref()
                    .and_then(|c| c.get("traceparent"))
                    .and_then(|p| p.split('-').nth(1))
                    .is_some_and(|id| id == trace_id)
            })
            .collect())
    }

    /// What `tenant`'s flow `trace_id` is waiting on, as far as it is indexed (see
    /// `integraal_wrappers::list_trace_index`): its timers, and the barriers, callbacks and batch jobs
    /// created in it that still exist. A batch job reports `done` once the partner's `done.json` is
    /// there. `covers` names what the answer includes: batch jobs are indexed only where the tenant
    /// also has a db, and an index row that could not be written is missing.
    pub fn pending(&self, tenant: &str, trace_id: &str) -> Result<serde_json::Value, String> {
        use serde_json::json;
        let timers = self.pending_timers(tenant, trace_id)?;
        let db = self.item_backend().ok_or("the config has no db")?;
        let now = now_ms() / 1000;
        let (mut barriers, mut callbacks, mut batches) = (vec![], vec![], vec![]);
        for (kind, id, created_s) in
            integraal_wrappers::list_trace_index(&*db, tenant, trace_id, 10_000)?
        {
            match kind.as_str() {
                "barrier" => barriers.extend(integraal_wrappers::describe_barrier(
                    &*db, tenant, &id, now,
                )?),
                "callback" => callbacks.extend(integraal_wrappers::describe_callback(
                    &*db, tenant, &id, now,
                )?),
                "batch" => {
                    let done = match &self.batch {
                        Some((backend, base)) => backend
                            .get(&format!("{base}{tenant}/jobs/{id}/done.json"))?
                            .is_some(),
                        None => false,
                    };
                    batches.push(
                        json!({ "kind": "batch", "id": id, "created_s": created_s, "done": done }),
                    );
                }
                _ => {}
            }
        }
        Ok(json!({
            "covers": ["timers", "barriers", "callbacks", "batch"],
            "timers": timers, "barriers": barriers, "callbacks": callbacks, "batch": batches,
        }))
    }

    /// The flow log store, for reading flows back.
    pub fn flow_logs(&self) -> Option<&crate::flow_log::FlowLogSink> {
        self.flow_logs.as_ref()
    }

    /// Enables `ig.crypto`. Each tenant's keys are derived from these and the tenant name.
    pub fn with_master_keys(mut self, keys: MasterKeys) -> Self {
        self.master_keys = Some(keys);
        self
    }

    /// The credential vault, once both `with_db` and `with_master_keys` are set. Operators write to it
    /// (the server's admin API); scripts can only reference secrets from `ig.http` requests.
    pub fn vault(&self) -> Option<Vault> {
        let (backend, _) = self.db.as_ref()?;
        Some(Vault::new(backend.clone(), self.master_keys.clone()?))
    }

    /// Limits outbound HTTP per URL prefix, across all tenants in this process (Fargate; not used on Lambda).
    pub fn with_throttle(mut self, throttle: Throttle) -> Self {
        self.throttle = Some(Arc::new(throttle));
        self
    }

    /// Lets `igp.batch` jobs carry `job:http` requests that the process performs after the
    /// invocation ends (Fargate; not used on Lambda, whose sandbox freezes between invocations).
    pub fn with_background_http(mut self) -> Self {
        self.background_http = true;
        self
    }

    pub async fn invoke(&self, inv: Invocation) -> Result<serde_json::Value, Error> {
        self.invoke_within(inv, None).await
    }

    /// `<tenant>/config.json`, or the defaults when the tenant has none.
    pub(crate) async fn tenant_config(&self, tenant: &str) -> Result<TenantConfig, Error> {
        match self.loader.load(&ScriptKey::config(tenant)?).await {
            Ok(c) => {
                let config: TenantConfig =
                    serde_json::from_str(&c.source).map_err(|e| Error::Config(e.to_string()))?;
                if let Some(d) = &config.debug {
                    d.validate().map_err(Error::Config)?;
                }
                Ok(config)
            }
            Err(LoadError::NotFound(_)) => Ok(TenantConfig::default()),
            Err(e) => Err(e.into()),
        }
    }

    pub(crate) fn loader(&self) -> &Arc<CachedLoader> {
        &self.loader
    }

    pub(crate) fn queue_backend(&self) -> Option<Arc<dyn QueueBackend>> {
        self.queue.clone()
    }

    pub(crate) fn item_backend(&self) -> Option<Arc<dyn ItemBackend>> {
        self.db.as_ref().map(|(backend, _)| backend.clone())
    }

    /// Like `invoke`, additionally capping the timeout (e.g. to the time a Lambda has left).
    pub async fn invoke_within(
        &self,
        inv: Invocation,
        max_time: Option<Duration>,
    ) -> Result<serde_json::Value, Error> {
        use tracing::Instrument;
        let span = tracing::info_span!(
            "invoke",
            tenant = %inv.tenant,
            script = inv.script.as_deref().unwrap_or("<routed>"),
            trigger = ?inv.trigger,
            outcome = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        let (tenant, trigger) = (inv.tenant.clone(), inv.trigger);
        let about = FlowSegment::about(&inv, now_ms());
        let _running = {
            use opentelemetry::trace::TraceContextExt;
            use tracing_opentelemetry::OpenTelemetrySpanExt;
            let sc = span.context().span().span_context().clone();
            let span_id = sc.span_id().to_string();
            if let Ok(mut m) = self.running.lock() {
                m.insert(
                    span_id.clone(),
                    serde_json::json!({
                        "tenant": about.tenant, "script": about.script, "trigger": about.trigger,
                        "attempt": about.attempt, "start_ms": about.start_ms,
                        "trace_id": sc.trace_id().to_string(), "span_id": span_id,
                    }),
                );
            }
            RunningGuard {
                map: self.running.clone(),
                span_id,
            }
        };
        let start = Instant::now();
        let flow_log = std::sync::OnceLock::new();
        let debug = std::sync::OnceLock::new();
        let summary = Summary::new();
        let result = self
            .invoke_inner(inv, max_time, &flow_log, &debug, &summary)
            .instrument(span.clone())
            .await;
        let outcome = match &result {
            Ok(_) => "ok",
            Err(Error::Timeout) => "timeout",
            Err(Error::Script(_)) => "script_error",
            Err(_) => "error",
        };
        span.record("outcome", outcome);
        if result.is_err() {
            span.record("otel.status_code", "ERROR");
        }
        crate::telemetry::invocation(&tenant, trigger, outcome, start.elapsed());
        if let Some(tasks) = &self.tasks {
            let line = about.task_line(&span, &summary, outcome, start.elapsed(), &result);
            if tasks.record(&line) {
                self.flush_tasks().await;
            }
        }
        if let (Some(run), Some(sink)) = (debug.into_inner(), &self.debug_sink) {
            let error = result.as_ref().err().map(|e| e.to_string());
            about
                .write_debug(sink, run, &span, outcome, start.elapsed(), error.as_deref())
                .await;
        }
        if let (Some(log), Some(sink)) = (flow_log.into_inner(), &self.flow_logs) {
            let error = result.as_ref().err().map(|e| e.to_string());
            about
                .write(sink, log, &span, outcome, start.elapsed(), error)
                .await;
        }
        result
    }

    async fn invoke_inner(
        &self,
        inv: Invocation,
        max_time: Option<Duration>,
        flow_slot: &std::sync::OnceLock<FlowLog>,
        debug_slot: &std::sync::OnceLock<DebugRun>,
        summary: &Summary,
    ) -> Result<serde_json::Value, Error> {
        let config = Arc::new(self.tenant_config(&inv.tenant).await?);
        // A routed invocation is matched by its router's name: the script it picks is not known yet.
        let debug = match (&self.debug_sink, &config.debug) {
            (Some(_), Some(d)) => {
                let name = inv
                    .script
                    .as_deref()
                    .or(config.router.as_deref())
                    .unwrap_or("");
                d.watching(name, now_ms()).map(|captures| {
                    let run = DebugRun::new(captures);
                    if captures.contains(&Capture::Event) {
                        run.collector.put("event", inv.event.clone());
                    }
                    let _ = debug_slot.set(run.clone());
                    run
                })
            }
            _ => None,
        };
        let flow_log = (config.flow_logs && self.flow_logs.is_some()).then(FlowLog::new);
        if let Some(f) = &flow_log {
            let _ = flow_slot.set(f.clone());
        }
        let mut limits = self.limits.resolve(&self.ceiling, &config.limits);
        if let Some(max) = max_time {
            limits.timeout = limits.timeout.min(max);
        }
        let mut stores = StoreScopes::new();
        for (name, access) in &config.stores {
            let (backend, base) = self.stores.get(name).ok_or_else(|| {
                Error::Config(format!(
                    "store {name:?} is not configured on this deployment"
                ))
            })?;
            let mut scope = StoreScope::new(backend.clone(), format!("{base}{}/", inv.tenant))
                .with_max_object_bytes(limits.max_object_bytes)
                .with_access(*access);
            if let Some(io) = self.store_io.get(name) {
                let (tenant, dir) = (inv.tenant.clone(), name.clone());
                scope = scope.with_io(
                    *io,
                    Some(Arc::new(move |d, n| {
                        crate::telemetry::fs_bytes(&tenant, &dir, d, n)
                    })),
                );
            }
            stores.insert(name.clone(), scope);
        }
        let batch = if config.batch {
            let (backend, base) = self.batch.as_ref().ok_or_else(|| {
                Error::Config("the batch bucket is not configured on this deployment".into())
            })?;
            Some(
                StoreScope::new(backend.clone(), format!("{base}{}/", inv.tenant))
                    .with_max_object_bytes(limits.max_object_bytes),
            )
        } else {
            None
        };
        let db = self
            .db
            .as_ref()
            .map(|(b, s)| DbScope::new(b.clone(), inv.tenant.as_str()).with_signer(s.clone()));
        let queue = if config.queue {
            Some(self.queue.clone().ok_or_else(|| {
                Error::Config("the job queue is not configured on this deployment".into())
            })?)
        } else {
            None
        };
        let svc = Services {
            stores,
            batch,
            db,
            cb_url: self.callback_base_url.clone(),
            queue,
            throttle: self.throttle.clone(),
            background_http: self.background_http,
            master_keys: self.master_keys.clone(),
            flow_log,
            summary: Some(summary.clone()),
            debug: debug.clone(),
            vault: self.vault().map(Arc::new),
            modules: Some(crate::modules::ModuleHost {
                loader: self.loader.clone(),
                handle: tokio::runtime::Handle::current(),
                debug,
            }),
        };

        // Luau states are not Send-safe to hold across awaits; run on a blocking thread.
        match inv.script.clone() {
            Some(script) => {
                let key = ScriptKey::new(&inv.tenant, &script, inv.version.as_deref())?;
                let source = self.loader.load(&key).await?;
                let call = Call {
                    tenant: inv.tenant,
                    script,
                    trigger: inv.trigger,
                    event: inv.event,
                    attempt: inv.attempt,
                };
                let span = tracing::Span::current(); // the VM thread would otherwise lose the trace context
                tokio::task::spawn_blocking(move || {
                    span.in_scope(|| run(&source.source, &call, &limits, &svc, &config))
                })
                .await
            }
            None => {
                // No script named: the tenant's router picks one, and the chosen script runs in the same VM.
                let router = config.router.clone().ok_or(Error::NoRoute)?;
                let source = self
                    .loader
                    .load(&ScriptKey::new(&inv.tenant, &router, None)?)
                    .await?;
                let (loader, handle) = (self.loader.clone(), tokio::runtime::Handle::current());
                let span = tracing::Span::current();
                tokio::task::spawn_blocking(move || {
                    span.in_scope(|| {
                        run_routed(
                            &source.source,
                            router,
                            &inv,
                            &limits,
                            &svc,
                            &config,
                            &loader,
                            &handle,
                        )
                    })
                })
                .await
            }
        }
        .map_err(|e| Error::Internal(e.to_string()))?
    }
}

/// What a flow log segment says about its invocation, taken before the invocation is consumed.
struct FlowSegment {
    tenant: String,
    script: String,
    version: Option<String>,
    trigger: Trigger,
    attempt: u32,
    continues_trace: bool,
    start_ms: u64,
}

impl FlowSegment {
    fn about(inv: &Invocation, start_ms: u64) -> Self {
        Self {
            tenant: inv.tenant.clone(),
            script: inv.script.clone().unwrap_or_else(|| "<routed>".into()),
            version: inv.version.clone(),
            trigger: inv.trigger,
            attempt: inv.attempt,
            continues_trace: inv.continues_trace,
            start_ms,
        }
    }

    /// The task record: one flat line per invocation (see `crate::tasks`). The engine's fields
    /// come first; the summary's (the script that really ran, counts, what the script attached)
    /// are added to them.
    fn task_line(
        &self,
        span: &tracing::Span,
        summary: &Summary,
        outcome: &str,
        took: Duration,
        result: &Result<serde_json::Value, Error>,
    ) -> serde_json::Value {
        use opentelemetry::trace::TraceContextExt;
        use tracing_opentelemetry::OpenTelemetrySpanExt;
        let sc = span.context().span().span_context().clone();
        let (trace_id, span_id) = if sc.is_valid() {
            (sc.trace_id().to_string(), sc.span_id().to_string())
        } else {
            (String::new(), String::new())
        };
        let error = result.as_ref().err().map(|e| {
            let e = e.to_string();
            let mut end = e.len().min(500);
            while !e.is_char_boundary(end) {
                end -= 1;
            }
            e[..end].to_string()
        });
        let mut line = serde_json::json!({
            "v": 1, "tenant": self.tenant, "script": self.script,
            "routed": self.script == "<routed>", "version": self.version,
            "trigger": self.trigger, "attempt": self.attempt,
            "continues_trace": self.continues_trace, "trace_id": trace_id, "span_id": span_id,
            "start_ms": self.start_ms, "duration_ms": took.as_millis() as u64,
            "outcome": outcome, "error": error,
        });
        for (k, v) in summary.fields() {
            line[k] = v;
        }
        line
    }

    /// Writes the capture of a watched invocation. A failure is logged and nothing more.
    async fn write_debug(
        &self,
        sink: &crate::debug::DebugSink,
        run: DebugRun,
        span: &tracing::Span,
        outcome: &str,
        took: Duration,
        error: Option<&str>,
    ) {
        use opentelemetry::trace::TraceContextExt;
        use tracing_opentelemetry::OpenTelemetrySpanExt;
        let sc = span.context().span().span_context().clone();
        if !sc.is_valid() {
            tracing::warn!("debug capture skipped: the invocation has no trace id");
            return;
        }
        let (trace_id, span_id) = (sc.trace_id().to_string(), sc.span_id().to_string());
        let header = serde_json::json!({
            "tenant": self.tenant, "trace_id": trace_id, "span_id": span_id,
            "script": self.script, "version": self.version, "trigger": self.trigger,
            "attempt": self.attempt, "start_ms": self.start_ms,
            "duration_ms": took.as_millis() as u64,
        });
        let capture = crate::debug::capture_object(header, run.collector.take(), outcome, error);
        let key = sink.key(&self.tenant, &trace_id, self.start_ms, &span_id);
        let sink = sink.clone();
        match tokio::task::spawn_blocking(move || sink.put(&key, &capture)).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::warn!(error = %e, "could not write the debug capture"),
            Err(e) => tracing::warn!(error = %e, "debug capture task failed"),
        }
    }

    /// Writes the segment, and for the first invocation of a flow the index entry. A failure is
    /// logged and nothing more: the flow log must never fail the invocation it describes.
    async fn write(
        self,
        sink: &crate::flow_log::FlowLogSink,
        log: FlowLog,
        span: &tracing::Span,
        outcome: &str,
        took: Duration,
        error: Option<String>,
    ) {
        use opentelemetry::trace::TraceContextExt;
        use tracing_opentelemetry::OpenTelemetrySpanExt;
        let cx = span.context();
        let sc = cx.span().span_context().clone();
        if !sc.is_valid() {
            tracing::warn!("flow log skipped: the invocation has no trace id");
            return;
        }
        let (trace_id, span_id) = (sc.trace_id().to_string(), sc.span_id().to_string());
        if let Some(e) = &error {
            log.event("error", e);
        }
        let header = serde_json::json!({
            "type": "header", "v": 1, "tenant": self.tenant, "trace_id": trace_id,
            "span_id": span_id, "script": self.script, "version": self.version,
            "trigger": self.trigger, "attempt": self.attempt, "start_ms": self.start_ms,
            "continues_trace": self.continues_trace,
        });
        let footer = serde_json::json!({
            "type": "end", "t": now_ms(), "outcome": outcome,
            "duration_ms": took.as_millis() as u64, "error": error,
        });
        let body = log.render(header, footer);
        let segment = sink.segment_key(&self.tenant, &trace_id, self.start_ms, &span_id);
        let index = (!self.continues_trace).then(|| {
            let entry = serde_json::json!({
                "trace_id": trace_id, "tenant": self.tenant, "script": self.script,
                "trigger": self.trigger, "start_ms": self.start_ms,
            });
            (
                sink.index_key(&self.tenant, self.start_ms, &trace_id),
                entry.to_string().into_bytes(),
            )
        });
        let sink = sink.clone();
        let written = tokio::task::spawn_blocking(move || {
            sink.put(&segment, body)?;
            if let Some((key, entry)) = index {
                sink.put(&key, entry)?;
            }
            Ok::<_, String>(())
        })
        .await;
        match written {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::warn!(error = %e, "could not write the flow log"),
            Err(e) => tracing::warn!(error = %e, "flow log task failed"),
        }
    }
}

impl Services {
    /// No stores, table, vault or keys: what a script that may only compute gets.
    pub(crate) fn bare() -> Self {
        Self {
            stores: StoreScopes::new(),
            batch: None,
            db: None,
            cb_url: None,
            queue: None,
            throttle: None,
            background_http: false,
            master_keys: None,
            flow_log: None,
            summary: None,
            debug: None,
            vault: None,
            modules: None,
        }
    }
}

/// A Luau state set up for one invocation: native API installed, sandboxed, limits armed.
pub(crate) struct Vm {
    pub(crate) lua: Lua,
    pub(crate) version: u32,
    deadline: Instant,
    script: ScriptName,
    stores: StoreScopes,
    batch: Option<StoreScope>,
    capture: CaptureSlot,
    flow_log: Option<FlowLog>,
    summary: Option<Summary>,
    debug: Option<DebugRun>,
}

pub(crate) fn new_vm(
    version: u32,
    tenant: &str,
    script: &str,
    limits: &Limits,
    svc: &Services,
    config: &TenantConfig,
) -> Result<Vm, Error> {
    let http = config.http.expand(&config.vars).map_err(Error::Config)?;
    let lua = Lua::new();
    let hard = Instant::now() + limits.timeout;
    let deadline = Deadline::new(hard);
    lua.set_memory_limit(limits.memory_bytes)
        .map_err(internal)?;
    let script = Arc::new(Mutex::new(script.to_string()));
    let vm_stores = svc.stores.clone();
    install(
        &lua,
        version,
        &HostContext {
            tenant: tenant.to_string(),
            script: script.clone(),
            stores: svc.stores.clone(),
            batch: svc.batch.clone(),
            db: svc.db.clone(),
            queue: svc.queue.clone(),
            callback_base_url: svc.cb_url.clone(),
            throttle: svc.throttle.clone(),
            background_http: svc.background_http,
            master_keys: svc.master_keys.clone(),
            vault: svc.vault.clone(),
            http,
            vars: config.vars.clone(),
            limits: HostLimits {
                deadline: deadline.clone(),
                max_http_response_bytes: limits.max_http_response_bytes,
                max_object_bytes: limits.max_object_bytes,
                max_stream_bytes: limits.max_stream_bytes,
                max_sql_bytes: limits.max_sql_bytes,
                max_http_requests: limits.max_http_requests,
                max_data_ops: limits.max_data_ops,
                max_http_timeout: limits.max_http_timeout,
            },
            flow_log: svc.flow_log.clone(),
            summary: svc.summary.clone(),
        },
    )?;
    if let Some(host) = &svc.modules {
        crate::modules::install(&lua, host.clone(), tenant).map_err(internal)?;
    }
    lua.sandbox(true).map_err(internal)?;
    let watch = deadline;
    let capture = CaptureSlot::default();
    let on_timeout = capture.clone();
    let debug = svc.debug.clone();
    if debug.as_ref().is_some_and(|d| d.wants(Capture::Coverage)) {
        lua.set_compiler(mlua::chunk::Compiler::new().set_coverage_level(1));
    }
    let sampler = debug.clone();
    lua.set_interrupt(move |lua| match watch.check() {
        Ok(()) => {
            if let Some(d) = &sampler {
                d.tick(lua);
            }
            Ok(VmState::Continue)
        }
        Err(marker) => {
            if marker == integraal_wrappers::TIMEOUT_MARKER {
                on_timeout.timed_out(lua);
            }
            Err(mlua::Error::runtime(marker))
        }
    });
    Ok(Vm {
        lua,
        version,
        deadline: hard,
        script,
        stores: vm_stores,
        batch: svc.batch.clone(),
        capture,
        flow_log: svc.flow_log.clone(),
        summary: svc.summary.clone(),
        debug,
    })
}

impl Vm {
    /// Turns a Luau error into the engine's `Error`, keeping the structured failure (kind, frames)
    /// for the flow log.
    fn fail(&self, e: mlua::Error) -> Error {
        let err = script_err(e.clone(), self.deadline);
        let (kind, text) = match (&err, &e) {
            (Error::Timeout, _) => (Kind::Timeout, "script timed out".to_string()),
            (Error::Script(m), _) if m == "memory limit exceeded" => (Kind::Memory, m.clone()),
            (_, mlua::Error::SyntaxError { .. }) => (Kind::Syntax, e.to_string()),
            _ => (Kind::Runtime, e.to_string()),
        };
        let failure = self.capture.finish(kind, &text);
        self.keep(&failure);
        match err {
            // The handler saw the stack: its message plus our traceback replaces mlua's text.
            Error::Script(_) if !failure.frames.is_empty() && kind != Kind::Memory => {
                Error::Script(format!("{}\n{}", failure.message, failure.traceback()))
            }
            other => other,
        }
    }

    /// Records a failure that is not a Luau error (an unusable handler result).
    fn bad_result(&self, message: String) -> Error {
        let failure = Failure {
            kind: Kind::BadResult,
            message: message.clone(),
            error_keys: Vec::new(),
            frames: Vec::new(),
        };
        self.keep(&failure);
        Error::Script(message)
    }

    fn keep(&self, failure: &Failure) {
        let value = serde_json::to_value(failure).unwrap_or_default();
        if let Some(d) = &self.debug {
            d.collector.put_first("failure", value.clone());
        }
        if let Some(log) = &self.flow_log {
            log.failure(value.clone());
        }
        if let Some(s) = &self.summary {
            s.failure(value);
        }
    }
}

/// Loads `source` in the VM and calls the handler matching `call.trigger`.
fn call_in_vm(vm: &Vm, source: &str, call: &Call) -> Result<serde_json::Value, Error> {
    let result = call_in_vm_inner(vm, source, call);
    if let Some(d) = &vm.debug {
        d.finish(&vm.lua);
    }
    result
}

fn call_in_vm_inner(vm: &Vm, source: &str, call: &Call) -> Result<serde_json::Value, Error> {
    let lua = &vm.lua;
    *vm.script.lock().unwrap() = call.script.clone();
    if let Some(s) = &vm.summary {
        s.script(&call.script);
    }

    let chunk_name = format!("={}/{}", call.tenant, call.script);
    let chunk = lua
        .load(source)
        .set_name(chunk_name)
        .into_function()
        .map_err(|e| vm.fail(e))?;
    if let Some(d) = &vm.debug {
        d.register(&format!("{}/{}", call.tenant, call.script), &chunk);
    }
    let module: Table = chunk.call(()).map_err(|e| vm.fail(e))?;

    let handler: Function = call
        .trigger
        .handlers()
        .iter()
        .find_map(|name| module.get::<Option<Function>>(*name).ok().flatten())
        .ok_or(Error::NoHandler(call.trigger, call.trigger.handlers()))?;

    let ctx = lua.create_table().map_err(internal)?;
    ctx.set("tenant", call.tenant.as_str()).map_err(internal)?;
    ctx.set("script", call.script.as_str()).map_err(internal)?;
    ctx.set("trigger", format!("{:?}", call.trigger).to_lowercase())
        .map_err(internal)?;
    ctx.set("runtime", vm.version).map_err(internal)?;
    ctx.set("attempt", call.attempt.max(1)).map_err(internal)?;

    let event = lua.to_value(&call.event).map_err(internal)?;
    // `xpcall` runs our handler at the point of the error, while the Luau stack still exists.
    let capture = vm.capture.clone();
    let on_err = lua
        .create_function(move |lua, e: mlua::Value| capture.handler(lua, e))
        .map_err(internal)?;
    let xpcall: Function = lua.globals().get("xpcall").map_err(internal)?;
    let (ok, out): (bool, mlua::Value) = xpcall
        .call((handler, on_err, ctx, event))
        .map_err(|e| vm.fail(e))?;
    if !ok {
        let message = match &out {
            mlua::Value::String(s) => s.to_string_lossy().to_string(),
            other => other.to_string().unwrap_or_default(),
        };
        return Err(vm.fail(mlua::Error::runtime(message)));
    }
    let result: serde_json::Value = lua
        .from_value(out)
        .map_err(|e| vm.bad_result(format!("handler result is not JSON-serializable: {e}")))?;
    if call.trigger == Trigger::Queue {
        apply_deletes(&result, &call.event, &vm.stores, vm.batch.as_ref())?;
    }
    Ok(result)
}

/// Most objects one handler result may ask the engine to delete.
const MAX_RESULT_DELETES: usize = 100;

/// Queue handlers can return `delete = true` (the object named by the event's `store` and `key`) or
/// `delete = { { store = "In", key = "a.xml" }, ... }`. The engine deletes them only after the handler
/// returned without error, through the tenant's own grants (so it still needs the `delete` grant).
fn apply_deletes(
    result: &serde_json::Value,
    event: &serde_json::Value,
    stores: &StoreScopes,
    batch: Option<&StoreScope>,
) -> Result<(), Error> {
    use serde_json::Value;
    let bad = |m: &str| Error::Script(format!("invalid delete in handler result: {m}"));
    let str_of = |v: &Value, field: &str| v.get(field).and_then(Value::as_str).map(str::to_string);
    let targets: Vec<(String, String)> = match result.get("delete") {
        None | Some(Value::Null) | Some(Value::Bool(false)) => return Ok(()),
        Some(Value::Bool(true)) => match (str_of(event, "store"), str_of(event, "key")) {
            (Some(b), Some(k)) => vec![(b, k)],
            _ => return Err(bad("`true` needs an event with `store` and `key`")),
        },
        Some(Value::Array(items)) if items.len() <= MAX_RESULT_DELETES => items
            .iter()
            .map(|i| match (str_of(i, "store"), str_of(i, "key")) {
                (Some(b), Some(k)) => Ok((b, k)),
                _ => Err(bad("each entry needs `store` and `key`")),
            })
            .collect::<Result<_, _>>()?,
        Some(Value::Array(_)) => {
            return Err(bad(&format!("more than {MAX_RESULT_DELETES} entries")))
        }
        Some(Value::Object(o)) if o.is_empty() => return Ok(()), // an empty Lua table
        Some(_) => return Err(bad("expected true or a list of { store, key }")),
    };
    for (store, key) in targets {
        let scope = if store == integraal_wrappers::BATCH_STORE {
            batch
        } else {
            stores.get(&store)
        };
        let scope = scope.ok_or_else(|| {
            Error::Script(format!(
                "delete: store {store:?} is not available to this tenant"
            ))
        })?;
        scope
            .delete(&key)
            .map_err(|e| Error::Script(format!("delete {store}/{key}: {e}")))?;
    }
    Ok(())
}

fn run(
    source: &str,
    call: &Call,
    limits: &Limits,
    svc: &Services,
    config: &TenantConfig,
) -> Result<serde_json::Value, Error> {
    let vm = new_vm(
        parse_runtime_version(source)?,
        &call.tenant,
        &call.script,
        limits,
        svc,
        config,
    )?;
    call_in_vm(&vm, source, call)
}

/// Runs the router, then the script it picked, in one VM: one native-API install, one heap,
/// one deadline and one set of call budgets. If the target pins a different runtime version
/// (which fixes the `ig` / `igp` API at VM creation) it gets a fresh VM with the remaining time.
#[allow(clippy::too_many_arguments)]
fn run_routed(
    router_src: &str,
    router: String,
    inv: &Invocation,
    limits: &Limits,
    svc: &Services,
    config: &TenantConfig,
    loader: &CachedLoader,
    handle: &tokio::runtime::Handle,
) -> Result<serde_json::Value, Error> {
    let vm = new_vm(
        parse_runtime_version(router_src)?,
        &inv.tenant,
        &router,
        limits,
        svc,
        config,
    )?;
    let route_call = Call {
        tenant: inv.tenant.clone(),
        script: router,
        trigger: Trigger::Route,
        event: inv.event.clone(),
        attempt: inv.attempt,
    };
    let decision = call_in_vm(&vm, router_src, &route_call)?;

    // The router returns a script name, or `{ script, version?, event? }` to also rewrite the event.
    let (script, version, event) = match decision {
        serde_json::Value::String(s) => (s, inv.version.clone(), inv.event.clone()),
        serde_json::Value::Object(mut o) => {
            let script = o
                .get("script")
                .and_then(|s| s.as_str())
                .map(str::to_string)
                .ok_or(Error::NoRoute)?;
            let version = o
                .get("version")
                .and_then(|v| v.as_str())
                .map(str::to_string)
                .or_else(|| inv.version.clone());
            (
                script,
                version,
                o.remove("event").unwrap_or_else(|| inv.event.clone()),
            )
        }
        _ => return Err(Error::NoRoute),
    };

    let key = ScriptKey::new(&inv.tenant, &script, version.as_deref())?;
    let target = handle.block_on(loader.load(&key))?;
    let call = Call {
        tenant: inv.tenant.clone(),
        script,
        trigger: inv.trigger,
        event,
        attempt: inv.attempt,
    };

    if parse_runtime_version(&target.source)? == vm.version {
        call_in_vm(&vm, &target.source, &call)
    } else {
        let remaining = Limits {
            timeout: vm.deadline.saturating_duration_since(Instant::now()),
            ..limits.clone()
        };
        run(&target.source, &call, &remaining, svc, config)
    }
}

pub(crate) fn internal(e: mlua::Error) -> Error {
    Error::Internal(e.to_string())
}

pub(crate) fn script_err(e: mlua::Error, deadline: Instant) -> Error {
    if Instant::now() >= deadline || e.to_string().contains("__timeout__") {
        Error::Timeout
    } else if e.to_string().contains("not enough memory") {
        Error::Script("memory limit exceeded".into())
    } else {
        Error::Script(e.to_string())
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::Trigger;
    use integraal_loader::{Fetched, MemoryStore, ScriptStore};
    use serde_json::json;

    struct Shared(Arc<MemoryStore>);
    #[async_trait::async_trait]
    impl ScriptStore for Shared {
        async fn fetch(&self, k: &ScriptKey, e: Option<&str>) -> Result<Fetched, LoadError> {
            self.0.fetch(k, e).await
        }
    }

    fn engine(src: &str, limits: Limits) -> Engine {
        let store = Arc::new(MemoryStore::default());
        store.put(&ScriptKey::new("acme", "s", None).unwrap(), src);
        let loader = Arc::new(CachedLoader::new(Shared(store), Duration::from_secs(60)));
        Engine::new(loader, limits)
    }

    fn engine_cfg(src: &str, limits: Limits, config: &str) -> Engine {
        let store = Arc::new(MemoryStore::default());
        store.put(&ScriptKey::new("acme", "s", None).unwrap(), src);
        store.put(&ScriptKey::config("acme").unwrap(), config);
        Engine::new(
            Arc::new(CachedLoader::new(Shared(store), Duration::from_secs(60))),
            limits,
        )
    }

    fn inv(trigger: Trigger, event: serde_json::Value) -> Invocation {
        Invocation {
            tenant: "acme".into(),
            script: Some("s".into()),
            version: None,
            trigger,
            event,
            attempt: 0,
            continues_trace: false,
        }
    }

    #[tokio::test]
    async fn dispatches_by_trigger_with_fallback() {
        let e = engine(
            r#"--!runtime 1
            return {
              on_queue = function(ctx, ev) return { via = "queue", n = ev.n } end,
              handle = function(ctx, ev) return { via = ctx.trigger, v = ig.json.encode({a = 1}) } end,
            }"#,
            Limits::default(),
        );
        assert_eq!(
            e.invoke(inv(Trigger::Queue, json!({"n": 7})))
                .await
                .unwrap(),
            json!({"via":"queue","n":7})
        );
        assert_eq!(
            e.invoke(inv(Trigger::Http, json!({}))).await.unwrap(),
            json!({"via":"http","v":"{\"a\":1}"})
        );
    }

    #[tokio::test]
    async fn require_loads_tenant_lib_and_versioned_shared_modules() {
        let store = Arc::new(MemoryStore::default());
        let put = |k: ScriptKey, src: &str| store.put(&k, src);
        put(
            ScriptKey::new("acme", "s", None).unwrap(),
            r#"
            local util = require("@lib/util/math")
            assert(require("@lib/patterns") == require("@native/patterns@1"))
            local again = require("@lib/util/math")
            local csv = require("@shared/csv@2.0.0")
            return { handle = function()
              return { sum = util.add(1, 2), same = util == again, runs = util.runs(),
                       csv = csv.join({ "a", "b" }), v = csv.version }
            end }"#,
        );
        put(
            ScriptKey::lib("acme", "util/math").unwrap(),
            "local runs = 0; runs += 1; return { add = function(a, b) return a + b end, runs = function() return runs end }",
        );
        put(
            ScriptKey::lib("acme", "patterns").unwrap(),
            r#"return require("@native/patterns@1")"#,
        );
        put(
            ScriptKey::lib("acme", "needs_shared").unwrap(),
            r#"return require("@shared/csv@2.0.0")"#,
        );
        put(
            ScriptKey::shared("csv", "2.0.0").unwrap(),
            r#"return { version = 2, join = function(t) return table.concat(t, ",") end }"#,
        );
        put(
            ScriptKey::shared("csv", "1.0.0").unwrap(),
            r#"return { version = 1 }"#,
        );
        let engine = |store: &Arc<MemoryStore>| {
            Engine::new(
                Arc::new(CachedLoader::new(
                    Shared(store.clone()),
                    Duration::from_secs(60),
                )),
                Limits::default(),
            )
        };
        let out = engine(&store)
            .invoke(inv(Trigger::Invoke, json!(null)))
            .await
            .unwrap();
        assert_eq!(
            out,
            json!({"sum": 3, "same": true, "runs": 1, "csv": "a,b", "v": 2})
        );

        // Failures surface as script errors naming the module.
        for (src, needle) in [
            (r#"require("@lib/missing"); return {}"#, "not found"),
            (r#"require("@shared/csv"); return {}"#, "need a version"),
            (r#"require("@shared/csv@latest"); return {}"#, "latest"),
            (r#"require("@lib/../beta/x"); return {}"#, "invalid"),
            (r#"require("./util"); return {}"#, "expected"),
        ] {
            put(ScriptKey::new("acme", "s", None).unwrap(), src);
            match engine(&store)
                .invoke(inv(Trigger::Invoke, json!(null)))
                .await
            {
                Err(Error::Script(m)) => assert!(m.contains(needle), "{src}: {m}"),
                other => panic!("{src}: {other:?}"),
            }
        }

        // Circular requires fail instead of recursing.
        put(
            ScriptKey::lib("acme", "a").unwrap(),
            r#"return require("@lib/b")"#,
        );
        put(
            ScriptKey::lib("acme", "b").unwrap(),
            r#"return require("@lib/a")"#,
        );
        put(
            ScriptKey::new("acme", "s", None).unwrap(),
            r#"require("@lib/a"); return {}"#,
        );
        match engine(&store)
            .invoke(inv(Trigger::Invoke, json!(null)))
            .await
        {
            Err(Error::Script(m)) => assert!(m.contains("circular"), "{m}"),
            other => panic!("{other:?}"),
        }

        // A tenant's lib/ is its own: another tenant cannot reach it.
        put(
            ScriptKey::new("beta", "s", None).unwrap(),
            r#"require("@lib/util/math"); return {}"#,
        );
        let mut other = inv(Trigger::Invoke, json!(null));
        other.tenant = "beta".into();
        assert!(matches!(
            engine(&store).invoke(other).await,
            Err(Error::Script(m)) if m.contains("not found")
        ));
    }

    #[tokio::test]
    async fn sandbox_blocks_os_and_enforces_timeout() {
        let e = engine(
            "return { handle = function() return os.execute ~= nil end }",
            Limits::default(),
        );
        assert_eq!(
            e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap(),
            json!(false)
        );

        let e = engine(
            "return { handle = function() while true do end end }",
            Limits {
                timeout: Duration::from_millis(100),
                ..Limits::default()
            },
        );
        assert!(matches!(
            e.invoke(inv(Trigger::Invoke, json!(null))).await,
            Err(Error::Timeout)
        ));
    }

    /// Runs `src` on a bare VM with a flow log attached; returns the error and the failure the
    /// engine recorded for it.
    fn failure_of(src: &str, limits: Limits) -> (Error, serde_json::Value) {
        let log = FlowLog::new();
        let svc = Services {
            flow_log: Some(log.clone()),
            ..Services::bare()
        };
        let call = Call {
            tenant: "acme".into(),
            script: "s".into(),
            trigger: Trigger::Invoke,
            event: json!(null),
            attempt: 1,
        };
        let err = run(src, &call, &limits, &svc, &TenantConfig::default()).unwrap_err();
        (err, log.recorded_failure().expect("a failure was recorded"))
    }

    #[test]
    fn runtime_errors_carry_kind_and_frames() {
        let (err, f) = failure_of(
            "local function inner(t) return t.x.y end\n\
             return { handle = function() return inner({}) end }",
            Limits::default(),
        );
        eprintln!("{err}\n{f:#}");
        assert_eq!(f["kind"], "runtime");
        assert_eq!(f["frames"][0]["chunk"], "acme/s");
        assert_eq!(f["frames"][0]["line"], 1);
        assert_eq!(f["frames"][0]["function"], "inner");
        assert!(
            matches!(&err, Error::Script(m) if m.contains("acme/s:1") && m.contains("stack traceback"))
        );
    }

    #[test]
    fn thrown_tables_are_described_by_keys_only() {
        let (err, f) = failure_of(
            r#"return { handle = function() error({ code = "E1", secret = "hunter2" }) end }"#,
            Limits::default(),
        );
        assert_eq!(f["error_keys"], json!(["code", "secret"]));
        assert!(!f.to_string().contains("hunter2"));
        assert!(!err.to_string().contains("hunter2"));
    }

    #[test]
    fn syntax_errors_are_their_own_kind() {
        let (_, f) = failure_of("return {", Limits::default());
        assert_eq!(f["kind"], "syntax");
    }

    #[test]
    fn timeouts_name_where_the_script_was() {
        let (err, f) = failure_of(
            "local function spin() while true do end end\n\
             return { handle = function() spin() end }",
            Limits {
                timeout: Duration::from_millis(100),
                ..Limits::default()
            },
        );
        eprintln!("{f:#}");
        assert!(matches!(err, Error::Timeout));
        assert_eq!(f["kind"], "timeout");
        assert_eq!(f["frames"][0]["function"], "spin");
        assert_eq!(f["frames"][0]["line"], 1);
    }

    #[test]
    fn memory_exhaustion_is_classified() {
        let (err, f) = failure_of(
            "return { handle = function() local t = {} for i = 1, 1e9 do t[i] = string.rep('x', 1000) end end }",
            Limits {
                memory_bytes: 4 * 1024 * 1024,
                ..Limits::default()
            },
        );
        assert!(matches!(&err, Error::Script(m) if m == "memory limit exceeded"));
        assert_eq!(f["kind"], "memory");
    }

    #[test]
    fn unusable_results_are_bad_result() {
        let (_, f) = failure_of(
            "return { handle = function() return function() end end }",
            Limits::default(),
        );
        assert_eq!(f["kind"], "bad_result");
    }

    /// A tenant with `debug` gets a capture per watched invocation: coverage of the script and its
    /// libs, a sampling profile, the input event and the failure; other tenants get nothing.
    #[tokio::test]
    async fn watched_invocations_write_a_capture() {
        use opentelemetry::trace::TracerProvider as _;
        use tracing_subscriber::layer::SubscriberExt;
        let tracer = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::registry()
                .with(tracing_opentelemetry::layer().with_tracer(tracer.tracer("test"))),
        );
        let store = Arc::new(MemoryStore::default());
        store.put(
            &ScriptKey::new("acme", "s", None).unwrap(),
            "local util = require(\"@lib/util\")\n\
             return { handle = function(ctx, ev)\n\
               local n = 0\n\
               for i = 1, 3000000 do n = n + i end\n\
               if ev.fail then\n\
                 error(\"boom\")\n\
               end\n\
               return util.twice(n)\n\
             end }",
        );
        store.put(
            &ScriptKey::lib("acme", "util").unwrap(),
            "return { twice = function(x)\n  return x * 2\nend, unused = function()\n  return 0\nend }",
        );
        store.put(
            &ScriptKey::config("acme").unwrap(),
            r#"{"debug": {"until": "2999-01-01T00:00:00Z"}}"#,
        );
        let sink = crate::debug::DebugSink::new(vec![Arc::new(MemBackend::default())], "dbg/");
        let e = Engine::new(
            Arc::new(CachedLoader::new(Shared(store), Duration::from_secs(60))),
            Limits::default(),
        )
        .with_debug_sink(sink.clone());

        e.invoke(inv(Trigger::Invoke, json!({"fail": false})))
            .await
            .unwrap();
        e.invoke(inv(Trigger::Invoke, json!({"fail": true})))
            .await
            .unwrap_err();

        let keys = sink.keys_for_test();
        assert_eq!(keys.len(), 2, "{keys:?}");
        let all: Vec<_> = keys
            .iter()
            .map(|k| {
                sink.read("acme", k.split('/').nth(3).unwrap())
                    .unwrap()
                    .remove(0)
            })
            .collect();
        let ok = all.iter().find(|c| c["outcome"] == "ok").unwrap();
        let failed = all.iter().find(|c| c["outcome"] == "script_error").unwrap();
        eprintln!("{ok:#}\n{failed:#}");
        assert_eq!(ok["event"], json!({"fail": false}));
        let cov = ok["coverage"].as_array().unwrap();
        let script = cov.iter().find(|c| c["chunk"] == "acme/s").unwrap();
        let util = cov
            .iter()
            .find(|c| c["chunk"] == "acme/lib/util.luau")
            .unwrap();
        assert!(
            script["missed"].as_array().unwrap().contains(&json!(6)),
            "the error line did not run"
        );
        assert!(!script["missed"].as_array().unwrap().contains(&json!(8)));
        assert!(
            util["missed"].as_array().unwrap().contains(&json!(4)),
            "unused() body missed"
        );
        assert!(ok["profile"]["samples"].as_u64().unwrap() > 0);
        assert!(ok["profile"]["folded"][0]
            .as_str()
            .unwrap()
            .contains("acme/s"));
        assert_eq!(failed["failure"]["kind"], "runtime");
        let cov = failed["coverage"].as_array().unwrap();
        let script = cov.iter().find(|c| c["chunk"] == "acme/s").unwrap();
        assert!(
            !script["missed"].as_array().unwrap().contains(&json!(6)),
            "the error line ran"
        );
    }

    #[tokio::test]
    async fn utf8_library_is_available() {
        let e = engine(
            "return { handle = function() return utf8.len(\"h\\u{e9}llo\") end }",
            Limits::default(),
        );
        assert_eq!(
            e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap(),
            json!(5)
        );
    }

    /// A tenant with `flow_logs` gets one segment per invocation under its trace id, and the first
    /// invocation of a flow also gets an index entry.
    #[tokio::test]
    async fn flow_logs_keep_native_log_lines_per_trace() {
        use opentelemetry::trace::TracerProvider as _;
        use tracing_subscriber::layer::SubscriberExt;
        opentelemetry::global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
        let tracer = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::registry()
                .with(tracing_opentelemetry::layer().with_tracer(tracer.tracer("test"))),
        );

        let logs = Arc::new(MemBackend::default());
        let e = engine_cfg(
            r#"return { handle = function(ctx, ev)
                 ig.log("info", "step " .. ev.n)
                 if ev.fail then error("boom") end
                 return ev.n
               end }"#,
            Limits::default(),
            r#"{"flow_logs": true}"#,
        )
        .with_flow_logs(logs.clone(), "logs/");
        let sink = e.flow_logs().unwrap().clone();

        // The first invocation starts a flow; the second claims to continue one, and fails.
        e.invoke(inv(Trigger::Invoke, json!({"n": 1})))
            .await
            .unwrap();
        let first_key = logs
            .0
            .lock()
            .unwrap()
            .keys()
            .find(|k| k.starts_with("logs/acme/flows/"))
            .cloned()
            .unwrap();
        let trace_id = first_key.split('/').nth(4).unwrap().to_string();
        assert!(first_key.starts_with(&format!("logs/acme/flows/{}/", &trace_id[..2])));

        let keys: Vec<_> = logs.0.lock().unwrap().keys().cloned().collect();
        assert_eq!(keys.len(), 2, "segment and index entry: {keys:?}");
        assert!(keys
            .iter()
            .any(|k| k.starts_with("logs/acme/flow-index/") && k.ends_with(&trace_id)));

        let lines = sink.read_flow("acme", &trace_id).unwrap();
        let kinds: Vec<_> = lines.iter().map(|l| l["type"].as_str().unwrap()).collect();
        assert_eq!(kinds, ["header", "log", "end"]);
        assert_eq!(lines[1]["msg"], "step 1");
        assert_eq!(lines[2]["outcome"], "ok");
        assert_eq!(lines[0]["trigger"], "invoke");

        let mut second = inv(Trigger::Invoke, json!({"n": 2, "fail": true}));
        second.continues_trace = true;
        e.invoke(second).await.unwrap_err();
        let after: Vec<_> = logs.0.lock().unwrap().keys().cloned().collect();
        assert_eq!(
            after.len(),
            3,
            "a continuing invocation adds a segment but no index entry"
        );
        let failed = after
            .iter()
            .find(|k| !keys.contains(k))
            .map(|k| logs.0.lock().unwrap()[k].clone())
            .unwrap();
        let mut text = String::new();
        std::io::Read::read_to_string(&mut flate2::read::GzDecoder::new(&failed[..]), &mut text)
            .unwrap();
        assert!(text.contains(r#""outcome":"script_error""#), "{text}");
        assert!(text.contains("boom"));

        // A tenant without the flag writes nothing.
        let quiet = engine_cfg(
            "return { handle = function() return 1 end }",
            Limits::default(),
            "{}",
        )
        .with_flow_logs(logs.clone(), "logs/");
        quiet
            .invoke(inv(Trigger::Invoke, json!(null)))
            .await
            .unwrap();
        assert_eq!(logs.0.lock().unwrap().len(), 3);
    }

    #[tokio::test]
    async fn decimal_global_is_exact() {
        let e = engine(
            "return { handle = function() return tostring(D\"0.1\" + D\"0.2\") end }",
            Limits::default(),
        );
        assert_eq!(
            e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap(),
            json!("0.3")
        );
    }

    /// Spans must be exported and must follow the script onto the blocking VM thread, or every
    /// log line and wrapper span inside a script would be orphaned from its invocation.
    #[tokio::test]
    async fn invocation_span_is_exported_and_reaches_the_vm_thread() {
        use opentelemetry::trace::TracerProvider as _;
        use opentelemetry_sdk::trace::{InMemorySpanExporter, SdkTracerProvider};
        use tracing_subscriber::{
            layer::{Context, SubscriberExt},
            registry::LookupSpan,
            util::SubscriberInitExt,
            Layer,
        };

        /// (message, name of the span the event was emitted in)
        type Logged = Vec<(String, Option<String>)>;
        #[derive(Clone, Default)]
        struct LogSpans(Arc<std::sync::Mutex<Logged>>);
        struct Msg(String);
        impl tracing::field::Visit for Msg {
            fn record_debug(&mut self, f: &tracing::field::Field, v: &dyn std::fmt::Debug) {
                if f.name() == "message" {
                    self.0 = format!("{v:?}");
                }
            }
        }
        impl<S: tracing::Subscriber + for<'a> LookupSpan<'a>> Layer<S> for LogSpans {
            fn on_event(&self, ev: &tracing::Event<'_>, ctx: Context<'_, S>) {
                let mut msg = Msg(String::new());
                ev.record(&mut msg);
                let span = ctx.event_span(ev).map(|s| s.name().to_string());
                self.0.lock().unwrap().push((msg.0, span));
            }
        }

        let exporter = InMemorySpanExporter::default();
        let provider = SdkTracerProvider::builder()
            .with_simple_exporter(exporter.clone())
            .build();
        let logs = LogSpans::default();
        // Global, not scoped: the VM runs on a blocking thread that has no thread-local subscriber.
        tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(provider.tracer("test")))
            .with(logs.clone())
            .try_init()
            .expect("no other test installs a global subscriber");

        let e = engine(
            r#"return { handle = function() ig.log("info", "hello-otel") return 1 end }"#,
            Limits::default(),
        );
        e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap();

        let spans = exporter.get_finished_spans().unwrap();
        let invoke = spans
            .iter()
            .find(|s| s.name == "invoke")
            .expect("an invoke span was exported");
        let attr = |k: &str| {
            invoke
                .attributes
                .iter()
                .find(|kv| kv.key.as_str() == k)
                .map(|kv| kv.value.to_string())
        };
        assert_eq!(attr("tenant").as_deref(), Some("acme"));
        assert_eq!(attr("outcome").as_deref(), Some("ok"));

        let logged = logs.0.lock().unwrap();
        let hello = logged.iter().find(|(m, _)| m == "hello-otel").unwrap();
        assert_eq!(hello.1.as_deref(), Some("invoke"));
    }

    /// Every invocation of every tenant leaves one task record, flow logs or not, including the
    /// ones that fail after attaching data, and a flush writes them as a single object.
    #[tokio::test]
    async fn every_task_leaves_a_record_with_what_the_script_attached() {
        let bucket = Arc::new(MemBackend::default());
        let e = engine(
            r#"return { handle = function(ctx, ev)
                 ig.task.title("import " .. ev.n)
                 ig.task.metric("rows", 10)
                 ig.task.add("rows", 5)
                 ig.task.attr("region", "eu")
                 ig.task.detail({ skipped = { 1, 2 } })
                 ig.log("error", "noisy")
                 if ev.fail then error("boom") end
                 return ev.n
               end }"#,
            Limits::default(),
        )
        .with_tasks(bucket.clone(), "logs/");

        e.invoke(inv(Trigger::Invoke, json!({"n": 1})))
            .await
            .unwrap();
        e.invoke(inv(Trigger::Invoke, json!({"n": 2, "fail": true})))
            .await
            .unwrap_err();
        assert!(
            bucket.0.lock().unwrap().is_empty(),
            "nothing is written before the flush"
        );
        e.flush_tasks().await;

        let objects = bucket.0.lock().unwrap().clone();
        assert_eq!(objects.len(), 1, "{:?}", objects.keys());
        let (key, body) = objects.iter().next().unwrap();
        assert!(key.starts_with("logs/tasks/20"), "{key}");
        assert!(key.ends_with(".ndjson.gz"));
        let mut text = String::new();
        std::io::Read::read_to_string(&mut flate2::read::GzDecoder::new(&body[..]), &mut text)
            .unwrap();
        let lines: Vec<serde_json::Value> = text
            .lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect();
        assert_eq!(lines.len(), 2);

        let ok = &lines[0];
        assert_eq!(ok["tenant"], "acme");
        assert_eq!(ok["outcome"], "ok");
        assert_eq!(ok["title"], "import 1");
        assert_eq!(ok["metrics"], json!({"rows": 15.0}));
        assert_eq!(ok["attrs"], json!({"region": "eu"}));
        assert_eq!(ok["detail"], json!({"skipped": [1, 2]}));
        assert_eq!(
            (ok["log_lines"].clone(), ok["log_errors"].clone()),
            (1.into(), 1.into())
        );
        assert_eq!(ok["trigger"], "invoke");
        assert_eq!(ok["routed"], false);
        assert!(ok["start_ms"].as_u64().unwrap() > 0);

        let failed = &lines[1];
        assert_eq!(failed["outcome"], "script_error");
        assert_eq!(
            failed["title"], "import 2",
            "what was set before the failure survives"
        );
        assert_eq!(failed["failure_kind"], "runtime");
        assert!(failed["error"].as_str().unwrap().contains("boom"));

        // A second flush with nothing buffered writes nothing.
        e.flush_tasks().await;
        assert_eq!(bucket.0.lock().unwrap().len(), 1);
    }

    #[derive(Default)]
    pub(crate) struct MemBackend(
        pub(crate) Arc<std::sync::Mutex<std::collections::BTreeMap<String, Vec<u8>>>>,
    );
    impl ObjectBackend for MemBackend {
        fn get(&self, k: &str) -> Result<Option<Vec<u8>>, String> {
            Ok(self.0.lock().unwrap().get(k).cloned())
        }
        fn put(&self, k: &str, b: Vec<u8>) -> Result<(), String> {
            self.0.lock().unwrap().insert(k.into(), b);
            Ok(())
        }
        fn open(&self, k: &str) -> Result<Option<Box<dyn std::io::Read + Send>>, String> {
            Ok(self
                .0
                .lock()
                .unwrap()
                .get(k)
                .cloned()
                .map(|v| Box::new(std::io::Cursor::new(v)) as Box<dyn std::io::Read + Send>))
        }
        fn create(&self, k: &str) -> Result<Box<dyn integraal_wrappers::ObjectWriter>, String> {
            struct W(
                Arc<std::sync::Mutex<std::collections::BTreeMap<String, Vec<u8>>>>,
                String,
                Vec<u8>,
            );
            impl integraal_wrappers::ObjectWriter for W {
                fn write(&mut self, d: &[u8]) -> Result<(), String> {
                    self.2.extend_from_slice(d);
                    Ok(())
                }
                fn commit(self: Box<Self>) -> Result<(), String> {
                    self.0.lock().unwrap().insert(self.1, self.2);
                    Ok(())
                }
                fn abort(self: Box<Self>) {}
            }
            Ok(Box::new(W(self.0.clone(), k.into(), Vec::new())))
        }
        fn delete(&self, k: &str) -> Result<(), String> {
            self.0.lock().unwrap().remove(k);
            Ok(())
        }
        fn list(&self, p: &str, limit: usize) -> Result<Vec<String>, String> {
            Ok(self
                .0
                .lock()
                .unwrap()
                .keys()
                .filter(|k| k.starts_with(p))
                .take(limit)
                .cloned()
                .collect())
        }
        fn presign(
            &self,
            m: integraal_wrappers::PresignMethod,
            key: &str,
            ttl: Duration,
        ) -> Result<String, String> {
            Ok(format!("mem://{key}?{m:?}&ttl={}", ttl.as_secs()))
        }
    }

    #[tokio::test]
    async fn data_stores_are_confined_and_access_controlled() {
        let (inbox, outbox) = (
            Arc::new(MemBackend::default()),
            Arc::new(MemBackend::default()),
        );
        inbox.put("data/other/secret", b"x".to_vec()).unwrap();
        inbox.put("data/acme/drop.csv", b"a,b".to_vec()).unwrap();
        let e = engine_cfg(
            r#"return { handle = function()
                local out, inn = ig.store.Out, ig.store.In
                out.put("notes/a", "hello")
                return {
                    got = out.get("notes/a"), keys = out.list("notes/"), missing = out.get("nope") == nil,
                    escaped = pcall(out.get, "../other/secret"),
                    read_in = inn.get("drop.csv"), listed_in = inn.list(),
                    put_in = pcall(inn.put, "x", "y"), delete_in = pcall(inn.delete, "drop.csv"), gone = inn.get("drop.csv") == nil,
                    ungranted = pcall(function() return ig.store.Backup.get("x") end),
                    put_url = inn.presign_put("up/new.csv", 60), get_url = out.presign_get("res.csv"),
                    get_url_in = pcall(inn.presign_get, "drop.csv"), put_url_out = pcall(out.presign_put, "res.csv"),
                }
            end }"#,
            Limits::default(),
            r#"{"stores":{"In":["read","delete","upload"],"Out":["readwrite","download"]}}"#,
        )
        .with_store("In", inbox.clone(), "data/")
        .with_store("Out", outbox.clone(), "data/")
        .with_store("Backup", Arc::new(MemBackend::default()), "data/");
        assert_eq!(
            e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap(),
            json!({"got":"hello","keys":["notes/a"],"missing":true,"escaped":false,
                   "read_in":"a,b","listed_in":["drop.csv"],"put_in":false,"delete_in":true,"gone":true,"ungranted":false,
                   "put_url":"mem://data/acme/up/new.csv?Put&ttl=60","get_url":"mem://data/acme/res.csv?Get&ttl=900",
                   "get_url_in":false,"put_url_out":false})
        );
        assert!(outbox.get("data/acme/notes/a").unwrap().is_some());
        assert!(
            inbox.get("data/acme/drop.csv").unwrap().is_none(),
            "delete grant must delete"
        );
        assert!(
            inbox.get("data/acme/x").unwrap().is_none(),
            "put without write grant must fail"
        );
    }

    fn delete_engine(src: &str, grants: &str) -> (Engine, Arc<MemBackend>) {
        let backend = Arc::new(MemBackend::default());
        backend.put("data/acme/a.csv", b"1".to_vec()).unwrap();
        backend.put("data/acme/b.csv", b"2".to_vec()).unwrap();
        let cfg = format!(r#"{{"stores":{{"In":{grants}}}}}"#);
        (
            engine_cfg(src, Limits::default(), &cfg).with_store("In", backend.clone(), "data/"),
            backend,
        )
    }

    #[tokio::test]
    async fn queue_result_deletes_after_success_only() {
        let present = |b: &MemBackend, k: &str| b.get(&format!("data/acme/{k}")).unwrap().is_some();
        let ev = json!({"store": "In", "key": "a.csv"});

        // `delete = true` removes the event's own object, and nothing else
        let (e, b) = delete_engine(
            "return { on_queue = function() return { ok = true, delete = true } end }",
            r#"["read","delete"]"#,
        );
        e.invoke(inv(Trigger::Queue, ev.clone())).await.unwrap();
        assert!(!present(&b, "a.csv") && present(&b, "b.csv"));

        // an explicit list can name several objects
        let (e, b) = delete_engine(
            r#"return { on_queue = function() return { delete = { { store = "In", key = "a.csv" }, { store = "In", key = "b.csv" } } } end }"#,
            r#"["delete"]"#,
        );
        e.invoke(inv(Trigger::Queue, ev.clone())).await.unwrap();
        assert!(!present(&b, "a.csv") && !present(&b, "b.csv"));

        // a failing handler deletes nothing
        let (e, b) = delete_engine(
            r#"return { on_queue = function() error("boom") end }"#,
            r#"["delete"]"#,
        );
        assert!(e.invoke(inv(Trigger::Queue, ev.clone())).await.is_err());
        assert!(present(&b, "a.csv"));

        // other triggers ignore `delete`
        let (e, b) = delete_engine(
            "return { handle = function() return { delete = true } end }",
            r#"["delete"]"#,
        );
        e.invoke(inv(Trigger::Http, ev.clone())).await.unwrap();
        assert!(present(&b, "a.csv"));
    }

    #[tokio::test]
    async fn result_delete_obeys_grants_and_shape() {
        let present = |b: &MemBackend| b.get("data/acme/a.csv").unwrap().is_some();
        let ev = json!({"store": "In", "key": "a.csv"});
        // no delete grant: the invocation fails and the object stays
        let (e, b) = delete_engine(
            "return { on_queue = function() return { delete = true } end }",
            r#""read""#,
        );
        assert!(matches!(
            e.invoke(inv(Trigger::Queue, ev.clone())).await,
            Err(Error::Script(_))
        ));
        assert!(present(&b));
        // a store the tenant was not granted
        let (e, b) = delete_engine(
            r#"return { on_queue = function() return { delete = { { store = "Out", key = "a.csv" } } } end }"#,
            r#""delete""#,
        );
        assert!(matches!(
            e.invoke(inv(Trigger::Queue, ev.clone())).await,
            Err(Error::Script(_))
        ));
        assert!(present(&b));
        // `true` without store and key in the event, and malformed entries
        let (e, _) = delete_engine(
            "return { on_queue = function() return { delete = true } end }",
            r#""delete""#,
        );
        assert!(matches!(
            e.invoke(inv(Trigger::Queue, json!({}))).await,
            Err(Error::Script(_))
        ));
        let (e, _) = delete_engine(
            r#"return { on_queue = function() return { delete = "a.csv" } end }"#,
            r#""delete""#,
        );
        assert!(matches!(
            e.invoke(inv(Trigger::Queue, ev)).await,
            Err(Error::Script(_))
        ));
    }

    #[test]
    fn data_grants_parse_from_a_name_or_a_list() {
        let a: Access = serde_json::from_str(r#""readwrite""#).unwrap();
        assert!(a.read && a.write && a.delete && !a.upload && !a.download);
        let a: Access = serde_json::from_str(r#"["write","download"]"#).unwrap();
        assert!(!a.read && a.write && !a.delete && !a.upload && a.download);
        assert!(serde_json::from_str::<Access>(r#""admin""#).is_err());
        assert!(serde_json::from_str::<Access>("[]").is_err());
    }

    #[tokio::test]
    async fn granting_an_unconfigured_store_fails_the_invocation() {
        let e = engine_cfg(
            "return { handle = function() return 1 end }",
            Limits::default(),
            r#"{"stores":{"nope":"read"}}"#,
        );
        assert!(matches!(
            e.invoke(inv(Trigger::Invoke, json!(null))).await,
            Err(Error::Config(_))
        ));
    }

    #[derive(Default)]
    struct MemItems(
        std::sync::Mutex<std::collections::BTreeMap<(String, String), integraal_wrappers::Item>>,
    );
    impl ItemBackend for MemItems {
        fn get(&self, pk: &str, sk: &str) -> Result<Option<integraal_wrappers::Item>, String> {
            Ok(self.0.lock().unwrap().get(&(pk.into(), sk.into())).cloned())
        }
        fn put(&self, pk: &str, item: integraal_wrappers::Item) -> Result<(), String> {
            self.0
                .lock()
                .unwrap()
                .insert((pk.into(), item.sk.clone()), item);
            Ok(())
        }
        fn delete(&self, pk: &str, sk: &str) -> Result<(), String> {
            self.0.lock().unwrap().remove(&(pk.into(), sk.into()));
            Ok(())
        }
        fn take(&self, pk: &str, sk: &str) -> Result<Option<integraal_wrappers::Item>, String> {
            Ok(self.0.lock().unwrap().remove(&(pk.into(), sk.into())))
        }
        fn put_if_absent(&self, pk: &str, item: integraal_wrappers::Item) -> Result<bool, String> {
            let mut m = self.0.lock().unwrap();
            let key = (pk.to_string(), item.sk.clone());
            let absent = !m.contains_key(&key);
            if absent {
                m.insert(key, item);
            }
            Ok(absent)
        }
        fn put_if_free(
            &self,
            pk: &str,
            item: integraal_wrappers::Item,
            now: u64,
        ) -> Result<bool, String> {
            let mut m = self.0.lock().unwrap();
            let key = (pk.to_string(), item.sk.clone());
            let free = m.get(&key).is_none_or(|old| old.ttl <= now);
            if free {
                m.insert(key, item);
            }
            Ok(free)
        }
        fn replace_if_current(
            &self,
            pk: &str,
            expected: &integraal_wrappers::Item,
            replacement: integraal_wrappers::Item,
            now: u64,
        ) -> Result<bool, String> {
            if expected.sk != replacement.sk {
                return Err("sort key changed".into());
            }
            let mut rows = self.0.lock().unwrap();
            let key = (pk.to_string(), expected.sk.clone());
            if rows
                .get(&key)
                .is_some_and(|row| row == expected && row.ttl > now)
            {
                rows.insert(key, replacement);
                Ok(true)
            } else {
                Ok(false)
            }
        }
        fn delete_if_value(&self, pk: &str, sk: &str, value: &str) -> Result<bool, String> {
            let mut m = self.0.lock().unwrap();
            let key = (pk.to_string(), sk.to_string());
            let mine = m.get(&key).is_some_and(|i| i.value == value);
            if mine {
                m.remove(&key);
            }
            Ok(mine)
        }
        fn add(&self, pk: &str, sk: &str, delta: i64, ttl: u64, now: u64) -> Result<i64, String> {
            let mut m = self.0.lock().unwrap();
            let key = (pk.to_string(), sk.to_string());
            let base = m
                .get(&key)
                .filter(|old| old.ttl > now)
                .and_then(|old| old.value.parse::<i64>().ok())
                .unwrap_or(0);
            let total = base + delta;
            m.insert(
                key,
                integraal_wrappers::Item {
                    sk: sk.to_string(),
                    value: total.to_string(),
                    ttl,
                },
            );
            Ok(total)
        }
        fn query(
            &self,
            pk: &str,
            p: &str,
            after: &str,
            limit: usize,
        ) -> Result<Vec<integraal_wrappers::Item>, String> {
            let m = self.0.lock().unwrap();
            Ok(m.iter()
                .filter(|((k, sk), _)| k == pk && sk.starts_with(p) && sk.as_str() > after)
                .take(limit)
                .map(|(_, i)| i.clone())
                .collect())
        }
    }

    #[tokio::test]
    async fn db_is_confined_to_tenant_and_expires_in_a_week() {
        let backend = Arc::new(MemItems::default());
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        // another tenant's row, and an already-expired row of ours
        backend
            .put(
                "TEN#other#cart",
                integraal_wrappers::Item {
                    sk: "".into(),
                    value: "\"secret\"".into(),
                    ttl: now + 1000,
                },
            )
            .unwrap();
        backend
            .put(
                "TEN#acme#cart",
                integraal_wrappers::Item {
                    sk: "old".into(),
                    value: "1".into(),
                    ttl: now - 1,
                },
            )
            .unwrap();
        let e = engine(
            r#"return { handle = function()
                ig.db.put("cart", "a", { n = 1 })
                ig.db.put("cart", "b", "x")
                ig.db.put("solo", nil, true)
                return { a = ig.db.get("cart", "a"), solo = ig.db.get("solo"), miss = ig.db.get("cart", "zz") == nil,
                         expired = ig.db.get("cart", "old") == nil, other = ig.db.get("other") == nil,
                         rows = ig.db.query("cart", { sk_prefix = "a" }), all = #ig.db.query("cart"),
                         bad = pcall(ig.db.get, "") }
            end }"#,
            Limits::default(),
        )
        .with_db(backend.clone());
        assert_eq!(
            e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap(),
            json!({"a":{"n":1},"solo":true,"miss":true,"expired":true,"other":true,
                   "rows":[{"sk":"a","value":{"n":1}}],"all":2,"bad":false})
        );
        let ttl = backend.get("TEN#acme#cart", "a").unwrap().unwrap().ttl;
        assert!((now + 604_800..=now + 604_805).contains(&ttl));
    }

    #[tokio::test]
    async fn db_claims_and_counters_are_atomic_and_tenant_scoped() {
        let backend = Arc::new(MemItems::default());
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        // an expired claim and an expired counter of ours: both start over
        for (pk, v) in [("TEN#acme#claim", "\"old\""), ("TEN#acme#hits", "41")] {
            backend
                .put(
                    pk,
                    integraal_wrappers::Item {
                        sk: "".into(),
                        value: v.into(),
                        ttl: now - 1,
                    },
                )
                .unwrap();
        }
        let e = engine(
            r#"return { handle = function()
                local first = ig.db.put_if_absent("claim", nil, "mine")
                local second = ig.db.put_if_absent("claim", nil, "yours")
                return { first = first, second = second, held = ig.db.get("claim"),
                         a = ig.db.add("hits"), b = ig.db.add("hits", nil, 4), c = ig.db.add("hits", nil, -2),
                         read = ig.db.get("hits"), sub = ig.db.add("hits", "other") }
            end }"#,
            Limits::default(),
        )
        .with_db(backend.clone());
        assert_eq!(
            e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap(),
            json!({"first":true,"second":false,"held":"mine","a":1,"b":5,"c":3,"read":3,"sub":1})
        );
        let other = backend.get("TEN#other#hits", "").unwrap();
        assert!(other.is_none(), "counters never touch other tenants");
    }

    #[tokio::test]
    async fn pending_timers_are_found_by_trace_and_running_empties_when_invocations_end() {
        let backend = Arc::new(MemItems::default());
        let e = engine(
            r#"return { handle = function() return 1 end }"#,
            Limits::default(),
        )
        .with_db(backend.clone());
        let trace = |id: &str| {
            Some(std::collections::BTreeMap::from([(
                "traceparent".to_string(),
                format!("00-{id}-00f067aa0ba902b7-01"),
            )]))
        };
        let (a, b) = (
            "4bf92f3577b34da6a3ce929d0e0e4736",
            "5bf92f3577b34da6a3ce929d0e0e4736",
        );
        for (id, due, t) in [
            ("t1", 200, trace(a)),
            ("t2", 100, trace(a)),
            ("t3", 150, trace(b)),
            ("t4", 120, None),
        ] {
            let payload = integraal_wrappers::TimerPayload {
                script: None,
                version: None,
                event: json!({}),
                trace: t,
            };
            integraal_wrappers::register_timer(&*backend, "acme", id, due, &payload, 50).unwrap();
        }
        let ids = |t: &str| -> Vec<String> {
            e.pending_timers("acme", t)
                .unwrap()
                .into_iter()
                .map(|p| p.id)
                .collect()
        };
        assert_eq!(ids(a), ["t2", "t1"]);
        assert_eq!(ids(&b.to_uppercase()), ["t3"]);
        assert!(e.pending_timers("other", a).unwrap().is_empty());

        e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap();
        assert!(e.running().is_empty());
    }

    #[test]
    fn pending_lists_what_the_index_points_to_and_drops_what_is_gone() {
        let backend = Arc::new(MemItems::default());
        let e = engine("return {}", Limits::default()).with_db(backend.clone());
        let a = "4bf92f3577b34da6a3ce929d0e0e4736";
        let trace = std::collections::BTreeMap::from([(
            "traceparent".to_string(),
            format!("00-{a}-00f067aa0ba902b7-01"),
        )]);
        let far = now_ms() / 1000 + 1000;
        let put = |pk: &str, sk: &str, value: &str| {
            backend
                .put(
                    pk,
                    integraal_wrappers::Item {
                        sk: sk.into(),
                        value: value.into(),
                        ttl: far,
                    },
                )
                .unwrap();
        };
        // a barrier with one of two members still pending, and a callback
        put(
            "TEN#acme#\u{1}barrier#b1",
            "",
            r#"{"total":2,"ready":true,"due_s":5,"then":"next"}"#,
        );
        put("TEN#acme#\u{1}barrier#b1", "p#m2", "1");
        put("TEN#acme#cb#c1", "", "{}");
        for (kind, id) in [
            ("barrier", "b1"),
            ("callback", "c1"),
            ("callback", "taken"),
            ("batch", "j1"),
        ] {
            integraal_wrappers::trace_index::record(&*backend, "acme", &trace, kind, id, far, 10);
        }
        let got = e.pending("acme", a).unwrap();
        assert_eq!(got["barriers"][0]["pending"], 1);
        assert_eq!(got["barriers"][0]["then"], "next");
        assert_eq!(
            got["callbacks"].as_array().unwrap().len(),
            1,
            "a taken callback is gone"
        );
        assert_eq!(got["callbacks"][0]["id"], "c1");
        assert_eq!(got["batch"][0]["done"], false);
        assert!(e.pending("other", a).unwrap()["barriers"]
            .as_array()
            .unwrap()
            .is_empty());
    }

    #[derive(Default)]
    struct MemQueue(std::sync::Mutex<Vec<(serde_json::Value, u32)>>);
    impl QueueBackend for MemQueue {
        fn send(&self, messages: &[integraal_wrappers::OutMessage]) -> Result<(), String> {
            let mut m = self.0.lock().unwrap();
            for o in messages {
                m.push((serde_json::from_str(&o.body).unwrap(), o.delay_secs));
            }
            Ok(())
        }
    }

    #[tokio::test]
    async fn ticks_reach_only_opted_in_tenants_and_the_minute_tick_sweeps_timers() {
        use crate::{schedule::offset_secs, Every, Spread, Tick};
        let store = Arc::new(MemoryStore::default());
        store.put(
            &ScriptKey::tenants(),
            r#"["acme", "globex", "idle", "nocfg", "../bad"]"#,
        );
        store.put(
            &ScriptKey::config("acme").unwrap(),
            r#"{"schedule":["hour","day"]}"#,
        );
        store.put(
            &ScriptKey::config("globex").unwrap(),
            r#"{"schedule":["day"]}"#,
        );
        store.put(&ScriptKey::config("idle").unwrap(), "{}");
        let queue = Arc::new(MemQueue::default());
        let items = Arc::new(MemItems::default());
        let engine = Engine::new(
            Arc::new(CachedLoader::new(Shared(store), Duration::from_secs(60))),
            Limits::default(),
        )
        .with_queue(queue.clone())
        .with_db(items.clone());
        let spread = Spread::default();
        let tick = |every| Tick {
            every,
            at: "2026-10-25T14:00:00Z".into(),
        };

        let r = engine.tick(&tick(Every::Hour), &spread).await.unwrap();
        assert_eq!(r.sent, 1);
        assert_eq!(
            *queue.0.lock().unwrap(),
            vec![(
                json!({"tenant":"acme","event":{"type":"schedule","every":"hour","at":"2026-10-25T14:00:00Z"}}),
                offset_secs("acme", Every::Hour, spread.hour),
            )]
        );
        queue.0.lock().unwrap().clear();
        assert_eq!(
            engine.tick(&tick(Every::Day), &spread).await.unwrap().sent,
            2
        );
        queue.0.lock().unwrap().clear();

        // The minute tick sends due timers; nobody asked for minute ticks, so that is all it sends.
        let payload = integraal_wrappers::TimerPayload {
            script: None,
            version: None,
            event: json!({"type":"timer"}),
            trace: None,
        };
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        integraal_wrappers::register_timer(&*items, "acme", "t1", now - 1, &payload, now - 5)
            .unwrap();
        let r = engine.tick(&tick(Every::Minute), &spread).await.unwrap();
        assert_eq!((r.swept.unwrap().fired, r.sent), (1, 0));
        assert_eq!(
            *queue.0.lock().unwrap(),
            vec![(json!({"tenant":"acme","event":{"type":"timer"}}), 0)]
        );
    }

    #[tokio::test]
    async fn ticks_without_tenants_json_or_a_queue() {
        use crate::{Every, Spread, Tick};
        let store = Arc::new(MemoryStore::default());
        let loader = Arc::new(CachedLoader::new(Shared(store), Duration::from_secs(60)));
        let tick = Tick {
            every: Every::Hour,
            at: "x".into(),
        };
        let bare = Engine::new(loader.clone(), Limits::default());
        assert!(bare.tick(&tick, &Spread::default()).await.is_err());
        let engine = bare.with_queue(Arc::new(MemQueue::default()));
        assert_eq!(
            engine.tick(&tick, &Spread::default()).await.unwrap().sent,
            0
        );
    }

    #[tokio::test]
    async fn barrier_collects_acks_and_continues_once() {
        let src = r#"local igp = require("@native/patterns@1")
return { handle = function(ctx, ev)
            if ev.step == "create" then
                return igp.barrier.create({ id = "b1", members = { "a.xml", "b.xml" },
                    continue_with = "finalize", state = { batch = 7 } })
            elseif ev.step == "ack" then
                local r = igp.barrier.ack("b1", ev.member, ev.outcome)
                return { duplicate = r.duplicate, complete = r.complete, dispatched = r.dispatched }
            elseif ev.step == "pending" then
                return igp.barrier.pending("b1")
            else
                local b = igp.barrier.hydrate({ barrier = "b1" })
                if not b then return "not complete" end
                local seen = {}
                for member, outcome in b:results() do
                    table.insert(seen, member .. "=" .. tostring(outcome.ok) .. ":" .. tostring(outcome.error))
                end
                return { total = b.total, batch = b.state.batch, seen = seen, failed = #b:failures(),
                         unknown = pcall(igp.barrier.ack, "b1", "zzz") }
            end
        end }"#;
        let backend = Arc::new(MemItems::default());
        let queue = Arc::new(MemQueue::default());
        let e = engine_cfg(src, Limits::default(), r#"{"queue":true}"#)
            .with_db(backend.clone())
            .with_queue(queue.clone());
        let call = |ev: serde_json::Value| e.invoke(inv(Trigger::Queue, ev));
        assert_eq!(call(json!({"step":"create"})).await.unwrap(), json!("b1"));
        assert_eq!(
            call(json!({"step":"done"})).await.unwrap(),
            json!("not complete")
        );
        assert_eq!(
            call(json!({"step":"ack","member":"b.xml","outcome":{"ok":false,"error":"schema"}}))
                .await
                .unwrap(),
            json!({"duplicate":false,"complete":false,"dispatched":false})
        );
        assert_eq!(
            call(json!({"step":"pending"})).await.unwrap(),
            json!(["a.xml"])
        );
        assert_eq!(
            call(json!({"step":"ack","member":"a.xml"})).await.unwrap(),
            json!({"duplicate":false,"complete":true,"dispatched":true})
        );
        assert_eq!(
            *queue.0.lock().unwrap(),
            vec![(
                json!({"tenant":"acme","script":"finalize","event":{"type":"barrier.complete","barrier":"b1"}}),
                0
            )]
        );
        assert_eq!(
            call(json!({"step":"done"})).await.unwrap(),
            json!({"total":2,"batch":7,"seen":["a.xml=true:nil","b.xml=false:schema"],"failed":1,"unknown":false})
        );
    }

    #[tokio::test]
    async fn consumer_claims_recover_and_store_results_through_the_luau_api() {
        let src = r#"local igp = require("@native/patterns@1")
return { handle = function(ctx, ev)
            if ev.op == "complete" then return igp.claim.complete("job", ev.token, ev.result)
            elseif ev.op == "renew" then return igp.claim.renew("job", ev.token, { lease_s = 120 })
            else return igp.claim.acquire("job", { lease_s = 60 }) end
        end }"#;
        let backend = Arc::new(MemItems::default());
        // Claims need the database, but neither a queue nor a tenant queue grant.
        let e = engine_cfg(src, Limits::default(), "{}").with_db(backend.clone());
        let call = |ev| e.invoke(inv(Trigger::Queue, ev));
        let first = call(json!({})).await.unwrap();
        assert_eq!(first["state"], "acquired");
        assert_eq!(call(json!({})).await.unwrap()["state"], "busy");
        assert!(
            call(json!({"op":"renew","token":first["token"]}))
                .await
                .unwrap()
                .as_u64()
                .unwrap()
                >= first["expires_s"].as_u64().unwrap()
        );
        let pk = "TEN#acme#\u{1}claim#job";
        let mut row = backend.get(pk, "").unwrap().unwrap();
        row.ttl = 1;
        backend.put(pk, row).unwrap();
        assert_eq!(
            call(json!({"op":"complete","token":first["token"],"result":false}))
                .await
                .unwrap(),
            json!(false)
        );
        let second = call(json!({})).await.unwrap();
        assert_eq!(second["state"], "acquired");
        assert_ne!(first["token"], second["token"]);
        assert_eq!(
            call(json!({"op":"complete","token":first["token"]}))
                .await
                .unwrap(),
            json!(false)
        );
        assert_eq!(
            call(json!({"op":"complete","token":second["token"],"result":false}))
                .await
                .unwrap(),
            json!(true)
        );
        assert_eq!(
            call(json!({})).await.unwrap(),
            json!({"state":"complete","result":false})
        );
        assert_eq!(
            call(json!({"op":"renew","token":second["token"]}))
                .await
                .unwrap(),
            json!(null)
        );
        assert!(engine(src, Limits::default())
            .invoke(inv(Trigger::Queue, json!({})))
            .await
            .is_err());
        let capped = engine_cfg(src, Limits::default(), r#"{"limits":{"max_data_ops":1}}"#)
            .with_db(Arc::new(MemItems::default()));
        assert!(capped.invoke(inv(Trigger::Queue, json!({}))).await.is_err());
    }

    #[tokio::test]
    async fn durable_handoff_survives_script_failure_and_the_minute_tick_delivers_it() {
        let src = r#"return { handle = function(ctx, ev)
            local created = ig.queue.handoff("order-7", { script = "work", version = "v1", event = { order = ev.order } })
            if ev.fail then error("crash after persisting") end
            return created
        end }"#;
        let backend = Arc::new(MemItems::default());
        let queue = Arc::new(MemQueue::default());
        let e = engine_cfg(src, Limits::default(), r#"{"queue":true}"#)
            .with_db(backend.clone())
            .with_queue(queue.clone());
        assert!(e
            .invoke(inv(Trigger::Queue, json!({"order":7,"fail":true})))
            .await
            .is_err());
        assert!(queue.0.lock().unwrap().is_empty());
        assert_eq!(
            e.invoke(inv(Trigger::Queue, json!({"order":7})))
                .await
                .unwrap(),
            json!(false)
        );
        assert!(e
            .invoke(inv(Trigger::Queue, json!({"order":8})))
            .await
            .is_err());
        let tick = crate::Tick {
            every: crate::Every::Minute,
            at: "2026-10-26T12:00:00Z".into(),
        };
        assert_eq!(
            e.tick(&tick, &crate::Spread::default())
                .await
                .unwrap()
                .outbox
                .unwrap()
                .fired,
            1
        );
        assert_eq!(
            e.tick(&tick, &crate::Spread::default())
                .await
                .unwrap()
                .outbox
                .unwrap()
                .fired,
            0
        );
        assert_eq!(
            *queue.0.lock().unwrap(),
            vec![(
                json!({"tenant":"acme","script":"work","version":"v1","event":{"order":7}}),
                0
            )]
        );
        assert_eq!(
            e.invoke(inv(Trigger::Queue, json!({"order":7})))
                .await
                .unwrap(),
            json!(false)
        );
        let disabled = engine_cfg(src, Limits::default(), "{}")
            .with_db(backend)
            .with_queue(queue);
        assert!(disabled
            .invoke(inv(Trigger::Queue, json!({"order":7})))
            .await
            .is_err());
    }

    #[tokio::test]
    async fn queue_send_stays_in_the_tenant_and_needs_opt_in() {
        let src = r#"return { handle = function(ctx, ev)
            local one = ig.queue.send({ script = "work", event = { n = 1 }, delay_s = 30 })
            local many = ig.queue.send({ { script = "work", version = "v2", event = { n = 2 } }, { event = "routed" } })
            local bad_name = pcall(ig.queue.send, { script = "../x" })
            local bad_delay = pcall(ig.queue.send, { script = "x", delay_s = 901 })
            return { one = one, many = many, bad_name = bad_name, bad_delay = bad_delay, attempt = ctx.attempt }
        end }"#;
        let queue = Arc::new(MemQueue::default());
        let mut call = inv(Trigger::Queue, json!(null));
        call.attempt = 3;
        let e = engine_cfg(src, Limits::default(), r#"{"queue":true}"#).with_queue(queue.clone());
        assert_eq!(
            e.invoke(call).await.unwrap(),
            json!({"one":1,"many":2,"bad_name":false,"bad_delay":false,"attempt":3})
        );
        assert_eq!(
            *queue.0.lock().unwrap(),
            vec![
                (json!({"tenant":"acme","script":"work","event":{"n":1}}), 30),
                (
                    json!({"tenant":"acme","script":"work","version":"v2","event":{"n":2}}),
                    0
                ),
                (json!({"tenant":"acme","event":"routed"}), 0),
            ]
        );

        // not opted in: the API errors instead of sending
        let off = engine_cfg(src, Limits::default(), "{}").with_queue(queue.clone());
        let err = off
            .invoke(inv(Trigger::Invoke, json!(null)))
            .await
            .unwrap_err();
        assert!(
            err.to_string().contains("ig.queue is not available"),
            "{err}"
        );
        // opted in but the deployment has no queue
        let none = engine_cfg(src, Limits::default(), r#"{"queue":true}"#);
        assert!(matches!(
            none.invoke(inv(Trigger::Invoke, json!(null))).await,
            Err(Error::Config(_))
        ));
    }

    #[tokio::test]
    async fn callbacks_are_signed_single_use_and_tenant_scoped() {
        use integraal_wrappers::Take;
        let backend = Arc::new(MemItems::default());
        let e = engine(
            r#"local igp = require("@native/patterns@1")
return { handle = function()
                local ok = pcall(igp.callback.create, { script = "../x" })
                return { url = igp.callback.create({ script = "on-pay", state = { order = 7 }, ttl_s = 60 }), bad_script = ok }
            end }"#,
            Limits::default(),
        )
        .with_db(backend.clone())
        .with_callback_base_url("https://hooks.example.com/");
        let out = e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap();
        assert_eq!(out["bad_script"], json!(false));
        let token = out["url"]
            .as_str()
            .unwrap()
            .strip_prefix("https://hooks.example.com/cb/acme/")
            .expect("url shape")
            .to_string();
        assert_eq!(token.len(), 80);

        // the verifier (a different process, here a fresh scope) reads the same stored key
        let acme = DbScope::new(backend.clone(), "acme");
        let other = DbScope::new(backend.clone(), "other");
        assert!(
            matches!(other.take_callback(&token).unwrap(), Take::Invalid),
            "wrong tenant"
        );
        let mut forged = token.clone().into_bytes();
        forged[0] = if forged[0] == b'0' { b'1' } else { b'0' }; // different id, same tag
        assert!(matches!(
            acme.take_callback(&String::from_utf8(forged).unwrap())
                .unwrap(),
            Take::Invalid
        ));
        let mut bad_exp = token.clone();
        bad_exp.replace_range(32..48, "ffffffffffffffff");
        assert!(
            matches!(acme.take_callback(&bad_exp).unwrap(), Take::Invalid),
            "expiry is signed"
        );
        assert!(matches!(acme.take_callback("nope").unwrap(), Take::Invalid));

        let Take::Found(taken) = acme.take_callback(&token).unwrap() else {
            panic!("first take must succeed")
        };
        assert_eq!(taken.callback.script.as_deref(), Some("on-pay"));
        assert_eq!(taken.callback.state, json!({"order": 7}));
        assert!(
            matches!(acme.take_callback(&token).unwrap(), Take::Gone),
            "second take must fail"
        );

        // a failed delivery can put it back with its original expiry, and it is single-use again
        acme.restore_callback(taken).unwrap();
        assert!(matches!(
            acme.take_callback(&token).unwrap(),
            Take::Found(_)
        ));
        assert!(matches!(acme.take_callback(&token).unwrap(), Take::Gone));

        // the signing key exists once, outside every tenant partition
        assert_eq!(
            backend
                .0
                .lock()
                .unwrap()
                .keys()
                .filter(|(pk, _)| pk.starts_with("SYS#"))
                .count(),
            1
        );
    }

    #[tokio::test]
    async fn expired_callback_is_rejected_without_a_lookup() {
        use integraal_wrappers::Take;
        let backend = Arc::new(MemItems::default());
        let scope = DbScope::new(backend.clone(), "acme");
        let token = scope
            .create_callback(None, json!(null), Duration::from_secs(5))
            .unwrap();
        assert!(matches!(
            scope.take_callback(&token).unwrap(),
            Take::Found(_)
        ));
        let token = scope
            .create_callback(None, json!(null), Duration::from_secs(5))
            .unwrap();
        tokio::time::sleep(Duration::from_millis(6100)).await;
        backend.0.lock().unwrap().clear(); // even the rows and key are gone: only the signed expiry can answer
        assert!(matches!(
            scope.take_callback(&token).unwrap(),
            Take::Expired
        ));
    }

    #[tokio::test]
    async fn callback_requires_configuration() {
        let e = engine(
            r#"local igp = require("@native/patterns@1")
return { handle = function() return (pcall(igp.callback.create)) end }"#,
            Limits::default(),
        )
        .with_db(Arc::new(MemItems::default()));
        assert_eq!(
            e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap(),
            json!(false)
        );
    }

    #[tokio::test]
    async fn http_requires_allowlist_from_tenant_config() {
        let store = Arc::new(MemoryStore::default());
        let script = r#"return { handle = function()
            local ok, err = pcall(ig.http.get, "https://evil.example.org/x")
            return { ok = ok, err = tostring(err) }
        end }"#;
        store.put(&ScriptKey::new("acme", "s", None).unwrap(), script);
        let loader = Arc::new(CachedLoader::new(Shared(store.clone()), Duration::ZERO));
        let e = Engine::new(loader, Limits::default());
        let denied = e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap();
        assert_eq!(denied["ok"], json!(false));
        assert!(denied["err"].as_str().unwrap().contains("allowlist"));

        store.put(
            &ScriptKey::config("acme").unwrap(),
            r#"{"http":{"allow":["https://evil.example.org/"]}}"#,
        );
        let allowed = e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap();
        assert!(!allowed["err"].as_str().unwrap().contains("allowlist"));
    }

    #[tokio::test]
    async fn vault_secrets_are_injected_only_where_allowed_and_never_readable() {
        let script = r#"return { handle = function()
            local try = function(url, headers, vault)
                local ok, err = pcall(ig.http.get, url, headers, vault)
                return tostring(err)
            end
            local on = { "header" }
            local h = { authorization = "Bearer {{vault:key.api.token}}" }
            return {
                other = try("https://other.invalid/x", h, on),
                missing = try("https://api.invalid/x", { a = "{{vault:nope}}" }, on),
                allowed = try("https://api.invalid/x", h, on),
                in_url = try("https://api.invalid/{{vault:key.api.token}}", {}, { "url" }),
                url_elsewhere = try("https://other.invalid/{{vault:key.api.token}}", {}, { "url" }),
                off = try("https://api.invalid/x", { a = "{{vault:nope}}" }),
                bad_target = try("https://api.invalid/x", h, { "cookie" }),
                has_read = ig.vault ~= nil,
            }
        end }"#;
        let config = r#"{"http":{"allow":["https://api.invalid/","https://other.invalid/"]}}"#;
        let keys = integraal_wrappers::MasterKeys::single(
            integraal_wrappers::MasterKey::from_bytes([9; 32]),
        );

        // no vault configured
        let e = engine_cfg(script, Limits::default(), config);
        let out = e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap();
        assert!(
            out["other"].as_str().unwrap().contains("not configured"),
            "{out}"
        );

        let e = engine_cfg(script, Limits::default(), config)
            .with_db(Arc::new(MemItems::default()))
            .with_master_keys(keys);
        e.vault()
            .unwrap()
            .put(
                "acme",
                "key",
                &json!({"api": {"token": "sk_live_abcdef"}}),
                &["https://api.invalid/".into()],
                false,
            )
            .unwrap();
        let out = e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap();
        assert!(
            out["other"].as_str().unwrap().contains("may not be sent"),
            "{out}"
        );
        assert!(
            out["missing"].as_str().unwrap().contains("does not exist"),
            "{out}"
        );
        // passes the vault check and goes on to fail at the (unresolvable) network
        let allowed = out["allowed"].as_str().unwrap();
        assert!(allowed.contains("http request failed"), "{out}");
        assert!(!out.to_string().contains("sk_live_abcdef"), "{out}");
        // a secret in the url is also checked against the final url, and errors never echo it
        assert!(
            out["in_url"]
                .as_str()
                .unwrap()
                .contains("http request failed"),
            "{out}"
        );
        assert!(
            out["url_elsewhere"]
                .as_str()
                .unwrap()
                .contains("may not be sent"),
            "{out}"
        );
        // without opting in, placeholders stay literal (so untrusted text cannot pull in secrets)
        assert!(
            out["off"].as_str().unwrap().contains("http request failed"),
            "{out}"
        );
        assert!(
            out["bad_target"]
                .as_str()
                .unwrap()
                .contains("unknown vault target"),
            "{out}"
        );
        assert_eq!(
            out["has_read"],
            json!(false),
            "scripts have no way to read secrets"
        );
    }

    #[tokio::test]
    async fn throttled_http_gives_up_at_the_deadline() {
        let script = r#"return { handle = function()
            local errs = {}
            for i = 1, 2 do
                local ok, err = pcall(ig.http.get, "https://api.partner.invalid/v1/x")
                errs[i] = tostring(err)
            end
            return errs
        end }"#;
        let config = r#"{"http":{"allow":["https://api.partner.invalid/"]}}"#;
        // one start per 100 s: the first call goes out, the second could only start after the deadline
        let rules =
            Throttle::from_json(r#"[{"url":"https://api.partner.invalid/v1/","per_second":0.01}]"#)
                .unwrap();
        let e = engine_cfg(script, Limits::default(), config).with_throttle(rules);
        let out = e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap();
        assert!(!out[0].as_str().unwrap().contains("throttled"), "{out}");
        assert!(out[1].as_str().unwrap().contains("throttled"), "{out}");

        // without a throttle, neither call is held back
        let out = engine_cfg(script, Limits::default(), config)
            .invoke(inv(Trigger::Invoke, json!(null)))
            .await
            .unwrap();
        assert!(!out[1].as_str().unwrap().contains("throttled"), "{out}");
    }

    #[tokio::test]
    async fn xml_pull_parser_streams_from_data_and_strings() {
        // ~12 MB document: larger than the materialized-read cap.
        let mut doc = String::from("<feed>");
        for i in 0..200_000 {
            doc.push_str(&format!("<entry id=\"{i}\"><v>x &amp; y</v><e/></entry>"));
        }
        doc.push_str("</feed>");
        assert!(doc.len() > 5 * 1024 * 1024);
        let backend = Arc::new(MemBackend::default());
        backend.put("data/acme/big.xml", doc.into_bytes()).unwrap();
        let e = engine_cfg(
            r#"return { handle = function()
                local n, last, text = 0, nil, nil
                for ev in ig.xml.events(ig.store.In.open("big.xml")) do
                    if ev.type == "start" and ev.tag == "entry" then n += 1; last = ev.attrs.id end
                    if ev.type == "text" then text = ev.text end
                end
                local depths = {}
                for ev in ig.xml.events("<a><b/></a>") do table.insert(depths, ev.type .. ev.depth) end
                local dtd = pcall(function() for _ in ig.xml.events("<!DOCTYPE a []><a/>") do end end)
                return { n = n, last = last, text = text, depths = depths, dtd = dtd }
            end }"#,
            Limits { timeout: Duration::from_secs(60), ..Limits::default() },
            r#"{"stores":{"In":"read"}}"#,
        )
        .with_store("In", backend, "data/");
        assert_eq!(
            e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap(),
            json!({"n":200000,"last":"199999","text":"x & y","depths":["start0","start1","end1","end0"],"dtd":false})
        );
    }

    fn make_zip() -> Vec<u8> {
        use std::io::Write;
        use zip::{write::SimpleFileOptions, CompressionMethod, ZipWriter};
        let big = "<r>".to_string() + &"<i>v</i>".repeat(50_000) + "</r>";
        let deflated = SimpleFileOptions::default().compression_method(CompressionMethod::Deflated);
        let stored = SimpleFileOptions::default().compression_method(CompressionMethod::Stored);
        let mut out = Vec::new();
        let mut w = ZipWriter::new(std::io::Cursor::new(&mut out));
        w.start_file("a.txt", deflated).unwrap();
        w.write_all(b"hello").unwrap();
        w.start_file("skipped.bin", deflated).unwrap();
        w.write_all(big.as_bytes()).unwrap();
        w.start_file("dir/b.xml", deflated).unwrap();
        w.write_all(big.as_bytes()).unwrap();
        w.add_directory("d/", deflated).unwrap();
        w.start_file("stored.txt", stored).unwrap();
        w.write_all(b"plain").unwrap();
        w.finish().unwrap();
        out
    }

    /// An archive as produced by a streaming writer: sizes only in trailing data descriptors.
    fn make_streamed_zip(files: &[(&str, &[u8])]) -> Vec<u8> {
        use flate2::{write::DeflateEncoder, Compression};
        use std::io::Write;
        let mut out = Vec::new();
        for (name, data) in files {
            out.extend(0x0403_4b50u32.to_le_bytes());
            out.extend([
                20, 0, 8, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0,
            ]);
            out.extend((name.len() as u16).to_le_bytes());
            out.extend([0, 0]);
            out.extend(name.as_bytes());
            let mut enc = DeflateEncoder::new(Vec::new(), Compression::default());
            enc.write_all(data).unwrap();
            let comp = enc.finish().unwrap();
            out.extend(&comp);
            out.extend(0x0807_4b50u32.to_le_bytes());
            out.extend(crc32fast::hash(data).to_le_bytes());
            out.extend((comp.len() as u32).to_le_bytes());
            out.extend((data.len() as u32).to_le_bytes());
        }
        out.extend(0x0605_4b50u32.to_le_bytes());
        out.extend([0u8; 18]);
        out
    }

    async fn run_zip(script: &str, zip: Vec<u8>) -> Result<serde_json::Value, crate::Error> {
        let backend = Arc::new(MemBackend::default());
        backend.put("data/acme/z.zip", zip).unwrap();
        engine_cfg(script, Limits::default(), r#"{"stores":{"In":"read"}}"#)
            .with_store("In", backend, "data/")
            .invoke(inv(Trigger::Invoke, json!(null)))
            .await
    }

    #[tokio::test]
    async fn zip_entries_iterate_skip_open_and_detect_corruption() {
        let script = r#"return { handle = function()
            local seen, a, n, stale = {}, nil, 0, nil
            local first
            for entry in ig.zip.entries(ig.store.In.open("z.zip")) do
                table.insert(seen, entry.name)
                if entry.name == "a.txt" then
                    a = entry:read()
                    first = entry
                elseif entry.name == "dir/b.xml" then
                    for ev in ig.xml.events(entry:open()) do
                        if ev.type == "start" and ev.tag == "i" then n += 1 end
                    end
                elseif entry.name == "stored.txt" then
                    stale = select(2, pcall(function() return first:open() end))
                    a = a .. "+" .. entry:read()
                end
            end
            return { seen = seen, a = a, n = n, reuse = tostring(stale):find("already") ~= nil }
        end }"#;
        assert_eq!(
            run_zip(script, make_zip()).await.unwrap(),
            json!({"seen":["a.txt","skipped.bin","dir/b.xml","d/","stored.txt"],"a":"hello+plain","n":50000,"reuse":true})
        );

        let streamed = make_streamed_zip(&[
            ("one", b"first"),
            ("two", &[7u8; 100_000]),
            ("three", b"last"),
        ]);
        let script = r#"return { handle = function()
            local seen = {}
            for entry in ig.zip.entries(ig.store.In.open("z.zip")) do
                table.insert(seen, entry.name .. ":" .. (entry.size or "?"))
                if entry.name ~= "two" then seen[#seen] ..= ":" .. entry:read() end
            end
            return seen
        end }"#;
        assert_eq!(
            run_zip(script, streamed.clone()).await.unwrap(),
            json!(["one:?:first", "two:?", "three:?:last"])
        );

        // A flipped byte in stored data must trip the checksum.
        let mut bad = make_zip();
        let at = bad.windows(5).position(|w| w == b"plain").unwrap();
        bad[at] = b'X';
        let script = r#"return { handle = function()
            for entry in ig.zip.entries(ig.store.In.open("z.zip")) do entry:read() end
        end }"#;
        let err = run_zip(script, bad).await.unwrap_err().to_string();
        assert!(err.contains("checksum"), "{err}");

        // A cut-off archive is an error, not a silently shorter listing.
        let mut cut = streamed;
        cut.truncate(cut.len() - 40);
        let err = run_zip(script, cut).await.unwrap_err().to_string();
        assert!(err.contains("truncated") || err.contains("zip"), "{err}");
    }

    #[tokio::test]
    async fn zip_writer_streams_to_bucket_and_reads_back() {
        use std::io::Read;
        let big = "<r>".to_string() + &"<i>v</i>".repeat(100_000) + "</r>";
        let input = Arc::new(MemBackend::default());
        input
            .put("in/acme/src.xml", big.clone().into_bytes())
            .unwrap();
        let output = Arc::new(MemBackend::default());
        let e = engine_cfg(
            r#"return { handle = function()
                local out = ig.store.Out.create("pack.zip")
                local z = ig.zip.writer(out)
                z:add("hello.txt", "hello")
                z:add("dir/copy.xml", ig.store.In.open("src.xml"))
                z:add("parts.txt"); z:write("a"); z:write("b")
                z:add("empty/")
                z:finish()
                local bytes = out:close()
                local closed = pcall(function() out:write("x") end)

                local aborted = ig.store.Out.create("aborted.zip")
                aborted:write("junk"); aborted:abort()

                local seen = {}
                for e in ig.zip.entries(ig.store.Out.open("pack.zip")) do
                    seen[#seen + 1] = e.name .. "=" .. #e:read()
                end
                local mem = ig.zip.writer(); mem:add("m.txt", "mem")
                local s = mem:finish()
                local n = 0
                for e in ig.zip.entries(s) do n += #e:read() end
                return { seen = seen, wrote = bytes > 0, closed = closed, mem_n = n,
                         gone = ig.store.Out.get("aborted.zip") == nil }
            end }"#,
            Limits::default(),
            r#"{"stores":{"In":"read","Out":"readwrite"}}"#,
        )
        .with_store("In", input, "in/")
        .with_store("Out", output.clone(), "out/");
        let res = e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap();
        assert_eq!(
            res,
            json!({"seen":["hello.txt=5","dir/copy.xml=800007","parts.txt=2","empty/=0"],
                   "wrote":true,"closed":false,"mem_n":3,"gone":true})
        );

        // An independent reader (central directory based) must accept the archive too.
        let bytes = output.get("out/acme/pack.zip").unwrap().unwrap();
        let mut ar = zip::ZipArchive::new(std::io::Cursor::new(bytes)).unwrap();
        assert_eq!(ar.len(), 4);
        let mut text = String::new();
        ar.by_name("dir/copy.xml")
            .unwrap()
            .read_to_string(&mut text)
            .unwrap();
        assert_eq!(text, big);
        text.clear();
        ar.by_name("parts.txt")
            .unwrap()
            .read_to_string(&mut text)
            .unwrap();
        assert_eq!(text, "ab");
        assert!(ar.by_name("empty/").unwrap().is_dir());
    }

    #[tokio::test]
    async fn gzip_streams_and_strings_roundtrip() {
        use flate2::{write::GzEncoder, Compression};
        use std::io::{Read, Write};
        // Two members back to back: a valid .gz file may contain several.
        let mut gz = Vec::new();
        for part in ["<r><i>1</i>", "<i>2</i></r>"] {
            let mut e = GzEncoder::new(Vec::new(), Compression::default());
            e.write_all(part.as_bytes()).unwrap();
            gz.extend(e.finish().unwrap());
        }
        let input = Arc::new(MemBackend::default());
        input.put("in/acme/f.xml.gz", gz).unwrap();
        let mut bad = input.get("in/acme/f.xml.gz").unwrap().unwrap();
        let n = bad.len();
        bad[n - 6] ^= 0xff; // inside the last member's checksum/size trailer
        input.put("in/acme/bad.gz", bad).unwrap();
        let output = Arc::new(MemBackend::default());
        let e = engine_cfg(
            r#"return { handle = function()
                local n = 0
                for ev in ig.xml.events(ig.gzip.open(ig.store.In.open("f.xml.gz"))) do
                    if ev.type == "start" and ev.tag == "i" then n += 1 end
                end
                local s = ig.gzip.compress(string.rep("abc", 1000))
                local back = ig.gzip.decompress(s)
                local out = ig.store.Out.create("o.gz")
                local w = ig.gzip.writer(out)
                for i = 1, 200 do w:write(string.rep("line " .. i .. "\n", 100)) end
                w:finish(); out:close()
                local mem = ig.gzip.writer(); mem:write("hi");
                local bad = pcall(function()
                    local st = ig.gzip.open(ig.store.In.open("bad.gz"))
                    for _ in ig.xml.events(st) do end
                end)
                return { n = n, small = #s < 100, back = #back, mem = ig.gzip.decompress(mem:finish()), bad = bad }
            end }"#,
            Limits::default(),
            r#"{"stores":{"In":"read","Out":"readwrite"}}"#,
        )
        .with_store("In", input, "in/")
        .with_store("Out", output.clone(), "out/");
        let res = e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap();
        assert_eq!(
            res,
            json!({"n":2,"small":true,"back":3000,"mem":"hi","bad":false})
        );
        let mut text = String::new();
        flate2::read::GzDecoder::new(&output.get("out/acme/o.gz").unwrap().unwrap()[..])
            .read_to_string(&mut text)
            .unwrap();
        assert!(text.starts_with("line 1\nline 1\n") && text.ends_with("line 200\n"));
    }

    #[tokio::test]
    async fn sql_scratch_db_saves_loads_and_is_locked_down() {
        let store = Arc::new(MemBackend::default());
        let script = r#"return {
            on_queue = function(ctx, ev)
                local db = ig.sql.open()
                db:exec("create table seen (id text primary key, amount real, note text)")
                for i = 1, 1000 do
                    db:exec("insert into seen values (?, ?, ?)", { "id" .. i, i * 1.5, ig.sql.null })
                end
                local out = ig.store.Temp.create("job.db")
                local bytes = db:save(out)
                out:close()
                return { bytes = bytes > 0 }
            end,
            handle = function()
                local db = ig.sql.load(ig.store.Temp.open("job.db"))
                local top = db:query("select id, amount, note from seen order by amount desc limit 2")
                db:exec("insert into seen values ('new', 1, 'x')")  -- a loaded db is writable
                local c = db:get("select count(*) as n, sum(amount) as total from seen")
                local function fails(sql)
                    local ok = pcall(function() db:exec(sql) end)
                    return not ok
                end
                local dup = pcall(function() db:exec("insert into seen values ('id1', 1, 'x')") end)
                local d2 = ig.sql.load(db:dump())
                return {
                    top = top, n = c.n, total = c.total, dup_rejected = not dup,
                    attach = fails("attach database ':memory:' as x"),
                    pragma = fails("pragma journal_mode = off"),
                    redump = d2:get("select count(*) as n from seen").n,
                    bad = not pcall(ig.sql.load, "not a database"),
                }
            end }"#;
        let mk = |store: Arc<MemBackend>| {
            engine_cfg(
                script,
                Limits::default(),
                r#"{"stores":{"Temp":"readwrite"}}"#,
            )
            .with_store("Temp", store, "tmp/")
        };
        let first = mk(store.clone())
            .invoke(inv(Trigger::Queue, json!({})))
            .await
            .unwrap();
        assert_eq!(first, json!({"bytes": true}));
        // A later invocation, with a fresh engine, picks the file up from the store.
        let out = mk(store)
            .invoke(inv(Trigger::Invoke, json!(null)))
            .await
            .unwrap();
        assert_eq!(
            out["top"],
            json!([{"id":"id1000","amount":1500},{"id":"id999","amount":1498.5}])
        );
        assert_eq!(out["n"], json!(1001));
        assert_eq!(out["total"], json!(750_751));
        for k in ["dup_rejected", "attach", "pragma", "bad"] {
            assert_eq!(out[k], json!(true), "{k}: {out}");
        }
        assert_eq!(out["redump"], json!(1001));
    }

    #[tokio::test]
    async fn sql_scratch_db_respects_size_and_time_limits() {
        let e = engine_cfg(
            r#"return { handle = function()
                local db = ig.sql.open()
                db:exec("create table t (b blob)")
                local full = pcall(function()
                    for i = 1, 100 do db:exec("insert into t values (zeroblob(100000))") end
                end)
                return { full = full }
            end }"#,
            Limits {
                max_sql_bytes: 1024 * 1024,
                ..Limits::default()
            },
            "{}",
        );
        assert_eq!(
            e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap(),
            json!({"full": false})
        );
        let e = engine(
            r#"return { handle = function()
                local db = ig.sql.open()
                db:exec("with recursive r(n) as (select 1 union all select n + 1 from r) select count(*) from r")
            end }"#,
            Limits {
                timeout: Duration::from_millis(300),
                ..Limits::default()
            },
        );
        assert!(matches!(
            e.invoke(inv(Trigger::Invoke, json!(null))).await,
            Err(Error::Timeout)
        ));
    }

    #[tokio::test]
    async fn deadline_blocks_interrupt_and_nest() {
        let e = engine(
            r#"return { handle = function()
                local out = {}
                -- passes results and errors through when the time is not up
                out.value = { ig.deadline.with(1000, function() return 1, "two" end) }
                local ok, err = pcall(ig.deadline.with, 1000, function() error("boom") end)
                out.plain_err = tostring(err):find("boom") ~= nil

                -- a loop that outlives its block is interrupted; the caller can catch it
                local ok1, err1 = pcall(ig.deadline.with, 50, function() while true do end end)
                out.expired = not ok1 and tostring(err1):find("deadline exceeded") ~= nil

                -- a pcall inside the block cannot swallow the interrupt
                local ok2, err2 = pcall(ig.deadline.with, 50, function()
                    while true do pcall(function() while true do end end) end
                end)
                out.swallow = not ok2 and tostring(err2):find("deadline exceeded") ~= nil

                -- nested: the inner block ends first and only it is reported
                local inner
                local ok3 = pcall(ig.deadline.with, 5000, function()
                    inner = select(2, pcall(ig.deadline.with, 50, function() while true do end end))
                    return "outer finished"
                end)
                out.inner = ok3 and tostring(inner):find("deadline exceeded") ~= nil

                -- nested the other way: the outer block ends first and unwinds past the inner
                local ok4, err4 = pcall(ig.deadline.with, 50, function()
                    ig.deadline.with(5000, function() while true do end end)
                    return "unreachable"
                end)
                out.outer = not ok4 and tostring(err4):find("deadline exceeded") ~= nil

                -- the limit is gone after the block
                out.after = ig.deadline.with(5000, function() return "ok" end)
                local t = 0
                for i = 1, 200000 do t = t + 1 end
                out.free = t
                return out
            end }"#,
            Limits::default(),
        );
        let out = e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap();
        assert_eq!(out["value"], json!([1, "two"]));
        for k in ["plain_err", "expired", "swallow", "inner", "outer"] {
            assert_eq!(out[k], json!(true), "{k}: {out}");
        }
        assert_eq!(out["after"], json!("ok"));
        assert_eq!(out["free"], json!(200000));
    }

    #[tokio::test]
    async fn deadline_blocks_cannot_outlast_the_invocation() {
        let e = engine(
            "return { handle = function()
                pcall(ig.deadline.with, 60000, function() while true do end end)
                return 1
            end }",
            Limits {
                timeout: Duration::from_millis(100),
                ..Limits::default()
            },
        );
        assert!(matches!(
            e.invoke(inv(Trigger::Invoke, json!(null))).await,
            Err(Error::Timeout)
        ));
    }

    #[tokio::test]
    async fn deadline_block_interrupts_native_calls() {
        let e = engine(
            r#"return { handle = function()
                local db = ig.sql.open()
                local ok, err = pcall(ig.deadline.with, 100, function()
                    db:exec("with recursive r(n) as (select 1 union all select n + 1 from r) select count(*) from r")
                end)
                return { ok = ok, err = tostring(err):find("deadline exceeded") ~= nil }
            end }"#,
            Limits::default(),
        );
        assert_eq!(
            e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap(),
            json!({"ok": false, "err": true})
        );
    }

    #[tokio::test]
    async fn local_locks_exclude_across_invocations_and_release_on_every_path() {
        // The table is process-wide, so two engines (two invocations) contend.
        let holder = engine(
            r#"local igp = require("@native/patterns@1")
return { handle = function()
                return { igp.lock.with("tl-a", nil, function()
                    local t = ig.time.now_ms()
                    while ig.time.now_ms() - t < 400 do end
                    return "held"
                end) }
            end }"#,
            Limits::default(),
        );
        let rival = |wait: u32| {
            engine(
                &format!(
                    r#"local igp = require("@native/patterns@1")
return {{ handle = function()
                        return {{ igp.lock.with("tl-a", {{ wait_ms = {wait} }}, function() return "got" end) }}
                    end }}"#
                ),
                Limits::default(),
            )
        };
        let (held, busy, patient) = tokio::join!(
            holder.invoke(inv(Trigger::Invoke, json!(null))),
            async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                rival(0).invoke(inv(Trigger::Invoke, json!(null))).await
            },
            async {
                tokio::time::sleep(Duration::from_millis(100)).await;
                rival(5000).invoke(inv(Trigger::Invoke, json!(null))).await
            },
        );
        assert_eq!(held.unwrap(), json!([true, "held"]));
        assert_eq!(busy.unwrap(), json!([false]));
        assert_eq!(patient.unwrap(), json!([true, "got"]));

        // Other tenants' names do not collide, and a lease kills the holder and frees the lock.
        let e = engine(
            r#"local igp = require("@native/patterns@1")
return { handle = function()
                local ok, err = pcall(igp.lock.with, "tl-b", { lease_ms = 50 }, function() while true do end end)
                local again = { igp.lock.with("tl-b", nil, function() return 1 end) }
                local boom = pcall(igp.lock.with, "tl-b", nil, function() error("x") end)
                local after_error = { igp.lock.with("tl-b", nil, function() return 2 end) }
                local nested = { igp.lock.with("tl-c", nil, function()
                    return igp.lock.with("tl-c", nil, function() return "inner" end)
                end) }
                return { killed = not ok and tostring(err):find("deadline exceeded") ~= nil,
                         again = again, boom = boom, after_error = after_error, nested = nested,
                         bad_scope = pcall(igp.lock.with, "x", { scope = "nope" }, function() end),
                         bad_lease = pcall(igp.lock.with, "x", { lease_ms = 0 }, function() end),
                         no_db = pcall(igp.lock.with, "x", { scope = "db" }, function() end) }
            end }"#,
            Limits::default(),
        );
        assert_eq!(
            e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap(),
            json!({"killed": true, "again": [true, 1], "boom": false, "after_error": [true, 2],
                   "nested": [true, false], "bad_scope": false, "bad_lease": false, "no_db": false})
        );
    }

    #[tokio::test]
    async fn db_locks_are_leases_owned_by_the_holder() {
        let backend = Arc::new(MemItems::default());
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let lock_item = |value: &str, ttl: u64| integraal_wrappers::Item {
            sk: "".into(),
            value: value.into(),
            ttl,
        };
        // held by someone else, lease still running / lease long gone but row not yet swept
        backend
            .put("TEN#acme#\u{1}lock#busy", lock_item("other", now + 600))
            .unwrap();
        backend
            .put("TEN#acme#\u{1}lock#stale", lock_item("other", now - 5))
            .unwrap();
        let e = engine(
            r#"local igp = require("@native/patterns@1")
return { handle = function()
                local o = {}
                o.busy = { igp.lock.with("busy", { scope = "db" }, function() return "no" end) }
                local t = ig.time.now_ms()
                o.waited = { igp.lock.with("busy", { scope = "db", wait_ms = 300 }, function() return "no" end) }
                o.waited_ms = ig.time.now_ms() - t
                o.stale = { igp.lock.with("stale", { scope = "db" }, function() return "took over" end) }
                o.free = { igp.lock.with("free", { scope = "db", lease_ms = 5000 }, function()
                    -- same tenant, but a different holder: the lock is taken
                    local again = { igp.lock.with("free", { scope = "db" }, function() return "no" end) }
                    return again[1]
                end) }
                local ok, err = pcall(igp.lock.with, "slow", { scope = "db", lease_ms = 100 }, function() while true do end end)
                o.killed = not ok and tostring(err):find("deadline exceeded") ~= nil
                o.reusable = { igp.lock.with("slow", { scope = "db" }, function() return 1 end) }
                o.invisible = not pcall(ig.db.query, "\u{1}lock#free") and #ig.db.query("lock#free") == 0
                return o
            end }"#,
            Limits::default(),
        )
        .with_db(backend.clone());
        let out = e.invoke(inv(Trigger::Invoke, json!(null))).await;
        let out = out.unwrap_or_else(|err| panic!("{err:?}"));
        assert_eq!(out["busy"], json!([false]));
        assert_eq!(out["waited"], json!([false]));
        assert!(out["waited_ms"].as_u64().unwrap() >= 250, "{out}");
        assert_eq!(out["stale"], json!([true, "took over"]));
        assert_eq!(out["free"], json!([true, false]));
        assert_eq!(out["killed"], json!(true));
        assert_eq!(out["invisible"], json!(true));
        assert_eq!(out["reusable"], json!([true, 1]));
        // released on every path, and the other holder's row is untouched
        for name in ["free", "slow", "stale"] {
            assert!(
                backend
                    .get(&format!("TEN#acme#\u{1}lock#{name}"), "")
                    .unwrap()
                    .is_none(),
                "{name} still locked"
            );
        }
        assert_eq!(
            backend
                .get("TEN#acme#\u{1}lock#busy", "")
                .unwrap()
                .unwrap()
                .value,
            "other"
        );
    }

    #[tokio::test]
    async fn core_and_patterns_have_separate_versioned_namespaces() {
        let store = Arc::new(MemoryStore::default());
        store.put(
            &ScriptKey::new("acme", "s", None).unwrap(),
            r#"local igp = require("@native/patterns@1")
return { handle = function()
                assert(native == nil and getfenv().igp == nil)
                assert(rawequal(igp, require("@native/patterns@1")))
                for _, spec in ipairs({"@native/patterns", "@native/patterns@2", "@native/patterns@latest", "@native/unknown@1"}) do
                    local ok, err = pcall(require, spec)
                    assert(not ok and string.find(tostring(err), "native") ~= nil, spec)
                end
                assert(ig.version == 1 and igp.version == 1)
                assert(igp.invocation_marker == nil)
                igp.invocation_marker = true
                for _, name in ipairs({"claim", "lock", "callback", "barrier", "batch"}) do
                    assert(type(igp[name]) == "table", name)
                    assert(ig[name] == nil, name)
                end
                for _, name in ipairs({"store", "db", "queue", "http", "task", "ndjson"}) do
                    assert(type(ig[name]) == "table", name)
                    assert(igp[name] == nil, name)
                end
                return ig.json.decode(ig.json.encode({ok = true}))
            end }"#,
        );
        let e = Engine::new(
            Arc::new(CachedLoader::new(Shared(store), Duration::ZERO)),
            Limits::default(),
        );
        for _ in 0..2 {
            assert_eq!(
                e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap(),
                json!({"ok": true})
            );
        }
    }

    #[tokio::test]
    async fn config_vars_reach_script_and_allowlist() {
        let store = Arc::new(MemoryStore::default());
        store.put(
            &ScriptKey::new("acme", "s", None).unwrap(),
            r#"return { handle = function()
                local ok, err = pcall(ig.http.get, ig.config.api_base .. "ping")
                return { base = ig.config.api_base, region = ig.config.opts.region,
                         blocked = not ok and string.find(tostring(err), "allowlist") ~= nil }
            end }"#,
        );
        store.put(
            &ScriptKey::config("acme").unwrap(),
            r#"{"vars":{"api_base":"https://127.0.0.1.invalid/v1/","opts":{"region":"eu"}},
                "http":{"allow":["${api_base}"]}}"#,
        );
        let e = Engine::new(
            Arc::new(CachedLoader::new(Shared(store), Duration::ZERO)),
            Limits::default(),
        );
        let out = e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap();
        // the url matched the interpolated allowlist, so any failure is a network error, not a policy denial
        assert_eq!(
            out,
            json!({"base":"https://127.0.0.1.invalid/v1/","region":"eu","blocked":false})
        );
    }

    #[test]
    fn tenant_limits_are_clamped_to_ceiling() {
        let (defaults, ceiling) = (Limits::default(), Limits::ceiling());
        let t: TenantLimits =
            serde_json::from_str(r#"{"timeout_ms": 30000, "memory_mb": 99999, "max_data_ops": 5}"#)
                .unwrap();
        let l = defaults.resolve(&ceiling, &t);
        assert_eq!(l.timeout, Duration::from_secs(30)); // raised within ceiling
        assert_eq!(l.memory_bytes, ceiling.memory_bytes); // clamped
        assert_eq!(l.max_data_ops, 5); // lowered
        assert_eq!(l.max_http_requests, defaults.max_http_requests); // untouched
        assert!(serde_json::from_str::<TenantLimits>(r#"{"timeout_s": 1}"#).is_err());
    }

    #[tokio::test]
    async fn call_counters_come_from_tenant_config() {
        let store = Arc::new(MemoryStore::default());
        store.put(
            &ScriptKey::new("acme", "s", None).unwrap(),
            r#"return { handle = function()
                local n = 0
                for i = 1, 10 do if pcall(ig.store.Out.get, "k") then n += 1 end end
                return n
            end }"#,
        );
        store.put(
            &ScriptKey::config("acme").unwrap(),
            r#"{"stores":{"Out":"read"},"limits":{"max_data_ops":3}}"#,
        );
        let backend = Arc::new(MemBackend::default());
        let e = Engine::new(
            Arc::new(CachedLoader::new(Shared(store), Duration::ZERO)),
            Limits::default(),
        )
        .with_store("Out", backend, "data/");
        assert_eq!(
            e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap(),
            json!(3)
        );
    }

    /// The shipped, type-checked examples must also run on the real VM
    /// (annotations are stripped at compile time).
    #[tokio::test]
    async fn typed_example_scripts_run() {
        let config = include_str!("../../../examples/acme/config.json");
        let backend = Arc::new(MemBackend::default());
        backend
            .put(
                "data/acme/in.xml",
                b"<feed><entry/><entry/><entry/></feed>".to_vec(),
            )
            .unwrap();
        let e = engine_cfg(
            include_str!("../../../examples/acme/feed/latest.luau"),
            Limits::default(),
            config,
        )
        .with_store("In", backend, "data/")
        .with_store("Out", Arc::new(MemBackend::default()), "data/");
        let out = e
            .invoke(inv(Trigger::Queue, json!({"key": "in.xml"})))
            .await
            .unwrap();
        assert_eq!(out, json!({"entries": 3, "found": true}));

        let e = engine_cfg(
            include_str!("../../../examples/acme/hello/latest.luau"),
            Limits::default(),
            config,
        )
        .with_store("In", Arc::new(MemBackend::default()), "data/")
        .with_store("Out", Arc::new(MemBackend::default()), "data/");
        let out = e.invoke(inv(Trigger::Http, json!({"x": 1}))).await.unwrap();
        assert_eq!(
            out,
            json!({"trigger": "http", "count": 1, "event": {"x": 1}})
        );
    }

    fn routed(store: &Arc<MemoryStore>) -> Engine {
        Engine::new(
            Arc::new(CachedLoader::new(Shared(store.clone()), Duration::ZERO)),
            Limits::default(),
        )
    }

    fn put(store: &MemoryStore, script: &str, src: &str) {
        store.put(&ScriptKey::new("acme", script, None).unwrap(), src);
    }

    fn msg(event: serde_json::Value) -> Invocation {
        Invocation {
            tenant: "acme".into(),
            script: None,
            version: None,
            trigger: Trigger::Queue,
            event,
            attempt: 0,
            continues_trace: false,
        }
    }

    #[tokio::test]
    async fn router_picks_script_and_can_rewrite_event() {
        let store = Arc::new(MemoryStore::default());
        put(&store, "parse-order", "return { on_queue = function(ctx, ev) return { by = 'parse-order', key = ev.key, trigger = ctx.trigger } end }");
        put(
            &store,
            "audit",
            "return { on_queue = function(_, ev) return { by = 'audit', kind = ev.kind } end }",
        );
        put(
            &store,
            "router",
            r#"return { on_route = function(ctx, ev)
                if ctx.trigger ~= "route" then error("bad trigger") end
                if ev.key and string.sub(ev.key, 1, 16) == "incoming/orders/" then return "parse-order" end
                if ev.kind == "audit" then return "audit" end
                return { script = "audit", event = { kind = "rewritten" } }
            end }"#,
        );
        store.put(
            &ScriptKey::config("acme").unwrap(),
            r#"{"router":"router"}"#,
        );
        let e = routed(&store);
        // target sees the original trigger, not "route"
        let out = e
            .invoke(msg(json!({"key": "incoming/orders/1.xml"})))
            .await
            .unwrap();
        assert_eq!(
            out,
            json!({"by": "parse-order", "key": "incoming/orders/1.xml", "trigger": "queue"})
        );
        assert_eq!(
            e.invoke(msg(json!({"kind": "audit"}))).await.unwrap(),
            json!({"by": "audit", "kind": "audit"})
        );
        assert_eq!(
            e.invoke(msg(json!({"kind": "other"}))).await.unwrap(),
            json!({"by": "audit", "kind": "rewritten"})
        );
    }

    #[tokio::test]
    async fn router_and_target_share_one_budget() {
        let store = Arc::new(MemoryStore::default());
        put(
            &store,
            "router",
            r#"return { on_route = function() ig.store.Out.get("a"); ig.store.Out.get("b"); return "t" end }"#,
        );
        put(
            &store,
            "t",
            r#"return { on_queue = function()
                local ok = 0
                for _ = 1, 5 do if pcall(ig.store.Out.get, "k") then ok += 1 end end
                return ok
            end }"#,
        );
        store.put(
            &ScriptKey::config("acme").unwrap(),
            r#"{"router":"router","stores":{"Out":"read"},"limits":{"max_data_ops":3}}"#,
        );
        let e = routed(&store).with_store("Out", Arc::new(MemBackend::default()), "data/");
        // 3 allowed in total; the router used 2, so the target gets exactly 1
        assert_eq!(e.invoke(msg(json!({}))).await.unwrap(), json!(1));
    }

    #[tokio::test]
    async fn unroutable_message_is_an_error() {
        let store = Arc::new(MemoryStore::default());
        let e = routed(&store);
        assert!(matches!(
            e.invoke(msg(json!({"key": "x"}))).await,
            Err(Error::NoRoute)
        ));
    }

    fn gz(text: &str) -> Vec<u8> {
        use std::io::Write;
        let mut e = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        e.write_all(text.as_bytes()).unwrap();
        e.finish().unwrap()
    }

    fn sha(bytes: &[u8]) -> String {
        use sha2::Digest;
        sha2::Sha256::digest(bytes)
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    }

    const BATCH_CONFIG: &str = r#"{"router":"router","batch":true}"#;

    fn batch_world() -> (Engine, Arc<MemBackend>) {
        let store = Arc::new(MemoryStore::default());
        put(
            &store,
            "orders",
            r#"local igp = require("@native/patterns@1")
return { on_invoke = function(ctx, ev)
                local job = igp.batch.new({ id = "j1" })
                job:input_rows("seed", { { id = 1 }, { id = 2 } })
                local customers = job:query("customers", "customers_by_id", { ids = ev.ids })
                job:query("orders", "open_orders")
                job:state({ ref = ev.ref, customers = customers })
                job:continue_with("settle")
                return { id = job:submit(), customers = customers }
            end }"#,
        );
        put(
            &store,
            "router",
            r#"local igp = require("@native/patterns@1")
return { on_route = function(ctx, ev)
                local r = igp.batch.route(ev)
                if r then return r.script and { script = r.script, event = r.event } or "gone" end
                return "orders"
            end }"#,
        );
        put(
            &store,
            "gone",
            "return { on_queue = function() return { gone = true } end }",
        );
        put(
            &store,
            "settle",
            r#"local igp = require("@native/patterns@1")
return { on_queue = function(ctx, ev)
                local job = igp.batch.hydrate(ev)
                if not job then return { gone = true } end
                local ids, orders = {}, 0
                for row in job:rows("customers") do table.insert(ids, row.id) end
                for _ in job:rows("orders") do orders += 1 end
                return { job = job.id, state = job.state, ids = ids, orders = orders,
                         parts = job.results.customers.parts, delete = job:keys() }
            end }"#,
        );
        put(
            &store,
            "probe",
            r#"local igp = require("@native/patterns@1")
return { on_queue = function(ctx, ev)
                local ok, err = pcall(function()
                    local job = igp.batch.hydrate(ev, { allow_failure = ev.allow })
                    if not job.ok then return "failed: " .. job.error end
                    local n = 0
                    for _ in job:rows("customers") do n += 1 end
                    return n
                end)
                return { ok = ok, out = tostring(err) }
            end }"#,
        );
        store.put(&ScriptKey::config("acme").unwrap(), BATCH_CONFIG);
        let backend = Arc::new(MemBackend::default());
        let e = routed(&store).with_batch_bucket(backend.clone(), "data/");
        (e, backend)
    }

    fn partner_done(backend: &MemBackend, done: serde_json::Value) {
        backend
            .put(
                "data/acme/jobs/j1/done.json",
                serde_json::to_vec(&done).unwrap(),
            )
            .unwrap();
    }

    fn done_event() -> serde_json::Value {
        json!({"store": "$batch", "key": "jobs/j1/done.json"})
    }

    fn direct(script: &str, event: serde_json::Value) -> Invocation {
        Invocation {
            script: Some(script.into()),
            ..msg(event)
        }
    }

    #[tokio::test]
    async fn batch_job_round_trip_through_a_partner() {
        let (e, backend) = batch_world();
        let mut submit = msg(json!({"ref": 7, "ids": [1, 2, 3]}));
        submit.script = Some("orders".into());
        submit.trigger = Trigger::Invoke;
        let out = e.invoke(submit).await.unwrap();
        assert_eq!(out, json!({"id": "j1", "customers": "customers"}));

        // The partner sees the work but not our state; the state sits outside the job folder.
        let request: serde_json::Value = serde_json::from_slice(
            &backend
                .get("data/acme/jobs/j1/request.json")
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(request["queries"][0]["name"], "customers");
        assert_eq!(request["queries"][0]["params"], json!({"ids": [1, 2, 3]}));
        assert_eq!(request["queries"][1]["params"], json!({}));
        assert!(!request.to_string().contains("\"ref\""));
        assert!(backend.get("data/acme/state/j1.json").unwrap().is_some());

        // Not done yet: a hydrate on the (premature) event finds nothing.
        assert_eq!(
            e.invoke(direct("settle", done_event())).await.unwrap(),
            json!({"gone": true})
        );

        // The partner answers with several parts, mixed gzip and plain, and commits with done.json.
        let p0 = gz("{\"id\":1}\n{\"id\":2}\n");
        let p1 = b"\n{\"id\":3}\n".to_vec();
        let o0 = gz("{\"n\":1}\n");
        backend
            .put("data/acme/jobs/j1/customers.0000.ndjson.gz", p0.clone())
            .unwrap();
        backend
            .put("data/acme/jobs/j1/customers.0001.ndjson", p1.clone())
            .unwrap();
        backend
            .put("data/acme/jobs/j1/orders.0000.ndjson.gz", o0.clone())
            .unwrap();
        partner_done(
            &backend,
            json!({"ok": true, "results": {
                "customers": {"rows": 3, "parts": [
                    {"key": "customers.0000.ndjson.gz", "bytes": p0.len(), "sha256": sha(&p0)},
                    {"key": "customers.0001.ndjson", "bytes": p1.len()}]},
                "orders": {"parts": [{"key": "orders.0000.ndjson.gz"}]},
            }}),
        );

        // Through the router: the continuation named at submit time runs, with its state back.
        let out = e.invoke(msg(done_event())).await.unwrap();
        assert_eq!(out["state"], json!({"ref": 7, "customers": "customers"}));
        assert_eq!(out["ids"], json!([1, 2, 3]));
        assert_eq!(out["orders"], 1);
        assert_eq!(out["parts"], 2);

        // The handler's `delete = job:keys()` removed the whole job after it succeeded.
        assert!(backend.list("data/acme/jobs", 100).unwrap().is_empty());
        assert!(backend.get("data/acme/state/j1.json").unwrap().is_none());

        // A duplicate delivery finds the job gone and the router sends it to the fallback.
        assert_eq!(
            e.invoke(msg(done_event())).await.unwrap(),
            json!({"gone": true})
        );
        // A data-store object that merely looks like a done.json is not a batch event.
        let mut other = msg(json!({"store": "In", "key": "jobs/j1/done.json", "ids": []}));
        other.trigger = Trigger::Invoke;
        assert_eq!(e.invoke(other).await.unwrap()["id"], "j1");
    }

    async fn probe(e: &Engine, event: serde_json::Value) -> serde_json::Value {
        e.invoke(direct("probe", event)).await.unwrap()
    }

    async fn submitted() -> (Engine, Arc<MemBackend>) {
        let (e, backend) = batch_world();
        let mut submit = msg(json!({"ref": 1, "ids": []}));
        submit.script = Some("orders".into());
        submit.trigger = Trigger::Invoke;
        e.invoke(submit).await.unwrap();
        (e, backend)
    }

    #[tokio::test]
    async fn batch_failure_and_corruption_are_reported() {
        let (e, backend) = submitted().await;
        let part = gz("{\"id\":1}\n{\"id\":2}\n");
        backend.put("data/acme/jobs/j1/c.gz", part.clone()).unwrap();
        let manifest = |c: serde_json::Value| json!({"ok": true, "results": {"customers": c, "orders": {"parts": []}}});
        let text = |v: &serde_json::Value| v["out"].as_str().unwrap().to_string();

        // The partner says it failed.
        partner_done(&backend, json!({"ok": false, "error": "partner timeout"}));
        let r = probe(&e, done_event()).await;
        assert_eq!(r["ok"], false);
        assert!(text(&r).contains("batch j1 failed: partner timeout"));
        let mut ev = done_event();
        ev["allow"] = json!(true);
        assert!(text(&probe(&e, ev).await).contains("failed: partner timeout"));

        // Good manifest.
        partner_done(
            &backend,
            manifest(json!({"rows": 2, "parts": [
            {"key": "c.gz", "bytes": part.len(), "sha256": sha(&part)}]})),
        );
        assert_eq!(text(&probe(&e, done_event()).await), "2");

        // Wrong row count, size, checksum; a missing part; an escaping key; a missing result.
        let cases = [
            (
                manifest(json!({"rows": 5, "parts": [{"key": "c.gz"}]})),
                "has 2 rows, manifest says 5",
            ),
            (
                manifest(json!({"parts": [{"key": "c.gz", "bytes": 1}]})),
                "manifest says 1 bytes",
            ),
            (
                manifest(json!({"parts": [{"key": "c.gz", "sha256": sha(b"x")}]})),
                "sha256 mismatch",
            ),
            (
                manifest(json!({"parts": [{"key": "nope.gz"}]})),
                "part \"nope.gz\" is missing",
            ),
            (
                manifest(json!({"parts": [{"key": "../../state/j1.json"}]})),
                "invalid part key",
            ),
            (
                json!({"ok": true, "results": {"orders": {"parts": []}}}),
                "result \"customers\" is missing",
            ),
        ];
        for (done, want) in cases {
            partner_done(&backend, done);
            let r = probe(&e, done_event()).await;
            assert_eq!(r["ok"], false, "{want}");
            assert!(text(&r).contains(want), "{want}: got {}", text(&r));
        }

        // Garbage in a row names the line.
        backend
            .put("data/acme/jobs/j1/c.gz", gz("{\"id\":1}\nnot json\n"))
            .unwrap();
        partner_done(&backend, manifest(json!({"parts": [{"key": "c.gz"}]})));
        assert!(text(&probe(&e, done_event()).await).contains("line 2"));
    }

    #[tokio::test]
    async fn ndjson_rows_reads_strings_and_streams() {
        let backend = Arc::new(MemBackend::default());
        backend
            .put("data/acme/a.ndjson", b"{\"a\":1}\n\n[2]\n3".to_vec())
            .unwrap();
        let e = engine_cfg(
            r#"return { handle = function()
                local out = {}
                for v in ig.ndjson.rows(ig.store.In.open("a.ndjson")) do table.insert(out, v) end
                local n = 0
                for _ in ig.ndjson.rows('{"x":1}\n{"x":2}\n') do n += 1 end
                local ok = pcall(function() for _ in ig.ndjson.rows("{oops}\n") do end end)
                return { first = out[1].a, second = out[2][1], third = out[3], n = n, bad = ok }
            end }"#,
            Limits::default(),
            r#"{"stores":{"In":"read"}}"#,
        )
        .with_store("In", backend, "data/");
        assert_eq!(
            e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap(),
            json!({"first": 1, "second": 2, "third": 3, "n": 2, "bad": false})
        );
    }

    #[tokio::test]
    async fn batch_requests_can_carry_input_files() {
        let store = Arc::new(MemoryStore::default());
        put(
            &store,
            "feeder",
            r#"local igp = require("@native/patterns@1")
return { on_invoke = function(ctx, ev)
                local job = igp.batch.new({ id = "j2" })
                job:input_rows("ids", { { id = 1 }, { id = 2 }, { id = 3 } })
                job:input("blob", "raw bytes", { ext = ".bin" })
                job:input("blob", "second", { ext = ".bin" })
                job:query("lookup", "by_ids", { input = "ids" })
                job:submit()
                return true
            end }"#,
        );
        store.put(&ScriptKey::config("acme").unwrap(), r#"{"batch":true}"#);
        let backend2 = Arc::new(MemBackend::default());
        let e2 = routed(&store).with_batch_bucket(backend2.clone(), "data/");
        let mut inv = msg(json!(null));
        inv.script = Some("feeder".into());
        inv.trigger = Trigger::Invoke;
        assert_eq!(e2.invoke(inv).await.unwrap(), json!(true));

        let ids = backend2
            .get("data/acme/jobs/j2/inputs/ids.0000.ndjson")
            .unwrap()
            .unwrap();
        assert_eq!(ids, b"{\"id\":1}\n{\"id\":2}\n{\"id\":3}\n");
        let request: serde_json::Value = serde_json::from_slice(
            &backend2
                .get("data/acme/jobs/j2/request.json")
                .unwrap()
                .unwrap(),
        )
        .unwrap();
        assert_eq!(request["inputs"]["ids"]["rows"], 3);
        assert_eq!(
            request["inputs"]["ids"]["parts"][0]["key"],
            "inputs/ids.0000.ndjson"
        );
        assert_eq!(request["inputs"]["ids"]["parts"][0]["bytes"], ids.len());
        assert_eq!(request["inputs"]["ids"]["parts"][0]["sha256"], sha(&ids));
        assert_eq!(request["inputs"]["blob"]["rows"], serde_json::Value::Null);
        assert_eq!(
            request["inputs"]["blob"]["parts"].as_array().unwrap().len(),
            2
        );
        assert_eq!(
            backend2
                .get("data/acme/jobs/j2/inputs/blob.0001.bin")
                .unwrap()
                .unwrap(),
            b"second"
        );
    }

    #[tokio::test]
    async fn batch_needs_tenant_grant_and_deployment_bucket() {
        let store = Arc::new(MemoryStore::default());
        put(
            &store,
            "s",
            r#"local igp = require("@native/patterns@1")
return { on_invoke = function() return igp.batch.new({ id = "x" }):submit() end }"#,
        );
        let call = || {
            let mut i = msg(json!(null));
            i.script = Some("s".into());
            i.trigger = Trigger::Invoke;
            i
        };
        // No grant in the tenant config: the script gets an error, not the bucket.
        let backend = Arc::new(MemBackend::default());
        let e = routed(&store).with_batch_bucket(backend.clone(), "data/");
        assert!(e
            .invoke(call())
            .await
            .unwrap_err()
            .to_string()
            .contains("batch"));
        // Granted, but the deployment has no batch bucket: a configuration error.
        store.put(&ScriptKey::config("acme").unwrap(), r#"{"batch":true}"#);
        let e = routed(&store);
        assert!(matches!(e.invoke(call()).await, Err(Error::Config(_))));
        // A job needs a query.
        let e = routed(&store).with_batch_bucket(backend, "data/");
        assert!(e
            .invoke(call())
            .await
            .unwrap_err()
            .to_string()
            .contains("at least one query"));
    }

    /// A one-shot HTTP server that answers after `delay`; returns its port.
    fn slow_server(delay: Duration, body: &'static str) -> u16 {
        use std::io::{Read, Write};
        let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = l.local_addr().unwrap().port();
        std::thread::spawn(move || {
            let (mut c, _) = l.accept().unwrap();
            let mut buf = [0u8; 4096];
            let _ = c.read(&mut buf);
            std::thread::sleep(delay);
            let _ = write!(
                c,
                "HTTP/1.1 200 OK\r\ncontent-type: text/plain\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
        });
        port
    }

    fn http_batch_world(port: u16, background: bool) -> (Engine, Arc<MemBackend>) {
        let store = Arc::new(MemoryStore::default());
        put(
            &store,
            "start",
            r#"local igp = require("@native/patterns@1")
return { on_invoke = function(ctx, ev)
                local job = igp.batch.new({ id = "h1" })
                job:http("resp", { url = ev.url, headers = { accept = "text/plain" } })
                job:state({ ref = 9 })
                job:continue_with("after")
                return job:submit()
            end }"#,
        );
        put(
            &store,
            "after",
            r#"local igp = require("@native/patterns@1")
return { on_queue = function(ctx, ev)
                local job = igp.batch.hydrate(ev, { allow_failure = true })
                if not job.ok then return { failed = job.error } end
                local r = job.results.resp
                return { status = r.status, ctype = r.headers["content-type"],
                         body = job:body("resp"), state = job.state }
            end }"#,
        );
        store.put(
            &ScriptKey::config("acme").unwrap(),
            &format!(r#"{{"batch":true,"http":{{"allow":["http://localhost:{port}/"]}}}}"#),
        );
        let backend = Arc::new(MemBackend::default());
        let mut e = routed(&store).with_batch_bucket(backend.clone(), "data/");
        if background {
            e = e.with_background_http();
        }
        (e, backend)
    }

    #[tokio::test]
    async fn batch_http_returns_control_and_delivers_the_response() {
        let port = slow_server(Duration::from_millis(800), "slow hello");
        let (e, backend) = http_batch_world(port, true);
        let mut submit = msg(json!({"url": format!("http://localhost:{port}/x")}));
        submit.script = Some("start".into());
        submit.trigger = Trigger::Invoke;
        let started = std::time::Instant::now();
        assert_eq!(e.invoke(submit).await.unwrap(), json!("h1"));
        assert!(
            started.elapsed() < Duration::from_millis(700),
            "submit waited for the response"
        );
        // The request itself is never written; only the state is.
        assert!(backend
            .get("data/acme/jobs/h1/request.json")
            .unwrap()
            .is_none());
        assert!(backend.get("data/acme/state/h1.json").unwrap().is_some());

        let done = "data/acme/jobs/h1/done.json";
        for _ in 0..100 {
            if backend.get(done).unwrap().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(backend.get(done).unwrap().is_some(), "no done.json");
        let mut after = msg(json!({"store": "$batch", "key": "jobs/h1/done.json"}));
        after.script = Some("after".into());
        let out = e.invoke(after).await.unwrap();
        assert_eq!(
            out,
            json!({"status": 200, "ctype": "text/plain", "body": "slow hello", "state": {"ref": 9}})
        );
    }

    #[tokio::test]
    async fn batch_http_failure_and_lambda_refusal() {
        // Nothing listens on this port: the job completes with ok = false.
        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        let (e, backend) = http_batch_world(port, true);
        let mut submit = msg(json!({"url": format!("http://localhost:{port}/x")}));
        submit.script = Some("start".into());
        submit.trigger = Trigger::Invoke;
        e.invoke(submit).await.unwrap();
        let done = "data/acme/jobs/h1/done.json";
        for _ in 0..100 {
            if backend.get(done).unwrap().is_some() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        let mut after = msg(json!({"store": "$batch", "key": "jobs/h1/done.json"}));
        after.script = Some("after".into());
        let out = e.invoke(after).await.unwrap();
        assert!(
            out["failed"]
                .as_str()
                .unwrap()
                .contains("http request failed"),
            "{out}"
        );

        // Without the Fargate server's background executor, job:http is refused.
        let (e, _) = http_batch_world(port, false);
        let mut submit = msg(json!({"url": format!("http://localhost:{port}/x")}));
        submit.script = Some("start".into());
        submit.trigger = Trigger::Invoke;
        let err = e.invoke(submit).await.unwrap_err().to_string();
        assert!(
            err.contains("only available on the Fargate server"),
            "{err}"
        );
        // A URL outside the allowlist fails at submit, in the script.
        let (e, _) = http_batch_world(port, true);
        let mut submit = msg(json!({"url": "https://elsewhere.example/x"}));
        submit.script = Some("start".into());
        submit.trigger = Trigger::Invoke;
        let err = e.invoke(submit).await.unwrap_err().to_string();
        assert!(err.contains("allowlist"), "{err}");
    }

    #[tokio::test]
    async fn rejects_unknown_runtime() {
        let e = engine("--!runtime 99\nreturn {}", Limits::default());
        assert!(matches!(
            e.invoke(inv(Trigger::Invoke, json!(null))).await,
            Err(Error::Wrappers(_))
        ));
    }
}
