use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use integraal_loader::{CachedLoader, LoadError, ScriptKey};
use integraal_wrappers::{
    install, parse_runtime_version, Access, DataScope, DataScopes, DbScope, Throttle, HostContext, HostLimits, HttpPolicy, ItemBackend, ObjectBackend, ScriptName,
};
use mlua::{Function, Lua, LuaSerdeExt, Table, VmState};

use crate::{Error, Invocation, Trigger};

#[derive(Debug, Clone)]
pub struct Limits {
    pub timeout: Duration,
    pub memory_bytes: usize,
    pub max_http_response_bytes: u64,
    pub max_object_bytes: usize,
    pub max_stream_bytes: u64,
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
            max_http_requests: 100,
            max_data_ops: 1000,
            max_http_timeout: Duration::from_secs(10),
        }
    }
}

impl Limits {
    /// Operator ceiling: the most any tenant config may request.
    /// `MAX_TIMEOUT_MS` and `MAX_MEMORY_MB` override the corresponding values.
    pub fn ceiling_from_env() -> Self {
        let env = |k: &str| std::env::var(k).ok().and_then(|v| v.parse::<u64>().ok());
        let mut c = Self {
            timeout: Duration::from_secs(60),
            memory_bytes: (256 * MB) as usize,
            max_http_response_bytes: 100 * MB,
            max_object_bytes: (100 * MB) as usize,
            max_stream_bytes: 4096 * MB,
            max_http_requests: 1000,
            max_data_ops: 10_000,
            max_http_timeout: Duration::from_secs(30),
        };
        if let Some(ms) = env("MAX_TIMEOUT_MS") {
            c.timeout = Duration::from_millis(ms);
        }
        if let Some(mb) = env("MAX_MEMORY_MB") {
            c.memory_bytes = (mb * MB) as usize;
        }
        c
    }

    /// Tenant request over defaults, clamped to the ceiling.
    fn resolve(&self, ceiling: &Limits, t: &TenantLimits) -> Limits {
        let mb = |v: Option<u64>, d: u64| v.map(|v| v.max(1) * MB).unwrap_or(d);
        let ms = |v: Option<u64>, d: Duration| v.map(|v| Duration::from_millis(v.max(1))).unwrap_or(d);
        Limits {
            timeout: ms(t.timeout_ms, self.timeout).min(ceiling.timeout),
            memory_bytes: (mb(t.memory_mb, self.memory_bytes as u64) as usize).min(ceiling.memory_bytes),
            max_http_response_bytes: mb(t.max_http_response_mb, self.max_http_response_bytes).min(ceiling.max_http_response_bytes),
            max_object_bytes: (mb(t.max_object_mb, self.max_object_bytes as u64) as usize).min(ceiling.max_object_bytes),
            max_stream_bytes: mb(t.max_stream_mb, self.max_stream_bytes).min(ceiling.max_stream_bytes),
            max_http_requests: t.max_http_requests.unwrap_or(self.max_http_requests).min(ceiling.max_http_requests),
            max_data_ops: t.max_data_ops.unwrap_or(self.max_data_ops).min(ceiling.max_data_ops),
            max_http_timeout: ms(t.max_http_timeout_ms, self.max_http_timeout).min(ceiling.max_http_timeout),
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
    pub max_http_requests: Option<u32>,
    pub max_data_ops: Option<u32>,
    pub max_http_timeout_ms: Option<u64>,
}

/// Per-tenant policy, read from `<tenant>/config.json` in the script bucket.
/// Tenants cannot write there (scripts only reach the data bucket), so it is
/// the operator's lever over what a tenant may touch. Absent file = no access.
#[derive(Debug, Default, serde::Deserialize)]
pub struct TenantConfig {
    #[serde(default)]
    pub http: HttpPolicy,
    /// Arbitrary JSON available to scripts as `native.config`; string values can
    /// be interpolated into `http.allow` as `${name}`. Not a place for secrets.
    #[serde(default)]
    pub vars: serde_json::Map<String, serde_json::Value>,
    #[serde(default)]
    pub limits: TenantLimits,
    /// Buckets this tenant may use as `native.data.<name>`, and how: a grant or list of grants from
    /// `read`, `write` (put), `delete`, `readwrite` (all three), `upload` (presigned PUT) and `download` (presigned GET).
    /// Absent = no access. Names must match the deployment's `DATA_BUCKETS`.
    #[serde(default)]
    pub data: HashMap<String, Access>,
    /// Script whose `on_route`/`route` handler picks the script for messages that don't name one.
    #[serde(default)]
    pub router: Option<String>,
}

/// Parses `DATA_BUCKETS` (`name=bucket,name=bucket`) into `(name, bucket)` pairs.
pub(crate) fn parse_data_buckets(spec: &str) -> Result<Vec<(String, String)>, String> {
    spec.split(',')
        .map(str::trim)
        .filter(|p| !p.is_empty())
        .map(|pair| {
            let (name, bucket) = pair.split_once('=').ok_or_else(|| format!("DATA_BUCKETS entry {pair:?} is not name=bucket"))?;
            let ok = !name.is_empty() && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_');
            if !ok || bucket.is_empty() {
                return Err(format!("DATA_BUCKETS entry {pair:?} is invalid (names are letters, digits and _)"));
            }
            Ok((name.to_string(), bucket.to_string()))
        })
        .collect()
}

/// A fully resolved unit of work: which script runs, with which event.
struct Call {
    tenant: String,
    script: String,
    trigger: Trigger,
    event: serde_json::Value,
}

#[derive(Clone)]
pub struct Engine {
    loader: Arc<CachedLoader>,
    limits: Limits,
    ceiling: Limits,
    /// Named buckets with the base key prefix each tenant's directory sits under.
    data: HashMap<String, (Arc<dyn ObjectBackend>, String)>,
    db: Option<(Arc<dyn ItemBackend>, Arc<integraal_wrappers::CallbackSigner>)>,
    callback_base_url: Option<String>,
    throttle: Option<Arc<Throttle>>,
}

/// Everything besides limits and config that one invocation's native API is wired to.
#[derive(Clone)]
struct Services {
    data: DataScopes,
    db: Option<DbScope>,
    cb_url: Option<String>,
    throttle: Option<Arc<Throttle>>,
}

impl Engine {
    pub fn new(loader: Arc<CachedLoader>, limits: Limits) -> Self {
        Self { loader, limits, ceiling: Limits::ceiling_from_env(), data: HashMap::new(), db: None, callback_base_url: None, throttle: None }
    }

    /// Overrides the operator ceiling (default: `Limits::ceiling_from_env()`).
    pub fn with_ceiling(mut self, ceiling: Limits) -> Self {
        self.ceiling = ceiling;
        self
    }

    /// Registers a named bucket for `native.data.<name>`. Each tenant is confined to
    /// `<base_prefix><tenant>/` in it, and only reaches it if its config grants access.
    pub fn with_data(mut self, name: impl Into<String>, backend: Arc<dyn ObjectBackend>, base_prefix: impl Into<String>) -> Self {
        self.data.insert(name.into(), (backend, base_prefix.into()));
        self
    }

    /// Registers every bucket in `DATA_BUCKETS` (`name=bucket,name=bucket`) under `DATA_PREFIX`.
    /// Must be called from within a Tokio runtime. No `DATA_BUCKETS` means no buckets.
    pub async fn with_data_from_env(mut self) -> Result<Self, String> {
        let prefix = std::env::var("DATA_PREFIX").unwrap_or_default();
        for (name, bucket) in parse_data_buckets(&std::env::var("DATA_BUCKETS").unwrap_or_default())? {
            self = self.with_data(name, Arc::new(crate::S3Backend::from_env(bucket).await), prefix.clone());
        }
        Ok(self)
    }

    /// Enables `native.db`. Each tenant is confined to partition keys starting `TEN#<tenant>#`.
    pub fn with_db(mut self, backend: Arc<dyn ItemBackend>) -> Self {
        let signer = Arc::new(integraal_wrappers::CallbackSigner::new(backend.clone()));
        self.db = Some((backend, signer));
        self
    }

    /// Enables `native.callback` (requires `with_db`): callback URLs are `<base_url>/cb/<tenant>/<id>`.
    pub fn with_callback_base_url(mut self, base_url: impl Into<String>) -> Self {
        self.callback_base_url = Some(base_url.into());
        self
    }

    /// Limits outbound HTTP per URL prefix, across all tenants in this process (Fargate; not used on Lambda).
    pub fn with_throttle(mut self, throttle: Throttle) -> Self {
        self.throttle = Some(Arc::new(throttle));
        self
    }

    pub async fn invoke(&self, inv: Invocation) -> Result<serde_json::Value, Error> {
        self.invoke_within(inv, None).await
    }

    /// Like `invoke`, additionally capping the timeout (e.g. to the time a Lambda has left).
    pub async fn invoke_within(&self, inv: Invocation, max_time: Option<Duration>) -> Result<serde_json::Value, Error> {
        let config = match self.loader.load(&ScriptKey::config(&inv.tenant)?).await {
            Ok(c) => serde_json::from_str::<TenantConfig>(&c.source).map_err(|e| Error::Config(e.to_string()))?,
            Err(LoadError::NotFound(_)) => TenantConfig::default(),
            Err(e) => return Err(e.into()),
        };
        let config = Arc::new(config);
        let mut limits = self.limits.resolve(&self.ceiling, &config.limits);
        if let Some(max) = max_time {
            limits.timeout = limits.timeout.min(max);
        }
        let mut data = DataScopes::new();
        for (name, access) in &config.data {
            let (backend, base) = self
                .data
                .get(name)
                .ok_or_else(|| Error::Config(format!("data bucket {name:?} is not configured on this deployment")))?;
            let scope = DataScope::new(backend.clone(), format!("{base}{}/", inv.tenant))
                .with_max_object_bytes(limits.max_object_bytes)
                .with_access(*access);
            data.insert(name.clone(), scope);
        }
        let db = self.db.as_ref().map(|(b, s)| DbScope::new(b.clone(), inv.tenant.as_str()).with_signer(s.clone()));
        let svc = Services { data, db, cb_url: self.callback_base_url.clone(), throttle: self.throttle.clone() };

        // Luau states are not Send-safe to hold across awaits; run on a blocking thread.
        match inv.script.clone() {
            Some(script) => {
                let key = ScriptKey::new(&inv.tenant, &script, inv.version.as_deref())?;
                let source = self.loader.load(&key).await?;
                let call = Call { tenant: inv.tenant, script, trigger: inv.trigger, event: inv.event };
                tokio::task::spawn_blocking(move || run(&source.source, &call, &limits, &svc, &config)).await
            }
            None => {
                // No script named: the tenant's router picks one, and the chosen script runs in the same VM.
                let router = config.router.clone().ok_or(Error::NoRoute)?;
                let source = self.loader.load(&ScriptKey::new(&inv.tenant, &router, None)?).await?;
                let (loader, handle) = (self.loader.clone(), tokio::runtime::Handle::current());
                tokio::task::spawn_blocking(move || {
                    run_routed(&source.source, router, &inv, &limits, &svc, &config, &loader, &handle)
                })
                .await
            }
        }
        .map_err(|e| Error::Internal(e.to_string()))?
    }
}

/// A Luau state set up for one invocation: native API installed, sandboxed, limits armed.
struct Vm {
    lua: Lua,
    version: u32,
    deadline: Instant,
    script: ScriptName,
    data: DataScopes,
}

fn new_vm(version: u32, tenant: &str, script: &str, limits: &Limits, svc: &Services, config: &TenantConfig) -> Result<Vm, Error> {
    let http = config.http.expand(&config.vars).map_err(Error::Config)?;
    let lua = Lua::new();
    let deadline = Instant::now() + limits.timeout;
    lua.set_memory_limit(limits.memory_bytes).map_err(internal)?;
    let script = Arc::new(Mutex::new(script.to_string()));
    let vm_data = svc.data.clone();
    install(
        &lua,
        version,
        &HostContext {
            tenant: tenant.to_string(),
            script: script.clone(),
            data: svc.data.clone(),
            db: svc.db.clone(),
            callback_base_url: svc.cb_url.clone(),
            throttle: svc.throttle.clone(),
            http,
            vars: config.vars.clone(),
            limits: HostLimits {
                deadline,
                max_http_response_bytes: limits.max_http_response_bytes,
                max_object_bytes: limits.max_object_bytes,
                max_stream_bytes: limits.max_stream_bytes,
                max_http_requests: limits.max_http_requests,
                max_data_ops: limits.max_data_ops,
                max_http_timeout: limits.max_http_timeout,
            },
        },
    )?;
    lua.sandbox(true).map_err(internal)?;
    lua.set_interrupt(move |_| {
        if Instant::now() >= deadline {
            Err(mlua::Error::runtime("__timeout__"))
        } else {
            Ok(VmState::Continue)
        }
    });
    Ok(Vm { lua, version, deadline, script, data: vm_data })
}

/// Loads `source` in the VM and calls the handler matching `call.trigger`.
fn call_in_vm(vm: &Vm, source: &str, call: &Call) -> Result<serde_json::Value, Error> {
    let (lua, deadline) = (&vm.lua, vm.deadline);
    *vm.script.lock().unwrap() = call.script.clone();

    let chunk_name = format!("={}/{}", call.tenant, call.script);
    let module: Table = lua.load(source).set_name(chunk_name).eval().map_err(|e| script_err(e, deadline))?;

    let handler: Function = call
        .trigger
        .handlers()
        .iter()
        .find_map(|name| module.get::<Option<Function>>(*name).ok().flatten())
        .ok_or(Error::NoHandler(call.trigger, call.trigger.handlers()))?;

    let ctx = lua.create_table().map_err(internal)?;
    ctx.set("tenant", call.tenant.as_str()).map_err(internal)?;
    ctx.set("script", call.script.as_str()).map_err(internal)?;
    ctx.set("trigger", format!("{:?}", call.trigger).to_lowercase()).map_err(internal)?;
    ctx.set("runtime", vm.version).map_err(internal)?;

    let event = lua.to_value(&call.event).map_err(internal)?;
    let out: mlua::Value = handler.call((ctx, event)).map_err(|e| script_err(e, deadline))?;
    let result: serde_json::Value =
        lua.from_value(out).map_err(|e| Error::Script(format!("handler result is not JSON-serializable: {e}")))?;
    if call.trigger == Trigger::Queue {
        apply_deletes(&result, &call.event, &vm.data)?;
    }
    Ok(result)
}

/// Most objects one handler result may ask the engine to delete.
const MAX_RESULT_DELETES: usize = 100;

/// Queue handlers can return `delete = true` (the object named by the event's `bucket` and `key`) or
/// `delete = { { bucket = "In", key = "a.xml" }, ... }`. The engine deletes them only after the handler
/// returned without error, through the tenant's own grants (so it still needs the `delete` grant).
fn apply_deletes(result: &serde_json::Value, event: &serde_json::Value, data: &DataScopes) -> Result<(), Error> {
    use serde_json::Value;
    let bad = |m: &str| Error::Script(format!("invalid delete in handler result: {m}"));
    let str_of = |v: &Value, field: &str| v.get(field).and_then(Value::as_str).map(str::to_string);
    let targets: Vec<(String, String)> = match result.get("delete") {
        None | Some(Value::Null) | Some(Value::Bool(false)) => return Ok(()),
        Some(Value::Bool(true)) => match (str_of(event, "bucket"), str_of(event, "key")) {
            (Some(b), Some(k)) => vec![(b, k)],
            _ => return Err(bad("`true` needs an event with `bucket` and `key`")),
        },
        Some(Value::Array(items)) if items.len() <= MAX_RESULT_DELETES => items
            .iter()
            .map(|i| match (str_of(i, "bucket"), str_of(i, "key")) {
                (Some(b), Some(k)) => Ok((b, k)),
                _ => Err(bad("each entry needs `bucket` and `key`")),
            })
            .collect::<Result<_, _>>()?,
        Some(Value::Array(_)) => return Err(bad(&format!("more than {MAX_RESULT_DELETES} entries"))),
        Some(Value::Object(o)) if o.is_empty() => return Ok(()), // an empty Lua table
        Some(_) => return Err(bad("expected true or a list of { bucket, key }")),
    };
    for (bucket, key) in targets {
        let scope = data.get(&bucket).ok_or_else(|| Error::Script(format!("delete: bucket {bucket:?} is not available to this tenant")))?;
        scope.delete(&key).map_err(|e| Error::Script(format!("delete {bucket}/{key}: {e}")))?;
    }
    Ok(())
}

fn run(source: &str, call: &Call, limits: &Limits, svc: &Services, config: &TenantConfig) -> Result<serde_json::Value, Error> {
    let vm = new_vm(parse_runtime_version(source)?, &call.tenant, &call.script, limits, svc, config)?;
    call_in_vm(&vm, source, call)
}

/// Runs the router, then the script it picked, in one VM: one native-API install, one heap,
/// one deadline and one set of call budgets. If the target pins a different runtime version
/// (which fixes the `native` API at VM creation) it gets a fresh VM with the remaining time.
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
    let vm = new_vm(parse_runtime_version(router_src)?, &inv.tenant, &router, limits, svc, config)?;
    let route_call = Call { tenant: inv.tenant.clone(), script: router, trigger: Trigger::Route, event: inv.event.clone() };
    let decision = call_in_vm(&vm, router_src, &route_call)?;

    // The router returns a script name, or `{ script, version?, event? }` to also rewrite the event.
    let (script, version, event) = match decision {
        serde_json::Value::String(s) => (s, inv.version.clone(), inv.event.clone()),
        serde_json::Value::Object(mut o) => {
            let script = o.get("script").and_then(|s| s.as_str()).map(str::to_string).ok_or(Error::NoRoute)?;
            let version = o.get("version").and_then(|v| v.as_str()).map(str::to_string).or_else(|| inv.version.clone());
            (script, version, o.remove("event").unwrap_or_else(|| inv.event.clone()))
        }
        _ => return Err(Error::NoRoute),
    };

    let key = ScriptKey::new(&inv.tenant, &script, version.as_deref())?;
    let target = handle.block_on(loader.load(&key))?;
    let call = Call { tenant: inv.tenant.clone(), script, trigger: inv.trigger, event };

    if parse_runtime_version(&target.source)? == vm.version {
        call_in_vm(&vm, &target.source, &call)
    } else {
        let remaining = Limits { timeout: vm.deadline.saturating_duration_since(Instant::now()), ..limits.clone() };
        run(&target.source, &call, &remaining, svc, config)
    }
}

fn internal(e: mlua::Error) -> Error {
    Error::Internal(e.to_string())
}

fn script_err(e: mlua::Error, deadline: Instant) -> Error {
    if Instant::now() >= deadline || e.to_string().contains("__timeout__") {
        Error::Timeout
    } else if e.to_string().contains("not enough memory") {
        Error::Script("memory limit exceeded".into())
    } else {
        Error::Script(e.to_string())
    }
}

#[cfg(test)]
mod tests {
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
        Engine::new(Arc::new(CachedLoader::new(Shared(store), Duration::from_secs(60))), limits)
    }

    fn inv(trigger: Trigger, event: serde_json::Value) -> Invocation {
        Invocation { tenant: "acme".into(), script: Some("s".into()), version: None, trigger, event }
    }

    #[tokio::test]
    async fn dispatches_by_trigger_with_fallback() {
        let e = engine(
            r#"--!runtime 1
            return {
              on_queue = function(ctx, ev) return { via = "queue", n = ev.n } end,
              handle = function(ctx, ev) return { via = ctx.trigger, v = native.json.encode({a = 1}) } end,
            }"#,
            Limits::default(),
        );
        assert_eq!(e.invoke(inv(Trigger::Queue, json!({"n": 7}))).await.unwrap(), json!({"via":"queue","n":7}));
        assert_eq!(
            e.invoke(inv(Trigger::Http, json!({}))).await.unwrap(),
            json!({"via":"http","v":"{\"a\":1}"})
        );
    }

    #[tokio::test]
    async fn sandbox_blocks_os_and_enforces_timeout() {
        let e = engine("return { handle = function() return os.execute ~= nil end }", Limits::default());
        assert_eq!(e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap(), json!(false));

        let e = engine(
            "return { handle = function() while true do end end }",
            Limits { timeout: Duration::from_millis(100), ..Limits::default() },
        );
        assert!(matches!(e.invoke(inv(Trigger::Invoke, json!(null))).await, Err(Error::Timeout)));
    }

    #[derive(Default)]
    struct MemBackend(std::sync::Mutex<std::collections::BTreeMap<String, Vec<u8>>>);
    impl ObjectBackend for MemBackend {
        fn get(&self, k: &str) -> Result<Option<Vec<u8>>, String> {
            Ok(self.0.lock().unwrap().get(k).cloned())
        }
        fn put(&self, k: &str, b: Vec<u8>) -> Result<(), String> {
            self.0.lock().unwrap().insert(k.into(), b);
            Ok(())
        }
        fn open(&self, k: &str) -> Result<Option<Box<dyn std::io::Read + Send>>, String> {
            Ok(self.0.lock().unwrap().get(k).cloned().map(|v| Box::new(std::io::Cursor::new(v)) as Box<dyn std::io::Read + Send>))
        }
        fn delete(&self, k: &str) -> Result<(), String> {
            self.0.lock().unwrap().remove(k);
            Ok(())
        }
        fn list(&self, p: &str, limit: usize) -> Result<Vec<String>, String> {
            Ok(self.0.lock().unwrap().keys().filter(|k| k.starts_with(p)).take(limit).cloned().collect())
        }
        fn presign(&self, m: integraal_wrappers::PresignMethod, key: &str, ttl: Duration) -> Result<String, String> {
            Ok(format!("mem://{key}?{m:?}&ttl={}", ttl.as_secs()))
        }
    }

    #[tokio::test]
    async fn data_buckets_are_confined_and_access_controlled() {
        let (inbox, outbox) = (Arc::new(MemBackend::default()), Arc::new(MemBackend::default()));
        inbox.put("data/other/secret", b"x".to_vec()).unwrap();
        inbox.put("data/acme/drop.csv", b"a,b".to_vec()).unwrap();
        let e = engine_cfg(
            r#"return { handle = function()
                local out, inn = native.data.Out, native.data.In
                out.put("notes/a", "hello")
                return {
                    got = out.get("notes/a"), keys = out.list("notes/"), missing = out.get("nope") == nil,
                    escaped = pcall(out.get, "../other/secret"),
                    read_in = inn.get("drop.csv"), listed_in = inn.list(),
                    put_in = pcall(inn.put, "x", "y"), delete_in = pcall(inn.delete, "drop.csv"), gone = inn.get("drop.csv") == nil,
                    ungranted = pcall(function() return native.data.Backup.get("x") end),
                    put_url = inn.presign_put("up/new.csv", 60), get_url = out.presign_get("res.csv"),
                    get_url_in = pcall(inn.presign_get, "drop.csv"), put_url_out = pcall(out.presign_put, "res.csv"),
                }
            end }"#,
            Limits::default(),
            r#"{"data":{"In":["read","delete","upload"],"Out":["readwrite","download"]}}"#,
        )
        .with_data("In", inbox.clone(), "data/")
        .with_data("Out", outbox.clone(), "data/")
        .with_data("Backup", Arc::new(MemBackend::default()), "data/");
        assert_eq!(
            e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap(),
            json!({"got":"hello","keys":["notes/a"],"missing":true,"escaped":false,
                   "read_in":"a,b","listed_in":["drop.csv"],"put_in":false,"delete_in":true,"gone":true,"ungranted":false,
                   "put_url":"mem://data/acme/up/new.csv?Put&ttl=60","get_url":"mem://data/acme/res.csv?Get&ttl=900",
                   "get_url_in":false,"put_url_out":false})
        );
        assert!(outbox.get("data/acme/notes/a").unwrap().is_some());
        assert!(inbox.get("data/acme/drop.csv").unwrap().is_none(), "delete grant must delete");
        assert!(inbox.get("data/acme/x").unwrap().is_none(), "put without write grant must fail");
    }

    fn delete_engine(src: &str, grants: &str) -> (Engine, Arc<MemBackend>) {
        let backend = Arc::new(MemBackend::default());
        backend.put("data/acme/a.csv", b"1".to_vec()).unwrap();
        backend.put("data/acme/b.csv", b"2".to_vec()).unwrap();
        let cfg = format!(r#"{{"data":{{"In":{grants}}}}}"#);
        (engine_cfg(src, Limits::default(), &cfg).with_data("In", backend.clone(), "data/"), backend)
    }

    #[tokio::test]
    async fn queue_result_deletes_after_success_only() {
        let present = |b: &MemBackend, k: &str| b.get(&format!("data/acme/{k}")).unwrap().is_some();
        let ev = json!({"bucket": "In", "key": "a.csv"});

        // `delete = true` removes the event's own object, and nothing else
        let (e, b) = delete_engine("return { on_queue = function() return { ok = true, delete = true } end }", r#"["read","delete"]"#);
        e.invoke(inv(Trigger::Queue, ev.clone())).await.unwrap();
        assert!(!present(&b, "a.csv") && present(&b, "b.csv"));

        // an explicit list can name several objects
        let (e, b) = delete_engine(
            r#"return { on_queue = function() return { delete = { { bucket = "In", key = "a.csv" }, { bucket = "In", key = "b.csv" } } } end }"#,
            r#"["delete"]"#,
        );
        e.invoke(inv(Trigger::Queue, ev.clone())).await.unwrap();
        assert!(!present(&b, "a.csv") && !present(&b, "b.csv"));

        // a failing handler deletes nothing
        let (e, b) = delete_engine(r#"return { on_queue = function() error("boom") end }"#, r#"["delete"]"#);
        assert!(e.invoke(inv(Trigger::Queue, ev.clone())).await.is_err());
        assert!(present(&b, "a.csv"));

        // other triggers ignore `delete`
        let (e, b) = delete_engine("return { handle = function() return { delete = true } end }", r#"["delete"]"#);
        e.invoke(inv(Trigger::Http, ev.clone())).await.unwrap();
        assert!(present(&b, "a.csv"));
    }

    #[tokio::test]
    async fn result_delete_obeys_grants_and_shape() {
        let present = |b: &MemBackend| b.get("data/acme/a.csv").unwrap().is_some();
        let ev = json!({"bucket": "In", "key": "a.csv"});
        // no delete grant: the invocation fails and the object stays
        let (e, b) = delete_engine("return { on_queue = function() return { delete = true } end }", r#""read""#);
        assert!(matches!(e.invoke(inv(Trigger::Queue, ev.clone())).await, Err(Error::Script(_))));
        assert!(present(&b));
        // a bucket the tenant was not granted
        let (e, b) = delete_engine(r#"return { on_queue = function() return { delete = { { bucket = "Out", key = "a.csv" } } } end }"#, r#""delete""#);
        assert!(matches!(e.invoke(inv(Trigger::Queue, ev.clone())).await, Err(Error::Script(_))));
        assert!(present(&b));
        // `true` without bucket and key in the event, and malformed entries
        let (e, _) = delete_engine("return { on_queue = function() return { delete = true } end }", r#""delete""#);
        assert!(matches!(e.invoke(inv(Trigger::Queue, json!({}))).await, Err(Error::Script(_))));
        let (e, _) = delete_engine(r#"return { on_queue = function() return { delete = "a.csv" } end }"#, r#""delete""#);
        assert!(matches!(e.invoke(inv(Trigger::Queue, ev)).await, Err(Error::Script(_))));
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
    async fn granting_an_unconfigured_bucket_fails_the_invocation() {
        let e = engine_cfg("return { handle = function() return 1 end }", Limits::default(), r#"{"data":{"nope":"read"}}"#);
        assert!(matches!(e.invoke(inv(Trigger::Invoke, json!(null))).await, Err(Error::Config(_))));
    }

    #[derive(Default)]
    struct MemItems(std::sync::Mutex<std::collections::BTreeMap<(String, String), integraal_wrappers::Item>>);
    impl ItemBackend for MemItems {
        fn get(&self, pk: &str, sk: &str) -> Result<Option<integraal_wrappers::Item>, String> {
            Ok(self.0.lock().unwrap().get(&(pk.into(), sk.into())).cloned())
        }
        fn put(&self, pk: &str, item: integraal_wrappers::Item) -> Result<(), String> {
            self.0.lock().unwrap().insert((pk.into(), item.sk.clone()), item);
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
        fn query(&self, pk: &str, p: &str, limit: usize) -> Result<Vec<integraal_wrappers::Item>, String> {
            let m = self.0.lock().unwrap();
            Ok(m.iter().filter(|((k, sk), _)| k == pk && sk.starts_with(p)).take(limit).map(|(_, i)| i.clone()).collect())
        }
    }

    #[tokio::test]
    async fn db_is_confined_to_tenant_and_expires_in_24h() {
        let backend = Arc::new(MemItems::default());
        let now = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_secs();
        // another tenant's row, and an already-expired row of ours
        backend.put("TEN#other#cart", integraal_wrappers::Item { sk: "".into(), value: "\"secret\"".into(), ttl: now + 1000 }).unwrap();
        backend.put("TEN#acme#cart", integraal_wrappers::Item { sk: "old".into(), value: "1".into(), ttl: now - 1 }).unwrap();
        let e = engine(
            r#"return { handle = function()
                native.db.put("cart", "a", { n = 1 })
                native.db.put("cart", "b", "x")
                native.db.put("solo", nil, true)
                return { a = native.db.get("cart", "a"), solo = native.db.get("solo"), miss = native.db.get("cart", "zz") == nil,
                         expired = native.db.get("cart", "old") == nil, other = native.db.get("other") == nil,
                         rows = native.db.query("cart", { sk_prefix = "a" }), all = #native.db.query("cart"),
                         bad = pcall(native.db.get, "") }
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
        assert!((now + 86_400..=now + 86_405).contains(&ttl));
    }

    #[tokio::test]
    async fn callbacks_are_signed_single_use_and_tenant_scoped() {
        use integraal_wrappers::Take;
        let backend = Arc::new(MemItems::default());
        let e = engine(
            r#"return { handle = function()
                local ok = pcall(native.callback.create, { script = "../x" })
                return { url = native.callback.create({ script = "on-pay", state = { order = 7 }, ttl_s = 60 }), bad_script = ok }
            end }"#,
            Limits::default(),
        )
        .with_db(backend.clone())
        .with_callback_base_url("https://hooks.example.com/");
        let out = e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap();
        assert_eq!(out["bad_script"], json!(false));
        let token = out["url"].as_str().unwrap().strip_prefix("https://hooks.example.com/cb/acme/").expect("url shape").to_string();
        assert_eq!(token.len(), 80);

        // the verifier (a different process, here a fresh scope) reads the same stored key
        let acme = DbScope::new(backend.clone(), "acme");
        let other = DbScope::new(backend.clone(), "other");
        assert!(matches!(other.take_callback(&token).unwrap(), Take::Invalid), "wrong tenant");
        let mut forged = token.clone().into_bytes();
        forged[0] = if forged[0] == b'0' { b'1' } else { b'0' }; // different id, same tag
        assert!(matches!(acme.take_callback(&String::from_utf8(forged).unwrap()).unwrap(), Take::Invalid));
        let mut bad_exp = token.clone();
        bad_exp.replace_range(32..48, "ffffffffffffffff");
        assert!(matches!(acme.take_callback(&bad_exp).unwrap(), Take::Invalid), "expiry is signed");
        assert!(matches!(acme.take_callback("nope").unwrap(), Take::Invalid));

        let Take::Found(taken) = acme.take_callback(&token).unwrap() else { panic!("first take must succeed") };
        assert_eq!(taken.callback.script.as_deref(), Some("on-pay"));
        assert_eq!(taken.callback.state, json!({"order": 7}));
        assert!(matches!(acme.take_callback(&token).unwrap(), Take::Gone), "second take must fail");

        // a failed delivery can put it back with its original expiry, and it is single-use again
        acme.restore_callback(taken).unwrap();
        assert!(matches!(acme.take_callback(&token).unwrap(), Take::Found(_)));
        assert!(matches!(acme.take_callback(&token).unwrap(), Take::Gone));

        // the signing key exists once, outside every tenant partition
        assert_eq!(backend.0.lock().unwrap().keys().filter(|(pk, _)| pk.starts_with("SYS#")).count(), 1);
    }

    #[tokio::test]
    async fn expired_callback_is_rejected_without_a_lookup() {
        use integraal_wrappers::Take;
        let backend = Arc::new(MemItems::default());
        let scope = DbScope::new(backend.clone(), "acme");
        let token = scope.create_callback(None, json!(null), Duration::from_secs(5)).unwrap();
        assert!(matches!(scope.take_callback(&token).unwrap(), Take::Found(_)));
        let token = scope.create_callback(None, json!(null), Duration::from_secs(5)).unwrap();
        tokio::time::sleep(Duration::from_millis(6100)).await;
        backend.0.lock().unwrap().clear(); // even the rows and key are gone: only the signed expiry can answer
        assert!(matches!(scope.take_callback(&token).unwrap(), Take::Expired));
    }

    #[tokio::test]
    async fn callback_requires_configuration() {
        let e = engine(r#"return { handle = function() return (pcall(native.callback.create)) end }"#, Limits::default())
            .with_db(Arc::new(MemItems::default()));
        assert_eq!(e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap(), json!(false));
    }

    #[tokio::test]
    async fn http_requires_allowlist_from_tenant_config() {
        let store = Arc::new(MemoryStore::default());
        let script = r#"return { handle = function()
            local ok, err = pcall(native.http.get, "https://evil.example.org/x")
            return { ok = ok, err = tostring(err) }
        end }"#;
        store.put(&ScriptKey::new("acme", "s", None).unwrap(), script);
        let loader = Arc::new(CachedLoader::new(Shared(store.clone()), Duration::ZERO));
        let e = Engine::new(loader, Limits::default());
        let denied = e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap();
        assert_eq!(denied["ok"], json!(false));
        assert!(denied["err"].as_str().unwrap().contains("allowlist"));

        store.put(&ScriptKey::config("acme").unwrap(), r#"{"http":{"allow":["https://evil.example.org/"]}}"#);
        let allowed = e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap();
        assert!(!allowed["err"].as_str().unwrap().contains("allowlist"));
    }

    #[tokio::test]
    async fn throttled_http_gives_up_at_the_deadline() {
        let script = r#"return { handle = function()
            local errs = {}
            for i = 1, 2 do
                local ok, err = pcall(native.http.get, "https://api.partner.invalid/v1/x")
                errs[i] = tostring(err)
            end
            return errs
        end }"#;
        let config = r#"{"http":{"allow":["https://api.partner.invalid/"]}}"#;
        // one start per 100 s: the first call goes out, the second could only start after the deadline
        let rules = Throttle::from_json(r#"[{"url":"https://api.partner.invalid/v1/","per_second":0.01}]"#).unwrap();
        let e = engine_cfg(script, Limits::default(), config).with_throttle(rules);
        let out = e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap();
        assert!(!out[0].as_str().unwrap().contains("throttled"), "{out}");
        assert!(out[1].as_str().unwrap().contains("throttled"), "{out}");

        // without a throttle, neither call is held back
        let out = engine_cfg(script, Limits::default(), config).invoke(inv(Trigger::Invoke, json!(null))).await.unwrap();
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
                for ev in native.xml.events(native.data.In.open("big.xml")) do
                    if ev.type == "start" and ev.tag == "entry" then n += 1; last = ev.attrs.id end
                    if ev.type == "text" then text = ev.text end
                end
                local depths = {}
                for ev in native.xml.events("<a><b/></a>") do table.insert(depths, ev.type .. ev.depth) end
                local dtd = pcall(function() for _ in native.xml.events("<!DOCTYPE a []><a/>") do end end)
                return { n = n, last = last, text = text, depths = depths, dtd = dtd }
            end }"#,
            Limits { timeout: Duration::from_secs(60), ..Limits::default() },
            r#"{"data":{"In":"read"}}"#,
        )
        .with_data("In", backend, "data/");
        assert_eq!(
            e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap(),
            json!({"n":200000,"last":"199999","text":"x & y","depths":["start0","start1","end1","end0"],"dtd":false})
        );
    }

    #[tokio::test]
    async fn config_vars_reach_script_and_allowlist() {
        let store = Arc::new(MemoryStore::default());
        store.put(
            &ScriptKey::new("acme", "s", None).unwrap(),
            r#"return { handle = function()
                local ok, err = pcall(native.http.get, native.config.api_base .. "ping")
                return { base = native.config.api_base, region = native.config.opts.region,
                         blocked = not ok and string.find(tostring(err), "allowlist") ~= nil }
            end }"#,
        );
        store.put(
            &ScriptKey::config("acme").unwrap(),
            r#"{"vars":{"api_base":"https://127.0.0.1.invalid/v1/","opts":{"region":"eu"}},
                "http":{"allow":["${api_base}"]}}"#,
        );
        let e = Engine::new(Arc::new(CachedLoader::new(Shared(store), Duration::ZERO)), Limits::default());
        let out = e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap();
        // the url matched the interpolated allowlist, so any failure is a network error, not a policy denial
        assert_eq!(out, json!({"base":"https://127.0.0.1.invalid/v1/","region":"eu","blocked":false}));
    }

    #[test]
    fn tenant_limits_are_clamped_to_ceiling() {
        let (defaults, ceiling) = (Limits::default(), Limits::ceiling_from_env());
        let t: TenantLimits = serde_json::from_str(r#"{"timeout_ms": 30000, "memory_mb": 99999, "max_data_ops": 5}"#).unwrap();
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
                for i = 1, 10 do if pcall(native.data.Out.get, "k") then n += 1 end end
                return n
            end }"#,
        );
        store.put(&ScriptKey::config("acme").unwrap(), r#"{"data":{"Out":"read"},"limits":{"max_data_ops":3}}"#);
        let backend = Arc::new(MemBackend::default());
        let e = Engine::new(Arc::new(CachedLoader::new(Shared(store), Duration::ZERO)), Limits::default())
            .with_data("Out", backend, "data/");
        assert_eq!(e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap(), json!(3));
    }

    /// The shipped, type-checked examples must also run on the real VM
    /// (annotations are stripped at compile time).
    #[tokio::test]
    async fn typed_example_scripts_run() {
        let config = include_str!("../../../examples/acme/config.json");
        let backend = Arc::new(MemBackend::default());
        backend.put("data/acme/in.xml", b"<feed><entry/><entry/><entry/></feed>".to_vec()).unwrap();
        let e = engine_cfg(include_str!("../../../examples/acme/feed/latest.luau"), Limits::default(), config)
            .with_data("In", backend, "data/")
            .with_data("Out", Arc::new(MemBackend::default()), "data/");
        let out = e.invoke(inv(Trigger::Queue, json!({"key": "in.xml"}))).await.unwrap();
        assert_eq!(out, json!({"entries": 3, "found": true}));

        let e = engine_cfg(include_str!("../../../examples/acme/hello/latest.luau"), Limits::default(), config)
            .with_data("In", Arc::new(MemBackend::default()), "data/")
            .with_data("Out", Arc::new(MemBackend::default()), "data/");
        let out = e.invoke(inv(Trigger::Http, json!({"x": 1}))).await.unwrap();
        assert_eq!(out, json!({"trigger": "http", "count": 1, "event": {"x": 1}}));
    }

    fn routed(store: &Arc<MemoryStore>) -> Engine {
        Engine::new(Arc::new(CachedLoader::new(Shared(store.clone()), Duration::ZERO)), Limits::default())
    }

    fn put(store: &MemoryStore, script: &str, src: &str) {
        store.put(&ScriptKey::new("acme", script, None).unwrap(), src);
    }

    fn msg(event: serde_json::Value) -> Invocation {
        Invocation { tenant: "acme".into(), script: None, version: None, trigger: Trigger::Queue, event }
    }

    #[tokio::test]
    async fn router_picks_script_and_can_rewrite_event() {
        let store = Arc::new(MemoryStore::default());
        put(&store, "parse-order", "return { on_queue = function(ctx, ev) return { by = 'parse-order', key = ev.key, trigger = ctx.trigger } end }");
        put(&store, "audit", "return { on_queue = function(_, ev) return { by = 'audit', kind = ev.kind } end }");
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
        store.put(&ScriptKey::config("acme").unwrap(), r#"{"router":"router"}"#);
        let e = routed(&store);
        // target sees the original trigger, not "route"
        let out = e.invoke(msg(json!({"key": "incoming/orders/1.xml"}))).await.unwrap();
        assert_eq!(out, json!({"by": "parse-order", "key": "incoming/orders/1.xml", "trigger": "queue"}));
        assert_eq!(e.invoke(msg(json!({"kind": "audit"}))).await.unwrap(), json!({"by": "audit", "kind": "audit"}));
        assert_eq!(e.invoke(msg(json!({"kind": "other"}))).await.unwrap(), json!({"by": "audit", "kind": "rewritten"}));
    }

    #[tokio::test]
    async fn router_and_target_share_one_budget() {
        let store = Arc::new(MemoryStore::default());
        put(&store, "router", r#"return { on_route = function() native.data.Out.get("a"); native.data.Out.get("b"); return "t" end }"#);
        put(
            &store,
            "t",
            r#"return { on_queue = function()
                local ok = 0
                for _ = 1, 5 do if pcall(native.data.Out.get, "k") then ok += 1 end end
                return ok
            end }"#,
        );
        store.put(&ScriptKey::config("acme").unwrap(), r#"{"router":"router","data":{"Out":"read"},"limits":{"max_data_ops":3}}"#);
        let e = routed(&store).with_data("Out", Arc::new(MemBackend::default()), "data/");
        // 3 allowed in total; the router used 2, so the target gets exactly 1
        assert_eq!(e.invoke(msg(json!({}))).await.unwrap(), json!(1));
    }

    #[tokio::test]
    async fn unroutable_message_is_an_error() {
        let store = Arc::new(MemoryStore::default());
        let e = routed(&store);
        assert!(matches!(e.invoke(msg(json!({"key": "x"}))).await, Err(Error::NoRoute)));
    }

    #[tokio::test]
    async fn rejects_unknown_runtime() {
        let e = engine("--!runtime 99\nreturn {}", Limits::default());
        assert!(matches!(e.invoke(inv(Trigger::Invoke, json!(null))).await, Err(Error::Wrappers(_))));
    }
}
