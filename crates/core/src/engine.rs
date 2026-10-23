use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use integraal_loader::{CachedLoader, LoadError, ScriptKey};
use integraal_wrappers::{install, parse_runtime_version, DataScope, HostContext, HostLimits, HttpPolicy, ObjectBackend};
use mlua::{Function, Lua, LuaSerdeExt, Table, VmState};

use crate::{Error, Invocation};

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
}

#[derive(Clone)]
pub struct Engine {
    loader: Arc<CachedLoader>,
    limits: Limits,
    ceiling: Limits,
    data: Option<(Arc<dyn ObjectBackend>, String)>,
}

impl Engine {
    pub fn new(loader: Arc<CachedLoader>, limits: Limits) -> Self {
        Self { loader, limits, ceiling: Limits::ceiling_from_env(), data: None }
    }

    /// Overrides the operator ceiling (default: `Limits::ceiling_from_env()`).
    pub fn with_ceiling(mut self, ceiling: Limits) -> Self {
        self.ceiling = ceiling;
        self
    }

    /// Enables `native.data`. Each tenant is confined to `<base_prefix><tenant>/`.
    pub fn with_data(mut self, backend: Arc<dyn ObjectBackend>, base_prefix: impl Into<String>) -> Self {
        self.data = Some((backend, base_prefix.into()));
        self
    }

    pub async fn invoke(&self, inv: Invocation) -> Result<serde_json::Value, Error> {
        self.invoke_within(inv, None).await
    }

    /// Like `invoke`, additionally capping the timeout (e.g. to the time a Lambda has left).
    pub async fn invoke_within(&self, inv: Invocation, max_time: Option<Duration>) -> Result<serde_json::Value, Error> {
        let key = ScriptKey::new(&inv.tenant, &inv.script, inv.version.as_deref())?;
        let script = self.loader.load(&key).await?;
        let config = match self.loader.load(&ScriptKey::config(&inv.tenant)?).await {
            Ok(c) => serde_json::from_str(&c.source).map_err(|e| Error::Config(e.to_string()))?,
            Err(LoadError::NotFound(_)) => TenantConfig::default(),
            Err(e) => return Err(e.into()),
        };
        let mut limits = self.limits.resolve(&self.ceiling, &config.limits);
        if let Some(max) = max_time {
            limits.timeout = limits.timeout.min(max);
        }
        let data = self
            .data
            .as_ref()
            .map(|(b, base)| DataScope::new(b.clone(), format!("{base}{}/", inv.tenant)).with_max_object_bytes(limits.max_object_bytes));
        // Luau states are not Send-safe to hold across awaits; run on a blocking thread.
        tokio::task::spawn_blocking(move || run(&script.source, &inv, &limits, data, config))
            .await
            .map_err(|e| Error::Internal(e.to_string()))?
    }
}

fn run(source: &str, inv: &Invocation, limits: &Limits, data: Option<DataScope>, config: TenantConfig) -> Result<serde_json::Value, Error> {
    let version = parse_runtime_version(source)?;
    let http = config.http.expand(&config.vars).map_err(Error::Config)?;

    let lua = Lua::new();
    let deadline = Instant::now() + limits.timeout;
    lua.set_memory_limit(limits.memory_bytes).map_err(internal)?;
    install(&lua, version, &HostContext { tenant: inv.tenant.clone(), script: inv.script.clone(), data, http, vars: config.vars,
        limits: HostLimits {
            deadline: Instant::now() + limits.timeout,
            max_http_response_bytes: limits.max_http_response_bytes,
            max_object_bytes: limits.max_object_bytes,
            max_stream_bytes: limits.max_stream_bytes,
            max_http_requests: limits.max_http_requests,
            max_data_ops: limits.max_data_ops,
            max_http_timeout: limits.max_http_timeout,
        } })?;
    lua.sandbox(true).map_err(internal)?;

    lua.set_interrupt(move |_| {
        if Instant::now() >= deadline {
            Err(mlua::Error::runtime("__timeout__"))
        } else {
            Ok(VmState::Continue)
        }
    });

    let chunk_name = format!("={}/{}", inv.tenant, inv.script);
    let module: Table = lua.load(source).set_name(chunk_name).eval().map_err(|e| script_err(e, deadline))?;

    let handler: Function = inv
        .trigger
        .handlers()
        .iter()
        .find_map(|name| module.get::<Option<Function>>(*name).ok().flatten())
        .ok_or(Error::NoHandler(inv.trigger, inv.trigger.handlers()))?;

    let ctx = lua.create_table().map_err(internal)?;
    ctx.set("tenant", inv.tenant.as_str()).map_err(internal)?;
    ctx.set("script", inv.script.as_str()).map_err(internal)?;
    ctx.set("trigger", format!("{:?}", inv.trigger).to_lowercase()).map_err(internal)?;
    ctx.set("runtime", version).map_err(internal)?;

    let event = lua.to_value(&inv.event).map_err(internal)?;
    let out: mlua::Value = handler.call((ctx, event)).map_err(|e| script_err(e, deadline))?;
    lua.from_value(out).map_err(|e| Error::Script(format!("handler result is not JSON-serializable: {e}")))
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

    fn inv(trigger: Trigger, event: serde_json::Value) -> Invocation {
        Invocation { tenant: "acme".into(), script: "s".into(), version: None, trigger, event }
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
    }

    #[tokio::test]
    async fn data_is_confined_to_tenant_prefix() {
        let backend = Arc::new(MemBackend::default());
        backend.put("data/other/secret", b"x".to_vec()).unwrap();
        let e = engine(
            r#"return { handle = function()
                native.data.put("notes/a", "hello")
                local escaped = pcall(native.data.get, "../other/secret")
                return { got = native.data.get("notes/a"), keys = native.data.list("notes/"),
                         missing = native.data.get("nope") == nil, escaped = escaped }
            end }"#,
            Limits::default(),
        )
        .with_data(backend.clone(), "data/");
        assert_eq!(
            e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap(),
            json!({"got":"hello","keys":["notes/a"],"missing":true,"escaped":false})
        );
        assert!(backend.get("data/acme/notes/a").unwrap().is_some());
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
        let e = engine(
            r#"return { handle = function()
                local n, last, text = 0, nil, nil
                for ev in native.xml.events(native.data.open("big.xml")) do
                    if ev.type == "start" and ev.tag == "entry" then n += 1; last = ev.attrs.id end
                    if ev.type == "text" then text = ev.text end
                end
                local depths = {}
                for ev in native.xml.events("<a><b/></a>") do table.insert(depths, ev.type .. ev.depth) end
                local dtd = pcall(function() for _ in native.xml.events("<!DOCTYPE a []><a/>") do end end)
                return { n = n, last = last, text = text, depths = depths, dtd = dtd }
            end }"#,
            Limits { timeout: Duration::from_secs(60), ..Limits::default() },
        )
        .with_data(backend, "data/");
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
                for i = 1, 10 do if pcall(native.data.get, "k") then n += 1 end end
                return n
            end }"#,
        );
        store.put(&ScriptKey::config("acme").unwrap(), r#"{"limits":{"max_data_ops":3}}"#);
        let backend = Arc::new(MemBackend::default());
        let e = Engine::new(Arc::new(CachedLoader::new(Shared(store), Duration::ZERO)), Limits::default())
            .with_data(backend, "data/");
        assert_eq!(e.invoke(inv(Trigger::Invoke, json!(null))).await.unwrap(), json!(3));
    }

    /// The shipped, type-checked examples must also run on the real VM
    /// (annotations are stripped at compile time).
    #[tokio::test]
    async fn typed_example_scripts_run() {
        let backend = Arc::new(MemBackend::default());
        backend.put("data/acme/in.xml", b"<feed><entry/><entry/><entry/></feed>".to_vec()).unwrap();
        let e = engine(include_str!("../../../examples/acme/feed/latest.luau"), Limits::default()).with_data(backend, "data/");
        let out = e.invoke(inv(Trigger::Queue, json!({"key": "in.xml"}))).await.unwrap();
        assert_eq!(out, json!({"entries": 3, "found": true}));

        let e = engine(include_str!("../../../examples/acme/hello/latest.luau"), Limits::default())
            .with_data(Arc::new(MemBackend::default()), "data/");
        let out = e.invoke(inv(Trigger::Http, json!({"x": 1}))).await.unwrap();
        assert_eq!(out, json!({"trigger": "http", "count": 1, "event": {"x": 1}}));
    }

    #[tokio::test]
    async fn rejects_unknown_runtime() {
        let e = engine("--!runtime 99\nreturn {}", Limits::default());
        assert!(matches!(e.invoke(inv(Trigger::Invoke, json!(null))).await, Err(Error::Wrappers(_))));
    }
}
