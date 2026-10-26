mod peer;

use integraal_core::{DynamoBackend, GlobalConfig, S3Backend, SqsBackend, TenantConfig};
use integraal_wrappers::{DbScope, Deadline, HostContext, HostLimits, StoreScope};
use mlua::{Function, Lua};
use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

fn source(root: &Path, relative: &str) -> mlua::Result<(String, String)> {
    let path = root
        .join(relative)
        .canonicalize()
        .map_err(mlua::Error::external)?;
    if !path.starts_with(root) {
        return Err(mlua::Error::runtime("script path escapes root"));
    }
    let text = std::fs::read_to_string(&path).map_err(mlua::Error::external)?;
    let version =
        integraal_wrappers::parse_runtime_version(&text).map_err(mlua::Error::external)?;
    if version != 1 {
        return Err(mlua::Error::runtime(format!(
            "unsupported runtime version {version}"
        )));
    }
    Ok((text, format!("@{}", path.display())))
}

fn vm(root: &Path, host: &HostContext, suite: &Path) -> mlua::Result<Lua> {
    let lua = Lua::new();
    lua.set_memory_limit(64 * 1024 * 1024)?;
    let end = Instant::now() + Duration::from_secs(10);
    lua.set_interrupt(move |_| {
        if Instant::now() > end {
            Err(mlua::Error::runtime("test timeout"))
        } else {
            Ok(mlua::VmState::Continue)
        }
    });
    let peer_scope = host.batch.clone();
    let host = host.clone();
    lua.globals().set(
        "__install_native",
        lua.create_function(move |lua, ()| {
            let mut host = host.clone();
            host.limits.deadline = Deadline::new(Instant::now() + Duration::from_secs(10));
            integraal_wrappers::install(lua, 1, &host).map_err(mlua::Error::external)
        })?,
    )?;
    let script_root = root.to_owned();
    lua.globals().set(
        "__load_script",
        lua.create_function(move |lua, path: String| {
            let (text, name) = source(&script_root, &path)?;
            lua.load(text).set_name(name).into_function()
        })?,
    )?;
    let module_root = root.to_owned();
    lua.globals().set(
        "__load_module",
        lua.create_function(move |lua, spec: String| {
            let relative = if let Some(path) = spec.strip_prefix("@lib/") {
                format!("lib/{path}.luau")
            } else if let Some(path) = spec.strip_prefix("@shared/") {
                let (name, version) = path
                    .rsplit_once('@')
                    .ok_or_else(|| mlua::Error::runtime("shared module needs a version"))?;
                format!("@shared/{name}/{version}.luau")
            } else {
                return Err(mlua::Error::runtime("expected @lib/ or @shared/ module"));
            };
            let (text, name) = source(&module_root, &relative)?;
            lua.load(text).set_name(name).into_function()
        })?,
    )?;
    lua.sandbox(true)?;
    lua.load(include_str!("framework.luau"))
        .set_name("=integraal-test")
        .exec()?;
    peer::install(&lua, peer_scope)?;
    lua.load(std::fs::read_to_string(suite).map_err(mlua::Error::external)?)
        .set_name(format!("@{}", suite.display()))
        .exec()?;
    Ok(lua)
}

fn discover(dir: &Path, files: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            discover(&entry.path(), files)?;
        } else if entry.file_name().to_string_lossy().ends_with(".test.luau") {
            files.push(entry.path());
        }
    }
    Ok(())
}

#[tokio::main]
async fn main() {
    match run().await {
        Ok(true) => (),
        Ok(false) => std::process::exit(1),
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(2);
        }
    }
}

async fn run() -> Result<bool> {
    let args: Vec<_> = std::env::args().skip(1).collect();
    if args.len() != 2 {
        return Err("usage: integraal-test <tenant-directory> <deployment-config.json>".into());
    }
    // Require an explicit local endpoint before constructing any AWS clients.
    let endpoint = std::env::var("AWS_ENDPOINT_URL").unwrap_or_default();
    if !(endpoint.starts_with("http://localhost:") || endpoint.starts_with("http://127.0.0.1:")) {
        return Err(
            "set AWS_ENDPOINT_URL to your local roto endpoint (http://localhost:<port>)".into(),
        );
    }
    let root = PathBuf::from(&args[0]).canonicalize()?;
    let config = GlobalConfig::parse(&std::fs::read_to_string(&args[1])?)?;
    let tenant: TenantConfig =
        serde_json::from_str(&std::fs::read_to_string(root.join("config.json"))?)?;
    let name = root.file_name().unwrap().to_string_lossy().to_string();
    let mut stores = std::collections::HashMap::new();
    for (key, access) in &tenant.stores {
        let store = config
            .stores
            .get(key)
            .ok_or_else(|| format!("unknown store {key}"))?;
        let bucket = store
            .s3_bucket()
            .ok_or("test runner currently supports S3 stores only")?;
        stores.insert(
            key.clone(),
            StoreScope::new(
                Arc::new(S3Backend::from_env(bucket).await),
                format!("{}{name}/", store.prefix()),
            )
            .with_access(*access),
        );
    }
    let db = if let Some(db) = &config.db {
        Some(DbScope::new(
            Arc::new(DynamoBackend::from_env(&db.table).await),
            &name,
        ))
    } else {
        None
    };
    let queue = if tenant.queue {
        let q = config
            .queue
            .as_ref()
            .ok_or("tenant enables queue but deployment has no queue")?;
        Some(Arc::new(SqsBackend::from_env(&q.url).await)
            as Arc<dyn integraal_wrappers::QueueBackend>)
    } else {
        None
    };
    let batch = if tenant.batch {
        let b = config
            .batch
            .as_ref()
            .ok_or("tenant enables batch but deployment has no batch bucket")?;
        Some(StoreScope::new(
            Arc::new(S3Backend::from_env(&b.bucket).await),
            format!("{}{name}/", b.prefix),
        ))
    } else {
        None
    };
    let host = HostContext {
        tenant: name,
        script: Arc::new(Mutex::new("test".into())),
        stores,
        db,
        queue,
        batch,
        callback_base_url: config.callbacks.map(|c| c.base_url),
        throttle: None,
        background_http: false,
        http: Default::default(),
        vault: None,
        master_keys: None,
        vars: tenant.vars,
        flow_log: None,
        summary: None,
        limits: HostLimits {
            deadline: Deadline::new(Instant::now() + Duration::from_secs(10)),
            max_http_response_bytes: 5_000_000,
            max_object_bytes: 5_000_000,
            max_stream_bytes: 64_000_000,
            max_sql_bytes: 64_000_000,
            max_http_requests: 100,
            max_data_ops: 1000,
            max_http_timeout: Duration::from_secs(10),
        },
    };
    tokio::task::spawn_blocking(move || -> Result<bool> {
        let mut files = Vec::new();
        discover(&root.join("tests"), &mut files)?;
        files.sort();
        let (mut total, mut failed) = (0, 0);
        for file in files {
            let discovery = vm(&root, &host, &file)?;
            let names: Vec<String> = discovery
                .globals()
                .get::<Function>("__case_names")?
                .call(())?;
            for (i, name) in names.iter().enumerate() {
                total += 1;
                let result = vm(&root, &host, &file).and_then(|lua| {
                    lua.globals()
                        .get::<Function>("__run_case")?
                        .call::<()>(i + 1)
                });
                match result {
                    Ok(()) => println!("PASS {}: {name}", file.display()),
                    Err(e) => {
                        failed += 1;
                        eprintln!("FAIL {}: {name}\n{e}", file.display());
                    }
                }
            }
        }
        println!("{} passed, {failed} failed", total - failed);
        if total == 0 {
            return Err("no test cases found".into());
        }
        Ok(failed == 0)
    })
    .await?
}

#[cfg(test)]
mod tests {
    use super::*;

    fn framework() -> Lua {
        let lua = Lua::new();
        lua.load("function __install_native() ig = { http = {} }; local patterns = { claim = {} }; require = function(spec) assert(spec == \"@native/patterns@1\"); return patterns end end")
            .exec()
            .unwrap();
        lua.load(include_str!("framework.luau")).exec().unwrap();
        lua
    }

    #[test]
    fn http_matches_records_and_rejects_unregistered_calls() {
        framework().load(r#"
            test.http("POST", "https://partner/x", function(req)
                assert(req.body == "payload")
                return { status = 201, body = "ok", headers = {} }
            end)
            assert(ig.http.request({method="POST", url="https://partner/x", body="payload"}).status == 201)
            test.equal(test.http_calls, {{method="POST", url="https://partner/x", body="payload", headers={}}})
            test.throws(function() ig.http.get("https://partner/x") end, "unmocked HTTP")
            test.equal(#test.http_calls, 2)
        "#).exec().unwrap();
    }

    #[test]
    fn mocks_support_patterns_and_reject_old_namespace() {
        framework()
            .load(
                r#"local igp = require("@native/patterns@1")

            local spy = test.mock("igp.claim.acquire", function(id) return {token=id} end)
            assert(igp.claim.acquire("work").token == "work")
            assert(spy.calls[1][1] == "work")
            test.throws(function() test.mock("native.http.get", function() end) end, "mock path")
        "#,
            )
            .exec()
            .unwrap();
    }

    #[test]
    fn spies_preserve_nil_arguments_multiple_returns_and_errors() {
        framework()
            .load(
                r#"
            local spy = test.mock("ig.http.get", function(a, b, c)
                assert(a == "url" and b == nil and c == 3)
                return nil, "second"
            end)
            local a, b = ig.http.get("url", nil, 3)
            assert(a == nil and b == "second")
            assert(spy.calls[1].n == 3 and spy.calls[1][3] == 3)
            test.throws(function() test.equal({x=1}, {x=2}) end)
            test.throws(function() test.throws(function() end) end)
        "#,
            )
            .exec()
            .unwrap();
    }

    #[test]
    fn require_caches_modules_and_detects_cycles() {
        framework()
            .load(
                r#"
            local calls = 0
            function __load_module(spec)
                calls += 1
                if spec == "@lib/cycle" then return function() return require(spec) end end
                return function() return { value = 42 } end
            end
            assert(require("@lib/a") == require("@lib/a"))
            assert(calls == 1)
            test.throws(function() require("@lib/cycle") end, "circular require")
        "#,
            )
            .exec()
            .unwrap();
    }
}
