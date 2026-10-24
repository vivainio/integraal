use mlua::{Lua, LuaSerdeExt, Table, Value};

use std::sync::Arc;

use crate::{Budget, HostContext};

pub fn install(lua: &Lua, host: &HostContext) -> mlua::Result<()> {
    let budget = Arc::new(Budget::new(&host.limits));
    let native = lua.create_table()?;
    native.set("version", 1)?;

    let (tenant, script_name) = (host.tenant.clone(), host.script.clone());
    native.set(
        "log",
        lua.create_function(move |_, (level, msg): (String, String)| {
            let script = script_name.lock().unwrap().clone();
            match level.as_str() {
                "error" => tracing::error!(%tenant, %script, "{msg}"),
                "warn" => tracing::warn!(%tenant, %script, "{msg}"),
                "debug" => tracing::debug!(%tenant, %script, "{msg}"),
                _ => tracing::info!(%tenant, %script, "{msg}"),
            }
            Ok(())
        })?,
    )?;

    let json = lua.create_table()?;
    json.set(
        "encode",
        lua.create_function(|lua, v: Value| {
            let v: serde_json::Value = lua.from_value(v)?;
            serde_json::to_string(&v).map_err(mlua::Error::external)
        })?,
    )?;
    json.set(
        "decode",
        lua.create_function(|lua, s: String| {
            let v: serde_json::Value = serde_json::from_str(&s).map_err(mlua::Error::external)?;
            lua.to_value(&v)
        })?,
    )?;
    native.set("json", json)?;

    let time = lua.create_table()?;
    time.set(
        "now_ms",
        lua.create_function(|_, ()| {
            let d = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(mlua::Error::external)?;
            Ok(d.as_millis() as u64)
        })?,
    )?;
    native.set("time", time)?;

    native.set("bucket", buckets_api(lua, host, budget.clone())?)?;
    native.set(
        "batch",
        crate::batch::api(lua, host, budget.clone())?,
    )?;
    native.set(
        "ndjson",
        crate::ndjson::api(
            lua,
            host.limits.max_stream_bytes,
            host.limits.max_object_bytes,
            host.limits.deadline.clone(),
        )?,
    )?;
    native.set("db", db_api(lua, host, budget.clone())?)?;
    native.set(
        "queue",
        crate::queue::api(lua, &host.tenant, host.queue.clone(), budget.clone())?,
    )?;
    native.set("callback", callback_api(lua, host, budget.clone())?)?;
    native.set(
        "lock",
        crate::lock::api(
            lua,
            &host.tenant,
            host.db.clone(),
            budget.clone(),
            host.limits.deadline.clone(),
        )?,
    )?;
    native.set("http", http_api(lua, host, budget)?)?;
    native.set(
        "deadline",
        crate::deadline::api(lua, host.limits.deadline.clone())?,
    )?;
    native.set(
        "xml",
        crate::xml::api(
            lua,
            host.limits.max_stream_bytes,
            host.limits.max_object_bytes,
            host.limits.deadline.clone(),
        )?,
    )?;
    native.set(
        "zip",
        crate::zip::api(
            lua,
            host.limits.max_stream_bytes,
            host.limits.max_object_bytes,
            host.limits.deadline.clone(),
        )?,
    )?;
    native.set(
        "gzip",
        crate::gzip::api(
            lua,
            host.limits.max_stream_bytes,
            host.limits.max_object_bytes,
            host.limits.deadline.clone(),
        )?,
    )?;
    native.set(
        "sql",
        crate::sql::api(
            lua,
            host.limits.max_sql_bytes,
            host.limits.max_object_bytes,
            host.limits.deadline.clone(),
        )?,
    )?;
    native.set(
        "crypto",
        crate::crypto::api(
            lua,
            host.master_keys.clone(),
            &host.tenant,
            host.limits.max_object_bytes,
        )?,
    )?;
    native.set("config", lua.to_value(&host.vars)?)?;

    let native: Table = native;
    lua.globals().set("native", native)
}

/// One table of object functions per bucket the tenant was granted: `native.bucket.In.get(key)`.
/// Naming any other bucket raises a clear error instead of indexing nil.
fn buckets_api(lua: &Lua, host: &HostContext, budget: Arc<Budget>) -> mlua::Result<Table> {
    let data = lua.create_table()?;
    for (name, scope) in &host.buckets {
        let scope = scope
            .clone()
            .with_max_object_bytes(host.limits.max_object_bytes);
        data.set(
            name.as_str(),
            bucket_api(lua, scope, budget.clone(), &host.limits)?,
        )?;
    }
    let mt = lua.create_table()?;
    mt.set(
        "__index",
        lua.create_function(|_, (_, name): (Table, String)| -> mlua::Result<Value> {
            Err(mlua::Error::runtime(format!(
                "data bucket {name:?} is not available to this tenant"
            )))
        })?,
    )?;
    data.set_metatable(Some(mt))?;
    Ok(data)
}

fn bucket_api(
    lua: &Lua,
    scope: crate::BucketScope,
    budget: Arc<Budget>,
    limits: &crate::HostLimits,
) -> mlua::Result<Table> {
    let bucket = lua.create_table()?;
    let need = move || {
        budget.take_data().map_err(mlua::Error::runtime)?;
        Ok::<_, mlua::Error>(scope.clone())
    };
    let err = |e: String| mlua::Error::runtime(e);

    let n = need.clone();
    bucket.set(
        "get",
        lua.create_function(move |lua, key: String| match n()?.get(&key).map_err(err)? {
            Some(bytes) => Ok(Value::String(lua.create_string(bytes)?)),
            None => Ok(Value::Nil),
        })?,
    )?;

    let n = need.clone();
    bucket.set(
        "open",
        lua.create_function(
            move |lua, key: String| match n()?.open(&key).map_err(err)? {
                Some(r) => Ok(Value::UserData(
                    lua.create_userdata(crate::xml::ByteStream::new(r))?,
                )),
                None => Ok(Value::Nil),
            },
        )?,
    )?;

    let n = need.clone();
    bucket.set(
        "put",
        lua.create_function(move |_, (key, body): (String, mlua::LuaString)| {
            n()?.put(&key, body.as_bytes().to_vec()).map_err(err)
        })?,
    )?;

    // Streaming upload; the object appears when the sink is closed.
    let n = need.clone();
    let (max_stream, deadline) = (limits.max_stream_bytes, limits.deadline.clone());
    bucket.set(
        "create",
        lua.create_function(move |lua, key: String| {
            let w = n()?.create(&key).map_err(err)?;
            lua.create_userdata(crate::sink::Sink::new(w, max_stream, deadline.clone()))
        })?,
    )?;

    for (name, method) in [
        ("presign_get", crate::PresignMethod::Get),
        ("presign_put", crate::PresignMethod::Put),
    ] {
        let n = need.clone();
        bucket.set(
            name,
            lua.create_function(move |_, (key, ttl_s): (String, Option<u64>)| {
                n()?.presign(method, &key, ttl_s.map(std::time::Duration::from_secs))
                    .map_err(err)
            })?,
        )?;
    }

    let n = need.clone();
    bucket.set(
        "delete",
        lua.create_function(move |_, key: String| n()?.delete(&key).map_err(err))?,
    )?;

    let n = need;
    bucket.set(
        "list",
        lua.create_function(move |_, prefix: Option<String>| {
            n()?.list(prefix.as_deref().unwrap_or("")).map_err(err)
        })?,
    )?;
    Ok(bucket)
}

/// Values are any JSON-serializable Lua value, stored as JSON text. Shares the `max_data_ops` budget.
fn db_api(lua: &Lua, host: &HostContext, budget: Arc<Budget>) -> mlua::Result<Table> {
    let db = lua.create_table()?;
    let scope = host.db.clone();
    let need = move || {
        budget.take_data().map_err(mlua::Error::runtime)?;
        scope
            .clone()
            .ok_or_else(|| mlua::Error::runtime("native.db is not configured for this runtime"))
    };
    let err = |e: String| mlua::Error::runtime(e);

    let n = need.clone();
    db.set(
        "get",
        lua.create_function(move |lua, (pk, sk): (String, Option<String>)| {
            match n()?.get(&pk, sk.as_deref().unwrap_or("")).map_err(err)? {
                Some(json) => lua.to_value(
                    &serde_json::from_str::<serde_json::Value>(&json)
                        .map_err(mlua::Error::external)?,
                ),
                None => Ok(Value::Nil),
            }
        })?,
    )?;

    let n = need.clone();
    db.set(
        "put",
        lua.create_function(
            move |lua, (pk, sk, value): (String, Option<String>, Value)| {
                let json = serde_json::to_string(&lua.from_value::<serde_json::Value>(value)?)
                    .map_err(mlua::Error::external)?;
                n()?.put(&pk, sk.as_deref().unwrap_or(""), json)
                    .map_err(err)
            },
        )?,
    )?;

    let n = need.clone();
    db.set(
        "put_if_absent",
        lua.create_function(
            move |lua, (pk, sk, value): (String, Option<String>, Value)| {
                let json = serde_json::to_string(&lua.from_value::<serde_json::Value>(value)?)
                    .map_err(mlua::Error::external)?;
                n()?.put_if_absent(&pk, sk.as_deref().unwrap_or(""), json)
                    .map_err(err)
            },
        )?,
    )?;

    let n = need.clone();
    db.set(
        "add",
        lua.create_function(
            move |_, (pk, sk, delta): (String, Option<String>, Option<i64>)| {
                n()?.add(&pk, sk.as_deref().unwrap_or(""), delta.unwrap_or(1))
                    .map_err(err)
            },
        )?,
    )?;

    let n = need.clone();
    db.set(
        "delete",
        lua.create_function(move |_, (pk, sk): (String, Option<String>)| {
            n()?.delete(&pk, sk.as_deref().unwrap_or("")).map_err(err)
        })?,
    )?;

    let n = need;
    db.set(
        "query",
        lua.create_function(move |lua, (pk, opts): (String, Option<Table>)| {
            let (prefix, limit) = match opts {
                Some(o) => (
                    o.get::<Option<String>>("sk_prefix")?,
                    o.get::<Option<usize>>("limit")?,
                ),
                None => (None, None),
            };
            let rows = n()?
                .query(&pk, prefix.as_deref().unwrap_or(""), limit)
                .map_err(err)?;
            let out = lua.create_table()?;
            for (sk, json) in rows {
                let row = lua.create_table()?;
                row.set("sk", sk)?;
                row.set(
                    "value",
                    lua.to_value(
                        &serde_json::from_str::<serde_json::Value>(&json)
                            .map_err(mlua::Error::external)?,
                    )?,
                )?;
                out.push(row)?;
            }
            Ok(out)
        })?,
    )?;
    Ok(db)
}

/// Single-use callback URLs that route a later inbound request to a script, carrying saved state.
fn callback_api(lua: &Lua, host: &HostContext, budget: Arc<Budget>) -> mlua::Result<Table> {
    let cb = lua.create_table()?;
    let (scope, base, tenant) = (
        host.db.clone(),
        host.callback_base_url.clone(),
        host.tenant.clone(),
    );
    cb.set(
        "create",
        lua.create_function(move |lua, opts: Option<Table>| {
            budget.take_data().map_err(mlua::Error::runtime)?;
            let scope = scope.clone().ok_or_else(|| {
                mlua::Error::runtime("native.callback needs native.db (the config's db section)")
            })?;
            let base = base.clone().ok_or_else(|| {
                mlua::Error::runtime(
                    "native.callback is not configured (the config's callbacks section)",
                )
            })?;
            let (script, state, ttl_s) = match &opts {
                Some(o) => (
                    o.get::<Option<String>>("script")?,
                    o.get::<Value>("state")?,
                    o.get::<Option<u64>>("ttl_s")?,
                ),
                None => (None, Value::Nil, None),
            };
            if let Some(s) = &script {
                if !crate::queue::valid_name(s) {
                    return Err(mlua::Error::runtime(format!("invalid script name {s:?}")));
                }
            }
            let state: serde_json::Value = if state.is_nil() {
                serde_json::Value::Null
            } else {
                lua.from_value(state)?
            };
            let ttl =
                std::time::Duration::from_secs(ttl_s.unwrap_or(crate::DB_TTL.as_secs()).max(1));
            let id = scope
                .create_callback(script, state, ttl)
                .map_err(mlua::Error::runtime)?;
            Ok(format!("{}/cb/{tenant}/{id}", base.trim_end_matches('/')))
        })?,
    )?;
    Ok(cb)
}

/// Keeps the throttle slot held for as long as a streamed response body exists.
struct SlotReader<R> {
    inner: R,
    _slot: Option<crate::Slot>,
}

impl<R: std::io::Read> std::io::Read for SlotReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        self.inner.read(buf)
    }
}

fn http_api(lua: &Lua, host: &HostContext, budget: Arc<Budget>) -> mlua::Result<Table> {
    use std::{collections::HashMap, io::Read};

    let policy = host.http.clone();
    let (vault, tenant) = (host.vault.clone(), host.tenant.clone());
    let throttle = host.throttle.clone();
    let (max_timeout, max_body) = (
        host.limits.max_http_timeout,
        host.limits.max_http_response_bytes,
    );
    let do_request = move |lua: &Lua,
                           method: String,
                           url: String,
                           headers: HashMap<String, String>,
                           body: Option<mlua::LuaString>,
                           stream: bool,
                           expand: crate::vault::Expand| {
        budget.take_http().map_err(mlua::Error::runtime)?;
        let remaining = budget.deadline.remaining();
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .max_redirects(0) // a redirect could leave the allowlist; scripts follow explicitly
            .http_status_as_error(false)
            .timeout_global(Some(max_timeout.min(remaining)))
            .build()
            .into();
        let method = method.to_ascii_uppercase();
        // `{{vault:...}}` placeholders are expanded only in the parts the script asked for, and a
        // secret is sent only to URLs it allows (checked against the final URL, after the tenant's
        // own allowlist). The script never sees the substituted request or any error that contains it.
        let mut used: Vec<crate::vault::Secret> = Vec::new();
        let mut lookup = |reference: &str| -> Result<Vec<u8>, String> {
            budget.take_data()?;
            let secret = vault
                .as_ref()
                .ok_or("the credential vault is not configured for this runtime")?
                .lookup(&tenant, reference)?;
            let value = secret.value.clone();
            used.push(secret);
            Ok(value)
        };
        let expanded = (|| -> Result<_, String> {
            let url = if expand.url {
                let u = crate::vault::substitute(url.as_bytes(), &mut lookup)?;
                String::from_utf8(u).map_err(|_| "vault secret is not valid in a url")?
            } else {
                url.clone()
            };
            let headers = headers
                .iter()
                .map(|(k, v)| {
                    if !expand.header {
                        return Ok((k.clone(), v.clone()));
                    }
                    let v = crate::vault::substitute(v.as_bytes(), &mut lookup)?;
                    Ok((
                        k.clone(),
                        String::from_utf8(v)
                            .map_err(|_| "vault secret is not valid in a header")?,
                    ))
                })
                .collect::<Result<HashMap<_, _>, String>>()?;
            let body = match &body {
                Some(b) if expand.body => {
                    Some(crate::vault::substitute(&b.as_bytes(), &mut lookup)?)
                }
                Some(b) => Some(b.as_bytes().to_vec()),
                None => None,
            };
            Ok((url, headers, body))
        })();
        // Whatever fails from here on is reported with secret values scrubbed out.
        let scrub = |msg: String| {
            let values: Vec<Vec<u8>> = used.iter().map(|s| s.value.clone()).collect();
            mlua::Error::runtime(
                String::from_utf8_lossy(&crate::vault::redact(msg.as_bytes(), &values))
                    .into_owned(),
            )
        };
        let (url, headers, body) = expanded.map_err(scrub)?;
        let url = policy.check(&method, &url).map_err(scrub)?;
        for secret in &used {
            secret.check(&url).map_err(scrub)?;
        }
        // A stream cannot be scanned for secrets, so it is allowed only when the owner of every
        // secret used has vouched that the service never echoes it.
        if stream && used.iter().any(|s| !s.stream_unmasked) {
            return Err(mlua::Error::runtime(
                "requests using vault secrets cannot stream the response",
            ));
        }
        // Only values long enough not to mangle ordinary text are redacted from responses.
        let secrets: Vec<Vec<u8>> = used
            .iter()
            .filter(|s| s.value.len() >= crate::MIN_SECRET_BYTES)
            .map(|s| s.value.clone())
            .collect();
        // Only method and host go on the span: the path and query may carry secrets.
        let span = tracing::info_span!(
            "http.client",
            otel.kind = "client",
            http.request.method = %method,
            server.address = url.host_str().unwrap_or_default(),
            http.response.status_code = tracing::field::Empty,
            otel.status_code = tracing::field::Empty,
        );
        let _entered = span.enter();
        // Held until the response is fully read (or, when streaming, until the stream is dropped).
        let waited = std::time::Instant::now();
        let slot = throttle
            .as_ref()
            .map(|t| t.acquire(&url, budget.deadline.effective()))
            .transpose()
            .map_err(mlua::Error::runtime)?;
        if throttle.is_some() {
            crate::telemetry::throttle_wait(waited.elapsed());
        }
        let started = std::time::Instant::now();
        let mut req = ureq::http::Request::builder()
            .method(method.as_str())
            .uri(url.as_str());
        for (k, v) in &headers {
            req = req.header(k, v);
        }
        let body = body.unwrap_or_default();
        let host = url.host_str().unwrap_or_default().to_string();
        let resp = agent
            .run(req.body(body).map_err(mlua::Error::external)?)
            .map_err(|e| {
                span.record("otel.status_code", "ERROR");
                crate::telemetry::http_request(&method, &host, None, started.elapsed());
                scrub(format!("http request failed: {e}"))
            })?;
        let status = resp.status().as_u16();
        span.record("http.response.status_code", status);
        if status >= 500 {
            span.record("otel.status_code", "ERROR");
        }
        crate::telemetry::http_request(&method, &host, Some(status), started.elapsed());

        let out = lua.create_table()?;
        out.set("status", status)?;
        let hdrs = lua.create_table()?;
        for (k, v) in resp.headers() {
            if let Ok(v) = v.to_str() {
                let v = crate::vault::redact(v.as_bytes(), &secrets);
                hdrs.set(k.as_str(), String::from_utf8_lossy(&v).into_owned())?;
            }
        }
        out.set("headers", hdrs)?;
        if stream {
            let reader: Box<dyn Read + Send> = Box::new(SlotReader {
                inner: resp.into_body().into_reader(),
                _slot: slot,
            });
            out.set(
                "body",
                lua.create_userdata(crate::xml::ByteStream::new(reader))?,
            )?;
            return Ok(out);
        }
        let mut buf = Vec::new();
        resp.into_body()
            .into_reader()
            .take(max_body)
            .read_to_end(&mut buf)
            .map_err(|e| scrub(format!("reading response: {e}")))?;
        out.set(
            "body",
            lua.create_string(crate::vault::redact(&buf, &secrets))?,
        )?;
        Ok(out)
    };
    let do_request = std::sync::Arc::new(do_request);

    let http = lua.create_table()?;
    let r = do_request.clone();
    http.set(
        "request",
        lua.create_function(move |lua, opts: Table| {
            r(
                lua,
                opts.get::<Option<String>>("method")?
                    .unwrap_or_else(|| "GET".into()),
                opts.get("url")?,
                opts.get::<Option<HashMap<String, String>>>("headers")?
                    .unwrap_or_default(),
                opts.get("body")?,
                opts.get::<Option<bool>>("stream")?.unwrap_or(false),
                crate::vault::Expand::parse(opts.get::<Option<Vec<String>>>("vault")?)
                    .map_err(mlua::Error::runtime)?,
            )
        })?,
    )?;
    let r = do_request;
    http.set(
        "get",
        lua.create_function(
            move |lua,
                  (url, headers, vault): (
                String,
                Option<HashMap<String, String>>,
                Option<Vec<String>>,
            )| {
                r(
                    lua,
                    "GET".into(),
                    url,
                    headers.unwrap_or_default(),
                    None,
                    false,
                    crate::vault::Expand::parse(vault).map_err(mlua::Error::runtime)?,
                )
            },
        )?,
    )?;
    Ok(http)
}
