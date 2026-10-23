use mlua::{Lua, LuaSerdeExt, Table, Value};

use std::sync::Arc;

use crate::{Budget, HostContext};

pub fn install(lua: &Lua, host: &HostContext) -> mlua::Result<()> {
    let budget = Arc::new(Budget::new(&host.limits));
    let native = lua.create_table()?;
    native.set("version", 1)?;

    let (tenant, script) = (host.tenant.clone(), host.script.clone());
    native.set(
        "log",
        lua.create_function(move |_, (level, msg): (String, String)| {
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

    native.set("data", data_api(lua, host, budget.clone())?)?;
    native.set("http", http_api(lua, host, budget)?)?;
    native.set("xml", crate::xml::api(lua, host.limits.max_stream_bytes, host.limits.deadline)?)?;
    native.set("config", lua.to_value(&host.vars)?)?;

    let native: Table = native;
    lua.globals().set("native", native)
}

fn data_api(lua: &Lua, host: &HostContext, budget: Arc<Budget>) -> mlua::Result<Table> {
    let data = lua.create_table()?;
    let scope = host.data.clone().map(|d| d.with_max_object_bytes(host.limits.max_object_bytes));
    let need = move || {
        budget.take_data().map_err(mlua::Error::runtime)?;
        scope.clone().ok_or_else(|| mlua::Error::runtime("native.data is not configured for this runtime"))
    };
    let err = |e: String| mlua::Error::runtime(e);

    let n = need.clone();
    data.set(
        "get",
        lua.create_function(move |lua, key: String| {
            match n()?.get(&key).map_err(err)? {
                Some(bytes) => Ok(Value::String(lua.create_string(bytes)?)),
                None => Ok(Value::Nil),
            }
        })?,
    )?;

    let n = need.clone();
    data.set(
        "open",
        lua.create_function(move |lua, key: String| match n()?.open(&key).map_err(err)? {
            Some(r) => Ok(Value::UserData(lua.create_userdata(crate::xml::ByteStream::new(r))?)),
            None => Ok(Value::Nil),
        })?,
    )?;

    let n = need.clone();
    data.set(
        "put",
        lua.create_function(move |_, (key, body): (String, mlua::LuaString)| {
            n()?.put(&key, body.as_bytes().to_vec()).map_err(err)
        })?,
    )?;

    let n = need.clone();
    data.set("delete", lua.create_function(move |_, key: String| n()?.delete(&key).map_err(err))?)?;

    let n = need;
    data.set(
        "list",
        lua.create_function(move |_, prefix: Option<String>| {
            n()?.list(prefix.as_deref().unwrap_or("")).map_err(err)
        })?,
    )?;
    Ok(data)
}

fn http_api(lua: &Lua, host: &HostContext, budget: Arc<Budget>) -> mlua::Result<Table> {
    use std::{collections::HashMap, io::Read};

    let policy = host.http.clone();
    let (max_timeout, max_body) = (host.limits.max_http_timeout, host.limits.max_http_response_bytes);
    let do_request = move |lua: &Lua, method: String, url: String, headers: HashMap<String, String>, body: Option<mlua::LuaString>, stream: bool| {
        budget.take_http().map_err(mlua::Error::runtime)?;
        let remaining = budget.deadline.saturating_duration_since(std::time::Instant::now());
        let agent: ureq::Agent = ureq::Agent::config_builder()
            .max_redirects(0) // a redirect could leave the allowlist; scripts follow explicitly
            .http_status_as_error(false)
            .timeout_global(Some(max_timeout.min(remaining)))
            .build()
            .into();
        let method = method.to_ascii_uppercase();
        let url = policy.check(&method, &url).map_err(mlua::Error::runtime)?;
        let mut req = ureq::http::Request::builder().method(method.as_str()).uri(url.as_str());
        for (k, v) in &headers {
            req = req.header(k, v);
        }
        let body = body.map(|b| b.as_bytes().to_vec()).unwrap_or_default();
        let resp = agent
            .run(req.body(body).map_err(mlua::Error::external)?)
            .map_err(|e| mlua::Error::runtime(format!("http request failed: {e}")))?;

        let out = lua.create_table()?;
        out.set("status", resp.status().as_u16())?;
        let hdrs = lua.create_table()?;
        for (k, v) in resp.headers() {
            if let Ok(v) = v.to_str() {
                hdrs.set(k.as_str(), v)?;
            }
        }
        out.set("headers", hdrs)?;
        if stream {
            let reader: Box<dyn Read + Send> = Box::new(resp.into_body().into_reader());
            out.set("body", lua.create_userdata(crate::xml::ByteStream::new(reader))?)?;
            return Ok(out);
        }
        let mut buf = Vec::new();
        resp.into_body()
            .into_reader()
            .take(max_body)
            .read_to_end(&mut buf)
            .map_err(|e| mlua::Error::runtime(format!("reading response: {e}")))?;
        out.set("body", lua.create_string(buf)?)?;
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
                opts.get::<Option<String>>("method")?.unwrap_or_else(|| "GET".into()),
                opts.get("url")?,
                opts.get::<Option<HashMap<String, String>>>("headers")?.unwrap_or_default(),
                opts.get("body")?,
                opts.get::<Option<bool>>("stream")?.unwrap_or(false),
            )
        })?,
    )?;
    let r = do_request;
    http.set(
        "get",
        lua.create_function(move |lua, (url, headers): (String, Option<HashMap<String, String>>)| {
            r(lua, "GET".into(), url, headers.unwrap_or_default(), None, false)
        })?,
    )?;
    Ok(http)
}
