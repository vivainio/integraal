//! Fargate entrypoint. Serves HTTP (ALB target) and polls SQS, both feeding the same engine.
//!
//! HTTP:  POST /v1/{tenant}/{script}   (optional `x-script-version` header; body is the event)
//!        POST /v1/{tenant}            (script chosen by the tenant's routes / router)
//! Queue: SQS message body is `{tenant, script?, version?, event}`; no `script` means routed
//!
//! Admin: on its own port when `admin` is configured (see `admin_router`): manage the credential vault.
//!
//! Config: `--config <path>` or `./integraal.json` (see `GlobalConfig`); AWS credentials/region come from the usual AWS variables.

use std::time::Duration;

use aws_sdk_sqs::types::MessageSystemAttributeName;
use axum::{
    body::{Body, Bytes},
    extract::{DefaultBodyLimit, OriginalUri, Path, Query, State},
    http::{HeaderMap, Method, StatusCode},
    response::{IntoResponse, Response},
    routing::{any, get, post, put},
    Json, Router,
};
use integraal_core::{
    s3_events::{classify, disposition, Disposition, Message},
    Engine, Error, Every, GlobalConfig, Invocation, S3Events, Spread, Trigger, Vault,
};
use serde_json::{json, Value};
use tracing::Instrument;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = GlobalConfig::load().map_err(anyhow::Error::msg)?;
    if let Some(i) = std::env::args().position(|a| a == "flow") {
        let args: Vec<String> = std::env::args().skip(i + 1).collect();
        return flow_command(&cfg, &args).await;
    }
    if let Some(i) = std::env::args().position(|a| a == "debug") {
        let args: Vec<String> = std::env::args().skip(i + 1).collect();
        return debug_command(&cfg, &args).await;
    }
    let _telemetry = integraal_core::telemetry::init("integraal-server", cfg.log_filter())
        .map_err(anyhow::Error::msg)?;

    let mut engine = cfg
        .engine()
        .await
        .map_err(anyhow::Error::msg)?
        .with_background_http();
    if let Some(throttle) = cfg.throttle().map_err(anyhow::Error::msg)? {
        engine = engine.with_throttle(throttle);
    }

    if let Some(d) = cfg.debug.as_ref().filter(|d| d.dir.is_some()) {
        // Disk captures have no lifecycle rule: sweep the old ones hourly.
        let (dir, max_age) = (
            std::path::PathBuf::from(d.dir.clone().unwrap_or_default()),
            std::time::Duration::from_secs(d.retention_hours * 3600),
        );
        tokio::spawn(async move {
            loop {
                let (dir, max_age) = (dir.clone(), max_age);
                match tokio::task::spawn_blocking(move || {
                    integraal_core::debug::DebugSink::sweep_dirs(&dir, max_age)
                })
                .await
                {
                    Ok(n) if n > 0 => tracing::info!(removed = n, "swept old debug captures"),
                    _ => {}
                }
                tokio::time::sleep(std::time::Duration::from_secs(3600)).await;
            }
        });
    }

    if let Some(f) = &cfg.flow_logs {
        // Task records go out on a timer (earlier if the buffer fills up, and at shutdown).
        let (engine, every) = (engine.clone(), Duration::from_secs(f.task_flush_secs));
        tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            loop {
                tick.tick().await;
                engine.flush_tasks().await;
            }
        });
    }

    if let Some(queue) = &cfg.queue {
        tokio::spawn(poll_queue(
            engine.clone(),
            queue.url.clone(),
            S3Events::from_config(&cfg),
            cfg.schedule.spread_s,
        ));
    }

    if let Some((port, token)) = cfg.admin().map_err(anyhow::Error::msg)? {
        let vault = engine
            .vault()
            .expect("config validation requires db and crypto");
        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
        tracing::info!(%port, "admin listening");
        tokio::spawn(async move {
            let app = admin_router(AdminState {
                vault,
                token: token.into(),
            });
            if let Err(e) = axum::serve(listener, app).await {
                tracing::error!(error = %e, "admin server stopped");
            }
        });
    }

    if let Some(l) = &cfg.local_api {
        let engine = engine.clone();
        // Loopback only: reaching the port at all is the access check.
        let listener = tokio::net::TcpListener::bind(("127.0.0.1", l.port)).await?;
        tracing::info!(port = %l.port, "local api listening");
        tokio::spawn(async move {
            if let Err(e) = axum::serve(listener, local_router(engine)).await {
                tracing::error!(error = %e, "local api stopped");
            }
        });
    }

    let app = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .merge(web_routes())
        .route("/v1/{tenant}/{script}", post(http_invoke))
        .route("/v1/{tenant}", post(http_routed))
        .with_state(engine.clone());
    let port = cfg.server.port;
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    tracing::info!(%port, "listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(async move {
            stop_signal().await;
            // Background http requests die with this process; give their jobs an outcome now rather
            // than after the connections drain, which ECS may not wait for.
            let failed = tokio::task::spawn_blocking(integraal_core::fail_inflight_http)
                .await
                .unwrap_or(0);
            engine.flush_tasks().await;
            tracing::info!(failed, "stopping");
        })
        .await?;
    Ok(())
}

/// `integraal-server flow show <tenant> <trace id>` prints a flow's log, merged from all its
/// segments, as NDJSON. `flow list <tenant> <yyyy-mm-dd>` prints the flows that started that day.
async fn flow_command(cfg: &GlobalConfig, args: &[String]) -> anyhow::Result<()> {
    let usage =
        "usage: integraal-server flow show <tenant> <trace id> | flow list <tenant> <yyyy-mm-dd>";
    let [verb, tenant, what] = args else {
        anyhow::bail!(usage);
    };
    let engine = cfg.engine().await.map_err(anyhow::Error::msg)?;
    let sink = engine
        .flow_logs()
        .ok_or_else(|| anyhow::anyhow!("the config has no flow_logs section"))?
        .clone();
    let (tenant, what) = (tenant.clone(), what.clone());
    let lines = match verb.as_str() {
        "show" => tokio::task::spawn_blocking(move || sink.read_flow(&tenant, &what)).await?,
        "list" => tokio::task::spawn_blocking(move || sink.list_day(&tenant, &what)).await?,
        _ => anyhow::bail!(usage),
    }
    .map_err(anyhow::Error::msg)?;
    for l in lines {
        println!("{l}");
    }
    Ok(())
}

/// `integraal-server debug show <tenant> <trace id>` prints the captures of watched invocations in
/// a flow, as one JSON document per line.
async fn debug_command(cfg: &GlobalConfig, args: &[String]) -> anyhow::Result<()> {
    let [verb, tenant, trace] = args else {
        anyhow::bail!("usage: integraal-server debug show <tenant> <trace id>");
    };
    anyhow::ensure!(
        verb == "show",
        "usage: integraal-server debug show <tenant> <trace id>"
    );
    let engine = cfg.engine().await.map_err(anyhow::Error::msg)?;
    let sink = engine
        .debug_sink()
        .ok_or_else(|| anyhow::anyhow!("the config has no debug section"))?
        .clone();
    let (tenant, trace) = (tenant.clone(), trace.clone());
    let captures = tokio::task::spawn_blocking(move || sink.read(&tenant, &trace))
        .await?
        .map_err(anyhow::Error::msg)?;
    for c in captures {
        println!("{c}");
    }
    Ok(())
}

/// Read-only flow log queries, served on loopback only (`local_api`).
///
/// - `GET /flows/{tenant}?day=yyyy-mm-dd`          flows that started that UTC day (default: today)
/// - `GET /flows/{tenant}/{trace}?since=<ms>`      the merged log; `since` keeps only later lines
/// - `GET /flows/{tenant}/{trace}/summary`         one row per invocation
/// - `GET /flows/{tenant}/{trace}/state`           what the flow is waiting on: timers, barriers, callbacks, batch jobs
/// - `GET /debug/{tenant}/{trace}`                 captures of watched invocations (needs `debug`)
/// - `GET /running`                                invocations in flight in this process
fn local_router(engine: Engine) -> Router {
    let sink = engine
        .flow_logs()
        .expect("config validation requires flow_logs")
        .clone();
    let flows = Router::new()
        .route("/flows/{tenant}", get(local_list))
        .route("/flows/{tenant}/{trace}", get(local_show))
        .route("/flows/{tenant}/{trace}/summary", get(local_summary))
        .with_state(sink);
    Router::new()
        .route("/running", get(local_running))
        .route("/flows/{tenant}/{trace}/state", get(local_state))
        .route("/debug/{tenant}/{trace}", get(local_debug))
        .with_state(engine)
        .merge(flows)
}

async fn local_running(State(engine): State<Engine>) -> AdminResult {
    (StatusCode::OK, Json(Value::Array(engine.running())))
}

async fn local_debug(
    State(engine): State<Engine>,
    Path((tenant, trace)): Path<(String, String)>,
) -> AdminResult {
    let Some(sink) = engine.debug_sink().cloned() else {
        return (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({ "error": "the config has no debug section" })),
        );
    };
    blocking_read(move || sink.read(&tenant, &trace).map(Value::Array)).await
}

async fn local_state(
    State(engine): State<Engine>,
    Path((tenant, trace)): Path<(String, String)>,
) -> AdminResult {
    blocking_read(move || engine.pending(&tenant, &trace)).await
}

type Sink = State<integraal_core::flow_log::FlowLogSink>;

async fn blocking_read(f: impl FnOnce() -> Result<Value, String> + Send + 'static) -> AdminResult {
    vault_call(f, |v| (StatusCode::OK, Json(v))).await
}

async fn local_list(
    State(sink): Sink,
    Path(tenant): Path<String>,
    axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> AdminResult {
    let day = q
        .get("day")
        .cloned()
        .unwrap_or_else(|| integraal_core::flow_log::utc_day(now_ms()));
    blocking_read(move || sink.list_day(&tenant, &day).map(Value::Array)).await
}

async fn local_show(
    State(sink): Sink,
    Path((tenant, trace)): Path<(String, String)>,
    axum::extract::Query(q): axum::extract::Query<std::collections::HashMap<String, String>>,
) -> AdminResult {
    let since: u64 = q.get("since").and_then(|s| s.parse().ok()).unwrap_or(0);
    blocking_read(move || {
        let mut lines = sink.read_flow(&tenant, &trace)?;
        lines.retain(|l| {
            l["t"]
                .as_u64()
                .or_else(|| l["start_ms"].as_u64())
                .unwrap_or(0)
                > since
        });
        Ok(Value::Array(lines))
    })
    .await
}

async fn local_summary(
    State(sink): Sink,
    Path((tenant, trace)): Path<(String, String)>,
) -> AdminResult {
    blocking_read(move || {
        sink.read_flow(&tenant, &trace)
            .map(|l| integraal_core::flow_log::summarize(&l))
    })
    .await
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Resolves on SIGTERM (what ECS sends on a deploy or scale-in) or Ctrl-C.
async fn stop_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        if let Ok(mut term) = signal(SignalKind::terminate()) {
            tokio::select! {
                _ = term.recv() => {}
                _ = tokio::signal::ctrl_c() => {}
            }
            return;
        }
    }
    let _ = tokio::signal::ctrl_c().await;
}

#[derive(Clone)]
struct AdminState {
    vault: Vault,
    token: std::sync::Arc<str>,
}

/// Credential vault API. Every request needs `Authorization: Bearer <admin token>`.
///
/// - `GET    /admin/vault/{tenant}`            names and allowed URLs (never values)
/// - `PUT    /admin/vault/{tenant}/{name}`     `{"value": <any JSON>, "allow": ["https://api.example.com/v1/"], "stream_unmasked": false}`
/// - `DELETE /admin/vault/{tenant}/{name}`
/// - `POST   /admin/vault/{tenant}/rekey`      re-seal one tenant's entries under the active master key
/// - `POST   /admin/rekey`                     the same for every tenant that uses the vault
fn admin_router(state: AdminState) -> Router {
    Router::new()
        .route("/admin/vault/{tenant}", get(admin_list))
        .route("/admin/vault/{tenant}/rekey", post(admin_rekey))
        .route("/admin/rekey", post(admin_rekey_all))
        .route(
            "/admin/vault/{tenant}/{name}",
            put(admin_put).delete(admin_delete),
        )
        .layer(axum::middleware::from_fn_with_state(
            state.clone(),
            admin_auth,
        ))
        .with_state(state)
}

async fn admin_auth(
    State(state): State<AdminState>,
    req: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    use axum::response::IntoResponse;
    use subtle::ConstantTimeEq;
    let given = req
        .headers()
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.strip_prefix("Bearer "))
        .unwrap_or("");
    let ok: bool = given.as_bytes().ct_eq(state.token.as_bytes()).into();
    if ok {
        next.run(req).await
    } else {
        (
            StatusCode::UNAUTHORIZED,
            Json(json!({ "error": "unauthorized" })),
        )
            .into_response()
    }
}

#[derive(serde::Deserialize)]
struct PutSecret {
    value: Value,
    allow: Vec<String>,
    #[serde(default)]
    stream_unmasked: bool,
}

type AdminResult = (StatusCode, Json<Value>);

/// Vault calls block on DynamoDB, so they run off the async threads.
async fn vault_call<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, String> + Send + 'static,
    ok: impl FnOnce(T) -> AdminResult,
) -> AdminResult {
    match tokio::task::spawn_blocking(f).await {
        Ok(Ok(v)) => ok(v),
        Ok(Err(e)) => (StatusCode::BAD_REQUEST, Json(json!({ "error": e }))),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(json!({ "error": e.to_string() })),
        ),
    }
}

async fn admin_list(State(s): State<AdminState>, Path(tenant): Path<String>) -> AdminResult {
    vault_call(
        move || s.vault.list(&tenant),
        |v| (StatusCode::OK, Json(json!({ "entries": v }))),
    )
    .await
}

async fn admin_put(
    State(s): State<AdminState>,
    Path((tenant, name)): Path<(String, String)>,
    Json(body): Json<PutSecret>,
) -> AdminResult {
    tracing::info!(%tenant, %name, "vault secret written");
    vault_call(
        move || {
            s.vault.put(
                &tenant,
                &name,
                &body.value,
                &body.allow,
                body.stream_unmasked,
            )
        },
        |()| (StatusCode::OK, Json(json!({ "ok": true }))),
    )
    .await
}

async fn admin_delete(
    State(s): State<AdminState>,
    Path((tenant, name)): Path<(String, String)>,
) -> AdminResult {
    tracing::info!(%tenant, %name, "vault secret deleted");
    vault_call(
        move || s.vault.delete(&tenant, &name),
        |()| (StatusCode::OK, Json(json!({ "ok": true }))),
    )
    .await
}

async fn admin_rekey(State(s): State<AdminState>, Path(tenant): Path<String>) -> AdminResult {
    tracing::info!(%tenant, "vault rekeyed");
    vault_call(
        move || s.vault.rekey(&tenant),
        |n| (StatusCode::OK, Json(json!({ "rekeyed": n }))),
    )
    .await
}

async fn admin_rekey_all(State(s): State<AdminState>) -> AdminResult {
    tracing::info!("vault rekeyed for all tenants");
    vault_call(
        move || s.vault.rekey_all(),
        |(tenants, entries)| {
            (
                StatusCode::OK,
                Json(json!({ "tenants": tenants, "rekeyed": entries })),
            )
        },
    )
    .await
}

/// Tenant-authored HTTP apps are independent of the JSON invocation endpoints.
fn web_routes() -> Router<Engine> {
    Router::new()
        .route("/web/{tenant}", any(web_root))
        .route("/web/{tenant}/", any(web_root))
        .route("/web/{tenant}/{*path}", any(web_path))
        .layer(DefaultBodyLimit::max(1024 * 1024))
}

async fn web_root(
    State(engine): State<Engine>,
    Path(tenant): Path<String>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    Query(query): Query<std::collections::BTreeMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    run_web(
        engine,
        tenant,
        "/".into(),
        method,
        uri.query(),
        query,
        headers,
        body,
    )
    .await
}

async fn web_path(
    State(engine): State<Engine>,
    Path((tenant, path)): Path<(String, String)>,
    method: Method,
    OriginalUri(uri): OriginalUri,
    Query(query): Query<std::collections::BTreeMap<String, String>>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    run_web(
        engine,
        tenant,
        format!("/{path}"),
        method,
        uri.query(),
        query,
        headers,
        body,
    )
    .await
}

#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct WebResponse {
    #[serde(default = "web_ok")]
    status: u16,
    #[serde(default)]
    headers: std::collections::BTreeMap<String, String>,
    #[serde(default)]
    body: String,
}
fn web_ok() -> u16 {
    200
}

fn web_response(value: Value) -> Result<Response, String> {
    let reply: WebResponse =
        serde_json::from_value(value).map_err(|e| format!("invalid web response: {e}"))?;
    if !(200..=599).contains(&reply.status) {
        return Err("web status must be between 200 and 599".into());
    }
    if reply.body.len() > 1024 * 1024 {
        return Err("web response body exceeds 1 MB".into());
    }
    let mut response = Response::new(Body::from(reply.body));
    *response.status_mut() = StatusCode::from_u16(reply.status).map_err(|e| e.to_string())?;
    let headers = response.headers_mut();
    headers.insert("content-type", "text/plain; charset=utf-8".parse().unwrap());
    headers.insert("cache-control", "no-store".parse().unwrap());
    headers.insert("x-content-type-options", "nosniff".parse().unwrap());
    // Framing and hop-by-hop fields belong to the host HTTP server.
    let connection_fields: Vec<String> = reply
        .headers
        .iter()
        .filter(|(name, _)| name.eq_ignore_ascii_case("connection"))
        .flat_map(|(_, value)| value.split(',').map(|v| v.trim().to_ascii_lowercase()))
        .collect();
    for (name, value) in reply.headers {
        let name = axum::http::HeaderName::from_bytes(name.as_bytes())
            .map_err(|e| format!("invalid web header name: {e}"))?;
        let value = axum::http::HeaderValue::from_str(&value)
            .map_err(|e| format!("invalid web header value: {e}"))?;
        if matches!(
            name.as_str(),
            "connection"
                | "content-length"
                | "transfer-encoding"
                | "keep-alive"
                | "te"
                | "trailer"
                | "upgrade"
                | "proxy-authenticate"
                | "proxy-authorization"
        ) || connection_fields.iter().any(|field| field == name.as_str())
        {
            continue;
        }
        headers.insert(name, value);
    }
    Ok(response)
}

#[allow(clippy::too_many_arguments)]
async fn run_web(
    engine: Engine,
    tenant: String,
    path: String,
    method: Method,
    raw_query: Option<&str>,
    query: std::collections::BTreeMap<String, String>,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let body = match std::str::from_utf8(&body) {
        Ok(body) => body,
        Err(_) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error":"web request body must be UTF-8"})),
            )
                .into_response()
        }
    };
    let headers: std::collections::BTreeMap<_, _> = headers
        .iter()
        .filter_map(|(name, value)| value.to_str().ok().map(|value| (name.as_str(), value)))
        .collect();
    let event = json!({"method":method.as_str(), "path":path, "base_path":format!("/web/{tenant}"),
        "query":query, "raw_query":raw_query.unwrap_or(""), "headers":headers, "body":body});
    match engine.invoke_web(&tenant, event).await {
        Ok(Some(value)) => match web_response(value) {
            Ok(response) => response,
            Err(error) => (
                StatusCode::UNPROCESSABLE_ENTITY,
                Json(json!({"error":error})),
            )
                .into_response(),
        },
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error":"tenant has no web app"})),
        )
            .into_response(),
        Err(error) => {
            (status_for(&error), Json(json!({"error":error.to_string()}))).into_response()
        }
    }
}

async fn http_invoke(
    state: State<Engine>,
    Path((tenant, script)): Path<(String, String)>,
    headers: HeaderMap,
    body: Option<Json<Value>>,
) -> (StatusCode, Json<Value>) {
    run_http(state.0, tenant, Some(script), headers, body).await
}

/// No script in the path: the tenant's routes / router script choose one.
async fn http_routed(
    state: State<Engine>,
    Path(tenant): Path<String>,
    headers: HeaderMap,
    body: Option<Json<Value>>,
) -> (StatusCode, Json<Value>) {
    run_http(state.0, tenant, None, headers, body).await
}

async fn run_http(
    engine: Engine,
    tenant: String,
    script: Option<String>,
    headers: HeaderMap,
    body: Option<Json<Value>>,
) -> (StatusCode, Json<Value>) {
    let span = tracing::info_span!(
        "http_request",
        tenant = %tenant,
        script = script.as_deref().unwrap_or("<routed>"),
        otel.kind = "server",
        otel.status_code = tracing::field::Empty,
        http.response.status_code = tracing::field::Empty,
    );
    let continues_trace = integraal_core::telemetry::adopt_parent(&span, |k| {
        headers
            .get(k)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    });
    let status = span.clone();
    let reply = async move {
        let version = headers
            .get("x-script-version")
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        let inv = Invocation {
            capabilities: None,
            tenant,
            script,
            version,
            trigger: Trigger::Http,
            event: body.map(|b| b.0).unwrap_or(Value::Null),
            attempt: 0,
            continues_trace,
        };
        match engine.invoke(inv).await {
            Ok(result) => (StatusCode::OK, Json(result)),
            Err(e) => (status_for(&e), Json(json!({ "error": e.to_string() }))),
        }
    }
    .instrument(span)
    .await;
    status.record("http.response.status_code", reply.0.as_u16());
    if reply.0.is_server_error() {
        status.record("otel.status_code", "ERROR");
    }
    reply
}

fn status_for(e: &Error) -> StatusCode {
    match e {
        Error::Load(integraal_loader::LoadError::NotFound(_)) => StatusCode::NOT_FOUND,
        Error::Load(integraal_loader::LoadError::InvalidName(..)) => StatusCode::BAD_REQUEST,
        Error::Timeout => StatusCode::GATEWAY_TIMEOUT,
        Error::NoRoute
        | Error::Config(_)
        | Error::Script(_)
        | Error::NoHandler(..)
        | Error::BadModule
        | Error::Wrappers(_) => StatusCode::UNPROCESSABLE_ENTITY,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Long-polls SQS with fleet/tenant admission control. A message is deleted only once fully handled, so failures retry via
/// the visibility timeout and eventually land in the queue's DLQ. S3 notifications are split into
/// one invocation per object; when only some fail, just those are sent back (see `s3_events`).
async fn poll_queue(engine: Engine, queue_url: String, s3: S3Events, spread: Spread) {
    let client = aws_sdk_sqs::Client::new(&aws_config::load_from_env().await);
    let (control_tx, mut control) = tokio::sync::watch::channel(None);
    let watcher_engine = engine.clone();
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(Duration::from_secs(10));
        loop {
            tick.tick().await;
            let state = match watcher_engine.queue_pause_state().await {
                Ok(state) => Some(state),
                Err(error) => {
                    tracing::error!(%error, "pause control unavailable; queue admission stopped");
                    None // Fail closed until a successful consistent read.
                }
            };
            if control_tx.send(state).is_err() {
                break;
            }
        }
    });
    let mut receive_task = None;
    let mut receive_slots = 0;
    let mut active = tokio::task::JoinSet::new();
    let admission = Admission::default();
    let mut held: Vec<aws_sdk_sqs::types::Message> = Vec::new();
    let mut maintenance = tokio::time::interval(Duration::from_secs(10));
    loop {
        let snapshot = control.borrow_and_update().clone();
        // Resumed receipts take priority over new receives, avoiding starvation under steady load.
        let mut remaining = Vec::new();
        for message in held.drain(..) {
            let permit = (active.len() + receive_slots < 10)
                .then(|| {
                    admission.acquire(message.body().unwrap_or_default(), &s3, snapshot.as_ref())
                })
                .flatten();
            if let Some(permit) = permit {
                spawn_queue_message(
                    &mut active,
                    &engine,
                    &client,
                    &queue_url,
                    &s3,
                    spread,
                    message,
                    permit,
                );
            } else {
                remaining.push(message);
            }
        }
        held = remaining;
        let can_receive =
            snapshot.as_ref().is_some_and(|s| !s.fleet) && active.len() < 10 && held.len() < 100;
        if receive_task.is_none() && can_receive {
            receive_slots = (10 - active.len()).min(100 - held.len()).max(1);
            let request = client
                .receive_message()
                .queue_url(&queue_url)
                .max_number_of_messages(receive_slots as i32)
                .message_system_attribute_names(MessageSystemAttributeName::ApproximateReceiveCount)
                .message_attribute_names("traceparent")
                .message_attribute_names("tracestate")
                .wait_time_seconds(10);
            receive_task = Some(tokio::spawn(async move { request.send().await }));
        }
        tokio::select! {
            _ = control.changed() => {},
            _ = active.join_next(), if !active.is_empty() => {},
            _ = maintenance.tick() => {
                // Renew held receipts without receiving them again or spending SQS retry attempts.
                for batch in held.chunks(10) {
                    let entries: Vec<_> = batch.iter().enumerate().filter_map(|(i, m)| {
                        m.receipt_handle().map(|receipt| aws_sdk_sqs::types::ChangeMessageVisibilityBatchRequestEntry::builder()
                            .id(i.to_string()).receipt_handle(receipt).visibility_timeout(300).build().unwrap())
                    }).collect();
                    match client.change_message_visibility_batch().queue_url(&queue_url).set_entries(Some(entries)).send().await {
                        Ok(response) if response.failed().is_empty() => {},
                        Ok(response) => tracing::error!(failed = ?response.failed(), "held receipt renewal partially failed"),
                        Err(error) => tracing::error!(%error, "held receipt renewal failed"),
                    }
                }
            },
            response = async { receive_task.as_mut().unwrap().await }, if receive_task.is_some() => {
                receive_task = None;
                receive_slots = 0;
                match response {
                    Ok(Ok(response)) => {
                        let snapshot = control.borrow().clone();
                        for message in response.messages.unwrap_or_default() {
                            let permit = admission.acquire(message.body().unwrap_or_default(), &s3, snapshot.as_ref());
                            if permit.is_none() {
                                if let Some(receipt) = message.receipt_handle() {
                                    if let Err(error) = client.change_message_visibility().queue_url(&queue_url)
                                        .receipt_handle(receipt).visibility_timeout(300).send().await {
                                        tracing::error!(%error, "initial held receipt lease failed");
                                    }
                                }
                                held.push(message);
                            } else {
                                spawn_queue_message(&mut active, &engine, &client, &queue_url, &s3, spread, message, permit.unwrap());
                            }
                        }
                    }
                    Ok(Err(error)) => {
                        tracing::error!(%error, "sqs receive failed");
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                    Err(error) => tracing::error!(%error, "sqs receive task failed"),
                }
            },
        }
    }
}

fn queue_message_paused(
    body: &str,
    s3: &S3Events,
    state: Option<&integraal_core::pause::State>,
) -> bool {
    let Some(state) = state else {
        return true;
    };
    if state.fleet {
        return true;
    }
    match classify(body) {
        Ok(Message::Envelope(inv)) => state.tenant_paused(&inv.tenant),
        Ok(Message::S3 { records, .. }) => records.iter().any(|record| {
            s3.invocation(record)
                .ok()
                .flatten()
                .is_some_and(|inv| state.tenant_paused(&inv.tenant))
        }),
        _ => false,
    }
}

/// Admission is local to this worker. Reserve every tenant in a mixed S3 message
/// until the message finishes; its records are executed sequentially.
#[derive(Default)]
struct Admission(std::sync::Arc<std::sync::Mutex<std::collections::BTreeMap<String, u32>>>);

struct AdmissionPermit {
    counts: std::sync::Arc<std::sync::Mutex<std::collections::BTreeMap<String, u32>>>,
    tenants: std::collections::BTreeSet<String>,
}

impl Admission {
    fn acquire(
        &self,
        body: &str,
        s3: &S3Events,
        state: Option<&integraal_core::pause::State>,
    ) -> Option<AdmissionPermit> {
        if queue_message_paused(body, s3, state) {
            return None;
        }
        let state = state?;
        let tenants: std::collections::BTreeSet<String> = match classify(body) {
            Ok(Message::Envelope(inv)) => [inv.tenant].into_iter().collect(),
            Ok(Message::S3 { records, .. }) => records
                .iter()
                .filter_map(|r| s3.invocation(r).ok().flatten().map(|i| i.tenant))
                .collect(),
            _ => Default::default(),
        };
        let mut counts = self.0.lock().unwrap();
        if tenants.iter().any(|tenant| {
            state
                .max_concurrent
                .get(tenant)
                .is_some_and(|limit| counts.get(tenant).copied().unwrap_or(0) >= *limit)
        }) {
            return None;
        }
        for tenant in &tenants {
            *counts.entry(tenant.clone()).or_default() += 1;
        }
        Some(AdmissionPermit {
            counts: self.0.clone(),
            tenants,
        })
    }
}

impl Drop for AdmissionPermit {
    fn drop(&mut self) {
        let mut counts = self.counts.lock().unwrap();
        for tenant in &self.tenants {
            let count = counts.get_mut(tenant).expect("reserved tenant slot");
            *count -= 1;
            if *count == 0 {
                counts.remove(tenant);
            }
        }
    }
}

fn spawn_queue_message(
    tasks: &mut tokio::task::JoinSet<()>,
    engine: &Engine,
    client: &aws_sdk_sqs::Client,
    queue_url: &str,
    s3: &S3Events,
    spread: Spread,
    msg: aws_sdk_sqs::types::Message,
    permit: AdmissionPermit,
) {
    let (engine, client, queue_url, s3) = (
        engine.clone(),
        client.clone(),
        queue_url.to_string(),
        s3.clone(),
    );
    let span = tracing::info_span!("sqs_message", otel.kind = "consumer");
    let continues_trace = integraal_core::telemetry::adopt_parent(&span, |k| {
        msg.message_attributes()?
            .get(k)?
            .string_value()
            .map(str::to_string)
    });
    tasks.spawn(
        async move {
            let _permit = permit;
            let attempt = msg
                .attributes()
                .and_then(|a| a.get(&MessageSystemAttributeName::ApproximateReceiveCount))
                .and_then(|n| n.parse().ok())
                .unwrap_or(0);
            let acked = handle_message(
                &engine,
                &client,
                &queue_url,
                &s3,
                &spread,
                msg.body().unwrap_or_default(),
                Delivery {
                    attempt,
                    continues_trace,
                },
            )
            .await;
            integraal_core::telemetry::sqs_message(if acked { "acked" } else { "retry" });
            if acked {
                if let Some(receipt) = msg.receipt_handle() {
                    if let Err(error) = client
                        .delete_message()
                        .queue_url(&queue_url)
                        .receipt_handle(receipt)
                        .send()
                        .await
                    {
                        tracing::error!(%error, "sqs acknowledgement failed");
                    }
                }
            }
        }
        .instrument(span),
    );
}

/// How a queue message reached us: its delivery count and whether it carries a trace to continue.
#[derive(Clone, Copy)]
struct Delivery {
    attempt: u32,
    continues_trace: bool,
}

/// Runs one queue message; returns whether it may be deleted.
async fn handle_message(
    engine: &Engine,
    client: &aws_sdk_sqs::Client,
    queue_url: &str,
    s3: &S3Events,
    spread: &Spread,
    body: &str,
    delivery: Delivery,
) -> bool {
    let Delivery {
        attempt,
        continues_trace,
    } = delivery;
    match classify(body) {
        Err(e) => {
            tracing::error!(error = %e, "unparseable queue message");
            false // left for DLQ
        }
        Ok(Message::Ignored) => true,
        Ok(Message::Tick(tick)) => match engine.tick(&tick, spread).await {
            Ok(r) => {
                tracing::debug!(every = tick.every.as_str(), at = %tick.at, sent = r.sent, "tick handled");
                true
            }
            Err(e) => {
                tracing::error!(error = %e, every = tick.every.as_str(), at = %tick.at, "tick failed");
                // The minute tick comes again in a minute; the others are worth running again.
                tick.every == Every::Minute
            }
        },
        Ok(Message::Envelope(mut inv)) => {
            inv.trigger = Trigger::Queue;
            inv.attempt = attempt;
            inv.continues_trace = continues_trace;
            match engine.invoke(inv).await {
                Ok(_) => true,
                Err(e) => {
                    tracing::error!(error = %e, "queue handler failed");
                    false
                }
            }
        }
        Ok(Message::S3 { records, attempt }) => {
            let mut failed = Vec::new();
            for record in records {
                let outcome = match s3.invocation(&record) {
                    Ok(None) => Ok(()),
                    Ok(Some(mut inv)) => {
                        inv.attempt = attempt;
                        inv.continues_trace = continues_trace;
                        engine
                            .invoke(inv)
                            .await
                            .map(|_| ())
                            .map_err(|e| e.to_string())
                    }
                    Err(e) => Err(e),
                };
                if let Err(e) = outcome {
                    tracing::error!(error = %e, "s3 record failed");
                    failed.push(record);
                }
            }
            match disposition(failed, attempt) {
                Disposition::Done => true,
                Disposition::Retry => false,
                Disposition::Requeue { body, delay_secs } => {
                    let sent = client
                        .send_message()
                        .queue_url(queue_url)
                        .message_group_id(integraal_core::message_group_id(&body))
                        .message_body(body)
                        .delay_seconds(delay_secs)
                        .set_message_attributes(Some(
                            integraal_core::telemetry::sqs_attributes(
                                &integraal_core::telemetry::inject_current(),
                            )
                            .into_iter()
                            .collect(),
                        ))
                        .send()
                        .await;
                    if let Err(e) = &sent {
                        tracing::error!(error = %e, "could not requeue failed records; retrying the whole message");
                    }
                    if sent.is_ok() {
                        integraal_core::telemetry::sqs_message("requeued");
                    }
                    sent.is_ok()
                }
            }
        }
    }
}

#[cfg(test)]
mod queue_control_tests {
    use super::*;
    #[test]
    fn tenant_caps_are_local_dynamic_and_release_on_drop() {
        let s3 = S3Events::new([]);
        let admission = Admission::default();
        let other_worker = Admission::default();
        let mut state = integraal_core::pause::State::default();
        let body = |tenant| json!({"tenant":tenant,"event":{}}).to_string();
        // Count existing work even before an override is set.
        let first = admission.acquire(&body("acme"), &s3, Some(&state)).unwrap();
        state.max_concurrent.insert("acme".into(), 2);
        let second = admission.acquire(&body("acme"), &s3, Some(&state)).unwrap();
        assert!(admission
            .acquire(&body("acme"), &s3, Some(&state))
            .is_none());
        assert!(admission
            .acquire(&body("other"), &s3, Some(&state))
            .is_some());
        assert!(other_worker
            .acquire(&body("acme"), &s3, Some(&state))
            .is_some());
        state.max_concurrent.insert("acme".into(), 1);
        drop(first);
        assert!(admission
            .acquire(&body("acme"), &s3, Some(&state))
            .is_none());
        drop(second);
        let third = admission.acquire(&body("acme"), &s3, Some(&state)).unwrap();
        state.max_concurrent.remove("acme");
        assert!(admission
            .acquire(&body("acme"), &s3, Some(&state))
            .is_some());
        assert!(admission.acquire(&body("acme"), &s3, None).is_none());
        drop(third);
        assert!(admission.0.lock().unwrap().is_empty());
    }

    #[test]
    fn mixed_s3_messages_reserve_each_tenant_once_and_do_not_partially_acquire() {
        let s3 = S3Events::new([("In".into(), "input".into(), "tenants/".into())]);
        let admission = Admission::default();
        let mut state = integraal_core::pause::State::default();
        state.max_concurrent.insert("acme".into(), 1);
        let records = json!({"Records":[
            {"eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"input"},"object":{"key":"tenants/other/a","eTag":"a"}}},
            {"eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"input"},"object":{"key":"tenants/acme/b","eTag":"b"}}},
            {"eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"input"},"object":{"key":"tenants/acme/c","eTag":"c"}}}
        ]}).to_string();
        let first = admission.acquire(&records, &s3, Some(&state)).unwrap();
        assert_eq!(admission.0.lock().unwrap().get("acme"), Some(&1));
        assert!(admission.acquire(&records, &s3, Some(&state)).is_none());
        assert_eq!(admission.0.lock().unwrap().get("other"), Some(&1));
        drop(first);
        assert!(admission.0.lock().unwrap().is_empty());
    }

    #[test]
    fn pauses_gate_whole_messages_without_blocking_unrelated_tenants() {
        let s3 = S3Events::new([("In".into(), "input".into(), "tenants/".into())]);
        let mut state = integraal_core::pause::State::default();
        state.tenants.insert("acme".into());
        let envelope = |tenant| json!({"tenant":tenant,"event":{}}).to_string();
        assert!(queue_message_paused(&envelope("acme"), &s3, Some(&state)));
        assert!(!queue_message_paused(&envelope("other"), &s3, Some(&state)));
        assert!(
            queue_message_paused(&envelope("other"), &s3, None),
            "unavailable control fails closed"
        );
        let records = json!({"Records":[
            {"eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"input"},"object":{"key":"tenants/other/a","eTag":"a"}}},
            {"eventName":"ObjectCreated:Put","s3":{"bucket":{"name":"input"},"object":{"key":"tenants/acme/b","eTag":"b"}}}
        ]}).to_string();
        assert!(
            queue_message_paused(&records, &s3, Some(&state)),
            "mixed S3 notifications wait intact"
        );
        state.fleet = true;
        assert!(queue_message_paused(&envelope("other"), &s3, Some(&state)));
        assert!(queue_message_paused("malformed", &s3, Some(&state)));
        state.fleet = false;
        assert!(
            !queue_message_paused("malformed", &s3, Some(&state)),
            "normal error handling still reaches DLQ"
        );
    }
}

#[cfg(test)]
mod web_http_tests {
    use super::*;
    use axum::body::to_bytes;
    use axum::http::Request;
    use integraal_loader::{CachedLoader, MemoryStore, ScriptKey};
    use std::sync::Arc;
    use tower::ServiceExt;

    fn app(source: &str) -> Router {
        let store = MemoryStore::default();
        store.put(
            &ScriptKey::config("acme").unwrap(),
            r#"{"web":{"script":"site","version":"v1"}}"#,
        );
        store.put(&ScriptKey::new("acme", "site", Some("v1")).unwrap(), source);
        store.put(
            &ScriptKey::new("acme", "site", None).unwrap(),
            "error('latest must not run')",
        );
        let engine = Engine::new(
            Arc::new(CachedLoader::new(store, Duration::ZERO)),
            Default::default(),
        );
        web_routes()
            .route("/v1/{tenant}/{script}", post(http_invoke))
            .with_state(engine)
    }

    #[tokio::test]
    async fn tenant_web_apps_receive_http_and_control_the_response() {
        let app = app(r#"return {on_http=function(ctx, req)
            assert(ctx.tenant == "acme" and ctx.trigger == "http")
            assert(req.method == "POST" and req.path == "/hello/world")
            assert(req.base_path == "/web/acme" and req.query.name == "Jane Doe")
            assert(req.raw_query == "name=Jane%20Doe" and req.headers["x-test"] == "value")
            return {status=201, headers={["content-type"]="text/html", ["x-app"]="yes",
                ["content-length"]="999", ["connection"]="x-private", ["x-private"]="remove"},
                body="<h1>" .. req.body .. "</h1>"}
        end}"#);
        let reply = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/web/acme/hello/world?name=Jane%20Doe")
                    .header("x-test", "value")
                    .header("x-script-version", "latest")
                    .body(Body::from("Hello"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(reply.status(), StatusCode::CREATED);
        assert_eq!(reply.headers()["content-type"], "text/html");
        assert_eq!(reply.headers()["x-app"], "yes");
        assert_ne!(
            reply
                .headers()
                .get("content-length")
                .and_then(|h| h.to_str().ok()),
            Some("999")
        );
        assert!(!reply.headers().contains_key("connection"));
        assert!(!reply.headers().contains_key("x-private"));
        assert_eq!(
            to_bytes(reply.into_body(), 1024).await.unwrap(),
            "<h1>Hello</h1>"
        );
        let missing = app
            .oneshot(
                Request::builder()
                    .uri("/web/other")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(missing.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn web_roots_methods_and_response_validation() {
        let app = app(
            r#"return {on_http=function(_, req) return {body=req.method .. " " .. req.path} end}"#,
        );
        for path in ["/web/acme", "/web/acme/"] {
            let reply = app
                .clone()
                .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
                .await
                .unwrap();
            assert_eq!(reply.status(), StatusCode::OK);
            assert_eq!(to_bytes(reply.into_body(), 1024).await.unwrap(), "GET /");
        }
        let reply = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("DELETE")
                    .uri("/web/acme/item")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            to_bytes(reply.into_body(), 1024).await.unwrap(),
            "DELETE /item"
        );
        let reply = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/web/acme")
                    .body(Body::from(vec![0xff]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(reply.status(), StatusCode::BAD_REQUEST);
        let reply = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/web/acme")
                    .body(Body::from(vec![b'x'; 1024 * 1024 + 1]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(reply.status(), StatusCode::PAYLOAD_TOO_LARGE);
        for value in [
            json!({"status":101}),
            json!({"headers":{"x-bad":"bad\r\nvalue"}}),
            json!({"body":42}),
            json!({"body":"x".repeat(1024*1024+1)}),
        ] {
            assert!(web_response(value).is_err());
        }
    }

    #[tokio::test]
    async fn json_invocations_keep_their_existing_contract() {
        let app = app(
            r#"return {on_http=function(_, event) return {status=201, body=event.message} end}"#,
        );
        let reply = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/v1/acme/site")
                    .header("x-script-version", "v1")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"message":"hello"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(reply.status(), StatusCode::OK);
        let body: Value =
            serde_json::from_slice(&to_bytes(reply.into_body(), 1024).await.unwrap()).unwrap();
        assert_eq!(body, json!({"status":201,"body":"hello"}));
    }

    #[tokio::test]
    async fn read_only_example_serves_html_and_rejects_commands() {
        let app = app(include_str!("../../../examples/acme/web/latest.luau"));
        let reply = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/web/acme")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(reply.status(), StatusCode::OK);
        assert!(reply.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("text/html"));
        for method in ["POST", "PUT", "DELETE"] {
            let reply = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri("/web/acme/records")
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(reply.status(), StatusCode::METHOD_NOT_ALLOWED);
        }
        let reply = app
            .oneshot(
                Request::builder()
                    .uri("/web/acme/records")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(reply.status(), StatusCode::BAD_REQUEST);
    }
}
