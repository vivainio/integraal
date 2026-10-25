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
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post, put},
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
        .route("/v1/{tenant}/{script}", post(http_invoke))
        .route("/v1/{tenant}", post(http_routed))
        .with_state(engine);
    let port = cfg.server.port;
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    tracing::info!(%port, "listening");
    axum::serve(listener, app)
        .with_graceful_shutdown(async {
            stop_signal().await;
            // Background http requests die with this process; give their jobs an outcome now rather
            // than after the connections drain, which ECS may not wait for.
            let failed = tokio::task::spawn_blocking(integraal_core::fail_inflight_http)
                .await
                .unwrap_or(0);
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

/// Long-polls SQS. A message is deleted only once it is fully handled, so failures retry via
/// the visibility timeout and eventually land in the queue's DLQ. S3 notifications are split into
/// one invocation per object; when only some fail, just those are sent back (see `s3_events`).
async fn poll_queue(engine: Engine, queue_url: String, s3: S3Events, spread: Spread) {
    let client = aws_sdk_sqs::Client::new(&aws_config::load_from_env().await);
    loop {
        let resp = client
            .receive_message()
            .queue_url(&queue_url)
            .max_number_of_messages(10)
            .message_system_attribute_names(MessageSystemAttributeName::ApproximateReceiveCount)
            .message_attribute_names("traceparent")
            .message_attribute_names("tracestate")
            .wait_time_seconds(10)
            .send()
            .await;
        let messages = match resp {
            Ok(r) => r.messages.unwrap_or_default(),
            Err(e) => {
                tracing::error!(error = %e, "sqs receive failed");
                tokio::time::sleep(Duration::from_secs(5)).await;
                continue;
            }
        };
        let mut tasks = Vec::new();
        for msg in messages {
            let (engine, client, queue_url, s3) = (
                engine.clone(),
                client.clone(),
                queue_url.clone(),
                s3.clone(),
            );
            let span = tracing::info_span!("sqs_message", otel.kind = "consumer");
            let continues_trace = integraal_core::telemetry::adopt_parent(&span, |k| {
                msg.message_attributes()?
                    .get(k)?
                    .string_value()
                    .map(str::to_string)
            });
            tasks.push(tokio::spawn(
                async move {
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
                        attempt,
                        continues_trace,
                    )
                    .await;
                    integraal_core::telemetry::sqs_message(if acked { "acked" } else { "retry" });
                    if acked {
                        if let Some(handle) = msg.receipt_handle() {
                            let _ = client
                                .delete_message()
                                .queue_url(&queue_url)
                                .receipt_handle(handle)
                                .send()
                                .await;
                        }
                    }
                }
                .instrument(span),
            ));
        }
        for t in tasks {
            let _ = t.await;
        }
    }
}

/// Runs one queue message; returns whether it may be deleted.
async fn handle_message(
    engine: &Engine,
    client: &aws_sdk_sqs::Client,
    queue_url: &str,
    s3: &S3Events,
    spread: &Spread,
    body: &str,
    attempt: u32,
    continues_trace: bool,
) -> bool {
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
