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

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post, put},
    Json, Router,
};
use integraal_core::{
    s3_events::{classify, disposition, Disposition, Message},
    Engine, Error, GlobalConfig, Invocation, S3Events, Trigger, Vault,
};
use serde_json::{json, Value};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let cfg = GlobalConfig::load().map_err(anyhow::Error::msg)?;
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(tracing_subscriber::EnvFilter::try_new(cfg.log_filter())?)
        .init();

    let mut engine = cfg.engine().await.map_err(anyhow::Error::msg)?;
    if let Some(throttle) = cfg.throttle().map_err(anyhow::Error::msg)? {
        engine = engine.with_throttle(throttle);
    }

    if let Some(queue) = &cfg.queue {
        tokio::spawn(poll_queue(
            engine.clone(),
            queue.url.clone(),
            S3Events::from_config(&cfg),
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
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}

#[derive(Clone)]
struct AdminState {
    vault: Vault,
    token: std::sync::Arc<str>,
}

/// Credential vault API. Every request needs `Authorization: Bearer <admin token>`.
///
/// - `GET    /admin/vault/{tenant}`            names and allowed URLs (never values)
/// - `PUT    /admin/vault/{tenant}/{name}`     `{"value": <any JSON>, "allow": ["https://api.example.com/v1/"]}`
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
        move || s.vault.put(&tenant, &name, &body.value, &body.allow),
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
    };
    match engine.invoke(inv).await {
        Ok(result) => (StatusCode::OK, Json(result)),
        Err(e) => (status_for(&e), Json(json!({ "error": e.to_string() }))),
    }
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
async fn poll_queue(engine: Engine, queue_url: String, s3: S3Events) {
    let client = aws_sdk_sqs::Client::new(&aws_config::load_from_env().await);
    loop {
        let resp = client
            .receive_message()
            .queue_url(&queue_url)
            .max_number_of_messages(10)
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
            tasks.push(tokio::spawn(async move {
                let acked = handle_message(
                    &engine,
                    &client,
                    &queue_url,
                    &s3,
                    msg.body().unwrap_or_default(),
                )
                .await;
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
            }));
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
    body: &str,
) -> bool {
    match classify(body) {
        Err(e) => {
            tracing::error!(error = %e, "unparseable queue message");
            false // left for DLQ
        }
        Ok(Message::Ignored) => true,
        Ok(Message::Envelope(mut inv)) => {
            inv.trigger = Trigger::Queue;
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
                    Ok(Some(inv)) => engine
                        .invoke(inv)
                        .await
                        .map(|_| ())
                        .map_err(|e| e.to_string()),
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
                        .send()
                        .await;
                    if let Err(e) = &sent {
                        tracing::error!(error = %e, "could not requeue failed records; retrying the whole message");
                    }
                    sent.is_ok()
                }
            }
        }
    }
}
