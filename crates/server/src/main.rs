//! Fargate entrypoint. Serves HTTP (ALB target) and polls SQS, both feeding the same engine.
//!
//! HTTP:  POST /v1/{tenant}/{script}   (optional `x-script-version` header; body is the event)
//!        POST /v1/{tenant}            (script chosen by the tenant's routes / router)
//! Queue: SQS message body is `{tenant, script?, version?, event}`; no `script` means routed
//!
//! Config: `--config <path>` or `./integraal.json` (see `GlobalConfig`); AWS credentials/region come from the usual AWS variables.

use std::time::Duration;

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use integraal_core::{
    s3_events::{classify, disposition, Disposition, Message},
    Engine, Error, GlobalConfig, Invocation, S3Events, Trigger,
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
