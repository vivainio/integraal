//! Fargate entrypoint. Serves HTTP (ALB target) and polls SQS, both feeding the same engine.
//!
//! HTTP:  POST /v1/{tenant}/{script}   (optional `x-script-version` header; body is the event)
//! Queue: SQS message body is `{tenant, script, version?, event}`
//!
//! Env: SCRIPT_BUCKET, [SCRIPT_PREFIX], [DATA_BUCKET, DATA_PREFIX], [SQS_QUEUE_URL], [PORT=8080], [AWS_ENDPOINT_URL]

use std::{sync::Arc, time::Duration};

use axum::{
    extract::{Path, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
    Json, Router,
};
use integraal_core::{Engine, Error, Invocation, Limits, Trigger};
use integraal_loader::{CachedLoader, S3Store};
use serde_json::{json, Value};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().json().with_env_filter(tracing_subscriber::EnvFilter::from_default_env()).init();

    let store = S3Store::from_env(std::env::var("SCRIPT_BUCKET")?, std::env::var("SCRIPT_PREFIX").unwrap_or_default()).await;
    let engine = Engine::new(Arc::new(CachedLoader::new(store, Duration::from_secs(30))), Limits::default());
    let engine = match std::env::var("DATA_BUCKET") {
        Ok(bucket) => engine.with_data(
            Arc::new(integraal_core::S3Backend::from_env(bucket).await),
            std::env::var("DATA_PREFIX").unwrap_or_default(),
        ),
        Err(_) => engine,
    };

    if let Ok(queue_url) = std::env::var("SQS_QUEUE_URL") {
        tokio::spawn(poll_queue(engine.clone(), queue_url));
    }

    let app = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/v1/{tenant}/{script}", post(http_invoke))
        .with_state(engine);
    let port = std::env::var("PORT").unwrap_or_else(|_| "8080".into());
    let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{port}")).await?;
    tracing::info!(%port, "listening");
    axum::serve(listener, app).with_graceful_shutdown(async { let _ = tokio::signal::ctrl_c().await; }).await?;
    Ok(())
}

async fn http_invoke(
    State(engine): State<Engine>,
    Path((tenant, script)): Path<(String, String)>,
    headers: HeaderMap,
    body: Option<Json<Value>>,
) -> (StatusCode, Json<Value>) {
    let version = headers.get("x-script-version").and_then(|v| v.to_str().ok()).map(str::to_string);
    let inv = Invocation { tenant, script, version, trigger: Trigger::Http, event: body.map(|b| b.0).unwrap_or(Value::Null) };
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
        Error::Script(_) | Error::NoHandler(..) | Error::BadModule | Error::Wrappers(_) => StatusCode::UNPROCESSABLE_ENTITY,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Long-polls SQS. A message is deleted only on success, so failures retry via
/// the visibility timeout and eventually land in the queue's DLQ.
async fn poll_queue(engine: Engine, queue_url: String) {
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
            let (engine, client, queue_url) = (engine.clone(), client.clone(), queue_url.clone());
            tasks.push(tokio::spawn(async move {
                let parsed: Result<Invocation, _> = serde_json::from_str(msg.body().unwrap_or_default());
                let Ok(mut inv) = parsed else {
                    tracing::error!("unparseable queue message");
                    return; // left for DLQ
                };
                inv.trigger = Trigger::Queue;
                match engine.invoke(inv).await {
                    Ok(_) => {
                        if let Some(handle) = msg.receipt_handle() {
                            let _ = client.delete_message().queue_url(&queue_url).receipt_handle(handle).send().await;
                        }
                    }
                    Err(e) => tracing::error!(error = %e, "queue handler failed"),
                }
            }));
        }
        for t in tasks {
            let _ = t.await;
        }
    }
}
