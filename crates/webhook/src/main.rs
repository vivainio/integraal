//! Webhook ingress (Lambda Function URL, payload format 2.0). Public edge for callbacks:
//! it never runs scripts, it only turns a request into a queue message for the Fargate poller.
//!
//!   POST /cb/{tenant}/{token}  verifies the signed token, then consumes the single-use callback and enqueues
//!                           `{tenant, script?, event:{method, path, query, headers, body, body_base64, state}}`
//!
//! Replies: 202 enqueued, 404 invalid / unknown / already used, 410 expired, 413 body too large, 500 enqueue failed
//! (the callback is restored, so the sender's retry can succeed).
//!
//! Config: `integraal.json` bundled next to `bootstrap`; uses its `db` and `queue` sections.

use std::sync::Arc;

use integraal_core::{DynamoBackend, GlobalConfig};
use integraal_loader::ScriptKey;
use integraal_wrappers::{CallbackSigner, DbScope, Take};
use lambda_runtime::{service_fn, Error, LambdaEvent};
use serde_json::{json, Value};

/// SQS allows 256 KiB per message; leave room for headers and the envelope.
const MAX_BODY_BYTES: usize = 200 * 1024;

fn reply(status: u16, body: Value) -> Value {
    json!({ "statusCode": status, "headers": { "content-type": "application/json" }, "body": body.to_string() })
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let cfg = GlobalConfig::load()?;
    let telemetry = Arc::new(integraal_core::telemetry::init(
        "integraal-webhook",
        cfg.log_filter(),
    )?);

    let table = cfg
        .db
        .as_ref()
        .ok_or("config needs a db section")?
        .table
        .clone();
    let queue_url = cfg
        .queue
        .as_ref()
        .ok_or("config needs a queue section")?
        .url
        .clone();
    let db: Arc<dyn integraal_wrappers::ItemBackend> =
        Arc::new(DynamoBackend::from_env(table).await);
    let signer = Arc::new(CallbackSigner::new(db.clone()));
    let sqs = aws_sdk_sqs::Client::new(&aws_config::load_from_env().await);

    lambda_runtime::run(service_fn(|ev: LambdaEvent<Value>| {
        let (db, signer, sqs, queue_url, telemetry) = (
            db.clone(),
            signer.clone(),
            sqs.clone(),
            queue_url.clone(),
            telemetry.clone(),
        );
        async move {
            let out = handle(ev.payload, db, signer, &sqs, &queue_url).await;
            telemetry.flush(); // the environment may freeze as soon as we return
            Ok::<_, Error>(out)
        }
    }))
    .await
}

async fn handle(
    req: Value,
    backend: Arc<dyn integraal_wrappers::ItemBackend>,
    signer: Arc<CallbackSigner>,
    sqs: &aws_sdk_sqs::Client,
    queue_url: &str,
) -> Value {
    let path = req["rawPath"].as_str().unwrap_or_default();
    let method = req["requestContext"]["http"]["method"]
        .as_str()
        .unwrap_or_default();
    let segs: Vec<&str> = path.trim_matches('/').split('/').collect();
    let ["cb", tenant, token] = segs[..] else {
        return reply(404, json!({ "error": "not found" }));
    };
    if method != "POST" {
        return reply(405, json!({ "error": "use POST" }));
    }
    if ScriptKey::config(tenant).is_err() {
        return reply(404, json!({ "error": "not found" }));
    }
    let body = req["body"].as_str().unwrap_or_default();
    if body.len() > MAX_BODY_BYTES {
        return reply(413, json!({ "error": "body too large" })); // checked before consuming the callback
    }

    let scope = DbScope::new(backend, tenant).with_signer(signer);
    let taken = tokio::task::spawn_blocking({
        let (scope, token) = (scope.clone(), token.to_string());
        move || scope.take_callback(&token)
    })
    .await;
    let taken = match taken {
        Ok(Ok(Take::Found(t))) => t,
        Ok(Ok(Take::Invalid | Take::Gone)) => return reply(404, json!({ "error": "not found" })),
        Ok(Ok(Take::Expired)) => return reply(410, json!({ "error": "expired" })),
        Ok(Err(e)) => {
            tracing::error!(%tenant, error = %e, "callback lookup failed");
            return reply(500, json!({ "error": "internal error" }));
        }
        Err(e) => {
            tracing::error!(%tenant, error = %e, "callback lookup panicked");
            return reply(500, json!({ "error": "internal error" }));
        }
    };
    let cb = &taken.callback;

    let message = json!({
        "tenant": tenant,
        "script": cb.script,
        "event": {
            "method": method,
            "path": path,
            "query": req["queryStringParameters"].clone(),
            "headers": req["headers"].clone(),
            "body": body,
            "body_base64": req["isBase64Encoded"].as_bool().unwrap_or(false),
            "state": cb.state,
        },
    });
    match sqs
        .send_message()
        .queue_url(queue_url)
        .message_body(message.to_string())
        .set_message_attributes(Some(
            integraal_core::telemetry::sqs_attributes(&integraal_core::telemetry::inject_current())
                .into_iter()
                .collect(),
        ))
        .send()
        .await
    {
        Ok(_) => reply(202, json!({ "accepted": true })),
        Err(e) => {
            tracing::error!(%tenant, error = %e, "enqueue failed; restoring callback");
            let restored = tokio::task::spawn_blocking(move || scope.restore_callback(taken)).await;
            if !matches!(restored, Ok(Ok(()))) {
                tracing::error!(%tenant, "callback could not be restored; it is lost");
            }
            reply(500, json!({ "error": "could not enqueue" }))
        }
    }
}
