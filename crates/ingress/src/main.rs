//! Intake Lambda: consumes the intake queue in large batches and forwards each message, unchanged,
//! to the job queue of the worker size class its tenant is placed in (see `docs/ingress.md`).
//!
//! Wire it to the intake queue with an SQS event source mapping that has a large `BatchSize`, a
//! `MaximumBatchingWindowInSeconds`, a `MaximumConcurrency`, and `ReportBatchItemFailures` enabled.
//! Senders may also write to a job queue directly and skip this function.
//!
//! Config: `integraal.json` bundled next to `bootstrap`; uses its `scripts`, `stores` and `ingress`
//! sections.

use std::{collections::BTreeSet, time::Duration};

use integraal_core::telemetry::TraceContext;
use integraal_core::{
    ingress::{batches, plan, Incoming, Outgoing},
    GlobalConfig, IngressConfig, S3Events,
};
use lambda_runtime::{service_fn, Error, LambdaEvent};
use serde_json::{json, Value};

/// Time kept back for sending after placement, and for returning the result.
const SEND_RESERVE: Duration = Duration::from_secs(5);
/// The most the batch's placement scripts may spend altogether.
const MAX_PLACE_BUDGET: Duration = Duration::from_secs(60);
/// SQS batch requests in flight at once.
const SEND_CONCURRENCY: usize = 16;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let cfg = GlobalConfig::load()?;
    let telemetry = std::sync::Arc::new(integraal_core::telemetry::init(
        "integraal-ingress",
        cfg.log_filter(),
    )?);

    let ingress = cfg
        .ingress
        .clone()
        .ok_or("config needs an ingress section")?;
    let engine = cfg.engine().await?;
    let s3 = S3Events::from_config(&cfg);
    let sqs = aws_sdk_sqs::Client::new(&aws_config::load_from_env().await);

    lambda_runtime::run(service_fn(|ev: LambdaEvent<Value>| {
        let (engine, s3, sqs, ingress, telemetry) = (
            engine.clone(),
            s3.clone(),
            sqs.clone(),
            ingress.clone(),
            telemetry.clone(),
        );
        async move {
            let incoming = parse(&ev.payload);
            let now_ms = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_millis() as u64;
            let left = Duration::from_millis(ev.context.deadline.saturating_sub(now_ms));
            let budget = left
                .saturating_sub(SEND_RESERVE)
                .clamp(Duration::from_secs(1), MAX_PLACE_BUDGET);

            let out = plan(
                &engine,
                &ingress,
                &s3,
                &incoming,
                &ingress.place_options(budget),
            )
            .await;
            let failed = send(&sqs, &ingress, &out).await;
            tracing::info!(
                received = incoming.len(),
                forwarded = out.len(),
                failed = failed.len(),
                "batch done"
            );
            let failures: Vec<Value> = failed
                .into_iter()
                .map(|id| json!({ "itemIdentifier": id }))
                .collect();
            telemetry.flush(); // the environment may freeze as soon as we return
            Ok::<_, Error>(json!({ "batchItemFailures": failures }))
        }
    }))
    .await
}

/// The messages of an SQS event.
fn parse(event: &Value) -> Vec<Incoming> {
    event["Records"]
        .as_array()
        .into_iter()
        .flatten()
        .filter_map(|r| {
            Some(Incoming {
                id: r["messageId"].as_str()?.to_string(),
                body: r["body"].as_str()?.to_string(),
                trace: ["traceparent", "tracestate"]
                    .into_iter()
                    .filter_map(|k| {
                        let v = r["messageAttributes"][k]["stringValue"].as_str()?;
                        Some((k.to_string(), v.to_string()))
                    })
                    .collect(),
            })
        })
        .collect()
}

/// Sends everything; returns the intake messages with at least one message that could not be sent.
/// Those are redelivered, so a message that was partly forwarded may be forwarded twice.
async fn send(
    sqs: &aws_sdk_sqs::Client,
    cfg: &IngressConfig,
    out: &[Outgoing],
) -> BTreeSet<String> {
    let mut failed = BTreeSet::new();
    let all = batches(out);
    for window in all.chunks(SEND_CONCURRENCY) {
        let mut set = tokio::task::JoinSet::new();
        for (class, indices) in window {
            let Some(url) = cfg.queues.get(class).cloned() else {
                failed.extend(indices.iter().map(|&i| out[i].source.clone()));
                continue;
            };
            let entries: Vec<(usize, String, TraceContext)> = indices
                .iter()
                .map(|&i| (i, out[i].body.clone(), out[i].trace.clone()))
                .collect();
            let sqs = sqs.clone();
            set.spawn(async move { send_batch(&sqs, &url, entries).await });
        }
        while let Some(done) = set.join_next().await {
            match done {
                Ok(bad) => failed.extend(bad.into_iter().map(|i| out[i].source.clone())),
                Err(e) => {
                    // The task is lost with its indices, so nothing in this window can be trusted.
                    tracing::error!(error = %e, "send task failed");
                    for (_, indices) in window {
                        failed.extend(indices.iter().map(|&i| out[i].source.clone()));
                    }
                }
            }
        }
    }
    failed
}

/// One `SendMessageBatch`; returns the indices (into `out`) that were not accepted.
async fn send_batch(
    sqs: &aws_sdk_sqs::Client,
    url: &str,
    entries: Vec<(usize, String, TraceContext)>,
) -> Vec<usize> {
    use aws_sdk_sqs::types::SendMessageBatchRequestEntry;
    let all: Vec<usize> = entries.iter().map(|(i, ..)| *i).collect();
    let mut request = sqs.send_message_batch().queue_url(url);
    for (i, body, trace) in entries {
        match SendMessageBatchRequestEntry::builder()
            .id(i.to_string())
            .message_body(body)
            .set_message_attributes(Some(
                integraal_core::telemetry::sqs_attributes(&trace)
                    .into_iter()
                    .collect(),
            ))
            .build()
        {
            Ok(e) => request = request.entries(e),
            Err(e) => {
                tracing::error!(error = %e, "bad batch entry");
                return all;
            }
        }
    }
    match request.send().await {
        Ok(resp) => resp
            .failed()
            .iter()
            .filter_map(|f| {
                tracing::error!(
                    code = f.code(),
                    message = f.message().unwrap_or_default(),
                    "entry not sent"
                );
                f.id().parse().ok()
            })
            .collect(),
        Err(e) => {
            tracing::error!(error = %e, queue = url, "send batch failed");
            all
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn trace_context_comes_from_message_attributes() {
        let ev = json!({"Records": [{"messageId": "a", "body": "{}", "messageAttributes": {
            "traceparent": {"stringValue": "00-aa-bb-01", "dataType": "String"}}}]});
        assert_eq!(parse(&ev)[0].trace["traceparent"], "00-aa-bb-01");
    }

    #[test]
    fn sqs_event_records_become_incoming_messages() {
        let ev = json!({"Records": [
            {"messageId": "a", "body": "{}"},
            {"messageId": "b"},
            {"body": "no id"},
            {"messageId": "c", "body": "x"},
        ]});
        let ids: Vec<_> = parse(&ev).into_iter().map(|m| m.id).collect();
        assert_eq!(ids, ["a", "c"]);
        assert!(parse(&json!({})).is_empty());
    }
}
