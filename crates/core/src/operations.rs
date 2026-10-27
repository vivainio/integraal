//! Operator-only Luau API: queue admission control, flow inspection and audited DLQ replay.
use crate::{flow_log, Engine};
use anyhow::{anyhow, ensure, Context, Result};
use aws_sdk_sqs::{
    types::{Message, MessageSystemAttributeName},
    Client,
};
use mlua::LuaSerdeExt;
use serde::Deserialize;
use serde_json::{json, Value};

fn trace_id(message: &Message) -> Option<String> {
    let parent = message
        .message_attributes()?
        .get("traceparent")?
        .string_value()?;
    let id = parent.split('-').nth(1)?;
    (id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit())).then(|| id.to_ascii_lowercase())
}

fn view(message: &Message) -> Value {
    json!({
        "message_id": message.message_id(),
        "body": message.body(),
        "trace_id": trace_id(message),
        "attributes": message.attributes().map(|a| a.iter().map(|(k,v)| (k.as_str(), v)).collect::<std::collections::BTreeMap<_,_>>()),
        "traceparent": message.message_attributes().and_then(|a| a.get("traceparent")).and_then(|a| a.string_value()),
    })
}

async fn release(client: &Client, url: &str, message: &Message) -> Result<()> {
    client
        .change_message_visibility()
        .queue_url(url)
        .receipt_handle(
            message
                .receipt_handle()
                .ok_or_else(|| anyhow!("missing receipt"))?,
        )
        .visibility_timeout(0)
        .send()
        .await?;
    Ok(())
}

trait ReplayQueue {
    async fn send(&self, message: &Message) -> Result<Option<String>>;
    async fn delete(&self, message: &Message) -> Result<()>;
}

struct SqsReplay<'a> {
    client: &'a Client,
    source: &'a str,
    destination: &'a str,
}

impl ReplayQueue for SqsReplay<'_> {
    async fn send(&self, message: &Message) -> Result<Option<String>> {
        let body = message
            .body()
            .ok_or_else(|| anyhow!("missing message body"))?;
        let mut request = self
            .client
            .send_message()
            .queue_url(self.destination)
            .message_body(body)
            .set_message_attributes(message.message_attributes().cloned());
        let group = message
            .attributes()
            .and_then(|a| a.get(&MessageSystemAttributeName::MessageGroupId))
            .cloned();
        if let Some(group) = group.as_ref() {
            request = request.message_group_id(group);
        }
        if self.destination.ends_with(".fifo") {
            let group = group.unwrap_or_else(|| crate::message_group_id(body));
            // A fresh deduplication id ensures an explicit replay is not silently suppressed.
            request = request
                .message_group_id(group)
                .message_deduplication_id(format!(
                    "replay-{}-{}",
                    message.message_id().unwrap_or("unknown"),
                    now_ms()
                ));
        }
        Ok(request.send().await?.message_id().map(str::to_string))
    }

    async fn delete(&self, message: &Message) -> Result<()> {
        self.client
            .delete_message()
            .queue_url(self.source)
            .receipt_handle(
                message
                    .receipt_handle()
                    .ok_or_else(|| anyhow!("missing receipt"))?,
            )
            .send()
            .await?;
        Ok(())
    }
}

async fn replay_message(
    queue: &impl ReplayQueue,
    audit_sink: &impl ReplayAudit,
    mut record: Value,
    message: &Message,
) -> Result<Value> {
    record["operator"] = std::env::var("USER").unwrap_or_default().into();
    record["at_ms"] = now_ms().into();
    if record["replay_id"].is_null() {
        record["replay_id"] = format!(
            "{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        )
        .into();
    }
    record["status"] = "requested".into();
    audit_sink.write(&record).await?;
    let sent = match queue.send(message).await {
        Ok(sent) => sent,
        Err(error) => {
            record["status"] = "send_failed".into();
            record["error"] = error.to_string().into();
            audit_sink.write(&record).await?;
            return Err(error.context("replay send failed or response lost; original retained; check destination before retrying"));
        }
    };
    record["status"] = "sent".into();
    record["sent_message_id"] = json!(sent);
    audit_sink
        .write(&record)
        .await
        .context("replay sent but audit failed; original retained; duplicate possible")?;
    if let Err(error) = queue.delete(message).await {
        record["status"] = "delete_failed".into();
        record["error"] = error.to_string().into();
        audit_sink.write(&record).await?;
        return Err(error.context(
            "replay sent but DLQ delete failed; duplicate possible; inspect audit before retrying",
        ));
    }
    record["status"] = "completed".into();
    audit_sink
        .write(&record)
        .await
        .context("replay sent and original deleted but final audit failed; do not replay again")?;
    Ok(record)
}

async fn inspection(message: &Message, sink: Option<flow_log::FlowLogSink>) -> Result<Value> {
    let mut value = view(message);
    value["invocations"] = Value::Null;
    value["failure_context"] =
        "requires retained flow logs, a tenant envelope and traceparent".into();
    if let (Some(sink), Some(trace), Some(tenant)) = (
        sink,
        trace_id(message),
        message
            .body()
            .and_then(|b| serde_json::from_str::<Value>(b).ok())
            .and_then(|v| v["tenant"].as_str().map(str::to_string)),
    ) {
        value["failure_context"] = "retained flow log invocation summaries".into();
        value["invocations"] = tokio::task::spawn_blocking(move || {
            sink.read_flow(&tenant, &trace)
                .map(|lines| flow_log::summarize(&lines))
        })
        .await?
        .unwrap_or_else(|e| json!({"error": e}));
    }
    Ok(value)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

trait ReplayAudit {
    async fn write(&self, record: &Value) -> Result<()>;
}
/// Deployment-owned allowlist and audit destination for directly invoked operator operations.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperationsConfig {
    pub operator_tenant: String,
    #[serde(default)]
    pub queues: std::collections::BTreeMap<String, OperatorQueue>,
    pub audit_bucket: String,
    #[serde(default)]
    pub audit_prefix: String,
}
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct OperatorQueue {
    pub dlq_url: String,
    pub destination_url: String,
}

/// Internal commands issued by the imported operator module.
#[derive(Debug)]
enum Request {
    Pause {
        tenant: Option<String>,
        paused: bool,
        reason: String,
    },
    PauseStatus,
    FlowInspect {
        tenant: String,
        trace_id: String,
    },
    DlqInspect {
        queue: String,
    },
    DlqReplay {
        queue: String,
        message_id: String,
        reason: String,
    },
}

struct S3Audit {
    client: aws_sdk_s3::Client,
    bucket: String,
    prefix: String,
}
impl ReplayAudit for S3Audit {
    async fn write(&self, record: &Value) -> Result<()> {
        let id = record["replay_id"]
            .as_str()
            .ok_or_else(|| anyhow!("missing replay id"))?;
        let status = record["status"]
            .as_str()
            .ok_or_else(|| anyhow!("missing audit status"))?;
        self.client
            .put_object()
            .bucket(&self.bucket)
            .key(format!("{}{id}/{status}.json", self.prefix))
            .if_none_match("*")
            .content_type("application/json")
            .body(serde_json::to_vec(record)?.into())
            .send()
            .await?;
        Ok(())
    }
}

async fn invoke(
    engine: &Engine,
    config: &OperationsConfig,
    request: Request,
    request_id: &str,
) -> Result<Value> {
    ensure!(
        !request_id.is_empty()
            && request_id
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-'),
        "invalid request id"
    );
    match &request {
        Request::Pause {
            tenant,
            paused,
            reason,
        } => {
            let backend = engine
                .item_backend()
                .ok_or_else(|| anyhow!("pause control requires db"))?;
            let (tenant, paused, reason, id) = (
                tenant.clone(),
                *paused,
                reason.clone(),
                request_id.to_string(),
            );
            return tokio::task::spawn_blocking(move || {
                crate::pause::change(backend.as_ref(), tenant.as_deref(), paused, &reason, &id)
                    .and_then(|state| serde_json::to_value(state).map_err(anyhow::Error::from))
            })
            .await?;
        }
        Request::PauseStatus => {
            return serde_json::to_value(engine.queue_pause_state().await?)
                .map_err(anyhow::Error::from)
        }
        _ => {}
    }
    if let Request::FlowInspect { tenant, trace_id } = request {
        let sink = engine
            .flow_logs()
            .cloned()
            .ok_or_else(|| anyhow!("flow_logs not configured"))?;
        let engine = engine.clone();
        return tokio::task::spawn_blocking(move || {
            let lines = sink
                .read_flow(&tenant, &trace_id)
                .map_err(anyhow::Error::msg)?;
            let state = engine
                .pending(&tenant, &trace_id)
                .map_err(anyhow::Error::msg)?;
            Ok(json!({"invocations": flow_log::summarize(&lines), "state": state}))
        })
        .await?;
    }
    let (queue_name, replay) = match request {
        Request::DlqInspect { queue } => (queue, None),
        Request::DlqReplay {
            queue,
            message_id,
            reason,
        } => {
            ensure!(!reason.trim().is_empty(), "replay reason must not be empty");
            ensure!(!message_id.is_empty(), "message_id must not be empty");
            (queue, Some((message_id, reason)))
        }
        Request::FlowInspect { .. } | Request::Pause { .. } | Request::PauseStatus => {
            unreachable!()
        }
    };
    let queue = config
        .queues
        .get(&queue_name)
        .ok_or_else(|| anyhow!("unknown operator queue"))?;
    let aws = aws_config::load_from_env().await;
    let client = Client::new(&aws);
    let received = client
        .receive_message()
        .queue_url(&queue.dlq_url)
        .max_number_of_messages(10)
        .visibility_timeout(300)
        .wait_time_seconds(1)
        .message_system_attribute_names(MessageSystemAttributeName::All)
        .message_attribute_names("All")
        .send()
        .await?;
    let selected = replay.as_ref().and_then(|(id, _)| {
        received
            .messages()
            .iter()
            .position(|m| m.message_id() == Some(id.as_str()))
    });
    for (index, message) in received.messages().iter().enumerate() {
        if selected != Some(index) {
            release(&client, &queue.dlq_url, message).await?;
        }
    }
    if let Some((message_id, reason)) = replay {
        let index = selected.ok_or_else(|| {
            anyhow!("message {message_id} not in this bounded sample; nothing replayed")
        })?;
        let message = &received.messages()[index];
        let mut record = inspection(message, engine.flow_logs().cloned()).await?;
        record["replay_id"] = request_id.into();
        record["source"] = queue.dlq_url.clone().into();
        record["destination"] = queue.destination_url.clone().into();
        record["reason"] = reason.into();
        let audit = S3Audit {
            client: aws_sdk_s3::Client::new(&aws),
            bucket: config.audit_bucket.clone(),
            prefix: config.audit_prefix.clone(),
        };
        let replay_queue = SqsReplay {
            client: &client,
            source: &queue.dlq_url,
            destination: &queue.destination_url,
        };
        let result = replay_message(&replay_queue, &audit, record, message).await;
        if result.is_err() {
            let _ = release(&client, &queue.dlq_url, message).await;
        }
        result
    } else {
        let mut values = Vec::new();
        for message in received.messages() {
            values.push(inspection(message, engine.flow_logs().cloned()).await?);
        }
        Ok(json!({"messages": values, "sample_limit": 10}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::GlobalConfig;
    use std::cell::RefCell;

    fn message() -> Message {
        Message::builder()
            .message_id("original")
            .body(r#"{ "tenant":"acme", "event":{"flow":"f", "member":"m"} }"#)
            .message_attributes(
                "traceparent",
                aws_sdk_sqs::types::MessageAttributeValue::builder()
                    .data_type("String")
                    .string_value("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01")
                    .build()
                    .unwrap(),
            )
            .build()
    }

    #[test]
    fn inspection_preserves_body_and_extracts_flow_identity() {
        let message = message();
        let value = view(&message);
        assert_eq!(value["body"], message.body().unwrap());
        assert_eq!(value["trace_id"], "4bf92f3577b34da6a3ce929d0e0e4736");
        assert!(trace_id(&Message::builder().build()).is_none());
    }

    struct FakeQueue {
        calls: RefCell<Vec<&'static str>>,
        fail_send: bool,
        fail_delete: bool,
    }
    impl ReplayQueue for FakeQueue {
        async fn send(&self, message: &Message) -> Result<Option<String>> {
            self.calls.borrow_mut().push("send");
            assert_eq!(message.body(), self::message().body());
            assert_eq!(
                message.message_attributes(),
                self::message().message_attributes()
            );
            ensure!(!self.fail_send, "send failed");
            Ok(Some("new-id".into()))
        }
        async fn delete(&self, _: &Message) -> Result<()> {
            self.calls.borrow_mut().push("delete");
            ensure!(!self.fail_delete, "delete failed");
            Ok(())
        }
    }
    struct MemoryAudit {
        records: RefCell<Vec<Value>>,
        fail_at: Option<usize>,
    }
    impl ReplayAudit for MemoryAudit {
        async fn write(&self, record: &Value) -> Result<()> {
            ensure!(
                self.fail_at != Some(self.records.borrow().len()),
                "audit unavailable"
            );
            self.records.borrow_mut().push(record.clone());
            Ok(())
        }
    }

    #[tokio::test]
    async fn replay_audits_and_deletes_only_after_success() {
        let message = message();
        for (fail_send, fail_delete, statuses, calls) in [
            (
                false,
                false,
                vec!["requested", "sent", "completed"],
                vec!["send", "delete"],
            ),
            (true, false, vec!["requested", "send_failed"], vec!["send"]),
            (
                false,
                true,
                vec!["requested", "sent", "delete_failed"],
                vec!["send", "delete"],
            ),
        ] {
            let queue = FakeQueue {
                calls: Default::default(),
                fail_send,
                fail_delete,
            };
            let audit = MemoryAudit {
                records: Default::default(),
                fail_at: None,
            };
            let result = replay_message(&queue, &audit, view(&message), &message).await;
            assert_eq!(result.is_ok(), !fail_send && !fail_delete);
            assert_eq!(*queue.calls.borrow(), calls);
            let records = audit.records.borrow();
            assert_eq!(
                records
                    .iter()
                    .map(|r| r["status"].as_str().unwrap())
                    .collect::<Vec<_>>(),
                statuses
            );
            assert!(records
                .iter()
                .all(|r| r["message_id"] == "original" && r["body"] == message.body().unwrap()));
            assert!(records
                .iter()
                .all(|r| r["replay_id"] == records[0]["replay_id"]));
        }
    }

    #[tokio::test]
    async fn audit_failures_stop_the_next_external_effect() {
        let message = message();
        for (fail_at, calls) in [(0, vec![]), (1, vec!["send"]), (2, vec!["send", "delete"])] {
            let queue = FakeQueue {
                calls: Default::default(),
                fail_send: false,
                fail_delete: false,
            };
            let audit = MemoryAudit {
                records: Default::default(),
                fail_at: Some(fail_at),
            };
            assert!(replay_message(&queue, &audit, view(&message), &message)
                .await
                .is_err());
            assert_eq!(*queue.calls.borrow(), calls);
        }
    }

    #[test]
    fn operation_configuration_rejects_shared_buckets_and_bad_queue_pairs() {
        let good = json!({"scripts":{"bucket":"scripts"}, "db":{"table":"state"}, "operations": {
            "operator_tenant":"operator",
            "audit_bucket":"operator-audit", "audit_prefix":"replays/",
            "queues":{"jobs":{"dlq_url":"https://sqs/jobs-dlq", "destination_url":"https://sqs/jobs"}}
        }});
        assert!(GlobalConfig::parse(&good.to_string()).is_ok());
        for bucket in ["", "scripts"] {
            let mut bad = good.clone();
            bad["operations"]["audit_bucket"] = bucket.into();
            assert!(GlobalConfig::parse(&bad.to_string()).is_err());
        }
        let mut bad = good.clone();
        bad["operations"]["queues"]["jobs"]["destination_url"] = "https://sqs/jobs-dlq".into();
        assert!(GlobalConfig::parse(&bad.to_string()).is_err());
        let mut bad = good;
        bad["operations"]["audit_prefix"] = "invalid".into();
        assert!(GlobalConfig::parse(&bad.to_string()).is_err());
    }
}

#[derive(Clone)]
pub(crate) struct OpsHost {
    engine: Engine,
    config: OperationsConfig,
    deadline: std::time::Instant,
    calls: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}
impl OpsHost {
    pub(crate) fn new(
        engine: Engine,
        config: OperationsConfig,
        timeout: std::time::Duration,
    ) -> Self {
        Self {
            engine,
            config,
            deadline: std::time::Instant::now() + timeout,
            calls: Default::default(),
        }
    }
    fn call(&self, lua: &mlua::Lua, request: Request) -> mlua::Result<mlua::Value> {
        let call = self
            .calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if call >= 100 {
            return Err(mlua::Error::runtime(
                "at most 100 ops commands per invocation",
            ));
        }
        let budget = self
            .deadline
            .saturating_duration_since(std::time::Instant::now());
        if budget.is_zero() {
            return Err(mlua::Error::runtime("ops deadline exceeded"));
        }
        let id = format!(
            "ops-{}-{}-{call}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(mlua::Error::external)?
                .as_nanos()
        );
        tracing::info!(request_id = %id, "operator command");
        let result = tokio::runtime::Handle::current()
            .block_on(async {
                tokio::time::timeout(budget, invoke(&self.engine, &self.config, request, &id)).await
            })
            .map_err(|_| {
                mlua::Error::runtime(format!("ops {id} timed out; inspect audit before retrying"))
            })?
            .map_err(|e| mlua::Error::runtime(format!("ops {id}: {e:#}")))?;
        lua.to_value(&result)
    }
}

pub(crate) fn api(lua: &mlua::Lua, host: OpsHost) -> mlua::Result<mlua::Table> {
    let ops = lua.create_table()?;
    ops.set("version", 1)?;
    let flow = lua.create_table()?;
    let h = host.clone();
    flow.set(
        "inspect",
        lua.create_function(move |lua, (tenant, trace_id): (String, String)| {
            h.call(lua, Request::FlowInspect { tenant, trace_id })
        })?,
    )?;
    flow.set_readonly(true);
    ops.set("flow", flow)?;
    let dlq = lua.create_table()?;
    let h = host.clone();
    dlq.set(
        "inspect",
        lua.create_function(move |lua, queue: String| h.call(lua, Request::DlqInspect { queue }))?,
    )?;
    let h = host.clone();
    dlq.set(
        "replay",
        lua.create_function(
            move |lua, (queue, message_id, reason): (String, String, String)| {
                h.call(
                    lua,
                    Request::DlqReplay {
                        queue,
                        message_id,
                        reason,
                    },
                )
            },
        )?,
    )?;
    dlq.set_readonly(true);
    ops.set("dlq", dlq)?;
    let tenant = lua.create_table()?;
    for (name, paused) in [("pause", true), ("resume", false)] {
        let h = host.clone();
        tenant.set(
            name,
            lua.create_function(move |lua, (tenant, reason): (String, String)| {
                h.call(
                    lua,
                    Request::Pause {
                        tenant: Some(tenant),
                        paused,
                        reason,
                    },
                )
            })?,
        )?;
    }
    tenant.set_readonly(true);
    ops.set("tenant", tenant)?;
    let fleet = lua.create_table()?;
    for (name, paused) in [("pause", true), ("resume", false)] {
        let h = host.clone();
        fleet.set(
            name,
            lua.create_function(move |lua, reason: String| {
                h.call(
                    lua,
                    Request::Pause {
                        tenant: None,
                        paused,
                        reason,
                    },
                )
            })?,
        )?;
    }
    let h = host;
    fleet.set(
        "status",
        lua.create_function(move |lua, ()| h.call(lua, Request::PauseStatus))?,
    )?;
    fleet.set_readonly(true);
    ops.set("fleet", fleet)?;
    ops.set_readonly(true);
    Ok(ops)
}
