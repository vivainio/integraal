//! `native.queue`: hands new work to the deployment's job queue.
//!
//! Each message is the usual envelope `{tenant, script?, version?, event}`. The tenant is always the
//! caller's own, so a script can only queue work for itself.

use mlua::{Lua, LuaSerdeExt, Table, Value};
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use crate::Budget;

/// Most messages one invocation may send.
pub const MAX_SENT_PER_INVOCATION: usize = 1000;
/// Largest serialized message. SQS allows 256 KiB; this leaves room for the envelope.
pub const MAX_MESSAGE_BYTES: usize = 200 * 1024;
/// Longest delay SQS supports.
pub const MAX_DELAY_SECS: u32 = 900;
/// Messages per backend call (one `SendMessageBatch`), also the unit of the `max_data_ops` budget.
pub const SEND_CHUNK: usize = 10;

/// One serialized envelope to send, optionally after a delay.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OutMessage {
    pub body: String,
    pub delay_secs: u32,
    /// The trace the message belongs to; `None` means the sending invocation's own.
    pub trace: Option<crate::trace::TraceContext>,
}

/// Where `native.queue.send` delivers (SQS in production). Implementations may block and split the
/// slice as their service requires. A failure may leave part of the messages sent.
pub trait QueueBackend: Send + Sync {
    fn send(&self, messages: &[OutMessage]) -> Result<(), String>;
}

/// Group for messages that belong to no single tenant (ticks, S3 notification batches).
pub const SYSTEM_GROUP: &str = "system";

/// The SQS `MessageGroupId` of an envelope body: the tenant code, then `:` and the event's `type`
/// when it has one (`acme:schedule`), so a FIFO queue orders work per tenant and message type.
/// Tenant names never contain `:`. Bodies without a tenant go to `SYSTEM_GROUP`. At most 128 bytes.
pub fn message_group_id(body: &str) -> String {
    let v = serde_json::from_str::<serde_json::Value>(body).unwrap_or_default();
    let tenant = v["tenant"].as_str().filter(|t| !t.is_empty());
    let kind = v["event"]["type"].as_str().filter(|k| {
        !k.is_empty()
            && k.chars()
                .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
    });
    let mut id = tenant.unwrap_or(SYSTEM_GROUP).to_string();
    if let Some(k) = kind {
        id.push(':');
        id.push_str(k);
    }
    id.truncate(128);
    id
}

pub(crate) fn valid_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s != "."
        && s != ".."
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
}

pub(crate) fn api(
    lua: &Lua,
    tenant: &str,
    backend: Option<Arc<dyn QueueBackend>>,
    budget: Arc<Budget>,
) -> mlua::Result<Table> {
    let queue = lua.create_table()?;
    let tenant = tenant.to_string();
    let sent = Arc::new(AtomicUsize::new(0));
    queue.set(
        "send",
        lua.create_function(move |lua, messages: Table| {
            let backend = backend.clone().ok_or_else(|| {
                mlua::Error::runtime(
                    "native.queue is not available (the tenant's config needs \"queue\": true and the deployment a queue)",
                )
            })?;
            // A single message `{ script = ..., event = ... }` or an array of them.
            let items: Vec<Table> = if messages.contains_key("script")?
                || messages.contains_key("event")?
            {
                vec![messages]
            } else {
                messages.sequence_values::<Table>().collect::<Result<_, _>>()?
            };
            if items.is_empty() {
                return Ok(0);
            }
            let total = sent.load(Ordering::Relaxed) + items.len();
            if total > MAX_SENT_PER_INVOCATION {
                return Err(mlua::Error::runtime(format!(
                    "native.queue.send: more than {MAX_SENT_PER_INVOCATION} messages in one invocation"
                )));
            }
            let mut out = Vec::with_capacity(items.len());
            for m in &items {
                let script = m.get::<Option<String>>("script")?;
                let version = m.get::<Option<String>>("version")?;
                for (what, v) in [("script", &script), ("version", &version)] {
                    if let Some(v) = v {
                        if !valid_name(v) {
                            return Err(mlua::Error::runtime(format!("invalid {what} name {v:?}")));
                        }
                    }
                }
                let event: serde_json::Value = match m.get::<Value>("event")? {
                    Value::Nil => serde_json::Value::Null,
                    v => lua.from_value(v)?,
                };
                let delay = m.get::<Option<u32>>("delay_s")?.unwrap_or(0);
                if delay > MAX_DELAY_SECS {
                    return Err(mlua::Error::runtime(format!(
                        "delay_s is at most {MAX_DELAY_SECS}"
                    )));
                }
                let mut envelope = serde_json::json!({ "tenant": tenant, "event": event });
                if let Some(s) = script {
                    envelope["script"] = s.into();
                }
                if let Some(v) = version {
                    envelope["version"] = v.into();
                }
                let body = envelope.to_string();
                if body.len() > MAX_MESSAGE_BYTES {
                    return Err(mlua::Error::runtime(format!(
                        "message exceeds {MAX_MESSAGE_BYTES} bytes"
                    )));
                }
                out.push(OutMessage {
                    body,
                    delay_secs: delay,
                    trace: None,
                });
            }
            for chunk in out.chunks(SEND_CHUNK) {
                budget.take_data().map_err(mlua::Error::runtime)?;
                backend.send(chunk).map_err(mlua::Error::runtime)?;
                sent.fetch_add(chunk.len(), Ordering::Relaxed);
            }
            Ok(out.len())
        })?,
    )?;
    Ok(queue)
}

#[cfg(test)]
mod group_tests {
    use super::*;

    #[test]
    fn group_is_the_tenant_code() {
        assert_eq!(message_group_id(r#"{"tenant":"acme","event":1}"#), "acme");
        assert_eq!(
            message_group_id(r#"{"tenant":"acme","event":{"type":"schedule"}}"#),
            "acme:schedule"
        );
        assert_eq!(message_group_id(r#"{"Records":[]}"#), SYSTEM_GROUP);
        assert_eq!(message_group_id("not json"), SYSTEM_GROUP);
    }
}
