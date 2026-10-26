//! Explicit retry policy, independent of engine message dispositions.
use crate::{db::now_secs, timer, Budget, HostContext, Item, ItemBackend, NO_EXPIRY};
use mlua::{Lua, LuaSerdeExt, Table, Value};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

#[derive(Clone, Serialize, Deserialize)]
struct Intent {
    due_s: u64,
    payload: timer::Payload,
}

fn delay(attempt: u32, base: u64, cap: u64, jitter: bool, retry_after: u64) -> Result<u64, String> {
    if attempt == 0
        || base == 0
        || cap < base
        || cap > timer::MAX_TIMER_AHEAD_SECS
        || retry_after > timer::MAX_TIMER_AHEAD_SECS
    {
        return Err("retry: attempt must be positive; 1 <= base_delay_s <= max_delay_s <= 518400; retry_after_s <= 518400".into());
    }
    let ceiling = base
        .saturating_mul(1u64.checked_shl((attempt - 1).min(63)).unwrap_or(u64::MAX))
        .min(cap);
    let wait = if jitter {
        let mut bytes = [0; 8];
        getrandom::fill(&mut bytes).map_err(|e| e.to_string())?;
        u64::from_le_bytes(bytes) % (ceiling + 1)
    } else {
        ceiling
    };
    Ok(wait.max(retry_after))
}

fn schedule(
    db: &dyn ItemBackend,
    tenant: &str,
    id: &str,
    attempt: u32,
    payload: timer::Payload,
    wait: u64,
    now: u64,
) -> Result<u64, String> {
    if id.is_empty() || id.len() > 200 || id.chars().any(char::is_control) {
        return Err("retry id must be 1–200 bytes with no control characters".into());
    }
    let key = format!("{tenant}\u{1}{id}\u{1}{attempt}");
    let intent = Intent {
        due_s: now + wait,
        payload,
    };
    // Validate before saving an intent which could never be delivered.
    let p = &intent.payload;
    if p.script
        .as_ref()
        .is_none_or(|s| !crate::queue::valid_name(s))
        || p.version
            .as_ref()
            .is_some_and(|s| !crate::queue::valid_name(s))
        || p.envelope(tenant).len() > crate::MAX_MESSAGE_BYTES
    {
        return Err("retry needs a valid script/version and a message at most 200 KB".into());
    }
    db.put_if_absent(
        "SYS#retry",
        Item {
            sk: key.clone(),
            value: serde_json::to_string(&intent).map_err(|e| e.to_string())?,
            ttl: NO_EXPIRY,
        },
    )?;
    let row = db
        .get("SYS#retry", &key)?
        .ok_or("retry intent disappeared; retry delivery")?;
    let saved: Intent = serde_json::from_str(&row.value).map_err(|e| e.to_string())?;
    if saved.payload.script != p.script
        || saved.payload.version != p.version
        || saved.payload.event != p.event
    {
        return Err("retry id/attempt already exists with a different message".into());
    }
    timer::register(
        db,
        tenant,
        &format!("retry/{id}/{attempt}"),
        saved.due_s,
        &saved.payload,
        now,
    )?;
    Ok(saved.due_s)
}

pub(crate) fn api(lua: &Lua, host: &HostContext, budget: Arc<Budget>) -> mlua::Result<Table> {
    let api = lua.create_table()?;
    let db = host.db.clone();
    let enabled = host.queue.is_some();
    let tenant = host.tenant.clone();
    api.set(
        "schedule",
        lua.create_function(move |lua, (id, message, opts): (String, Table, Table)| {
            let err = mlua::Error::runtime;
            let attempt: u32 = opts.get("attempt")?;
            let max = opts.get::<Option<u32>>("max_attempts")?.unwrap_or(5);
            if max == 0 {
                return Err(err("retry max_attempts must be positive"));
            }
            let wait = delay(
                attempt,
                opts.get::<Option<u64>>("base_delay_s")?.unwrap_or(1),
                opts.get::<Option<u64>>("max_delay_s")?.unwrap_or(300),
                opts.get::<Option<bool>>("jitter")?.unwrap_or(true),
                opts.get::<Option<u64>>("retry_after_s")?.unwrap_or(0),
            )
            .map_err(mlua::Error::runtime)?;
            let result = lua.create_table()?;
            if attempt >= max {
                result.set("state", "exhausted")?;
                return Ok(result);
            }
            if !enabled {
                return Err(err("retry needs a queue"));
            }
            let db = db.as_ref().ok_or_else(|| err("retry needs ig.db"))?;
            for field in ["delay_s", "group"] {
                if message.contains_key(field)? {
                    return Err(err("retry does not support delay_s or group"));
                }
            }
            let event: serde_json::Value = match message.get::<Value>("event")? {
                Value::Nil => serde_json::Value::Null,
                v => lua.from_value(v)?,
            };
            let payload = timer::Payload {
                script: message.get("script")?,
                version: message.get("version")?,
                event: serde_json::json!({
                    "type": "retry", "id": id, "attempt": attempt + 1, "event": event,
                }),
                trace: Some(crate::trace::inject_current()),
            };
            // Reserve all operations before persisting anything.
            for _ in 0..3 {
                budget.take_data().map_err(mlua::Error::runtime)?;
            }
            let due = schedule(
                db.raw().as_ref(),
                &tenant,
                &id,
                attempt,
                payload,
                wait,
                now_secs(),
            )
            .map_err(mlua::Error::runtime)?;
            result.set("state", "scheduled")?;
            result.set("attempt", attempt + 1)?;
            result.set("due_s", due)?;
            Ok(result)
        })?,
    )?;
    Ok(api)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timer::tests::Mem;
    fn payload() -> timer::Payload {
        timer::Payload {
            script: Some("worker".into()),
            version: Some("v1".into()),
            event: serde_json::json!({"attempt":2}),
            trace: None,
        }
    }
    #[test]
    fn backoff_caps_and_respects_partner_delay() {
        assert_eq!(delay(1, 2, 30, false, 0).unwrap(), 2);
        assert_eq!(delay(4, 2, 30, false, 0).unwrap(), 16);
        assert_eq!(delay(u32::MAX, 2, 30, false, 40).unwrap(), 40);
        for _ in 0..100 {
            assert!((3..=30).contains(&delay(9, 2, 30, true, 3).unwrap()));
        }
        assert!(delay(0, 1, 30, false, 0).is_err());
        assert!(delay(1, 10, 2, false, 0).is_err());
    }
    #[test]
    fn redelivery_repairs_missing_timer_and_preserves_original_due_time() {
        let db = Mem::default();
        assert_eq!(
            schedule(&db, "acme", "order", 1, payload(), 10, 100).unwrap(),
            110
        );
        timer::cancel(&db, "acme", "retry/order/1", 110).unwrap();
        assert_eq!(
            schedule(&db, "acme", "order", 1, payload(), 20, 105).unwrap(),
            110
        );
        let timers = timer::list(&db, "acme", 10).unwrap();
        assert_eq!(timers.len(), 1);
        assert_eq!(timers[0].payload, payload());
        let mut different = payload();
        different.event = serde_json::json!(false);
        assert!(schedule(&db, "acme", "order", 1, different, 10, 105).is_err());
        assert_eq!(
            schedule(&db, "other", "order", 1, payload(), 20, 105).unwrap(),
            125
        );
    }
}
