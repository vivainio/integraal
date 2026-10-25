//! Which pending work belongs to a flow. Barriers, callbacks and batch jobs are keyed by their own
//! ids, so asking "what is this flow waiting on" would otherwise scan the table. Creating one also
//! writes a small index row:
//!
//! ```text
//! pk "SYS#trace#<trace id>"   sk "<tenant>\u{1}<kind>\u{1}<id>"   value {"created_s": ...}
//! ```
//!
//! The row expires with the work it points to and is never deleted early; `pending` checks that the
//! work itself is still there. Writing the row is best effort: the flow must not fail because its
//! index could not be written, so a flow can have pending work that is not listed.

use serde_json::{json, Value as Json};

use crate::{db::DB_TTL, trace::TraceContext, Item, ItemBackend, MAX_QUERY_ITEMS};

const SEP: char = '\u{1}';

fn pk(trace_id: &str) -> String {
    format!("SYS#trace#{trace_id}")
}

/// The trace id inside a stored `traceparent`; `None` for an empty or malformed context.
pub fn trace_id(trace: &TraceContext) -> Option<String> {
    let id = trace.get("traceparent")?.split('-').nth(1)?;
    (id.len() == 32 && id.bytes().all(|b| b.is_ascii_hexdigit()) && id.bytes().any(|b| b != b'0'))
        .then(|| id.to_ascii_lowercase())
}

/// Records that `tenant`'s `kind` (`barrier`, `callback` or `batch`) `id` was created in this trace.
/// `ttl` is when the work's own row expires, in Unix seconds.
pub fn record(
    backend: &dyn ItemBackend,
    tenant: &str,
    trace: &TraceContext,
    kind: &str,
    id: &str,
    ttl: u64,
    now: u64,
) {
    let Some(trace_id) = trace_id(trace) else {
        return;
    };
    let item = Item {
        sk: format!("{tenant}{SEP}{kind}{SEP}{id}"),
        value: json!({ "created_s": now }).to_string(),
        ttl,
    };
    if let Err(e) = backend.put(&pk(&trace_id), item) {
        tracing::warn!(%tenant, %kind, %id, error = %e, "trace index row not written");
    }
}

/// The index rows of one tenant in a flow: `(kind, id, created_s)`, at most `limit`.
pub fn list(
    backend: &dyn ItemBackend,
    tenant: &str,
    trace_id: &str,
    limit: usize,
) -> Result<Vec<(String, String, u64)>, String> {
    let (pk, prefix) = (pk(&trace_id.to_ascii_lowercase()), format!("{tenant}{SEP}"));
    let (mut out, mut after) = (Vec::new(), String::new());
    loop {
        let page = backend.query(&pk, &prefix, &after, MAX_QUERY_ITEMS)?;
        let n = page.len();
        for row in page {
            after.clone_from(&row.sk);
            let mut parts = row.sk[prefix.len()..].splitn(2, SEP);
            let (Some(kind), Some(id)) = (parts.next(), parts.next()) else {
                continue;
            };
            let created = serde_json::from_str::<Json>(&row.value)
                .ok()
                .and_then(|v| v["created_s"].as_u64())
                .unwrap_or(0);
            out.push((kind.to_string(), id.to_string(), created));
            if out.len() >= limit {
                return Ok(out);
            }
        }
        if n < MAX_QUERY_ITEMS {
            return Ok(out);
        }
    }
}

/// The index row's expiry for work created now.
pub fn default_ttl(now: u64) -> u64 {
    now + DB_TTL.as_secs()
}

/// A barrier that still exists: its member counts, deadline and continuation. `None` once it expired
/// or was cleaned up.
pub fn describe_barrier(backend: &dyn ItemBackend, tenant: &str, id: &str, now: u64) -> Result<Option<Json>, String> {
    let pk = crate::db::barrier_partition(tenant, id);
    let Some(h) = backend.get(&pk, "")?.filter(|i| i.ttl > now) else {
        return Ok(None);
    };
    let h: Json = serde_json::from_str(&h.value).map_err(|e| format!("corrupt barrier: {e}"))?;
    let pending = backend.query(&pk, "p#", "", MAX_QUERY_ITEMS)?.len();
    Ok(Some(json!({
        "kind": "barrier", "id": id, "total": h["total"], "pending": pending,
        "ready": h["ready"], "due_s": h["due_s"], "then": h["then"],
    })))
}

/// A callback nobody has presented yet, with when it expires. `None` once it was taken or expired.
pub fn describe_callback(backend: &dyn ItemBackend, tenant: &str, id: &str, now: u64) -> Result<Option<Json>, String> {
    let pk = crate::db::callback_partition(tenant, id);
    Ok(backend
        .get(&pk, "")?
        .filter(|i| i.ttl > now)
        .map(|i| json!({ "kind": "callback", "id": id, "expires_s": i.ttl })))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timer::tests::Mem;

    fn ctx(id: &str) -> TraceContext {
        TraceContext::from([("traceparent".to_string(), format!("00-{id}-00f067aa0ba902b7-01"))])
    }

    #[test]
    fn rows_are_found_by_trace_and_tenant() {
        let db = Mem::default();
        let a = "4bf92f3577b34da6a3ce929d0e0e4736";
        record(&db, "acme", &ctx(a), "barrier", "b1", 999, 10);
        record(&db, "acme", &ctx(a), "callback", "c1", 999, 11);
        record(&db, "other", &ctx(a), "barrier", "b2", 999, 12);
        record(&db, "acme", &ctx("5bf92f3577b34da6a3ce929d0e0e4736"), "batch", "j1", 999, 13);
        record(&db, "acme", &TraceContext::new(), "batch", "j2", 999, 14);
        let got = list(&db, "acme", &a.to_uppercase(), 10).unwrap();
        assert_eq!(
            got,
            [("barrier".into(), "b1".into(), 10), ("callback".into(), "c1".into(), 11)]
        );
        assert_eq!(list(&db, "acme", a, 1).unwrap().len(), 1);
    }

    #[test]
    fn an_all_zero_or_short_trace_id_is_not_indexed() {
        assert!(trace_id(&ctx("00000000000000000000000000000000")).is_none());
        assert!(trace_id(&ctx("abc")).is_none());
        assert!(trace_id(&TraceContext::new()).is_none());
    }
}
