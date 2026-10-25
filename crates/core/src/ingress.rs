//! The intake Lambda's logic, free of AWS types: turns a batch of queue messages into messages
//! for the per-class job queues.
//!
//! Messages are forwarded **unchanged**, so a sender may equally well write to a job queue
//! directly and skip intake. The only exception is an S3 notification whose records belong to
//! different classes: it is split, one notification per class. Intake never rejects a message for
//! its content: whatever it cannot understand goes to the default class, where the worker's own
//! retry and dead-letter handling deals with it.

use std::collections::BTreeMap;

use serde_json::{json, Value};

use crate::{
    s3_events::{classify, Message, ATTEMPT_FIELD},
    telemetry::TraceContext,
    Engine, IngressConfig, Invocation, PlaceOptions, Placed, S3Events,
};

/// One message taken from the intake queue.
#[derive(Debug, Clone)]
pub struct Incoming {
    pub id: String,
    pub body: String,
    /// The sender's trace context, forwarded with the message.
    pub trace: TraceContext,
}

/// One message to send to a class's job queue.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Outgoing {
    pub class: String,
    pub body: String,
    /// The intake message this came from. It may only be acknowledged once every message
    /// derived from it has been sent.
    pub source: String,
    pub trace: TraceContext,
}

/// SQS accepts at most this many entries, and 256 KiB in total, per batch request.
pub const MAX_BATCH_ENTRIES: usize = 10;
pub const MAX_BATCH_BYTES: usize = 250 * 1024;

/// Decides where each message goes. Messages that need no work (S3 test events, records that are
/// not object creations) are dropped, which acknowledges them.
pub async fn plan(
    engine: &Engine,
    cfg: &IngressConfig,
    s3: &S3Events,
    incoming: &[Incoming],
    opts: &PlaceOptions,
) -> Vec<Outgoing> {
    enum Slot {
        Whole(usize),
        Record(usize, Value, u32),
    }
    let mut out = Vec::new();
    let mut items: Vec<Invocation> = Vec::new();
    let mut slots: Vec<Slot> = Vec::new();
    // (intake message, class) -> records and attempt count, for S3 notifications
    let mut records: BTreeMap<(usize, String), (Vec<Value>, u32)> = BTreeMap::new();

    for (i, m) in incoming.iter().enumerate() {
        match classify(&m.body) {
            Err(e) => {
                tracing::warn!(id = %m.id, error = %e, "unreadable message, forwarding to the default class");
                out.push(Outgoing {
                    class: cfg.default.clone(),
                    body: m.body.clone(),
                    source: m.id.clone(),
                    trace: m.trace.clone(),
                });
            }
            Ok(Message::Ignored) => {}
            // Ticks belong to no tenant. They go to the default class, whose server handles them.
            Ok(Message::Tick(_)) => out.push(Outgoing {
                class: cfg.default.clone(),
                body: m.body.clone(),
                source: m.id.clone(),
                trace: m.trace.clone(),
            }),
            Ok(Message::Envelope(inv)) => {
                items.push(inv);
                slots.push(Slot::Whole(i));
            }
            Ok(Message::S3 {
                records: recs,
                attempt,
            }) => {
                for rec in recs {
                    match s3.invocation(&rec) {
                        Ok(None) => {}
                        Ok(Some(inv)) => {
                            items.push(inv);
                            slots.push(Slot::Record(i, rec, attempt));
                        }
                        Err(e) => {
                            tracing::warn!(id = %incoming[i].id, error = %e, "unplaceable record, forwarding to the default class");
                            records
                                .entry((i, cfg.default.clone()))
                                .or_insert_with(|| (Vec::new(), attempt))
                                .0
                                .push(rec);
                        }
                    }
                }
            }
        }
    }

    let tenants: Vec<String> = items.iter().map(|i| i.tenant.clone()).collect();
    let placed = engine.place_batch(items, opts).await;
    for ((slot, placed), tenant) in slots.into_iter().zip(placed).zip(tenants) {
        let class = resolve(cfg, &tenant, &placed);
        match slot {
            Slot::Whole(i) => out.push(Outgoing {
                class,
                body: incoming[i].body.clone(),
                source: incoming[i].id.clone(),
                trace: incoming[i].trace.clone(),
            }),
            Slot::Record(i, rec, attempt) => records
                .entry((i, class))
                .or_insert_with(|| (Vec::new(), attempt))
                .0
                .push(rec),
        }
    }
    for ((i, class), (recs, attempt)) in records {
        let mut body = json!({ "Records": recs });
        if attempt > 0 {
            body[ATTEMPT_FIELD] = json!(attempt);
        }
        out.push(Outgoing {
            class,
            body: body.to_string(),
            source: incoming[i].id.clone(),
            trace: incoming[i].trace.clone(),
        });
    }
    out
}

/// The configured class a placement answer maps to; unknown classes fall back to the default.
fn resolve(cfg: &IngressConfig, tenant: &str, placed: &Placed) -> String {
    if let Some(e) = &placed.error {
        tracing::warn!(%tenant, error = %e, "placement fell back");
    }
    match &placed.class {
        Some(c) if cfg.queues.contains_key(c) => c.clone(),
        Some(c) => {
            tracing::warn!(%tenant, class = %c, "class has no queue on this deployment, using the default");
            cfg.default.clone()
        }
        None => cfg.default.clone(),
    }
}

/// Groups `out` by class and cuts each group into SQS-sized batches of indices into `out`.
pub fn batches(out: &[Outgoing]) -> Vec<(String, Vec<usize>)> {
    let mut by_class: BTreeMap<&str, Vec<usize>> = BTreeMap::new();
    for (i, o) in out.iter().enumerate() {
        by_class.entry(&o.class).or_default().push(i);
    }
    let mut result = Vec::new();
    for (class, indices) in by_class {
        let (mut cur, mut bytes) = (Vec::new(), 0);
        for i in indices {
            let len = out[i].body.len();
            if !cur.is_empty() && (cur.len() == MAX_BATCH_ENTRIES || bytes + len > MAX_BATCH_BYTES)
            {
                result.push((class.to_string(), std::mem::take(&mut cur)));
                bytes = 0;
            }
            cur.push(i);
            bytes += len;
        }
        if !cur.is_empty() {
            result.push((class.to_string(), cur));
        }
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::Limits;
    use integraal_loader::{CachedLoader, Fetched, LoadError, MemoryStore, ScriptKey, ScriptStore};
    use std::{sync::Arc, time::Duration};

    struct Shared(Arc<MemoryStore>);
    #[async_trait::async_trait]
    impl ScriptStore for Shared {
        async fn fetch(&self, k: &ScriptKey, e: Option<&str>) -> Result<Fetched, LoadError> {
            self.0.fetch(k, e).await
        }
    }

    fn engine(tenants: &[(&str, &str)]) -> Engine {
        let store = Arc::new(MemoryStore::default());
        for (tenant, config) in tenants {
            store.put(&ScriptKey::config(tenant).unwrap(), config);
        }
        store.put(
            &ScriptKey::new("big", "placement", None).unwrap(),
            r#"--!runtime 1
            return { on_place = function(ctx, ev)
              if ev.size and ev.size > 1000 then return "large" end
              return nil
            end }"#,
        );
        Engine::new(
            Arc::new(CachedLoader::new(Shared(store), Duration::from_secs(60))),
            Limits::default(),
        )
    }

    fn cfg() -> IngressConfig {
        IngressConfig {
            queues: [("small", "q-small"), ("large", "q-large")]
                .into_iter()
                .map(|(k, v)| (k.to_string(), v.to_string()))
                .collect(),
            default: "small".into(),
            place_timeout_ms: None,
            place_memory_mb: None,
        }
    }

    fn msg(id: &str, body: Value) -> Incoming {
        Incoming {
            id: id.into(),
            body: body.to_string(),
            trace: TraceContext::new(),
        }
    }

    fn record(key: &str, size: u64) -> Value {
        json!({"eventName": "ObjectCreated:Put", "s3": {"bucket": {"name": "acme-in"}, "object": {"key": key, "size": size, "eTag": "e"}}})
    }

    fn s3() -> S3Events {
        S3Events::new([(
            "In".to_string(),
            "acme-in".to_string(),
            "tenants/".to_string(),
        )])
    }

    #[tokio::test]
    async fn envelopes_are_forwarded_unchanged_to_the_tenants_class() {
        let e = engine(&[
            ("acme", r#"{"placement":{"class":"large"}}"#),
            ("nobody", "{}"),
            ("typo", r#"{"placement":{"class":"huge"}}"#),
        ]);
        let incoming = vec![
            msg("1", json!({"tenant":"acme","event":{"a":1}})),
            msg("2", json!({"tenant":"nobody","event":{}})),
            msg("3", json!({"tenant":"typo","event":{}})),
            Incoming {
                id: "4".into(),
                body: "not json".into(),
                trace: TraceContext::new(),
            },
        ];
        let out = plan(&e, &cfg(), &s3(), &incoming, &PlaceOptions::default()).await;
        let by_source: BTreeMap<_, _> = out.iter().map(|o| (o.source.as_str(), o)).collect();
        assert_eq!(by_source["1"].class, "large");
        assert_eq!(by_source["1"].body, incoming[0].body, "body is untouched");
        assert_eq!(by_source["2"].class, "small", "no placement: default");
        assert_eq!(
            by_source["3"].class, "small",
            "class without a queue: default"
        );
        assert_eq!(by_source["4"].class, "small");
        assert_eq!(by_source["4"].body, "not json");
    }

    #[tokio::test]
    async fn s3_notification_is_split_by_class_and_script_sees_the_object() {
        let e = engine(&[
            (
                "big",
                r#"{"placement":{"classes":["large"],"script":"placement"}}"#,
            ),
            ("acme", "{}"),
        ]);
        let body = json!({"Records": [
            record("tenants/big/small.csv", 10),
            record("tenants/big/huge.csv", 5000),
            record("tenants/acme/a.csv", 5000),
            {"eventName": "ObjectRemoved:Delete"},
        ], "integraal_attempt": 2});
        let out = plan(
            &e,
            &cfg(),
            &s3(),
            &[msg("m", body)],
            &PlaceOptions::default(),
        )
        .await;
        let keys = |class: &str| -> Vec<String> {
            out.iter()
                .filter(|o| o.class == class)
                .flat_map(|o| {
                    let v: Value = serde_json::from_str(&o.body).unwrap();
                    assert_eq!(
                        v["integraal_attempt"], 2,
                        "retry counter survives the split"
                    );
                    v["Records"]
                        .as_array()
                        .unwrap()
                        .iter()
                        .map(|r| r["s3"]["object"]["key"].as_str().unwrap().to_string())
                        .collect::<Vec<_>>()
                })
                .collect()
        };
        assert_eq!(keys("large"), ["tenants/big/huge.csv"]);
        assert_eq!(
            keys("small"),
            ["tenants/big/small.csv", "tenants/acme/a.csv"]
        );
        assert!(out.iter().all(|o| o.source == "m"));
    }

    #[tokio::test]
    async fn test_events_and_non_creations_are_dropped() {
        let e = engine(&[]);
        let incoming = vec![
            msg("t", json!({"Service":"Amazon S3","Event":"s3:TestEvent"})),
            msg(
                "r",
                json!({"Records":[{"eventName":"ObjectRemoved:Delete"}]}),
            ),
        ];
        assert!(plan(&e, &cfg(), &s3(), &incoming, &PlaceOptions::default())
            .await
            .is_empty());
    }

    #[test]
    fn batches_respect_sqs_limits() {
        let mk = |class: &str, len: usize| Outgoing {
            class: class.into(),
            body: "x".repeat(len),
            source: "s".into(),
            trace: TraceContext::new(),
        };
        let mut out: Vec<_> = (0..23).map(|_| mk("a", 10)).collect();
        out.push(mk("b", 10));
        let b = batches(&out);
        let sizes: Vec<_> = b.iter().map(|(c, v)| (c.as_str(), v.len())).collect();
        assert_eq!(sizes, [("a", 10), ("a", 10), ("a", 3), ("b", 1)]);

        let big = vec![mk("a", 200 * 1024), mk("a", 100 * 1024), mk("a", 10)];
        let sizes: Vec<_> = batches(&big).iter().map(|(_, v)| v.len()).collect();
        assert_eq!(sizes, [1, 2], "byte limit splits the batch");
    }
}
