//! S3 event notifications arriving on the queue: split into one invocation per object record,
//! and decide what to do with the message when only some records succeeded.
//!
//! A message is acknowledged when every record succeeded. Otherwise only the failed records are
//! sent back as a fresh message with an attempt counter; after `MAX_REQUEUES` rounds the (already
//! narrowed) message is left alone, so the queue's own redrive policy moves it to the DLQ.

use std::collections::HashMap;

use integraal_loader::ScriptKey;
use serde_json::{json, Value};

use crate::{Invocation, Trigger};

/// How many times failed records are re-sent before the queue's redrive policy takes over.
pub const MAX_REQUEUES: u32 = 5;

/// Marker field added to re-sent messages (S3 notification parsers ignore unknown fields).
pub(crate) const ATTEMPT_FIELD: &str = "integraal_attempt";

/// What a queue message body turned out to be.
#[derive(Debug)]
pub enum Message {
    /// The usual `{tenant, script?, version?, event}` envelope.
    Envelope(Invocation),
    /// S3 notification records, with how many times this set has already been re-sent.
    S3 { records: Vec<Value>, attempt: u32 },
    /// A tick from an EventBridge schedule (`{"type":"system.tick","every":"minute","at":"..."}`):
    /// not for any tenant, handled by the server itself.
    Tick(Tick),
    /// Nothing to do (such as S3's `s3:TestEvent`): acknowledge it.
    Ignored,
}

/// One firing of an operator schedule.
#[derive(Debug, Clone, PartialEq, Eq, serde::Deserialize)]
pub struct Tick {
    pub every: crate::schedule::Every,
    /// The scheduled time as the schedule reported it (`<aws.scheduler.scheduled-time>`), not the
    /// time the tick was handled: it is what handlers use to tell one firing from another.
    pub at: String,
}

pub fn classify(body: &str) -> Result<Message, String> {
    let v: Value = serde_json::from_str(body).map_err(|e| format!("not JSON: {e}"))?;
    if v.get("Event").and_then(Value::as_str) == Some("s3:TestEvent") {
        return Ok(Message::Ignored);
    }
    if v.get("type").and_then(Value::as_str) == Some("system.tick") {
        let tick: Tick = serde_json::from_value(v).map_err(|e| format!("bad tick: {e}"))?;
        if tick.at.is_empty() || tick.at.len() > 64 {
            return Err("bad tick: `at` must be 1 to 64 characters".into());
        }
        return Ok(Message::Tick(tick));
    }
    if let Some(records) = v.get("Records").and_then(Value::as_array) {
        let attempt = v.get(ATTEMPT_FIELD).and_then(Value::as_u64).unwrap_or(0) as u32;
        return Ok(Message::S3 {
            records: records.clone(),
            attempt,
        });
    }
    serde_json::from_value(v)
        .map(Message::Envelope)
        .map_err(|e| format!("not an invocation envelope: {e}"))
}

/// What to do with the queue message after running its records.
#[derive(Debug, PartialEq)]
pub enum Disposition {
    /// Everything succeeded: delete the message.
    Done,
    /// Send this body as a new message, with this delay, then delete the original.
    Requeue { body: String, delay_secs: i32 },
    /// Leave the message undeleted so the queue redelivers it and eventually dead-letters it.
    Retry,
}

pub fn disposition(failed: Vec<Value>, attempt: u32) -> Disposition {
    if failed.is_empty() {
        Disposition::Done
    } else if attempt >= MAX_REQUEUES {
        Disposition::Retry
    } else {
        let body = json!({ "Records": failed, ATTEMPT_FIELD: attempt + 1 }).to_string();
        Disposition::Requeue {
            body,
            delay_secs: (30 * (attempt as i32 + 1)).min(900),
        }
    }
}

/// Maps the object records of S3 notifications to invocations.
#[derive(Debug, Clone, Default)]
pub struct S3Events {
    /// physical bucket name -> (logical name (`In`, ...), the store's prefix)
    buckets: HashMap<String, (String, String)>,
    /// The dedicated batch bucket and its prefix, if configured.
    batch: Option<(String, String)>,
}

impl S3Events {
    /// `buckets` are `(logical, physical, prefix)`.
    pub fn new(buckets: impl IntoIterator<Item = (String, String, String)>) -> Self {
        Self {
            buckets: buckets
                .into_iter()
                .map(|(logical, physical, prefix)| (physical, (logical, prefix)))
                .collect(),
            batch: None,
        }
    }

    /// Also recognises notifications from the batch bucket (`physical`, tenant directories under
    /// `prefix`). Their events carry `bucket = "$batch"`.
    pub fn with_batch(mut self, physical: impl Into<String>, prefix: impl Into<String>) -> Self {
        self.batch = Some((physical.into(), prefix.into()));
        self
    }

    /// From the global config's `buckets` section.
    pub fn from_config(cfg: &crate::GlobalConfig) -> Self {
        let events = Self::new(cfg.buckets.stores.iter().filter_map(|(name, store)| {
            let bucket = store.s3_bucket()?;
            Some((name.clone(), bucket.to_string(), store.prefix().to_string()))
        }));
        match &cfg.batch {
            Some(b) => events.with_batch(b.bucket.clone(), b.prefix.clone()),
            None => events,
        }
    }

    /// One record -> one invocation (no script, so the tenant's router picks one) with
    /// `event = { bucket, key, size, etag, event }`: `bucket` is the logical name and `key` is relative
    /// to the tenant's prefix, so both can be passed straight to `native.bucket` or `delete`.
    /// `Ok(None)` means the record needs no processing (not a creation event, or a folder marker).
    pub fn invocation(&self, record: &Value) -> Result<Option<Invocation>, String> {
        let name = record
            .get("eventName")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if !name.starts_with("ObjectCreated") {
            return Ok(None);
        }
        let s3 = record.get("s3").ok_or("record has no s3 section")?;
        let physical = s3
            .pointer("/bucket/name")
            .and_then(Value::as_str)
            .ok_or("record has no bucket name")?;
        let raw_key = s3
            .pointer("/object/key")
            .and_then(Value::as_str)
            .ok_or("record has no object key")?;
        let (bucket, prefix) = match &self.batch {
            Some((name, prefix)) if name == physical => (integraal_wrappers::BATCH_BUCKET, prefix),
            _ => {
                let (logical, prefix) = self
                    .buckets
                    .get(physical)
                    .ok_or_else(|| format!("bucket {physical:?} is not an s3 store"))?;
                (logical.as_str(), prefix)
            }
        };

        let key = percent_decode(raw_key)?;
        let rest = key
            .strip_prefix(prefix.as_str())
            .ok_or_else(|| format!("key {key:?} is outside the bucket's prefix"))?;
        let (tenant, relative) = rest
            .split_once('/')
            .ok_or_else(|| format!("key {key:?} has no tenant directory"))?;
        ScriptKey::config(tenant).map_err(|e| e.to_string())?;
        if relative.is_empty() {
            return Ok(None);
        }
        Ok(Some(Invocation {
            tenant: tenant.to_string(),
            script: None,
            version: None,
            trigger: Trigger::Queue,
            event: json!({
                "type": "s3.object",
                "bucket": bucket,
                "key": relative,
                "size": s3.pointer("/object/size").cloned().unwrap_or(Value::Null),
                "etag": s3.pointer("/object/eTag").cloned().unwrap_or(Value::Null),
                "event": name,
            }),
            attempt: 0,
            continues_trace: false,
        }))
    }
}

/// Decodes the form-style encoding S3 uses in event keys (`+` is a space, `%XX` is a byte).
fn percent_decode(s: &str) -> Result<String, String> {
    let b = s.as_bytes();
    let mut out = Vec::with_capacity(b.len());
    let mut i = 0;
    while i < b.len() {
        match b[i] {
            b'+' => out.push(b' '),
            b'%' => {
                let hex = s
                    .get(i + 1..i + 3)
                    .and_then(|h| u8::from_str_radix(h, 16).ok())
                    .ok_or_else(|| format!("bad escape in key {s:?}"))?;
                out.push(hex);
                i += 2;
            }
            c => out.push(c),
        }
        i += 1;
    }
    String::from_utf8(out).map_err(|_| format!("key {s:?} is not UTF-8"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn events() -> S3Events {
        S3Events::new([(
            "In".to_string(),
            "acme-in".to_string(),
            "tenants/".to_string(),
        )])
    }

    fn record(bucket: &str, key: &str) -> Value {
        json!({"eventName": "ObjectCreated:Put", "s3": {"bucket": {"name": bucket}, "object": {"key": key, "size": 12, "eTag": "abc"}}})
    }

    #[test]
    fn ticks_are_recognised_and_checked() {
        let tick = |body: &str| classify(body);
        match tick(r#"{"type":"system.tick","every":"hour","at":"2026-10-25T14:00:00Z"}"#).unwrap()
        {
            Message::Tick(t) => assert_eq!(
                (t.every, t.at.as_str()),
                (crate::Every::Hour, "2026-10-25T14:00:00Z")
            ),
            other => panic!("{other:?}"),
        }
        assert!(tick(r#"{"type":"system.tick","every":"week","at":"x"}"#).is_err());
        assert!(tick(r#"{"type":"system.tick","every":"day"}"#).is_err());
        assert!(tick(r#"{"type":"system.tick","every":"day","at":""}"#).is_err());
        // an ordinary envelope whose event happens to have a type is still an envelope
        assert!(matches!(
            tick(r#"{"tenant":"acme","event":{"type":"system.tick"}}"#).unwrap(),
            Message::Envelope(_)
        ));
    }

    #[test]
    fn batch_bucket_events_carry_the_reserved_bucket_name() {
        let ev = events().with_batch("acme-batch", "jobs-root/");
        let inv = ev
            .invocation(&record("acme-batch", "jobs-root/acme/jobs/j1/done.json"))
            .unwrap()
            .unwrap();
        assert_eq!(inv.tenant, "acme");
        assert_eq!(inv.event["bucket"], "$batch");
        assert_eq!(inv.event["key"], "jobs/j1/done.json");
        // The data prefix does not apply to it, nor the batch prefix to data buckets.
        assert!(ev
            .invocation(&record("acme-batch", "tenants/acme/x"))
            .is_err());
        assert!(ev
            .invocation(&record("acme-in", "jobs-root/acme/x"))
            .is_err());
        // Without a batch bucket configured, its notifications are errors, not silent drops.
        assert!(events()
            .invocation(&record("acme-batch", "jobs-root/acme/x"))
            .is_err());
    }

    #[test]
    fn record_becomes_tenant_invocation_with_logical_bucket() {
        let inv = events()
            .invocation(&record("acme-in", "tenants/acme/incoming/my+file%2B1.xml"))
            .unwrap()
            .unwrap();
        assert_eq!(inv.tenant, "acme");
        assert!(inv.script.is_none());
        assert_eq!(inv.trigger, Trigger::Queue);
        assert_eq!(
            inv.event,
            json!({"type":"s3.object","bucket":"In","key":"incoming/my file+1.xml","size":12,"etag":"abc","event":"ObjectCreated:Put"})
        );
    }

    #[test]
    fn unusable_records_are_errors_or_skips() {
        let e = events();
        assert!(
            e.invocation(&record("other", "tenants/acme/x")).is_err(),
            "unknown bucket"
        );
        assert!(
            e.invocation(&record("acme-in", "elsewhere/acme/x"))
                .is_err(),
            "outside prefix"
        );
        assert!(
            e.invocation(&record("acme-in", "tenants/acme")).is_err(),
            "no tenant directory"
        );
        assert!(
            e.invocation(&record("acme-in", "tenants/../x/y")).is_err(),
            "bad tenant"
        );
        assert!(
            e.invocation(&record("acme-in", "tenants/acme/%zz"))
                .is_err(),
            "bad escape"
        );
        assert!(
            e.invocation(&record("acme-in", "tenants/acme/"))
                .unwrap()
                .is_none(),
            "folder marker"
        );
        let mut removed = record("acme-in", "tenants/acme/x");
        removed["eventName"] = json!("ObjectRemoved:Delete");
        assert!(e.invocation(&removed).unwrap().is_none());
    }

    #[test]
    fn classifies_messages() {
        assert!(matches!(
            classify(r#"{"Service":"Amazon S3","Event":"s3:TestEvent"}"#),
            Ok(Message::Ignored)
        ));
        assert!(matches!(
            classify(r#"{"tenant":"acme","event":{}}"#),
            Ok(Message::Envelope(_))
        ));
        assert!(classify("nope").is_err());
        assert!(classify(r#"{"hello":1}"#).is_err());
        let body = json!({"Records": [record("acme-in", "tenants/acme/a")]}).to_string();
        assert!(
            matches!(classify(&body), Ok(Message::S3 { ref records, attempt: 0 }) if records.len() == 1)
        );
    }

    #[test]
    fn only_failed_records_are_requeued_with_a_counter() {
        assert_eq!(disposition(vec![], 3), Disposition::Done);
        let failed = vec![record("acme-in", "tenants/acme/b")];
        let Disposition::Requeue { body, delay_secs } = disposition(failed.clone(), 0) else {
            panic!("expected requeue")
        };
        assert_eq!(delay_secs, 30);
        let Ok(Message::S3 { records, attempt }) = classify(&body) else {
            panic!("requeued body must parse")
        };
        assert_eq!((records, attempt), (failed.clone(), 1));
        // backoff grows and is capped, then the queue's redrive policy takes over
        assert!(matches!(
            disposition(failed.clone(), 4),
            Disposition::Requeue {
                delay_secs: 150,
                ..
            }
        ));
        assert_eq!(disposition(failed, MAX_REQUEUES), Disposition::Retry);
    }
}
