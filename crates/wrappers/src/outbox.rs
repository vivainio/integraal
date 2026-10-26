//! Durable queue handoffs. Intent and completion share one row so there is no index-write gap.
use crate::{
    db::KEY_TTL,
    queue::{valid_name, OutMessage, QueueBackend, MAX_MESSAGE_BYTES},
    timer::{Payload, SweepReport},
    Budget, HostContext, Item, ItemBackend,
};
use mlua::{Lua, LuaSerdeExt, Table, Value};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

const ROWS: &str = "SYS#outbox";
const CLAIMS: &str = "SYS#outbox-claim";
const CURSOR: &str = "SYS#outbox-cursor";
const SEP: char = '\u{1}';
#[derive(Serialize, Deserialize)]
struct Entry {
    payload: Payload,
    sent: bool,
}
fn key(tenant: &str, id: &str) -> Result<String, String> {
    if id.is_empty() || id.len() > 200 || id.chars().any(char::is_control) {
        return Err("handoff id must be 1–200 bytes with no control characters".into());
    }
    Ok(format!("{tenant}{SEP}{id}"))
}
fn item(sk: String, entry: &Entry) -> Result<Item, String> {
    Ok(Item {
        sk,
        value: serde_json::to_string(entry).map_err(|e| e.to_string())?,
        ttl: KEY_TTL,
    })
}
fn record(db: &dyn ItemBackend, tenant: &str, id: &str, payload: Payload) -> Result<bool, String> {
    let sk = key(tenant, id)?;
    // Validate before persisting an intent the sweeper must be able to deliver.
    for value in [&payload.script, &payload.version].into_iter().flatten() {
        if !valid_name(value) {
            return Err("invalid handoff script or version".into());
        }
    }
    if payload.envelope(tenant).len() > MAX_MESSAGE_BYTES {
        return Err("handoff message exceeds 200 KB".into());
    }
    let entry = Entry {
        payload,
        sent: false,
    };
    if db.put_if_absent(ROWS, item(sk.clone(), &entry)?)? {
        return Ok(true);
    }
    let old = db.get(ROWS, &sk)?.ok_or("handoff disappeared")?;
    let old: Entry = serde_json::from_str(&old.value).map_err(|e| e.to_string())?;
    if old.payload.script != entry.payload.script
        || old.payload.version != entry.payload.version
        || old.payload.event != entry.payload.event
    {
        return Err("handoff id already exists with a different message".into());
    }
    Ok(false)
}

/// Sweep pending handoffs. Sent receipts do not expire; delivery is at least once.
pub fn sweep(
    db: &dyn ItemBackend,
    queue: &dyn QueueBackend,
    now: u64,
    limit: usize,
) -> Result<SweepReport, String> {
    let mut report = SweepReport::default();
    if limit == 0 {
        return Ok(report);
    }
    let mut after = db
        .get(CURSOR, "")?
        .map_or_else(String::new, |row| row.value);
    let mut handled = 0;
    loop {
        let page = db.query(ROWS, "", &after, 1)?;
        let count = page.len();
        for row in page {
            after.clone_from(&row.sk);
            let mut entry: Entry = serde_json::from_str(&row.value).map_err(|e| e.to_string())?;
            handled += 1;
            if entry.sent {
                if handled >= limit {
                    db.put(
                        CURSOR,
                        Item {
                            sk: String::new(),
                            value: after,
                            ttl: KEY_TTL,
                        },
                    )?;
                    return Ok(report);
                }
                continue;
            }
            let (tenant, _) = row.sk.split_once(SEP).ok_or("invalid handoff key")?;
            let claim = Item {
                sk: row.sk.clone(),
                value: "sending".into(),
                ttl: now + 60,
            };
            if !db.put_if_free(CLAIMS, claim, now)? {
                report.skipped += 1;
            } else {
                // Re-read after claiming: another sweeper may have completed since our query.
                let fresh = db.get(ROWS, &row.sk)?.ok_or("handoff disappeared")?;
                let current: Entry =
                    serde_json::from_str(&fresh.value).map_err(|e| e.to_string())?;
                if current.sent {
                    report.skipped += 1;
                } else if let Err(error) = queue.send(&[OutMessage {
                    body: entry.payload.envelope(tenant),
                    delay_secs: 0,
                    trace: entry.payload.trace.clone(),
                    group: None,
                }]) {
                    tracing::error!(%tenant, %error, "handoff send failed");
                    report.failed += 1;
                } else {
                    entry.sent = true;
                    db.put(ROWS, item(row.sk.clone(), &entry)?)?;
                    report.fired += 1;
                }
                // Leave the lease to expire: deleting it could release a newer owner's lease.
            }
            if handled >= limit {
                db.put(
                    CURSOR,
                    Item {
                        sk: String::new(),
                        value: after,
                        ttl: KEY_TTL,
                    },
                )?;
                return Ok(report);
            }
        }
        if count == 0 {
            db.put(
                CURSOR,
                Item {
                    sk: String::new(),
                    value: String::new(),
                    ttl: KEY_TTL,
                },
            )?;
            return Ok(report);
        }
    }
}

pub(crate) fn install(
    lua: &Lua,
    queue: &Table,
    host: &HostContext,
    budget: Arc<Budget>,
) -> mlua::Result<()> {
    let db = host.db.clone();
    let enabled = host.queue.is_some();
    let tenant = host.tenant.clone();
    queue.set(
        "handoff",
        lua.create_function(move |lua, (id, message): (String, Table)| {
            if !enabled {
                return Err(mlua::Error::runtime("handoff needs a queue"));
            }
            let db = db
                .as_ref()
                .ok_or_else(|| mlua::Error::runtime("handoff needs native.db"))?;
            for field in ["delay_s", "group"] {
                if message.contains_key(field)? {
                    return Err(mlua::Error::runtime(
                        "handoff does not support delay_s or group",
                    ));
                }
            }
            let event = match message.get::<Value>("event")? {
                Value::Nil => serde_json::Value::Null,
                v => lua.from_value(v)?,
            };
            let payload = Payload {
                script: message.get("script")?,
                version: message.get("version")?,
                event,
                trace: Some(crate::trace::inject_current()),
            };
            // One conditional write and, for a duplicate, one read.
            budget.take_data().map_err(mlua::Error::runtime)?;
            budget.take_data().map_err(mlua::Error::runtime)?;
            record(db.raw().as_ref(), &tenant, &id, payload).map_err(mlua::Error::runtime)
        })?,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::timer::tests::Mem;
    use std::sync::Mutex;
    #[derive(Default)]
    struct Queue {
        sent: Mutex<Vec<String>>,
        fail: bool,
    }
    impl QueueBackend for Queue {
        fn send(&self, messages: &[OutMessage]) -> Result<(), String> {
            self.sent
                .lock()
                .unwrap()
                .extend(messages.iter().map(|m| m.body.clone()));
            if self.fail {
                Err("uncertain send".into())
            } else {
                Ok(())
            }
        }
    }
    fn payload(n: u32) -> Payload {
        Payload {
            script: Some("work".into()),
            version: Some("v1".into()),
            event: serde_json::json!({"n": n}),
            trace: None,
        }
    }
    #[test]
    fn stable_ids_deduplicate_forever_and_reject_changed_payloads() {
        let db = Mem::default();
        let q = Queue::default();
        assert!(record(&db, "acme", "job", payload(1)).unwrap());
        assert!(!record(&db, "acme", "job", payload(1)).unwrap());
        assert!(record(&db, "acme", "job", payload(2)).is_err());
        assert!(record(&db, "other", "job", payload(2)).unwrap());
        assert_eq!(sweep(&db, &q, 100, 100).unwrap().fired, 2);
        assert!(!record(&db, "acme", "job", payload(1)).unwrap());
        assert_eq!(sweep(&db, &q, 10_000_000, 100).unwrap().fired, 0);
        assert_eq!(
            db.get(ROWS, &key("acme", "job").unwrap())
                .unwrap()
                .unwrap()
                .ttl,
            crate::NO_EXPIRY
        );
    }
    #[test]
    fn uncertain_send_and_abandoned_lease_recover_on_a_later_sweep() {
        let db = Mem::default();
        let broken = Queue {
            fail: true,
            ..Queue::default()
        };
        let q = Queue::default();
        record(&db, "acme", "job", payload(1)).unwrap();
        assert_eq!(sweep(&db, &broken, 100, 100).unwrap().failed, 1);
        assert_eq!(sweep(&db, &q, 101, 100).unwrap().skipped, 1);
        assert_eq!(sweep(&db, &q, 160, 100).unwrap().fired, 1);
        assert_eq!(sweep(&db, &q, 221, 100).unwrap().fired, 0);
        assert_eq!(q.sent.lock().unwrap().len(), 1);
        // Send may have succeeded before its acknowledgement was lost: duplicate is expected.
        assert_eq!(*q.sent.lock().unwrap(), *broken.sent.lock().unwrap());
    }
    #[test]
    fn persistence_alone_survives_a_long_outage_and_sweep_is_bounded() {
        let db = Mem::default();
        let q = Queue::default();
        for n in 0..105 {
            record(&db, "acme", &format!("job-{n:03}"), payload(n)).unwrap();
        }
        assert_eq!(sweep(&db, &q, 10_000_000, 0).unwrap().fired, 0);
        assert_eq!(sweep(&db, &q, 10_000_000, 100).unwrap().fired, 100);
        assert_eq!(sweep(&db, &q, 10_000_001, 100).unwrap().fired, 5);
        assert_eq!(q.sent.lock().unwrap().len(), 105);
    }
    #[test]
    fn concurrent_sweeps_share_the_dispatch_lease() {
        let db = Mem::default();
        let q = Queue::default();
        record(&db, "acme", "job", payload(1)).unwrap();
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| sweep(&db, &q, 100, 100).unwrap());
            }
        });
        assert_eq!(q.sent.lock().unwrap().len(), 1);
    }

    #[test]
    fn rejects_invalid_intents_before_persisting() {
        let db = Mem::default();
        assert!(record(&db, "acme", "bad\n", payload(1)).is_err());
        let mut p = payload(1);
        p.script = Some("../bad".into());
        assert!(record(&db, "acme", "bad", p).is_err());
        let mut p = payload(1);
        p.event = "x".repeat(MAX_MESSAGE_BYTES).into();
        assert!(record(&db, "acme", "big", p).is_err());
        assert!(db.query(ROWS, "", "", 100).unwrap().is_empty());
    }
}
