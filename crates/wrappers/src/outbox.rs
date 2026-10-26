//! Durable handoffs: pending rows contain the message, and sent receipts retain deduplication.
use crate::{
    db::KEY_TTL,
    queue::{valid_name, OutMessage, QueueBackend, MAX_MESSAGE_BYTES},
    timer::{Payload, SweepReport},
    Budget, HostContext, Item, ItemBackend,
};
use mlua::{Lua, LuaSerdeExt, Table, Value};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

// Keep the old partition for receipts and migrate its unsent entries once.
const ROWS: &str = "SYS#outbox";
const PENDING: &str = "SYS#outbox-pending";
const CLAIMS: &str = "SYS#outbox-claim";
const CURSOR: &str = "SYS#outbox-pending-cursor";
const MIGRATION: &str = "SYS#outbox-migration";
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
fn parse(row: &Item) -> Result<Entry, String> {
    serde_json::from_str(&row.value).map_err(|e| e.to_string())
}
fn same_message(a: &Payload, b: &Payload) -> Result<(), String> {
    if a.script != b.script || a.version != b.version || a.event != b.event {
        return Err("handoff id already exists with a different message".into());
    }
    Ok(())
}
pub(crate) fn record(
    db: &dyn ItemBackend,
    tenant: &str,
    id: &str,
    payload: Payload,
) -> Result<bool, String> {
    let sk = key(tenant, id)?;
    for value in [&payload.script, &payload.version].into_iter().flatten() {
        if !valid_name(value) {
            return Err("invalid handoff script or version".into());
        }
    }
    if payload.envelope(tenant).len() > MAX_MESSAGE_BYTES {
        return Err("handoff message exceeds 200 KB".into());
    }
    if let Some(row) = db.get(ROWS, &sk)? {
        let old = parse(&row)?;
        same_message(&old.payload, &payload)?;
        if !old.sent {
            // Replaying a legacy intent also repairs its index before migration reaches it.
            db.put_if_absent(PENDING, item(sk.clone(), &old)?)?;
        }
        return Ok(false);
    }
    let entry = Entry {
        payload,
        sent: false,
    };
    let created = db.put_if_absent(PENDING, item(sk.clone(), &entry)?)?;
    // A dispatcher may have saved a receipt and removed the pending row since our first read.
    // Check the receipt again: a stale registration must never replace the original message.
    if let Some(row) = db.get(ROWS, &sk)? {
        same_message(&parse(&row)?.payload, &entry.payload)?;
        return Ok(false);
    }
    let pending = db
        .get(PENDING, &sk)?
        .ok_or("handoff changed during registration; retry")?;
    same_message(&parse(&pending)?.payload, &entry.payload)?;
    Ok(created)
}
fn save_cursor(db: &dyn ItemBackend, partition: &str, value: String) -> Result<(), String> {
    db.put(
        partition,
        Item {
            sk: String::new(),
            value,
            ttl: KEY_TTL,
        },
    )
}
#[derive(Default, Serialize, Deserialize)]
struct Migration {
    after: String,
    done: bool,
}
/// Move old unsent entries into the pending partition before advancing the migration cursor.
/// Never deletes legacy rows; receipts and duplicate registration remain compatible.
fn migrate(db: &dyn ItemBackend, limit: usize) -> Result<(), String> {
    let mut state: Migration = match db.get(MIGRATION, "")? {
        Some(row) => serde_json::from_str(&row.value).map_err(|e| e.to_string())?,
        None => Migration::default(),
    };
    if state.done {
        return Ok(());
    }
    for _ in 0..limit {
        // One row avoids mistaking DynamoDB's 1 MB page boundary for end-of-partition.
        let rows = db.query(ROWS, "", &state.after, 1)?;
        let Some(row) = rows.first() else {
            state.done = true;
            break;
        };
        let entry = parse(row)?;
        if !entry.sent {
            db.put_if_absent(PENDING, item(row.sk.clone(), &entry)?)?;
        }
        state.after = row.sk.clone();
    }
    save_cursor(
        db,
        MIGRATION,
        serde_json::to_string(&state).map_err(|e| e.to_string())?,
    )
}
fn dispatch(
    db: &dyn ItemBackend,
    queue: &dyn QueueBackend,
    row: &Item,
    now: u64,
) -> Result<bool, String> {
    let (tenant, _) = row.sk.split_once(SEP).ok_or("invalid handoff key")?;
    let claim = Item {
        sk: row.sk.clone(),
        value: "sending".into(),
        ttl: now + 60,
    };
    if !db.put_if_free(CLAIMS, claim, now)? {
        return Ok(false);
    }
    if let Some(receipt) = db.get(ROWS, &row.sk)? {
        if parse(&receipt)?.sent {
            // A crash after saving the receipt, or a stale registration, can leave a pending row.
            db.delete(PENDING, &row.sk)?;
            return Ok(false);
        }
    }
    let Some(current) = db.get(PENDING, &row.sk)? else {
        return Ok(false);
    };
    let mut entry = parse(&current)?;
    queue.send(&[OutMessage {
        body: entry.payload.envelope(tenant),
        delay_secs: 0,
        trace: entry.payload.trace.clone(),
        group: None,
    }])?;
    entry.sent = true;
    // Receipt first. A crash before deleting pending work is repaired without another send.
    db.put(ROWS, item(row.sk.clone(), &entry)?)?;
    db.delete(PENDING, &row.sk)?;
    // Leave the lease to expire: deleting it could release a newer owner's lease.
    Ok(true)
}

/// Scan only pending work after a bounded, restartable migration of the previous storage layout.
/// Receipts do not expire; delivery remains at least once.
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
    migrate(db, limit)?;
    let mut after = db
        .get(CURSOR, "")?
        .map_or_else(String::new, |row| row.value);
    for _ in 0..limit {
        let rows = db.query(PENDING, "", &after, 1)?;
        let Some(row) = rows.first() else {
            save_cursor(db, CURSOR, String::new())?;
            return Ok(report);
        };
        after = row.sk.clone();
        match dispatch(db, queue, row, now) {
            Ok(true) => report.fired += 1,
            Ok(false) => report.skipped += 1,
            Err(error) => {
                tracing::error!(id = %row.sk, %error, "handoff dispatch failed");
                report.failed += 1;
            }
        }
    }
    save_cursor(db, CURSOR, after)?;
    Ok(report)
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
                .ok_or_else(|| mlua::Error::runtime("handoff needs ig.db"))?;
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
            // Reserve the maximum four backend operations before persisting anything.
            for _ in 0..4 {
                budget.take_data().map_err(mlua::Error::runtime)?;
            }
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
    #[derive(Default)]
    struct FaultDb {
        mem: Mem,
        fail_write: Mutex<Option<&'static str>>,
        fail_delete: Mutex<Option<&'static str>>,
        receipt_queries: std::sync::atomic::AtomicUsize,
    }
    impl ItemBackend for FaultDb {
        fn get(&self, pk: &str, sk: &str) -> Result<Option<Item>, String> {
            self.mem.get(pk, sk)
        }
        fn put(&self, pk: &str, row: Item) -> Result<(), String> {
            let mut fail = self.fail_write.lock().unwrap();
            if *fail == Some(pk) {
                *fail = None;
                return Err("injected write failure".into());
            }
            self.mem.put(pk, row)
        }
        fn delete(&self, pk: &str, sk: &str) -> Result<(), String> {
            let mut fail = self.fail_delete.lock().unwrap();
            if *fail == Some(pk) {
                *fail = None;
                return Err("injected delete failure".into());
            }
            self.mem.delete(pk, sk)
        }
        fn take(&self, pk: &str, sk: &str) -> Result<Option<Item>, String> {
            self.mem.take(pk, sk)
        }
        fn put_if_absent(&self, pk: &str, row: Item) -> Result<bool, String> {
            self.mem.put_if_absent(pk, row)
        }
        fn put_if_free(&self, pk: &str, row: Item, now: u64) -> Result<bool, String> {
            self.mem.put_if_free(pk, row, now)
        }
        fn delete_if_value(&self, pk: &str, sk: &str, value: &str) -> Result<bool, String> {
            self.mem.delete_if_value(pk, sk, value)
        }
        fn add(&self, pk: &str, sk: &str, delta: i64, ttl: u64, now: u64) -> Result<i64, String> {
            self.mem.add(pk, sk, delta, ttl, now)
        }
        fn query(
            &self,
            pk: &str,
            prefix: &str,
            after: &str,
            limit: usize,
        ) -> Result<Vec<Item>, String> {
            if pk == ROWS {
                self.receipt_queries
                    .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            }
            self.mem.query(pk, prefix, after, limit)
        }
    }
    #[test]
    fn crashes_on_either_side_of_receipt_persistence_are_recoverable() {
        for before_receipt in [true, false] {
            let db = FaultDb::default();
            let q = Queue::default();
            record(&db, "acme", "job", payload(1)).unwrap();
            if before_receipt {
                *db.fail_write.lock().unwrap() = Some(ROWS);
            } else {
                *db.fail_delete.lock().unwrap() = Some(PENDING);
            }
            assert_eq!(sweep(&db, &q, 100, 100).unwrap().failed, 1);
            assert!(db
                .get(PENDING, &key("acme", "job").unwrap())
                .unwrap()
                .is_some());
            let retry = sweep(&db, &q, 160, 100).unwrap();
            assert_eq!(retry.fired, usize::from(before_receipt));
            assert_eq!(
                q.sent.lock().unwrap().len(),
                if before_receipt { 2 } else { 1 }
            );
            assert!(db
                .get(PENDING, &key("acme", "job").unwrap())
                .unwrap()
                .is_none());
            assert!(!record(&db, "acme", "job", payload(1)).unwrap());
        }
    }
    #[test]
    fn legacy_migration_is_bounded_restartable_and_stops_scanning_receipts() {
        let db = FaultDb::default();
        let q = Queue::default();
        for n in 0..105 {
            let entry = Entry {
                payload: payload(n),
                sent: n != 104,
            };
            db.put(
                ROWS,
                item(key("acme", &format!("old-{n:03}")).unwrap(), &entry).unwrap(),
            )
            .unwrap();
        }
        // An old receipt-scan cursor must not cause legacy pending work to be skipped.
        save_cursor(&db, "SYS#outbox-cursor", "zzzz".into()).unwrap();
        *db.fail_write.lock().unwrap() = Some(MIGRATION);
        assert!(sweep(&db, &q, 100, 100).is_err());
        assert_eq!(sweep(&db, &q, 100, 100).unwrap().fired, 0);
        assert_eq!(sweep(&db, &q, 100, 100).unwrap().fired, 1);
        let queries = db
            .receipt_queries
            .load(std::sync::atomic::Ordering::Relaxed);
        record(&db, "acme", "new", payload(106)).unwrap();
        assert_eq!(sweep(&db, &q, 101, 100).unwrap().fired, 1);
        assert_eq!(
            db.receipt_queries
                .load(std::sync::atomic::Ordering::Relaxed),
            queries
        );
        assert_eq!(q.sent.lock().unwrap().len(), 2);
    }
    #[test]
    fn migration_repairs_an_index_write_before_its_cursor_was_saved() {
        let db = FaultDb::default();
        let q = Queue::default();
        let sk = key("acme", "legacy").unwrap();
        db.put(
            ROWS,
            item(
                sk.clone(),
                &Entry {
                    payload: payload(1),
                    sent: false,
                },
            )
            .unwrap(),
        )
        .unwrap();
        *db.fail_write.lock().unwrap() = Some(MIGRATION);
        assert!(sweep(&db, &q, 100, 100).is_err());
        assert!(db.get(PENDING, &sk).unwrap().is_some());
        assert_eq!(sweep(&db, &q, 100, 100).unwrap().fired, 1);
        assert_eq!(q.sent.lock().unwrap().len(), 1);
    }
    #[test]
    fn stale_pending_rows_never_resend_completed_work() {
        let db = Mem::default();
        let q = Queue::default();
        record(&db, "acme", "job", payload(1)).unwrap();
        sweep(&db, &q, 100, 100).unwrap();
        // Model a registrar paused before its insert while the original dispatcher completed.
        let sk = key("acme", "job").unwrap();
        db.put(
            PENDING,
            item(
                sk.clone(),
                &Entry {
                    payload: payload(2),
                    sent: false,
                },
            )
            .unwrap(),
        )
        .unwrap();
        assert!(record(&db, "acme", "job", payload(2)).is_err());
        assert_eq!(sweep(&db, &q, 160, 100).unwrap().fired, 0);
        assert!(db.get(PENDING, &sk).unwrap().is_none());
        assert_eq!(q.sent.lock().unwrap().len(), 1);
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
