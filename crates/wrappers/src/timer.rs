//! Timers: "at time T, send this envelope to the queue".
//!
//! One partition, `SYS#timer`, sorted by due time, so finding what is due is a single ordered query
//! and needs no secondary index:
//!
//! ```text
//! pk "SYS#timer"        sk "<due, 12 digits>\u{1}<tenant>\u{1}<id>"   value {script?, version?, event}
//! pk "SYS#timer-claim"  sk the same                                   "sending", a short lease
//! ```
//!
//! The sweep knows nothing about what an event means. It moves envelopes: claim the row with a
//! lease, make sure the row is still there, send the envelope, delete the row, release the claim.
//! A sweeper that dies after sending lets the lease lapse and the next sweep sends again, so a
//! timer fires at least once and its handler must be safe to repeat.

use serde::{Deserialize, Serialize};
use serde_json::{json, Value as Json};

use crate::{
    db::DB_TTL,
    queue::{valid_name, OutMessage, QueueBackend, MAX_MESSAGE_BYTES},
    Item, ItemBackend, MAX_QUERY_ITEMS,
};

const TIMERS: &str = "SYS#timer";
const CLAIMS: &str = "SYS#timer-claim";
const SEP: char = '\u{1}';
/// How long a sweeper holds a timer before another may take it over.
const CLAIM_LEASE_SECS: u64 = 60;
/// A timer row outlives its due time by this much, so a sweep that was down for a while still fires it.
const GRACE_SECS: u64 = 24 * 60 * 60;
/// Furthest ahead a timer may be set: the rows must expire within `DB_TTL` of being written.
pub const MAX_TIMER_AHEAD_SECS: u64 = DB_TTL.as_secs() - GRACE_SECS;
const MAX_ID_LEN: usize = 300;

/// What a timer sends when it fires, besides the tenant (which is in the key).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Payload {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub script: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub version: Option<String>,
    #[serde(default)]
    pub event: Json,
    /// The trace the firing joins; `None` means the sweep's own.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace: Option<crate::trace::TraceContext>,
}

impl Payload {
    fn envelope(&self, tenant: &str) -> String {
        let mut e = json!({ "tenant": tenant, "event": self.event });
        if let Some(s) = &self.script {
            e["script"] = s.clone().into();
        }
        if let Some(v) = &self.version {
            e["version"] = v.clone().into();
        }
        e.to_string()
    }
}

/// What one sweep did.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct SweepReport {
    /// Envelopes sent.
    pub fired: usize,
    /// Due timers another sweeper holds, or had already fired.
    pub skipped: usize,
    /// Rows that could not be used (expired, malformed) and were deleted.
    pub dropped: usize,
    /// Timers whose envelope could not be sent; they stay and the next sweep tries again.
    pub failed: usize,
}

fn sk(due_s: u64, tenant: &str, id: &str) -> String {
    format!("{due_s:012}{SEP}{tenant}{SEP}{id}")
}

fn parse_sk(sk: &str) -> Option<(u64, &str, &str)> {
    let mut parts = sk.splitn(3, SEP);
    let due = parts.next()?.parse().ok()?;
    Some((due, parts.next()?, parts.next()?))
}

fn check_id(id: &str) -> Result<(), String> {
    if id.is_empty() || id.len() > MAX_ID_LEN || id.chars().any(char::is_control) {
        return Err(format!("invalid timer id {id:?}"));
    }
    Ok(())
}

/// Sets a timer. Setting the same `(due_s, tenant, id)` again replaces it, so it is safe to repeat.
pub fn register(
    backend: &dyn ItemBackend,
    tenant: &str,
    id: &str,
    due_s: u64,
    payload: &Payload,
    now: u64,
) -> Result<(), String> {
    check_id(id)?;
    for (what, v) in [("script", &payload.script), ("version", &payload.version)] {
        if let Some(v) = v {
            if !valid_name(v) {
                return Err(format!("invalid {what} name {v:?}"));
            }
        }
    }
    if due_s > now + MAX_TIMER_AHEAD_SECS {
        return Err(format!(
            "a timer can be at most {} days ahead",
            MAX_TIMER_AHEAD_SECS / 86_400
        ));
    }
    let value = serde_json::to_string(payload).map_err(|e| e.to_string())?;
    if payload.envelope(tenant).len() > MAX_MESSAGE_BYTES {
        return Err(format!("timer event exceeds {MAX_MESSAGE_BYTES} bytes"));
    }
    backend.put(
        TIMERS,
        Item {
            sk: sk(due_s, tenant, id),
            value,
            ttl: due_s.max(now) + GRACE_SECS,
        },
    )
}

/// Removes a timer that has not fired. Nothing happens if it already has.
pub fn cancel(backend: &dyn ItemBackend, tenant: &str, id: &str, due_s: u64) -> Result<(), String> {
    check_id(id)?;
    backend.delete(TIMERS, &sk(due_s, tenant, id))
}

/// A pending timer, as `list` reports it.
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct Pending {
    pub due_s: u64,
    pub tenant: String,
    pub id: String,
    pub payload: Payload,
}

/// The pending timers of one tenant, soonest first, at most `limit` of them. Rows that cannot be
/// read are skipped. Reads the whole timer partition, so it is for operators, not for hot paths.
pub fn list(backend: &dyn ItemBackend, tenant: &str, limit: usize) -> Result<Vec<Pending>, String> {
    let mut out = Vec::new();
    let mut after = String::new();
    loop {
        let page = backend.query(TIMERS, "", &after, MAX_QUERY_ITEMS)?;
        let n = page.len();
        for row in page {
            after.clone_from(&row.sk);
            let Some((due_s, t, id)) = parse_sk(&row.sk) else {
                continue;
            };
            if t != tenant {
                continue;
            }
            let Ok(payload) = serde_json::from_str::<Payload>(&row.value) else {
                continue;
            };
            out.push(Pending {
                due_s,
                tenant: t.to_string(),
                id: id.to_string(),
                payload,
            });
            if out.len() >= limit {
                return Ok(out);
            }
        }
        if n < MAX_QUERY_ITEMS {
            return Ok(out);
        }
    }
}

/// Sends the envelope of every timer due at or before `now`, at most `limit` of them, oldest first.
/// Safe to run on several tasks at once: each timer is claimed with a lease first.
pub fn sweep(
    backend: &dyn ItemBackend,
    queue: &dyn QueueBackend,
    now: u64,
    limit: usize,
) -> Result<SweepReport, String> {
    let mut report = SweepReport::default();
    let mut after = String::new();
    let mut handled = 0;
    'pages: loop {
        let page = backend.query(TIMERS, "", &after, MAX_QUERY_ITEMS)?;
        let n = page.len();
        for row in page {
            after.clone_from(&row.sk);
            let Some((due, tenant, id)) = parse_sk(&row.sk) else {
                backend.delete(TIMERS, &row.sk)?;
                report.dropped += 1;
                continue;
            };
            if due > now {
                break 'pages;
            }
            if row.ttl <= now {
                backend.delete(TIMERS, &row.sk)?;
                report.dropped += 1;
                continue;
            }
            let Ok(payload) = serde_json::from_str::<Payload>(&row.value) else {
                backend.delete(TIMERS, &row.sk)?;
                report.dropped += 1;
                continue;
            };
            handled += 1;
            let claim = Item {
                sk: row.sk.clone(),
                value: "sending".into(),
                ttl: now + CLAIM_LEASE_SECS,
            };
            if !backend.put_if_free(CLAIMS, claim, now)? {
                report.skipped += 1;
            } else if backend.get(TIMERS, &row.sk)?.is_none() {
                // Another sweeper fired it between our query and our claim.
                backend.delete(CLAIMS, &row.sk)?;
                report.skipped += 1;
            } else {
                let sent = queue.send(&[OutMessage {
                    body: payload.envelope(tenant),
                    delay_secs: 0,
                    trace: payload.trace.clone(),
                    group: None,
                }]);
                match sent {
                    Ok(()) => {
                        // The row goes first: whoever claims after the claim is released finds it gone.
                        backend.delete(TIMERS, &row.sk)?;
                        backend.delete(CLAIMS, &row.sk)?;
                        report.fired += 1;
                    }
                    Err(e) => {
                        tracing::error!(%tenant, %id, error = %e, "timer could not be sent");
                        backend.delete(CLAIMS, &row.sk)?;
                        report.failed += 1;
                    }
                }
            }
            if handled >= limit {
                break 'pages;
            }
        }
        if n < MAX_QUERY_ITEMS {
            break;
        }
    }
    Ok(report)
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use std::{collections::BTreeMap, sync::Mutex};

    #[derive(Default)]
    pub(crate) struct Mem(Mutex<BTreeMap<(String, String), Item>>);

    impl ItemBackend for Mem {
        fn get(&self, pk: &str, sk: &str) -> Result<Option<Item>, String> {
            Ok(self.0.lock().unwrap().get(&(pk.into(), sk.into())).cloned())
        }
        fn put(&self, pk: &str, item: Item) -> Result<(), String> {
            self.0
                .lock()
                .unwrap()
                .insert((pk.into(), item.sk.clone()), item);
            Ok(())
        }
        fn delete(&self, pk: &str, sk: &str) -> Result<(), String> {
            self.0.lock().unwrap().remove(&(pk.into(), sk.into()));
            Ok(())
        }
        fn take(&self, _: &str, _: &str) -> Result<Option<Item>, String> {
            unimplemented!()
        }
        fn put_if_absent(&self, _: &str, _: Item) -> Result<bool, String> {
            unimplemented!()
        }
        fn put_if_free(&self, pk: &str, item: Item, now: u64) -> Result<bool, String> {
            let mut m = self.0.lock().unwrap();
            let key = (pk.to_string(), item.sk.clone());
            let free = m.get(&key).is_none_or(|old| old.ttl <= now);
            if free {
                m.insert(key, item);
            }
            Ok(free)
        }
        fn delete_if_value(&self, _: &str, _: &str, _: &str) -> Result<bool, String> {
            unimplemented!()
        }
        fn add(&self, _: &str, _: &str, _: i64, _: u64, _: u64) -> Result<i64, String> {
            unimplemented!()
        }
        fn query(
            &self,
            pk: &str,
            prefix: &str,
            after: &str,
            limit: usize,
        ) -> Result<Vec<Item>, String> {
            Ok(self
                .0
                .lock()
                .unwrap()
                .iter()
                .filter(|((p, s), _)| p == pk && s.starts_with(prefix) && s.as_str() > after)
                .map(|(_, i)| i.clone())
                .take(limit)
                .collect())
        }
    }

    #[derive(Default)]
    struct Sent(Mutex<Vec<String>>);
    impl QueueBackend for Sent {
        fn send(&self, messages: &[OutMessage]) -> Result<(), String> {
            self.0
                .lock()
                .unwrap()
                .extend(messages.iter().map(|m| m.body.clone()));
            Ok(())
        }
    }

    struct Broken;
    impl QueueBackend for Broken {
        fn send(&self, _: &[OutMessage]) -> Result<(), String> {
            Err("queue down".into())
        }
    }

    fn payload(event: Json) -> Payload {
        Payload {
            script: None,
            version: None,
            event,
            trace: None,
        }
    }

    const NOW: u64 = 1_000_000;

    #[test]
    fn list_returns_one_tenants_pending_timers_in_due_order() {
        let db = Mem::default();
        let p = |n: &str| Payload {
            script: Some(n.into()),
            version: None,
            event: json!({}),
            trace: None,
        };
        register(&db, "acme", "b", 200, &p("b"), 100).unwrap();
        register(&db, "acme", "a", 150, &p("a"), 100).unwrap();
        register(&db, "other", "c", 120, &p("c"), 100).unwrap();
        let got = list(&db, "acme", 10).unwrap();
        let ids: Vec<_> = got.iter().map(|t| t.id.as_str()).collect();
        assert_eq!(ids, ["a", "b"]);
        assert_eq!(list(&db, "acme", 1).unwrap().len(), 1);
    }

    #[test]
    fn due_timers_fire_oldest_first_and_only_once() {
        let (mem, sent) = (Mem::default(), Sent::default());
        for (tenant, id, due) in [
            ("b", "x", NOW - 5),
            ("a", "y", NOW - 50),
            ("a", "z", NOW + 60),
        ] {
            register(
                &mem,
                tenant,
                id,
                due,
                &payload(json!({ "id": id })),
                NOW - 100,
            )
            .unwrap();
        }
        let r = sweep(&mem, &sent, NOW, 100).unwrap();
        assert_eq!((r.fired, r.skipped), (2, 0));
        let bodies: Vec<Json> = sent
            .0
            .lock()
            .unwrap()
            .iter()
            .map(|b| serde_json::from_str(b).unwrap())
            .collect();
        assert_eq!(bodies[0], json!({ "tenant": "a", "event": { "id": "y" } }));
        assert_eq!(bodies[1], json!({ "tenant": "b", "event": { "id": "x" } }));

        // Nothing left that is due; the future timer waits.
        assert_eq!(
            sweep(&mem, &sent, NOW, 100).unwrap(),
            SweepReport::default()
        );
        let r = sweep(&mem, &sent, NOW + 60, 100).unwrap();
        assert_eq!(r.fired, 1);
        assert_eq!(sent.0.lock().unwrap().len(), 3);
    }

    #[test]
    fn script_and_version_ride_in_the_envelope() {
        let (mem, sent) = (Mem::default(), Sent::default());
        let p = Payload {
            script: Some("finalize".into()),
            version: Some("v2".into()),
            event: json!({ "type": "timer" }),
            trace: None,
        };
        register(&mem, "acme", "t1", NOW, &p, NOW).unwrap();
        sweep(&mem, &sent, NOW, 10).unwrap();
        let msg: Json = serde_json::from_str(&sent.0.lock().unwrap()[0]).unwrap();
        assert_eq!(
            msg,
            json!({ "tenant": "acme", "script": "finalize", "version": "v2", "event": { "type": "timer" } })
        );
    }

    #[test]
    fn a_cancelled_timer_never_fires() {
        let (mem, sent) = (Mem::default(), Sent::default());
        register(&mem, "acme", "t1", NOW, &payload(json!(1)), NOW).unwrap();
        cancel(&mem, "acme", "t1", NOW).unwrap();
        cancel(&mem, "acme", "t1", NOW).unwrap(); // already gone: nothing happens
        assert_eq!(sweep(&mem, &sent, NOW, 10).unwrap(), SweepReport::default());
        assert!(sent.0.lock().unwrap().is_empty());
    }

    #[test]
    fn registering_again_replaces_instead_of_duplicating() {
        let (mem, sent) = (Mem::default(), Sent::default());
        for _ in 0..3 {
            register(&mem, "acme", "t1", NOW, &payload(json!(1)), NOW).unwrap();
        }
        assert_eq!(sweep(&mem, &sent, NOW, 10).unwrap().fired, 1);
    }

    #[test]
    fn a_live_claim_skips_the_timer_and_a_lapsed_one_is_taken_over() {
        let (mem, sent) = (Mem::default(), Sent::default());
        register(&mem, "acme", "t1", NOW, &payload(json!(1)), NOW).unwrap();
        let key = sk(NOW, "acme", "t1");
        let claim = |ttl| Item {
            sk: key.clone(),
            value: "sending".into(),
            ttl,
        };
        mem.put(CLAIMS, claim(NOW + 30)).unwrap();
        let r = sweep(&mem, &sent, NOW, 10).unwrap();
        assert_eq!((r.fired, r.skipped), (0, 1));
        mem.put(CLAIMS, claim(NOW - 1)).unwrap();
        assert_eq!(sweep(&mem, &sent, NOW, 10).unwrap().fired, 1);
        assert_eq!(sent.0.lock().unwrap().len(), 1);
        // the claim is released along with the row
        assert!(mem.get(CLAIMS, &key).unwrap().is_none());
    }

    #[test]
    fn a_sweeper_that_lost_the_race_does_not_send_a_second_time() {
        // B listed the row, then A fired it and released its claim; B's claim succeeds but the row is gone.
        let (mem, sent) = (Mem::default(), Sent::default());
        register(&mem, "acme", "t1", NOW, &payload(json!(1)), NOW).unwrap();
        let stale = mem.query(TIMERS, "", "", 10).unwrap();
        sweep(&mem, &sent, NOW, 10).unwrap();
        assert_eq!(stale.len(), 1);
        let claim = Item {
            sk: stale[0].sk.clone(),
            value: "sending".into(),
            ttl: NOW + CLAIM_LEASE_SECS,
        };
        assert!(mem.put_if_free(CLAIMS, claim, NOW).unwrap());
        assert!(mem.get(TIMERS, &stale[0].sk).unwrap().is_none());
        assert_eq!(sent.0.lock().unwrap().len(), 1);
    }

    #[test]
    fn a_failed_send_keeps_the_timer_for_the_next_sweep() {
        let (mem, sent) = (Mem::default(), Sent::default());
        register(&mem, "acme", "t1", NOW, &payload(json!(1)), NOW).unwrap();
        let r = sweep(&mem, &Broken, NOW, 10).unwrap();
        assert_eq!((r.fired, r.failed), (0, 1));
        assert_eq!(sweep(&mem, &sent, NOW, 10).unwrap().fired, 1);
    }

    #[test]
    fn the_limit_bounds_one_sweep_and_the_rest_waits() {
        let (mem, sent) = (Mem::default(), Sent::default());
        for i in 0..250 {
            register(
                &mem,
                "acme",
                &format!("t{i:03}"),
                NOW,
                &payload(json!(i)),
                NOW,
            )
            .unwrap();
        }
        assert_eq!(sweep(&mem, &sent, NOW, 100).unwrap().fired, 100);
        assert_eq!(sweep(&mem, &sent, NOW, 1000).unwrap().fired, 150); // pages past 100 rows
        assert_eq!(sent.0.lock().unwrap().len(), 250);
    }

    #[test]
    fn expired_and_malformed_rows_are_dropped() {
        let (mem, sent) = (Mem::default(), Sent::default());
        let row = |sk: String, value: &str, ttl| Item {
            sk,
            value: value.into(),
            ttl,
        };
        mem.put(TIMERS, row(sk(NOW - 9, "a", "old"), "{}", NOW - 1))
            .unwrap();
        mem.put(TIMERS, row(sk(NOW - 8, "a", "bad"), "not json", NOW + 99))
            .unwrap();
        mem.put(TIMERS, row("garbage".into(), "{}", NOW + 99))
            .unwrap();
        let r = sweep(&mem, &sent, NOW, 10).unwrap();
        assert_eq!((r.fired, r.dropped), (0, 3));
        assert!(mem.query(TIMERS, "", "", 10).unwrap().is_empty());
    }

    #[test]
    fn far_future_and_odd_timers_are_refused() {
        let mem = Mem::default();
        let p = payload(json!(1));
        let far = NOW + MAX_TIMER_AHEAD_SECS + 1;
        assert!(register(&mem, "a", "t", far, &p, NOW)
            .unwrap_err()
            .contains("days ahead"));
        assert!(register(&mem, "a", "", NOW, &p, NOW).is_err());
        assert!(register(&mem, "a", "bad\u{1}id", NOW, &p, NOW).is_err());
        let bad = Payload {
            script: Some("../x".into()),
            ..p
        };
        assert!(register(&mem, "a", "t", NOW, &bad, NOW).is_err());
    }
}
