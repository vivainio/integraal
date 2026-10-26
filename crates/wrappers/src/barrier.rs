//! `igp.barrier`: wait for a known set of acknowledgements, then continue once.
//!
//! A barrier is a work list: a set of member names (usually object keys) and one outcome per member.
//! Acks arrive by any channel (S3 events, queue messages, callbacks), each through
//! `igp.barrier.ack`; once sealed, the ack resolving the last pending member sends the continuation.
//! Open barriers accept atomic member additions; sealing an already resolved list dispatches it.
//!
//! Rows live in a partition of their own, which scripts cannot reach through `ig.db`:
//!
//! ```text
//! sk ""             header: total, continuation script, state, deadline, expiry
//! sk "p#<member>"   a member still pending (deleted when its ack is recorded)
//! sk "r#<member>"   the recorded outcome of a member (the first ack wins)
//! sk "~final"       "sending" (a short lease) then "sent": who dispatched the continuation
//! ```
//!
//! A barrier with a deadline also has a timer (see `timer`): when it comes due the sweep sends the
//! tenant `{ type = "barrier.overdue", barrier = id }`, and the call that dispatches the
//! continuation cancels it.
//!
//! `ack` is a sequence of idempotent steps (record the outcome if absent, delete the pending row,
//! look for any pending row left), so a redelivered ack after a crash repeats the steps and cannot
//! leave the barrier one short. There is no counter to drift. Each retiring ack also has a temporary
//! recovery entry; the shared minute tick sweeps only these entries, not active barriers or history.

use std::sync::Arc;

use mlua::{Lua, LuaSerdeExt, Table, Value};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as Json};

use crate::{
    db::{check, now_secs},
    queue::{valid_name, OutMessage, QueueBackend},
    timer::{self, Payload},
    trace::{inject_current, TraceContext},
    Budget, DbScope, HostContext, Item, ItemBackend, MAX_QUERY_ITEMS,
};

/// Most members one barrier may hold.
pub const MAX_BARRIER_MEMBERS: usize = 10_000;
const MAX_MEMBER_LEN: usize = 200; // "p#" + member must fit a sort key
const MAX_OUTCOME_BYTES: usize = 8 * 1024;
const MAX_STATE_BYTES: usize = 32 * 1024;
/// How long a call that is about to dispatch the continuation holds the claim before another may.
const CLAIM_LEASE_SECS: u64 = 60;

const HEADER: &str = "";
const PENDING: &str = "p#";
const RESULT: &str = "r#";
const FINAL: &str = "~final";
const RECOVERY: &str = "SYS#barrier-dispatch-pending";
const RECOVERY_CLAIMS: &str = "SYS#barrier-dispatch-claim";
const RECOVERY_CURSOR: &str = "SYS#barrier-dispatch-cursor";

#[derive(Serialize, Deserialize)]
struct Recovery {
    tenant: String,
    id: String,
    member: Option<String>,
}

fn recovery_key(tenant: &str, id: &str, member: Option<&str>) -> String {
    format!("{tenant}\u{1}{id}\u{1}{}", member.unwrap_or(""))
}

#[derive(Serialize, Deserialize)]
struct Header {
    total: usize,
    /// Set once every member row exists; acks are refused before that.
    ready: bool,
    /// Legacy and ordinary barriers are sealed; open lists can accept more members.
    #[serde(default = "default_sealed")]
    sealed: bool,
    /// Retained only until atomic open-list creation finishes, to validate crash retries.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    creation_members: Option<Vec<String>>,
    #[serde(rename = "then")]
    continue_with: Option<String>,
    /// Script that gets the overdue event; without it the tenant's router picks one.
    #[serde(default)]
    overdue_with: Option<String>,
    state: Json,
    expires_s: u64,
    due_s: Option<u64>,
    /// The trace of the invocation that created the barrier; the continuation joins it.
    #[serde(default)]
    trace: TraceContext,
}

fn default_sealed() -> bool {
    true
}

/// What one `ack` call found.
#[derive(Debug, PartialEq, Eq)]
pub struct AckOutcome {
    /// The member was already resolved (or this is a redelivery).
    pub duplicate: bool,
    /// Every member is resolved.
    pub complete: bool,
    /// This call sent the continuation.
    pub dispatched: bool,
}

/// What `create` takes.
pub struct CreateSpec {
    pub id: String,
    pub members: Vec<String>,
    pub sealed: bool,
    pub continue_with: Option<String>,
    pub overdue_with: Option<String>,
    pub state: Json,
    pub deadline_s: Option<u64>,
}

/// Timer id of a barrier's deadline.
fn timer_id(id: &str) -> String {
    format!("barrier.{id}")
}

/// The barrier operations, independent of Lua. Reads and logical writes spend `max_data_ops`;
/// a transaction charges each row written before committing.
pub struct Barriers {
    db: DbScope,
    tenant: String,
    queue: Option<Arc<dyn QueueBackend>>,
    budget: Arc<Budget>,
}

fn item(sk: &str, value: String, ttl: u64) -> Item {
    Item {
        sk: sk.to_string(),
        value,
        ttl,
    }
}

impl Barriers {
    fn op(&self) -> Result<&Arc<dyn ItemBackend>, String> {
        self.budget.take_data()?;
        Ok(self.db.raw())
    }

    fn header(&self, pk: &str) -> Result<Header, String> {
        let h = self
            .op()?
            .get(pk, HEADER)?
            .and_then(DbScope::live)
            .ok_or("no such barrier (unknown, expired or cleaned up)")?;
        serde_json::from_str(&h.value).map_err(|e| format!("corrupt barrier: {e}"))
    }

    fn live_header(&self, pk: &str) -> Result<Header, String> {
        let h = self.header(pk)?;
        if !h.ready {
            return Err("barrier is still being created".into());
        }
        if now_secs() >= h.expires_s {
            return Err("barrier expired".into());
        }
        Ok(h)
    }

    pub fn create(&self, spec: CreateSpec) -> Result<(), String> {
        let CreateSpec {
            id,
            members,
            sealed,
            continue_with,
            overdue_with,
            state,
            deadline_s,
        } = spec;
        let pk = self.db.barrier_pk(&id)?;
        if (sealed && members.is_empty()) || members.len() > MAX_BARRIER_MEMBERS {
            return Err(format!(
                "a barrier needs 1 to {MAX_BARRIER_MEMBERS} members, got {}",
                members.len()
            ));
        }
        if !sealed && members.len() > 99 {
            return Err(
                "an open barrier accepts at most 99 initial members; use add for more".into(),
            );
        }
        let mut seen = std::collections::HashSet::new();
        for m in &members {
            check("barrier member", m, false)?;
            if m.len() > MAX_MEMBER_LEN {
                return Err(format!("barrier member longer than {MAX_MEMBER_LEN} bytes"));
            }
            if !seen.insert(m.as_str()) {
                return Err(format!("duplicate barrier member {m:?}"));
            }
        }
        if let Some(s) = &overdue_with {
            if !valid_name(s) {
                return Err(format!("invalid script name {s:?}"));
            }
        }
        if deadline_s.is_some_and(|d| d > timer::MAX_TIMER_AHEAD_SECS) {
            return Err(format!(
                "deadline_s is at most {} days",
                timer::MAX_TIMER_AHEAD_SECS / 86_400
            ));
        }
        if let Some(s) = &continue_with {
            if !valid_name(s) {
                return Err(format!("invalid script name {s:?}"));
            }
            if self.queue.is_none() {
                return Err(
                    "continue_with needs ig.queue (the tenant's config needs \"queue\": true and the deployment a queue)"
                        .into(),
                );
            }
        }
        if serde_json::to_string(&state)
            .map_err(|e| e.to_string())?
            .len()
            > MAX_STATE_BYTES
        {
            return Err(format!("barrier state exceeds {MAX_STATE_BYTES} bytes"));
        }

        let now = now_secs();
        let ttl = crate::NO_EXPIRY;
        let mut initial = members.clone();
        initial.sort();
        let header = Header {
            total: members.len(),
            ready: false,
            sealed,
            creation_members: (!sealed).then_some(initial),
            continue_with,
            overdue_with,
            state,
            expires_s: crate::NO_EXPIRY,
            due_s: deadline_s.map(|d| now + d),
            trace: inject_current(),
        };
        let body = |h: &Header| serde_json::to_string(h).map_err(|e| e.to_string());
        if !self
            .op()?
            .put_if_free(&pk, item(HEADER, body(&header)?, ttl), now)?
        {
            // Creating again with the same id finishes a create that was cut short, and otherwise does nothing.
            let old = self.header(&pk)?;
            if old.ready {
                return Ok(());
            }
            if old.total != header.total
                || old.sealed != header.sealed
                || old.creation_members != header.creation_members
            {
                return Err("a barrier with this id exists with different members".into());
            }
        }
        if sealed {
            for m in &members {
                self.op()?.put_if_free(
                    &pk,
                    item(&format!("{PENDING}{m}"), "1".into(), ttl),
                    now,
                )?;
            }
        }
        let stored_row = self
            .op()?
            .get(&pk, HEADER)?
            .and_then(DbScope::live)
            .ok_or("barrier disappeared during create; retry")?;
        let stored: Header = serde_json::from_str(&stored_row.value).map_err(|e| e.to_string())?;
        if !sealed && stored.ready {
            return Ok(());
        }
        self.db.index_trace("barrier", &id, &stored.trace, ttl);
        // Registered from the stored header, so a create that is repeated after a crash arms the same timer.
        if let (Some(due), Some(_)) = (stored.due_s, &self.queue) {
            let payload = Payload {
                script: stored.overdue_with.clone(),
                version: None,
                event: json!({ "type": "barrier.overdue", "barrier": id }),
                trace: Some(stored.trace.clone()),
            };
            self.budget.take_data()?;
            timer::register(
                self.db.raw().as_ref(),
                &self.tenant,
                &timer_id(&id),
                due,
                &payload,
                now,
            )?;
        }
        let ready = Header {
            ready: true,
            creation_members: None,
            ..stored
        };
        let replacement = item(HEADER, body(&ready)?, ttl);
        if sealed {
            self.op()?.put(&pk, replacement)
        } else {
            let writes = members
                .iter()
                .map(|m| (pk.clone(), item(&format!("{PENDING}{m}"), "1".into(), ttl)))
                .collect();
            for _ in &members {
                self.budget.take_data()?;
            }
            if !self
                .op()?
                .transact_if_current(&pk, &stored_row, replacement, writes, now_secs())?
            {
                return Err("barrier changed during create; retry".into());
            }
            Ok(())
        }
    }

    fn live_header_row(&self, pk: &str) -> Result<(Item, Header), String> {
        let row = self
            .op()?
            .get(pk, HEADER)?
            .and_then(DbScope::live)
            .ok_or("no such barrier (unknown, expired or cleaned up)")?;
        let h: Header =
            serde_json::from_str(&row.value).map_err(|e| format!("corrupt barrier: {e}"))?;
        if !h.ready {
            return Err("barrier is still being created".into());
        }
        if now_secs() >= h.expires_s {
            return Err("barrier expired".into());
        }
        Ok((row, h))
    }

    /// Append up to 99 names atomically. A header compare fences concurrent additions and seal.
    /// Pending/outcome rows together are permanent proof of membership: ack records before delete.
    pub fn add(&self, id: &str, members: &[String]) -> Result<usize, String> {
        if members.is_empty() || members.len() > 99 {
            return Err("barrier.add needs 1 to 99 members per call".into());
        }
        let mut seen = std::collections::HashSet::new();
        for member in members {
            check("barrier member", member, false)?;
            if member.len() > MAX_MEMBER_LEN {
                return Err("barrier member longer than 200 bytes".into());
            }
            if !seen.insert(member) {
                return Err(format!("duplicate barrier member {member:?}"));
            }
        }
        let pk = self.db.barrier_pk(id)?;
        let (row, mut h) = self.live_header_row(&pk)?;
        if h.sealed {
            return Err("barrier is sealed; cannot add members".into());
        }
        let mut writes = Vec::new();
        for member in members {
            let p = format!("{PENDING}{member}");
            if self.op()?.get(&pk, &p)?.and_then(DbScope::live).is_some() {
                continue;
            }
            if self
                .op()?
                .get(&pk, &format!("{RESULT}{member}"))?
                .and_then(DbScope::live)
                .is_some()
            {
                continue;
            }
            writes.push((pk.clone(), item(&p, "1".into(), crate::NO_EXPIRY)));
        }
        let added = writes.len();
        if h.total + added > MAX_BARRIER_MEMBERS {
            return Err("barrier exceeds 10000 members".into());
        }
        h.total += added;
        // Even a duplicate-only add compares the header: a concurrent seal must reject it.
        for _ in 0..added {
            self.budget.take_data()?;
        }
        let replacement = item(
            HEADER,
            serde_json::to_string(&h).map_err(|e| e.to_string())?,
            row.ttl,
        );
        if !self
            .op()?
            .transact_if_current(&pk, &row, replacement, writes, now_secs())?
        {
            return Err("barrier changed during add; retry".into());
        }
        Ok(added)
    }

    /// Close the member list. Index dispatch recovery atomically with sealing, so a crash cannot
    /// leave an empty sealed barrier without a continuation-recovery source.
    pub fn seal(&self, id: &str) -> Result<bool, String> {
        let pk = self.db.barrier_pk(id)?;
        let (row, mut h) = self.live_header_row(&pk)?;
        if !h.sealed {
            h.sealed = true;
            let mut writes = Vec::new();
            if self.queue.is_some() {
                let recovery = Recovery {
                    tenant: self.tenant.clone(),
                    id: id.into(),
                    member: None,
                };
                writes.push((
                    RECOVERY.into(),
                    item(
                        &recovery_key(&self.tenant, id, None),
                        serde_json::to_string(&recovery).map_err(|e| e.to_string())?,
                        crate::NO_EXPIRY,
                    ),
                ));
                self.budget.take_data()?;
            }
            let replacement = item(
                HEADER,
                serde_json::to_string(&h).map_err(|e| e.to_string())?,
                row.ttl,
            );
            if !self
                .op()?
                .transact_if_current(&pk, &row, replacement, writes, now_secs())?
            {
                return Err("barrier changed during seal; retry".into());
            }
        }
        let sent = self.resume(id)?;
        // Pending members have their own recovery entries when they ack.
        self.clear_recovery(id, None)?;
        Ok(sent)
    }

    fn any_pending(&self, pk: &str) -> Result<bool, String> {
        Ok(self
            .op()?
            .query(pk, PENDING, "", 1)?
            .into_iter()
            .any(|i| DbScope::live(i).is_some()))
    }

    fn register_recovery(&self, id: &str, member: Option<&str>) -> Result<(), String> {
        if self.queue.is_some() {
            let recovery = Recovery {
                tenant: self.tenant.clone(),
                id: id.into(),
                member: member.map(str::to_string),
            };
            self.op()?.put_if_absent(
                RECOVERY,
                item(
                    &recovery_key(&self.tenant, id, member),
                    serde_json::to_string(&recovery).map_err(|e| e.to_string())?,
                    crate::NO_EXPIRY,
                ),
            )?;
        }
        Ok(())
    }

    fn clear_recovery(&self, id: &str, member: Option<&str>) -> Result<(), String> {
        if self.queue.is_some() {
            self.op()?
                .delete(RECOVERY, &recovery_key(&self.tenant, id, member))?;
        }
        Ok(())
    }

    pub fn ack(&self, id: &str, member: &str, outcome: &Json) -> Result<AckOutcome, String> {
        let pk = self.db.barrier_pk(id)?;
        check("barrier member", member, false)?;
        let body = serde_json::to_string(outcome).map_err(|e| e.to_string())?;
        if body.len() > MAX_OUTCOME_BYTES {
            return Err(format!("ack outcome exceeds {MAX_OUTCOME_BYTES} bytes"));
        }
        let header = self.live_header(&pk)?;
        let ttl = crate::NO_EXPIRY;
        let (p, r) = (format!("{PENDING}{member}"), format!("{RESULT}{member}"));

        let duplicate = match self.op()?.get(&pk, &p)?.and_then(DbScope::live) {
            Some(_) => {
                // Record the outcome first, then retire the pending row: a crash in between leaves the
                // member pending, and the redelivered ack finishes the job. The first outcome wins.
                self.op()?.put_if_absent(&pk, item(&r, body, ttl))?;
                false
            }
            None => {
                if self.op()?.get(&pk, &r)?.and_then(DbScope::live).is_none() {
                    return Err(format!("{member:?} is not a member of barrier {id:?}"));
                }
                true
            }
        };

        // Each retiring ack owns a recovery entry. A barrier-wide marker cannot safely be
        // cleared by an ack which sees another pending member: that member may retire next.
        self.register_recovery(id, Some(member))?;
        if !duplicate {
            self.op()?.delete(&pk, &p)?;
        }
        if self.any_pending(&pk)? {
            self.clear_recovery(id, Some(member))?;
            return Ok(AckOutcome {
                duplicate,
                complete: false,
                dispatched: false,
            });
        }
        let header = if header.sealed {
            header
        } else {
            self.live_header(&pk)?
        };
        if !header.sealed {
            self.clear_recovery(id, Some(member))?;
            return Ok(AckOutcome {
                duplicate,
                complete: false,
                dispatched: false,
            });
        }
        let dispatched = self.dispatch(&pk, id, &header)?;
        self.clear_recovery(id, Some(member))?;
        Ok(AckOutcome {
            duplicate,
            complete: true,
            dispatched,
        })
    }

    /// Sends the continuation once. The claim is a short lease taken before the send and made
    /// permanent after it: a call that dies in between lets a later ack send again after the lease,
    /// so the continuation runs at least once and must be safe to repeat.
    fn dispatch(&self, pk: &str, id: &str, header: &Header) -> Result<bool, String> {
        let Some(queue) = &self.queue else {
            return Ok(false);
        };
        let now = now_secs();
        let claim = item(FINAL, "sending".into(), now + CLAIM_LEASE_SECS);
        if !self.op()?.put_if_free(pk, claim, now)? {
            return match self.op()?.get(pk, FINAL)?.and_then(DbScope::live) {
                Some(i) if i.value == "sending" => {
                    Err("barrier completion is being dispatched by another call; retry".into())
                }
                _ => Ok(false),
            };
        }
        let mut envelope = json!({
            "tenant": self.tenant,
            "event": { "type": "barrier.complete", "barrier": id },
        });
        if let Some(s) = &header.continue_with {
            envelope["script"] = s.clone().into();
        }
        self.budget.take_data()?;
        queue.send(&[OutMessage {
            body: envelope.to_string(),
            delay_secs: 0,
            trace: Some(header.trace.clone()),
            group: None,
        }])?;
        self.op()?
            .put(pk, item(FINAL, "sent".into(), crate::NO_EXPIRY))?;
        // The deadline no longer matters. If this fails the overdue script still runs once, for a
        // barrier that is finished, and `status` tells it so.
        if let Some(due) = header.due_s {
            if self.budget.take_data().is_ok() {
                let _ = timer::cancel(self.db.raw().as_ref(), &self.tenant, &timer_id(id), due);
            }
        }
        Ok(true)
    }

    /// Retry continuation dispatch without replaying an acknowledgement. Pending members are
    /// unchanged; an active dispatch lease raises the same retryable error as `ack`.
    pub fn resume(&self, id: &str) -> Result<bool, String> {
        let pk = self.db.barrier_pk(id)?;
        let header = self.live_header(&pk)?;
        if !header.sealed || self.any_pending(&pk)? {
            return Ok(false);
        }
        self.register_recovery(id, None)?;
        let sent = self.dispatch(&pk, id, &header)?;
        self.clear_recovery(id, None)?;
        Ok(sent)
    }

    /// Check a bounded page of pending members against application-owned durable results.
    /// The checker returns None for unfinished work; outcomes use the ordinary ack path.
    pub fn reconcile(
        &self,
        id: &str,
        after: &str,
        limit: usize,
        mut check_result: impl FnMut(&str) -> Result<Option<Json>, String>,
    ) -> Result<Json, String> {
        if !(1..=MAX_QUERY_ITEMS).contains(&limit) {
            return Err("reconcile limit must be 1 to 100".into());
        }
        if !after.is_empty() {
            check("barrier member cursor", after, false)?;
            if after.len() > MAX_MEMBER_LEN {
                return Err("barrier member cursor exceeds 200 bytes".into());
            }
        }
        let pk = self.db.barrier_pk(id)?;
        self.live_header(&pk)?;
        let cursor = if after.is_empty() {
            String::new()
        } else {
            format!("{PENDING}{after}")
        };
        let page = self.op()?.query(&pk, PENDING, &cursor, limit)?;
        // Advance over raw rows too, including expired legacy members.
        let next_after = if page.len() == limit {
            page.last().map(|row| row.sk[PENDING.len()..].to_string())
        } else {
            None
        };
        let (mut checked, mut acknowledged, mut dispatched) = (0, 0, false);
        for row in page.into_iter().filter_map(DbScope::live) {
            self.budget.deadline.check().map_err(str::to_string)?;
            let member = &row.sk[PENDING.len()..];
            checked += 1;
            if let Some(outcome) = check_result(member)? {
                let ack = self.ack(id, member, &outcome)?;
                acknowledged += usize::from(!ack.duplicate);
                dispatched |= ack.dispatched;
            }
        }
        let complete = self.live_header(&pk)?.sealed && !self.any_pending(&pk)?;
        if complete {
            // Also repairs a complete barrier with an interrupted dispatch and no pending rows.
            dispatched |= self.resume(id)?;
        }
        Ok(json!({ "checked": checked, "acknowledged": acknowledged,
            "next_after": next_after, "complete": complete, "dispatched": dispatched }))
    }

    /// Up to `limit` (at most 100) members still pending, in name order.
    pub fn pending(&self, id: &str, limit: usize) -> Result<Vec<String>, String> {
        let pk = self.db.barrier_pk(id)?;
        self.header(&pk)?;
        let limit = limit.clamp(1, MAX_QUERY_ITEMS);
        Ok(self
            .op()?
            .query(&pk, PENDING, "", limit)?
            .into_iter()
            .filter_map(DbScope::live)
            .map(|i| i.sk[PENDING.len()..].to_string())
            .collect())
    }

    pub fn status(&self, id: &str) -> Result<Json, String> {
        let pk = self.db.barrier_pk(id)?;
        let h = self.header(&pk)?;
        let complete = h.ready && h.sealed && !self.any_pending(&pk)?;
        let sent = self
            .op()?
            .get(&pk, FINAL)?
            .and_then(DbScope::live)
            .is_some_and(|i| i.value == "sent");
        let overdue = !complete && h.due_s.is_some_and(|d| now_secs() >= d);
        Ok(json!({
            "id": id, "total": h.total, "ready": h.ready, "sealed": h.sealed, "complete": complete,
            "dispatched": sent, "overdue": overdue, "due_s": h.due_s,
        }))
    }

    /// One page of recorded outcomes after `after` (a member name; `""` starts at the beginning).
    fn outcomes(&self, pk: &str, after: &str) -> Result<Vec<(String, Json)>, String> {
        let after = if after.is_empty() {
            String::new()
        } else {
            format!("{RESULT}{after}")
        };
        self.op()?
            .query(pk, RESULT, &after, MAX_QUERY_ITEMS)?
            .into_iter()
            .filter_map(DbScope::live)
            .map(|i| {
                let v =
                    serde_json::from_str(&i.value).map_err(|e| format!("corrupt outcome: {e}"))?;
                Ok((i.sk[RESULT.len()..].to_string(), v))
            })
            .collect()
    }
}

/// Recover only interrupted acknowledgements and continuation sends, never barrier history.
/// One logical sweep is driven by the shared minute tick, not a poller in each worker.
/// The cursor bounds work and prevents failing/leased entries from starving later entries.
pub fn sweep(
    db: Arc<dyn ItemBackend>,
    queue: Arc<dyn QueueBackend>,
    now: u64,
    limit: usize,
) -> Result<timer::SweepReport, String> {
    let mut report = timer::SweepReport::default();
    if limit == 0 {
        return Ok(report);
    }
    let mut after = db
        .get(RECOVERY_CURSOR, "")?
        .map_or_else(String::new, |row| row.value);
    let mut visited = false;
    for _ in 0..limit {
        let rows = db.query(RECOVERY, "", &after, 1)?;
        let Some(row) = rows.first() else {
            // Keep idle sweeps read-only. Reset only an existing nonempty cursor.
            if !after.is_empty() {
                db.put(RECOVERY_CURSOR, item("", String::new(), crate::NO_EXPIRY))?;
            }
            return Ok(report);
        };
        visited = true;
        after = row.sk.clone();
        match recover(db.clone(), queue.clone(), row, now) {
            Ok(true) => report.fired += 1,
            Ok(false) => report.skipped += 1,
            Err(error) => {
                tracing::error!(key = %row.sk, %error, "barrier dispatch recovery failed");
                report.failed += 1;
            }
        }
    }
    if visited {
        db.put(RECOVERY_CURSOR, item("", after, crate::NO_EXPIRY))?;
    }
    Ok(report)
}

fn recover(
    db: Arc<dyn ItemBackend>,
    queue: Arc<dyn QueueBackend>,
    row: &Item,
    now: u64,
) -> Result<bool, String> {
    let recovery: Recovery = serde_json::from_str(&row.value).map_err(|e| e.to_string())?;
    let old = db.get(RECOVERY_CLAIMS, &row.sk)?;
    if old.as_ref().is_some_and(|claim| claim.ttl > now) {
        return Ok(false);
    }
    let attempts = old
        .and_then(|claim| claim.value.parse::<u32>().ok())
        .unwrap_or(0);
    // Persist backoff before trying: a crash or error leaves a bounded retry rate too.
    let delay = (60u64.saturating_mul(1u64 << attempts.min(6))).min(3600);
    if !db.put_if_free(
        RECOVERY_CLAIMS,
        item(&row.sk, attempts.saturating_add(1).to_string(), now + delay),
        now,
    )? {
        return Ok(false);
    }
    // A successful foreground ack may have removed the work after the query.
    if db.get(RECOVERY, &row.sk)?.is_none() {
        return Ok(false);
    }
    let barriers = Barriers {
        db: DbScope::new(db.clone(), &recovery.tenant),
        tenant: recovery.tenant,
        queue: Some(queue),
        budget: Arc::new(Budget {
            deadline: crate::Deadline::new(
                std::time::Instant::now() + std::time::Duration::from_secs(60),
            ),
            http_left: 0.into(),
            data_left: 32.into(),
        }),
    };
    let pk = barriers.db.barrier_pk(&recovery.id)?;
    // Legacy expired/cleaned-up barriers cannot be repaired; retire their stale index entries.
    let header = db.get(&pk, HEADER)?.and_then(DbScope::live);
    let expired = match header {
        None => true,
        Some(row) => {
            let h: Header = serde_json::from_str(&row.value).map_err(|e| e.to_string())?;
            h.expires_s <= now
        }
    };
    if expired {
        db.delete(RECOVERY, &row.sk)?;
        return Ok(false);
    }
    match recovery.member {
        Some(member) => {
            let outcome = db
                .get(&pk, &format!("{RESULT}{member}"))?
                .ok_or("recovery acknowledgement has no recorded outcome")?;
            let outcome: Json = serde_json::from_str(&outcome.value).map_err(|e| e.to_string())?;
            Ok(barriers.ack(&recovery.id, &member, &outcome)?.dispatched)
        }
        None => {
            let sent = barriers.resume(&recovery.id)?;
            // A stale resume entry for an incomplete barrier needs no dispatch recovery.
            barriers.clear_recovery(&recovery.id, None)?;
            Ok(sent)
        }
    }
}

fn err(e: impl std::fmt::Display) -> mlua::Error {
    mlua::Error::runtime(e.to_string())
}

fn outcome_json(lua: &Lua, v: Value) -> mlua::Result<Json> {
    let t = match v {
        Value::Nil => return Ok(json!({ "ok": true })),
        Value::Table(t) => t,
        _ => return Err(err("ack outcome must be a table { ok?, error?, result? }")),
    };
    let ok = t.get::<Option<bool>>("ok")?.unwrap_or(true);
    let mut out = json!({ "ok": ok });
    if let Some(e) = t.get::<Option<String>>("error")? {
        out["error"] = e.into();
    }
    if let v @ (Value::Table(_)
    | Value::String(_)
    | Value::Number(_)
    | Value::Integer(_)
    | Value::Boolean(_)) = t.get::<Value>("result")?
    {
        out["result"] = lua.from_value(v)?;
    }
    Ok(out)
}

pub(crate) fn api(lua: &Lua, host: &HostContext, budget: Arc<Budget>) -> mlua::Result<Table> {
    let barrier = lua.create_table()?;
    let make = {
        let (db, tenant, queue) = (host.db.clone(), host.tenant.clone(), host.queue.clone());
        move || -> mlua::Result<Arc<Barriers>> {
            let db = db
                .clone()
                .ok_or_else(|| err("igp.barrier needs ig.db (the config's db section)"))?;
            Ok(Arc::new(Barriers {
                db,
                tenant: tenant.clone(),
                queue: queue.clone(),
                budget: budget.clone(),
            }))
        }
    };

    let mk = make.clone();
    barrier.set(
        "create",
        lua.create_function(move |lua, opts: Table| {
            let b = mk()?;
            let id = match opts.get::<Option<String>>("id")? {
                Some(id) => id,
                None => {
                    let r = crate::db::random::<8>().map_err(err)?;
                    crate::db::hex(&r)
                }
            };
            let members: Vec<String> = match opts.get::<Value>("members")? {
                Value::Table(t) => t.sequence_values::<String>().collect::<Result<_, _>>()?,
                Value::Nil if opts.get::<Option<bool>>("sealed")? == Some(false) => Vec::new(),
                _ => return Err(err("igp.barrier.create needs members = { ... }")),
            };
            let state = match opts.get::<Value>("state")? {
                Value::Nil => Json::Null,
                v => lua.from_value(v)?,
            };
            b.create(CreateSpec {
                id: id.clone(),
                members,
                sealed: opts.get::<Option<bool>>("sealed")?.unwrap_or(true),
                continue_with: opts.get::<Option<String>>("continue_with")?,
                overdue_with: opts.get::<Option<String>>("overdue_with")?,
                state,
                deadline_s: opts.get::<Option<u64>>("deadline_s")?,
            })
            .map_err(err)?;
            Ok(id)
        })?,
    )?;

    let mk = make.clone();
    barrier.set(
        "add",
        lua.create_function(move |_, (id, members): (String, Table)| {
            let members = members
                .sequence_values::<String>()
                .collect::<mlua::Result<Vec<_>>>()?;
            mk()?.add(&id, &members).map_err(err)
        })?,
    )?;
    let mk = make.clone();
    barrier.set(
        "seal",
        lua.create_function(move |_, id: String| mk()?.seal(&id).map_err(err))?,
    )?;

    let mk = make.clone();
    barrier.set(
        "ack",
        lua.create_function(move |lua, (id, member, outcome): (String, String, Value)| {
            let outcome = outcome_json(lua, outcome)?;
            let r = mk()?.ack(&id, &member, &outcome).map_err(err)?;
            let t = lua.create_table()?;
            t.set("duplicate", r.duplicate)?;
            t.set("complete", r.complete)?;
            t.set("dispatched", r.dispatched)?;
            Ok(t)
        })?,
    )?;

    let mk = make.clone();
    barrier.set(
        "resume",
        lua.create_function(move |_, id: String| mk()?.resume(&id).map_err(err))?,
    )?;

    let mk = make.clone();
    barrier.set(
        "reconcile",
        lua.create_function(
            move |lua, (id, checker, opts): (String, mlua::Function, Option<Table>)| {
                let after = opts
                    .as_ref()
                    .map(|t| t.get::<Option<String>>("after"))
                    .transpose()?
                    .flatten()
                    .unwrap_or_default();
                let limit = opts
                    .as_ref()
                    .map(|t| t.get::<Option<usize>>("limit"))
                    .transpose()?
                    .flatten()
                    .unwrap_or(10);
                // Preserve the original Lua error, including its traceback, across the Rust checker.
                let mut failure = None;
                let report = mk()?.reconcile(&id, &after, limit, |member| {
                    let result = (|| -> mlua::Result<Option<Json>> {
                        match checker.call::<Value>(member)? {
                            Value::Nil => Ok(None),
                            v @ Value::Table(_) => outcome_json(lua, v).map(Some),
                            _ => Err(err("reconcile checker must return an outcome table or nil")),
                        }
                    })();
                    result.map_err(|e| {
                        failure = Some(e);
                        "reconcile checker failed".into()
                    })
                });
                if let Some(e) = failure {
                    return Err(e);
                }
                let report = report.map_err(err)?;
                let t = lua.create_table()?;
                for field in ["checked", "acknowledged", "complete", "dispatched"] {
                    t.set(field, lua.to_value(&report[field])?)?;
                }
                t.set("next_after", report["next_after"].as_str())?;
                Ok(t)
            },
        )?,
    )?;

    let mk = make.clone();
    barrier.set(
        "pending",
        lua.create_function(move |_, (id, limit): (String, Option<usize>)| {
            mk()?
                .pending(&id, limit.unwrap_or(MAX_QUERY_ITEMS))
                .map_err(err)
        })?,
    )?;

    let mk = make.clone();
    barrier.set(
        "status",
        lua.create_function(move |lua, id: String| lua.to_value(&mk()?.status(&id).map_err(err)?))?,
    )?;

    let mk = make;
    barrier.set(
        "hydrate",
        lua.create_function(move |lua, event: Table| {
            let Some(id) = event.get::<Option<String>>("barrier")? else {
                return Err(err("hydrate needs an event with a barrier id"));
            };
            let b = mk()?;
            let pk = b.db.barrier_pk(&id).map_err(err)?;
            let header = match b.header(&pk) {
                Ok(h) => h,
                Err(_) => return Ok(Value::Nil), // cleaned up or expired: a duplicate delivery
            };
            if !header.ready || !header.sealed || b.any_pending(&pk).map_err(err)? {
                return Ok(Value::Nil);
            }
            let t = lua.create_table()?;
            t.set("id", id)?;
            t.set("total", header.total)?;
            t.set("state", lua.to_value(&header.state)?)?;

            // Both iterate every outcome in member order, a page of 100 at a time.
            let (bb, pkk) = (b.clone(), pk.clone());
            t.set(
                "results",
                lua.create_function(move |lua, ()| {
                    let (bb, pkk) = (bb.clone(), pkk.clone());
                    let mut cursor = (String::new(), Vec::<(String, Json)>::new(), false);
                    lua.create_function_mut(move |lua, ()| {
                        let c = &mut cursor;
                        if c.1.is_empty() && !c.2 {
                            let page = bb.outcomes(&pkk, &c.0).map_err(err)?;
                            c.2 = page.len() < MAX_QUERY_ITEMS;
                            c.1 = page.into_iter().rev().collect();
                        }
                        match c.1.pop() {
                            Some((m, o)) => {
                                c.0 = m.clone();
                                Ok(mlua::MultiValue::from_iter([
                                    Value::String(lua.create_string(&m)?),
                                    lua.to_value(&o)?,
                                ]))
                            }
                            None => Ok(mlua::MultiValue::new()),
                        }
                    })
                })?,
            )?;

            let (bb, pkk) = (b, pk);
            t.set(
                "failures",
                lua.create_function(move |lua, ()| {
                    let out = lua.create_table()?;
                    let mut after = String::new();
                    loop {
                        let page = bb.outcomes(&pkk, &after).map_err(err)?;
                        let n = page.len();
                        for (m, o) in page {
                            after = m.clone();
                            if o["ok"] == Json::Bool(false) {
                                let row = lua.create_table()?;
                                row.set("member", m)?;
                                row.set("error", o.get("error").and_then(Json::as_str))?;
                                row.set("result", lua.to_value(&o["result"])?)?;
                                out.push(row)?;
                            }
                        }
                        if n < MAX_QUERY_ITEMS {
                            return Ok(out);
                        }
                    }
                })?,
            )?;
            Ok(Value::Table(t))
        })?,
    )?;
    Ok(barrier)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{Deadline, HostLimits};
    use std::{
        collections::BTreeMap,
        sync::Mutex,
        time::{Duration, Instant},
    };

    #[derive(Default)]
    struct Mem(Mutex<BTreeMap<(String, String), Item>>);

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
        fn take(&self, pk: &str, sk: &str) -> Result<Option<Item>, String> {
            Ok(self.0.lock().unwrap().remove(&(pk.into(), sk.into())))
        }
        fn put_if_absent(&self, pk: &str, item: Item) -> Result<bool, String> {
            let mut m = self.0.lock().unwrap();
            let key = (pk.to_string(), item.sk.clone());
            let absent = !m.contains_key(&key);
            if absent {
                m.insert(key, item);
            }
            Ok(absent)
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
        fn transact_if_current(
            &self,
            pk: &str,
            expected: &Item,
            replacement: Item,
            additional: Vec<(String, Item)>,
            now: u64,
        ) -> Result<bool, String> {
            let mut rows = self.0.lock().unwrap();
            let key = (pk.to_string(), expected.sk.clone());
            if rows
                .get(&key)
                .is_none_or(|row| row != expected || row.ttl <= now)
            {
                return Ok(false);
            }
            rows.insert(key, replacement);
            for (pk, row) in additional {
                rows.insert((pk, row.sk.clone()), row);
            }
            Ok(true)
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
    struct Sent(Mutex<Vec<String>>, Mutex<Vec<Option<TraceContext>>>);
    impl QueueBackend for Sent {
        fn send(&self, messages: &[OutMessage]) -> Result<(), String> {
            self.0
                .lock()
                .unwrap()
                .extend(messages.iter().map(|m| m.body.clone()));
            self.1
                .lock()
                .unwrap()
                .extend(messages.iter().map(|m| m.trace.clone()));
            Ok(())
        }
    }

    fn setup() -> (Barriers, Arc<Mem>, Arc<Sent>) {
        let mem = Arc::new(Mem::default());
        let sent = Arc::new(Sent::default());
        let limits = HostLimits {
            deadline: Deadline::new(Instant::now() + Duration::from_secs(60)),
            max_http_response_bytes: 0,
            max_object_bytes: 0,
            max_stream_bytes: 0,
            max_sql_bytes: 0,
            max_http_requests: 0,
            max_data_ops: 100_000,
            max_http_timeout: Duration::ZERO,
        };
        let b = Barriers {
            db: DbScope::new(mem.clone(), "acme"),
            tenant: "acme".into(),
            queue: Some(sent.clone()),
            budget: Arc::new(Budget::new(&limits)),
        };
        (b, mem, sent)
    }

    fn spec(id: &str, members: &[&str]) -> CreateSpec {
        CreateSpec {
            id: id.into(),
            members: members.iter().map(|m| m.to_string()).collect(),
            sealed: true,
            continue_with: Some("finalize".into()),
            overdue_with: None,
            state: json!({ "batch": 7 }),
            deadline_s: None,
        }
    }

    fn ok() -> Json {
        json!({ "ok": true })
    }

    fn open_spec(id: &str, members: &[&str]) -> CreateSpec {
        CreateSpec {
            sealed: false,
            ..spec(id, members)
        }
    }

    #[test]
    fn open_lists_accept_early_acks_and_deduplicate_adds_until_sealed() {
        let (b, _, sent) = setup();
        b.create(open_spec("grow", &[])).unwrap();
        assert_eq!(b.status("grow").unwrap()["sealed"], false);
        assert!(!b.resume("grow").unwrap());
        let r = b.reconcile("grow", "", 10, |_| panic!("empty")).unwrap();
        assert_eq!(r["complete"], false);
        assert_eq!(b.add("grow", &["a".into(), "b".into()]).unwrap(), 2);
        assert!(!b.ack("grow", "a", &ok()).unwrap().complete);
        assert!(!b.ack("grow", "b", &ok()).unwrap().complete);
        assert_eq!(
            b.add("grow", &["a".into(), "b".into(), "c".into()])
                .unwrap(),
            1
        );
        assert_eq!(b.pending("grow", 10).unwrap(), vec!["c"]);
        assert_eq!(b.status("grow").unwrap()["total"], 3);
        assert!(sent.0.lock().unwrap().is_empty());
        assert!(!b.seal("grow").unwrap());
        assert!(b.add("grow", &["a".into()]).unwrap_err().contains("sealed"));
        assert!(b.ack("grow", "c", &ok()).unwrap().dispatched);
        assert!(!b.seal("grow").unwrap());
        assert_eq!(sent.0.lock().unwrap().len(), 1);
    }

    #[test]
    fn sealing_empty_or_already_resolved_lists_dispatches_once() {
        for early_ack in [false, true] {
            let (b, _, sent) = setup();
            b.create(open_spec("grow", if early_ack { &["a"] } else { &[] }))
                .unwrap();
            if early_ack {
                b.ack("grow", "a", &ok()).unwrap();
            }
            assert!(b.seal("grow").unwrap());
            assert!(!b.seal("grow").unwrap());
            let status = b.status("grow").unwrap();
            assert_eq!(status["complete"], true);
            assert_eq!(status["sealed"], true);
            assert_eq!(sent.0.lock().unwrap().len(), 1);
        }
    }

    #[test]
    fn interrupted_open_create_and_add_can_be_retried_without_partial_membership() {
        use std::sync::atomic::Ordering;
        let (b, mem, _) = setup();
        b.budget.data_left.store(2, Ordering::Relaxed);
        assert!(b.create(open_spec("grow", &["initial"])).is_err());
        let pk = b.db.barrier_pk("grow").unwrap();
        assert!(
            !serde_json::from_str::<Header>(&mem.get(&pk, HEADER).unwrap().unwrap().value)
                .unwrap()
                .ready
        );
        b.budget.data_left.store(100, Ordering::Relaxed);
        assert!(b
            .create(open_spec("grow", &["different"]))
            .unwrap_err()
            .contains("different members"));
        b.create(open_spec("grow", &["initial"])).unwrap();
        b.ack("grow", "initial", &ok()).unwrap();
        for budget in 0..8 {
            b.budget.data_left.store(budget, Ordering::Relaxed);
            assert!(b.add("grow", &["a".into(), "b".into()]).is_err());
            assert!(mem.get(&pk, "p#a").unwrap().is_none());
            assert!(mem.get(&pk, "p#b").unwrap().is_none());
        }
        b.budget.data_left.store(100, Ordering::Relaxed);
        assert_eq!(b.add("grow", &["a".into(), "b".into()]).unwrap(), 2);
        // Treat a lost success response as a retry: no duplicate count or resurrected member.
        b.ack("grow", "a", &ok()).unwrap();
        assert_eq!(b.add("grow", &["a".into(), "b".into()]).unwrap(), 0);
        b.create(open_spec("grow", &["initial"])).unwrap();
        assert_eq!(b.pending("grow", 10).unwrap(), vec!["b"]);
        assert_eq!(b.status("grow").unwrap()["total"], 3);
    }

    #[test]
    fn seal_commit_is_recoverable_even_when_the_invocation_stops_before_dispatch() {
        use std::sync::atomic::Ordering;
        let (b, mem, sent) = setup();
        b.create(open_spec("grow", &[])).unwrap();
        b.budget.data_left.store(3, Ordering::Relaxed);
        assert!(b.seal("grow").is_err());
        assert_eq!(
            sweep(mem.clone(), sent.clone(), now_secs(), 100)
                .unwrap()
                .fired,
            1
        );
        assert_eq!(sent.0.lock().unwrap().len(), 1);
        b.budget.data_left.store(100, Ordering::Relaxed);
        assert!(!b.seal("grow").unwrap());
    }

    #[test]
    fn stale_add_transaction_cannot_cross_seal_and_legacy_headers_remain_sealed() {
        let (b, mem, _) = setup();
        b.create(open_spec("grow", &[])).unwrap();
        let pk = b.db.barrier_pk("grow").unwrap();
        let (row, mut h) = b.live_header_row(&pk).unwrap();
        b.seal("grow").unwrap();
        h.total += 1;
        assert!(!mem
            .transact_if_current(
                &pk,
                &row,
                item(HEADER, serde_json::to_string(&h).unwrap(), row.ttl),
                vec![(pk.clone(), item("p#late", "1".into(), crate::NO_EXPIRY))],
                now_secs()
            )
            .unwrap());
        assert!(mem.get(&pk, "p#late").unwrap().is_none());
        b.create(spec("old", &["a"])).unwrap();
        let pk = b.db.barrier_pk("old").unwrap();
        let row = mem.get(&pk, HEADER).unwrap().unwrap();
        let mut header: Json = serde_json::from_str(&row.value).unwrap();
        header.as_object_mut().unwrap().remove("sealed");
        mem.put(&pk, item(HEADER, header.to_string(), row.ttl))
            .unwrap();
        assert!(b.add("old", &["b".into()]).is_err());
        assert!(b.ack("old", "a", &ok()).unwrap().dispatched);
    }

    #[test]
    fn sealing_and_the_last_ack_cannot_miss_each_other() {
        for _ in 0..30 {
            let (b, _, sent) = setup();
            b.create(open_spec("grow", &["a"])).unwrap();
            std::thread::scope(|scope| {
                let ack = scope.spawn(|| b.ack("grow", "a", &ok()).map(|_| ()));
                let seal = scope.spawn(|| b.seal("grow").map(|_| ()));
                for result in [ack.join().unwrap(), seal.join().unwrap()] {
                    if let Err(error) = result {
                        assert!(error.contains("retry"), "{error}");
                    }
                }
            });
            assert_eq!(b.status("grow").unwrap()["complete"], true);
            assert_eq!(sent.0.lock().unwrap().len(), 1);
        }
    }

    #[test]
    fn growable_membership_validates_limits_without_writes_and_works_without_a_queue() {
        let (mut b, mem, _) = setup();
        b.queue = None;
        let mut spec = open_spec("grow", &[]);
        spec.continue_with = None;
        b.create(spec).unwrap();
        let before = mem.0.lock().unwrap().clone();
        for names in [
            vec![],
            vec!["a".into(), "a".into()],
            vec!["x".repeat(201)],
            (0..100).map(|i| i.to_string()).collect(),
        ] {
            assert!(b.add("grow", &names).is_err());
            assert_eq!(*mem.0.lock().unwrap(), before);
        }
        let pk = b.db.barrier_pk("grow").unwrap();
        let mut h = b.header(&pk).unwrap();
        h.total = MAX_BARRIER_MEMBERS;
        mem.put(
            &pk,
            item(HEADER, serde_json::to_string(&h).unwrap(), crate::NO_EXPIRY),
        )
        .unwrap();
        assert!(b
            .add("grow", &["new".into()])
            .unwrap_err()
            .contains("10000"));
        assert!(!b.seal("grow").unwrap());
        assert_eq!(b.status("grow").unwrap()["complete"], true);
    }

    #[test]
    fn the_continuation_joins_the_trace_that_created_the_barrier() {
        use opentelemetry::trace::TracerProvider as _;
        use tracing_subscriber::layer::SubscriberExt;
        opentelemetry::global::set_text_map_propagator(
            opentelemetry_sdk::propagation::TraceContextPropagator::new(),
        );
        let tracer = opentelemetry_sdk::trace::SdkTracerProvider::builder().build();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(tracer.tracer("test")));
        tracing::subscriber::with_default(subscriber, || {
            let (b, mem, sent) = setup();
            let created = tracing::info_span!("create").in_scope(|| {
                b.create(spec("t1", &["a"])).unwrap();
                inject_current()
            });
            // A different invocation, with a trace of its own, resolves the last member.
            tracing::info_span!("ack").in_scope(|| {
                assert!(b.ack("t1", "a", &json!({})).unwrap().dispatched);
            });
            let traces = sent.1.lock().unwrap();
            assert_eq!(traces.len(), 1);
            let trace = traces[0].as_ref().expect("dispatch names its trace");
            assert!(trace["traceparent"].starts_with("00-"));
            let id = |t: &TraceContext| t["traceparent"].split('-').nth(1).unwrap().to_string();
            assert_eq!(id(trace), id(&created));
            // Creating it also left a row that finds the barrier by its trace.
            let found = crate::trace_index::list(&*mem, "acme", &id(&created), 10).unwrap();
            assert_eq!(found.len(), 1);
            assert_eq!(
                (found[0].0.as_str(), found[0].1.as_str()),
                ("barrier", "t1")
            );
        });
    }

    #[test]
    fn last_ack_sends_the_continuation_once() {
        let (b, _, sent) = setup();
        b.create(spec("b1", &["a.xml", "b.xml", "c.xml"])).unwrap();
        for (m, last) in [("b.xml", false), ("a.xml", false), ("c.xml", true)] {
            let r = b.ack("b1", m, &ok()).unwrap();
            assert_eq!(
                (r.complete, r.dispatched, r.duplicate),
                (last, last, false),
                "{m}"
            );
        }
        let sent = sent.0.lock().unwrap();
        assert_eq!(sent.len(), 1);
        let msg: Json = serde_json::from_str(&sent[0]).unwrap();
        assert_eq!(msg["tenant"], "acme");
        assert_eq!(msg["script"], "finalize");
        assert_eq!(
            msg["event"],
            json!({ "type": "barrier.complete", "barrier": "b1" })
        );
    }

    #[test]
    fn duplicate_acks_change_nothing_and_never_resend() {
        let (b, _, sent) = setup();
        b.create(spec("b1", &["a", "b"])).unwrap();
        assert!(!b.ack("b1", "a", &ok()).unwrap().complete);
        let again = b
            .ack("b1", "a", &json!({ "ok": false, "error": "late" }))
            .unwrap();
        assert!(again.duplicate && !again.complete);
        b.ack("b1", "b", &ok()).unwrap();
        let after = b.ack("b1", "b", &ok()).unwrap();
        assert!(after.duplicate && after.complete && !after.dispatched);
        assert_eq!(sent.0.lock().unwrap().len(), 1);
        // the first outcome of "a" won
        let pk = b.db.barrier_pk("b1").unwrap();
        assert!(b
            .outcomes(&pk, "")
            .unwrap()
            .iter()
            .all(|(_, o)| o["ok"] == true));
    }

    #[test]
    fn a_redelivered_ack_repairs_a_crash_between_steps() {
        let (b, mem, sent) = setup();
        b.create(spec("b1", &["a", "b"])).unwrap();
        b.ack("b1", "a", &ok()).unwrap();
        // Crash after the outcome of "b" was recorded but before its pending row was retired.
        let pk = b.db.barrier_pk("b1").unwrap();
        mem.put_if_absent(&pk, item("r#b", ok().to_string(), u64::MAX))
            .unwrap();
        assert_eq!(b.pending("b1", 10).unwrap(), vec!["b"]);
        let r = b.ack("b1", "b", &ok()).unwrap();
        assert!(r.complete && r.dispatched && !r.duplicate);
        assert_eq!(sent.0.lock().unwrap().len(), 1);
    }

    #[test]
    fn a_redelivered_last_ack_dispatches_when_the_first_attempt_died_before_sending() {
        let (b, mem, sent) = setup();
        b.create(spec("b1", &["a"])).unwrap();
        // Everything recorded, nothing sent (the claim was never taken): the redelivery sends.
        let pk = b.db.barrier_pk("b1").unwrap();
        mem.delete(&pk, "p#a").unwrap();
        mem.put(&pk, item("r#a", ok().to_string(), u64::MAX))
            .unwrap();
        let r = b.ack("b1", "a", &ok()).unwrap();
        assert!(r.duplicate && r.complete && r.dispatched);
        assert_eq!(sent.0.lock().unwrap().len(), 1);
    }

    #[test]
    fn a_live_claim_makes_the_ack_retry_and_a_lapsed_one_is_taken_over() {
        let (b, mem, sent) = setup();
        b.create(spec("b1", &["a"])).unwrap();
        let pk = b.db.barrier_pk("b1").unwrap();
        mem.put(&pk, item(FINAL, "sending".into(), now_secs() + 30))
            .unwrap();
        assert!(b.ack("b1", "a", &ok()).unwrap_err().contains("retry"));
        mem.put(&pk, item(FINAL, "sending".into(), now_secs() - 1))
            .unwrap();
        assert!(b.ack("b1", "a", &ok()).unwrap().dispatched);
        assert_eq!(sent.0.lock().unwrap().len(), 1);
    }

    #[test]
    fn resume_recovers_abandoned_dispatch_without_replaying_an_ack() {
        let (b, mem, sent) = setup();
        b.create(spec("b1", &["a"])).unwrap();
        assert!(!b.resume("b1").unwrap());
        assert_eq!(b.pending("b1", 10).unwrap(), vec!["a"]);
        assert!(sent.0.lock().unwrap().is_empty());
        let pk = b.db.barrier_pk("b1").unwrap();
        // The last ack retired its member, then the process died holding the send lease.
        mem.put(&pk, item("r#a", ok().to_string(), u64::MAX))
            .unwrap();
        mem.delete(&pk, "p#a").unwrap();
        mem.put(&pk, item(FINAL, "sending".into(), now_secs() + 30))
            .unwrap();
        assert!(b.resume("b1").unwrap_err().contains("retry"));
        mem.put(&pk, item(FINAL, "sending".into(), now_secs() - 1))
            .unwrap();
        assert!(b.resume("b1").unwrap());
        assert!(!b.resume("b1").unwrap());
        assert_eq!(sent.0.lock().unwrap().len(), 1);
        assert_eq!(b.outcomes(&pk, "").unwrap(), vec![("a".into(), ok())]);
    }

    #[test]
    fn reconcile_pages_past_missing_results_and_can_start_a_new_pass() {
        let (b, _, sent) = setup();
        b.create(spec("b1", &["a", "b", "c"])).unwrap();
        let first = b
            .reconcile("b1", "", 2, |m| Ok((m == "b").then(ok)))
            .unwrap();
        assert_eq!(first["checked"], 2);
        assert_eq!(first["acknowledged"], 1);
        assert_eq!(first["next_after"], "b");
        let last = b.reconcile("b1", "b", 2, |_| Ok(Some(ok()))).unwrap();
        assert_eq!(last["next_after"], Json::Null);
        assert_eq!(last["complete"], false);
        assert_eq!(b.pending("b1", 100).unwrap(), vec!["a"]);
        let done = b.reconcile("b1", "", 2, |_| Ok(Some(ok()))).unwrap();
        assert_eq!(done["complete"], true);
        assert_eq!(done["dispatched"], true);
        let again = b
            .reconcile("b1", "", 2, |_| panic!("nothing pending"))
            .unwrap();
        assert_eq!(again["checked"], 0);
        assert_eq!(again["dispatched"], false);
        assert_eq!(sent.0.lock().unwrap().len(), 1);
    }

    #[test]
    fn reconcile_preserves_a_competing_ack_and_propagates_checker_errors() {
        let (b, _, sent) = setup();
        b.create(spec("b1", &["a", "b"])).unwrap();
        let failure = b
            .reconcile("b1", "", 10, |member| {
                if member == "b" {
                    return Err("result store unavailable".into());
                }
                b.ack(
                    "b1",
                    member,
                    &json!({"ok":false,"error":"partner rejected"}),
                )?;
                Ok(Some(ok()))
            })
            .unwrap_err();
        assert_eq!(failure, "result store unavailable");
        assert_eq!(b.pending("b1", 10).unwrap(), vec!["b"]);
        let done = b.reconcile("b1", "", 10, |_| Ok(Some(ok()))).unwrap();
        assert_eq!(done["acknowledged"], 1);
        let pk = b.db.barrier_pk("b1").unwrap();
        assert_eq!(b.outcomes(&pk, "").unwrap()[0].1["ok"], false);
        assert_eq!(sent.0.lock().unwrap().len(), 1);
    }

    #[test]
    fn simultaneous_reconcilers_share_ack_and_dispatch_claims() {
        let (b, _, sent) = setup();
        b.create(spec("b1", &["a"])).unwrap();
        let rendezvous = std::sync::Barrier::new(2);
        std::thread::scope(|scope| {
            let handles: Vec<_> = (0..2)
                .map(|_| {
                    scope.spawn(|| {
                        b.reconcile("b1", "", 10, |_| {
                            rendezvous.wait();
                            Ok(Some(ok()))
                        })
                    })
                })
                .collect();
            for h in handles {
                if let Err(e) = h.join().unwrap() {
                    assert!(e.contains("retry"), "{e}");
                }
            }
        });
        let done = b
            .reconcile("b1", "", 10, |_| panic!("already resolved"))
            .unwrap();
        assert_eq!(done["complete"], true);
        assert_eq!(sent.0.lock().unwrap().len(), 1);
    }

    #[test]
    fn reconcile_refuses_bad_limits_unready_and_expired_barriers_and_meters_work() {
        use std::sync::atomic::Ordering;
        let (b, mem, _) = setup();
        b.create(spec("b1", &["a"])).unwrap();
        for limit in [0, 101] {
            assert!(b
                .reconcile("b1", "", limit, |_| panic!("invalid request"))
                .is_err());
        }
        let pk = b.db.barrier_pk("b1").unwrap();
        let mut h = b.header(&pk).unwrap();
        h.ready = false;
        mem.put(
            &pk,
            item(HEADER, serde_json::to_string(&h).unwrap(), crate::NO_EXPIRY),
        )
        .unwrap();
        assert!(b.reconcile("b1", "", 10, |_| panic!("not ready")).is_err());
        h.ready = true;
        h.expires_s = now_secs() - 1;
        mem.put(
            &pk,
            item(HEADER, serde_json::to_string(&h).unwrap(), crate::NO_EXPIRY),
        )
        .unwrap();
        assert!(b.reconcile("b1", "", 10, |_| panic!("expired")).is_err());
        h.expires_s = crate::NO_EXPIRY;
        mem.put(
            &pk,
            item(HEADER, serde_json::to_string(&h).unwrap(), crate::NO_EXPIRY),
        )
        .unwrap();
        b.budget.data_left.store(2, Ordering::Relaxed);
        assert!(b
            .reconcile("b1", "", 10, |_| Ok(Some(ok())))
            .unwrap_err()
            .contains("limit"));
        assert!(mem.get(&pk, "p#a").unwrap().is_some());
    }

    #[test]
    fn orchestration_state_and_sent_receipts_do_not_expire() {
        let (b, mem, _) = setup();
        b.create(spec("b1", &["a"])).unwrap();
        let pk = b.db.barrier_pk("b1").unwrap();
        assert_eq!(b.header(&pk).unwrap().expires_s, crate::NO_EXPIRY);
        assert_eq!(mem.get(&pk, "p#a").unwrap().unwrap().ttl, crate::NO_EXPIRY);
        b.ack("b1", "a", &ok()).unwrap();
        for key in [HEADER, "r#a", FINAL] {
            assert_eq!(mem.get(&pk, key).unwrap().unwrap().ttl, crate::NO_EXPIRY);
        }
    }

    #[test]
    fn resume_refuses_unknown_expired_and_unready_barriers() {
        let (b, mem, _) = setup();
        assert!(b.resume("missing").is_err());
        b.create(spec("b1", &["a"])).unwrap();
        let pk = b.db.barrier_pk("b1").unwrap();
        let mut h = b.header(&pk).unwrap();
        h.ready = false;
        mem.put(
            &pk,
            item(HEADER, serde_json::to_string(&h).unwrap(), u64::MAX),
        )
        .unwrap();
        assert!(b.resume("b1").unwrap_err().contains("being created"));
        h.ready = true;
        h.expires_s = now_secs() - 1;
        mem.put(
            &pk,
            item(HEADER, serde_json::to_string(&h).unwrap(), u64::MAX),
        )
        .unwrap();
        assert!(b.resume("b1").unwrap_err().contains("expired"));
    }

    #[test]
    fn unknown_members_and_barriers_are_refused() {
        let (b, _, _) = setup();
        b.create(spec("b1", &["a"])).unwrap();
        assert!(b
            .ack("b1", "zzz", &ok())
            .unwrap_err()
            .contains("not a member"));
        assert!(b
            .ack("nope", "a", &ok())
            .unwrap_err()
            .contains("no such barrier"));
        assert!(b
            .create(spec("b2", &["a", "a"]))
            .unwrap_err()
            .contains("duplicate"));
        assert!(b.create(spec("b3", &[])).is_err());
    }

    #[test]
    fn create_is_idempotent_and_does_not_resurrect_acked_members() {
        let (b, _, _) = setup();
        b.create(spec("b1", &["a", "b"])).unwrap();
        b.ack("b1", "a", &ok()).unwrap();
        b.create(spec("b1", &["a", "b"])).unwrap();
        assert_eq!(b.pending("b1", 10).unwrap(), vec!["b"]);
    }

    #[test]
    fn a_failed_member_still_resolves_the_barrier() {
        let (b, _, sent) = setup();
        b.create(spec("b1", &["a", "b"])).unwrap();
        b.ack("b1", "a", &json!({ "ok": false, "error": "schema" }))
            .unwrap();
        assert!(b.ack("b1", "b", &ok()).unwrap().dispatched);
        assert_eq!(sent.0.lock().unwrap().len(), 1);
    }

    #[test]
    fn status_reports_progress_and_overdue() {
        let (b, _, _) = setup();
        let mut s = spec("b1", &["a", "b"]);
        s.deadline_s = Some(0);
        b.create(s).unwrap();
        let st = b.status("b1").unwrap();
        assert_eq!(
            (st["complete"].clone(), st["overdue"].clone()),
            (json!(false), json!(true))
        );
        b.ack("b1", "a", &ok()).unwrap();
        b.ack("b1", "b", &ok()).unwrap();
        let st = b.status("b1").unwrap();
        assert_eq!(
            (
                st["complete"].clone(),
                st["dispatched"].clone(),
                st["overdue"].clone()
            ),
            (json!(true), json!(true), json!(false))
        );
    }

    #[test]
    fn an_unresolved_barrier_gets_an_overdue_event_from_the_sweep() {
        let (b, mem, sent) = setup();
        let mut s = spec("b1", &["a", "b"]);
        s.deadline_s = Some(60);
        s.overdue_with = Some("chase".into());
        b.create(s).unwrap();
        b.ack("b1", "a", &ok()).unwrap();

        let now = now_secs();
        assert_eq!(
            timer::sweep(&*mem, &*sent, now, 10).unwrap().fired,
            0,
            "not due yet"
        );
        assert_eq!(timer::sweep(&*mem, &*sent, now + 61, 10).unwrap().fired, 1);
        let sent = sent.0.lock().unwrap();
        assert_eq!(sent.len(), 1);
        let msg: Json = serde_json::from_str(&sent[0]).unwrap();
        assert_eq!(
            msg,
            json!({
                "tenant": "acme", "script": "chase",
                "event": { "type": "barrier.overdue", "barrier": "b1" },
            })
        );
    }

    #[test]
    fn completing_the_barrier_cancels_its_deadline() {
        let (b, mem, sent) = setup();
        let mut s = spec("b1", &["a"]);
        s.deadline_s = Some(60);
        b.create(s).unwrap();
        assert!(b.ack("b1", "a", &ok()).unwrap().dispatched);
        assert_eq!(sent.0.lock().unwrap().len(), 1, "only the continuation");
        let r = timer::sweep(&*mem, &*sent, now_secs() + 3600, 10).unwrap();
        assert_eq!(r, timer::SweepReport::default());
        assert_eq!(sent.0.lock().unwrap().len(), 1);
    }

    #[test]
    fn a_repeated_create_arms_the_deadline_once_and_a_far_one_is_refused() {
        let (b, mem, sent) = setup();
        let mut s = spec("b1", &["a"]);
        s.deadline_s = Some(0);
        b.create(s).unwrap();
        let mut again = spec("b1", &["a"]);
        again.deadline_s = Some(0);
        b.create(again).unwrap();
        assert_eq!(
            timer::sweep(&*mem, &*sent, now_secs(), 10).unwrap().fired,
            1
        );

        let mut far = spec("b2", &["a"]);
        far.deadline_s = Some(timer::MAX_TIMER_AHEAD_SECS + 1);
        assert!(b.create(far).unwrap_err().contains("deadline_s is at most"));
    }

    #[test]
    fn outcomes_page_past_one_hundred() {
        let (b, _, _) = setup();
        let names: Vec<String> = (0..250).map(|i| format!("doc{i:04}")).collect();
        let refs: Vec<&str> = names.iter().map(String::as_str).collect();
        b.create(spec("big", &refs)).unwrap();
        for n in &names {
            b.ack("big", n, &json!({ "ok": true, "result": n }))
                .unwrap();
        }
        let pk = b.db.barrier_pk("big").unwrap();
        let (mut after, mut seen) = (String::new(), Vec::new());
        loop {
            let page = b.outcomes(&pk, &after).unwrap();
            let n = page.len();
            for (m, _) in page {
                after = m.clone();
                seen.push(m);
            }
            if n < MAX_QUERY_ITEMS {
                break;
            }
        }
        assert_eq!(seen, names);
    }

    #[test]
    fn sweep_recovers_each_interruption_after_indexing_without_an_ack_redelivery() {
        use std::sync::atomic::Ordering;
        for budget in 4..=10 {
            let (b, mem, sent) = setup();
            b.create(spec("b1", &["a"])).unwrap();
            b.budget.data_left.store(budget, Ordering::Relaxed);
            let ack = b.ack("b1", "a", &ok());
            assert_eq!(ack.is_ok(), budget == 10, "budget={budget}");
            let pk = b.db.barrier_pk("b1").unwrap();
            if mem
                .get(&pk, FINAL)
                .unwrap()
                .is_some_and(|r| r.value == "sending")
            {
                mem.put(&pk, item(FINAL, "sending".into(), now_secs() - 1))
                    .unwrap();
            }
            let report = sweep(mem.clone(), sent.clone(), now_secs(), 100).unwrap();
            assert_eq!(report.failed, 0, "budget={budget}");
            assert_eq!(mem.get(&pk, FINAL).unwrap().unwrap().value, "sent");
            assert!(mem.query(RECOVERY, "", "", 100).unwrap().is_empty());
            // A lost send receipt permits duplicates, never lost delivery.
            assert_eq!(
                sent.0.lock().unwrap().len(),
                if budget == 8 { 2 } else { 1 }
            );
        }
    }

    #[test]
    fn failed_index_registration_cannot_retire_the_last_member() {
        use std::sync::atomic::Ordering;
        let (b, mem, sent) = setup();
        b.create(spec("b1", &["a"])).unwrap();
        b.budget.data_left.store(3, Ordering::Relaxed);
        assert!(b.ack("b1", "a", &ok()).is_err());
        let pk = b.db.barrier_pk("b1").unwrap();
        assert!(mem.get(&pk, "p#a").unwrap().is_some());
        assert!(mem.query(RECOVERY, "", "", 100).unwrap().is_empty());
        assert_eq!(
            sweep(mem.clone(), sent.clone(), now_secs(), 100)
                .unwrap()
                .fired,
            0
        );
        b.budget.data_left.store(100, Ordering::Relaxed);
        assert!(b.ack("b1", "a", &ok()).unwrap().dispatched);
    }

    #[test]
    fn concurrent_last_members_have_independent_recovery_entries() {
        let (b, mem, sent) = setup();
        b.create(spec("b1", &["a", "b"])).unwrap();
        let pk = b.db.barrier_pk("b1").unwrap();
        // Both workers save outcomes and register before retiring their members, then crash.
        for member in ["a", "b"] {
            mem.put(
                &pk,
                item(&format!("r#{member}"), ok().to_string(), crate::NO_EXPIRY),
            )
            .unwrap();
            b.register_recovery("b1", Some(member)).unwrap();
            mem.delete(&pk, &format!("p#{member}")).unwrap();
        }
        let reports = std::thread::scope(|scope| {
            let handles: Vec<_> = (0..4)
                .map(|_| {
                    let mem = mem.clone();
                    let sent = sent.clone();
                    scope.spawn(move || sweep(mem, sent, now_secs(), 100).unwrap())
                })
                .collect();
            handles
                .into_iter()
                .map(|h| h.join().unwrap())
                .collect::<Vec<_>>()
        });
        assert_eq!(reports.iter().map(|r| r.fired).sum::<usize>(), 1);
        assert_eq!(sent.0.lock().unwrap().len(), 1);
        // A contender can observe the active dispatch lease; the next eligible sweep cleans it.
        sweep(mem.clone(), sent.clone(), now_secs() + 61, 100).unwrap();
        assert!(mem.query(RECOVERY, "", "", 100).unwrap().is_empty());
        assert_eq!(sent.0.lock().unwrap().len(), 1);
    }

    struct SelectiveFailure(Arc<Sent>);
    impl QueueBackend for SelectiveFailure {
        fn send(&self, messages: &[OutMessage]) -> Result<(), String> {
            if messages.iter().any(|m| m.body.contains("broken")) {
                return Err("queue unavailable for broken flow".into());
            }
            self.0.send(messages)
        }
    }

    fn interrupted(b: &Barriers, mem: &Mem, id: &str) {
        b.create(spec(id, &["a"])).unwrap();
        let pk = b.db.barrier_pk(id).unwrap();
        mem.put(&pk, item("r#a", ok().to_string(), crate::NO_EXPIRY))
            .unwrap();
        b.register_recovery(id, Some("a")).unwrap();
        mem.delete(&pk, "p#a").unwrap();
    }

    #[test]
    fn failed_dispatch_backs_off_and_the_cursor_does_not_starve_later_work() {
        let (b, mem, sent) = setup();
        interrupted(&b, &mem, "broken");
        interrupted(&b, &mem, "healthy");
        let queue = Arc::new(SelectiveFailure(sent.clone()));
        let now = now_secs();
        assert_eq!(sweep(mem.clone(), queue.clone(), now, 1).unwrap().failed, 1);
        let key = recovery_key("acme", "broken", Some("a"));
        let claim = mem.get(RECOVERY_CLAIMS, &key).unwrap().unwrap();
        assert_eq!(claim.ttl, now + 60);
        assert_eq!(sweep(mem.clone(), queue.clone(), now, 1).unwrap().fired, 1);
        assert_eq!(sent.0.lock().unwrap().len(), 1);
        // Cursor reaches the end, then wraps. The failed entry is skipped during backoff.
        sweep(mem.clone(), queue.clone(), now, 1).unwrap();
        assert_eq!(
            sweep(mem.clone(), queue.clone(), now + 1, 100)
                .unwrap()
                .skipped,
            1
        );
        let pk = b.db.barrier_pk("broken").unwrap();
        mem.put(&pk, item(FINAL, "sending".into(), now - 1))
            .unwrap();
        assert_eq!(
            sweep(mem.clone(), queue.clone(), now + 60, 100)
                .unwrap()
                .failed,
            1
        );
        let claim = mem.get(RECOVERY_CLAIMS, &key).unwrap().unwrap();
        assert_eq!(claim.ttl, now + 60 + 120);
        mem.put(&pk, item(FINAL, "sending".into(), now - 1))
            .unwrap();
        assert_eq!(
            sweep(mem.clone(), sent.clone(), now + 180, 100)
                .unwrap()
                .fired,
            1
        );
        assert!(mem.query(RECOVERY, "", "", 100).unwrap().is_empty());
    }

    #[test]
    fn idle_sweep_is_read_only_and_does_not_index_active_or_historical_barriers() {
        let (b, mem, sent) = setup();
        b.create(spec("active", &["a", "b"])).unwrap();
        b.ack("active", "a", &ok()).unwrap();
        b.create(spec("done", &["a"])).unwrap();
        b.ack("done", "a", &ok()).unwrap();
        let before = mem.0.lock().unwrap().clone();
        assert_eq!(
            sweep(mem.clone(), sent.clone(), now_secs(), 100).unwrap(),
            timer::SweepReport::default()
        );
        assert_eq!(*mem.0.lock().unwrap(), before);
        assert_eq!(sent.0.lock().unwrap().len(), 1);
        assert!(mem.query(RECOVERY, "", "", 100).unwrap().is_empty());
    }

    #[test]
    fn resume_failure_is_indexed_and_missing_barriers_are_retired() {
        let (mut b, mem, sent) = setup();
        interrupted(&b, &mem, "broken");
        // Legacy barriers do not have an ack index; an explicit resume enrolls them.
        b.clear_recovery("broken", Some("a")).unwrap();
        b.queue = Some(Arc::new(SelectiveFailure(sent.clone())));
        assert!(b.resume("broken").is_err());
        let pk = b.db.barrier_pk("broken").unwrap();
        mem.put(&pk, item(FINAL, "sending".into(), now_secs() - 1))
            .unwrap();
        assert_eq!(
            sweep(mem.clone(), sent.clone(), now_secs(), 100)
                .unwrap()
                .fired,
            1
        );
        interrupted(&b, &mem, "removed");
        mem.delete(&b.db.barrier_pk("removed").unwrap(), HEADER)
            .unwrap();
        assert_eq!(
            sweep(mem.clone(), sent.clone(), now_secs(), 100)
                .unwrap()
                .failed,
            0
        );
        assert!(mem.query(RECOVERY, "", "", 100).unwrap().is_empty());
    }

    #[test]
    fn scripts_cannot_reach_barrier_rows_through_native_db() {
        let (b, mem, _) = setup();
        b.create(spec("b1", &["a"])).unwrap();
        let scripts = DbScope::new(mem, "acme");
        assert!(scripts.query("barrier#b1", "", None).unwrap().is_empty());
    }
}
