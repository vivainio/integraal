//! Durable, replayed workflows. Journal writes are fenced by the invocation's control row.
use crate::{
    db::{hex, now_secs, random},
    queue::valid_name,
    timer::Payload,
    Budget, DbScope, HostContext, Item, NO_EXPIRY,
};
use mlua::{Function, Lua, LuaSerdeExt, Table, Value};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as Json};
use sha2::{Digest, Sha256};
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
};

pub(crate) const DRIVER: &str = "integraal.sequence.driver";
const LIMIT: usize = 64 * 1024;
const LEASE: u64 = 900;
const STOP: &str = "sequence suspended";

fn err(e: impl std::fmt::Display) -> mlua::Error {
    mlua::Error::runtime(e.to_string())
}
fn hash(parts: &[&str]) -> String {
    let mut h = Sha256::new();
    for p in parts {
        h.update((p.len() as u64).to_be_bytes());
        h.update(p.as_bytes());
    }
    hex(&h.finalize())
}
fn row(sk: &str, value: &impl Serialize) -> Result<Item, String> {
    let value = serde_json::to_string(value).map_err(|e| e.to_string())?;
    if value.len() > LIMIT {
        return Err("sequence record exceeds 64 KB; use storage references".into());
    }
    Ok(Item {
        sk: sk.into(),
        value,
        ttl: NO_EXPIRY,
    })
}
fn parse<T: serde::de::DeserializeOwned>(item: &Item) -> Result<T, String> {
    serde_json::from_str(&item.value).map_err(|e| format!("invalid sequence record: {e}"))
}
fn name(s: &str) -> Result<(), String> {
    if s.is_empty() || s.len() > 200 || s.chars().any(char::is_control) {
        Err("sequence step/signal name must be 1–200 bytes without control characters".into())
    } else {
        Ok(())
    }
}
#[derive(Clone, Serialize, Deserialize)]
struct State {
    script: String,
    version: String,
    input: Json,
    token: String,
    lease_until: u64,
    complete: bool,
    result: Json,
    // Incremented on every fenced write so the expected row always changes.
    revision: u64,
}
#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase")]
enum Step {
    Result { value: Json },
    Sleep { due_s: u64, delay_s: u64 },
    Wait,
    Yield,
}
#[derive(Clone)]
struct Env {
    db: DbScope,
    tenant: String,
    budget: Arc<Budget>,
}
impl Env {
    fn charge(&self, n: usize) -> Result<(), String> {
        for _ in 0..n {
            self.budget.take_data()?;
        }
        Ok(())
    }
    fn get(&self, pk: &str, sk: &str) -> Result<Option<Item>, String> {
        self.charge(1)?;
        self.db.raw().get(pk, sk)
    }
    fn send(&self, id: &str, payload: Payload) -> Result<bool, String> {
        self.charge(4)?;
        crate::outbox::record(self.db.raw().as_ref(), &self.tenant, id, payload)
    }
}
struct Session {
    env: Env,
    id: String,
    pk: String,
    current: Item,
    state: State,
    seen: HashSet<String>,
    journal: Option<HashMap<String, Item>>,
    journal_complete: bool,
    fresh_steps: usize,
    inside_step: bool,
    stopped: bool,
    active: bool,
}
impl Session {
    fn check(&self) -> Result<(), String> {
        if !self.active {
            return Err("sequence invocation has ended".into());
        }
        if self.stopped {
            return Err(STOP.into());
        }
        if self.state.lease_until <= now_secs() {
            return Err("sequence ownership expired; retry".into());
        }
        Ok(())
    }
    fn enter(&mut self, id: &str) -> Result<Option<Step>, String> {
        self.check()?;
        name(id)?;
        if self.inside_step {
            return Err("sequence steps cannot be nested".into());
        }
        if !self.seen.insert(id.into()) {
            return Err(format!("duplicate sequence step {id:?}"));
        }
        if self.journal.is_none() {
            let mut journal = HashMap::new();
            let mut after = String::new();
            let mut bytes = 0usize;
            loop {
                self.env.charge(1)?;
                let page = self.env.db.raw().query(&self.pk, "step/", &after, 100)?;
                if page.is_empty() {
                    self.journal_complete = true;
                    break;
                }
                after = page.last().unwrap().sk.clone();
                bytes += page
                    .iter()
                    .map(|r| r.value.len() + r.sk.len())
                    .sum::<usize>();
                journal.extend(page.into_iter().map(|r| (r.sk.clone(), r)));
                // Bound host-side caching independently of the Luau heap limit.
                if bytes >= 8 * 1024 * 1024 {
                    break;
                }
            }
            self.journal = Some(journal);
        }
        let saved = self
            .journal
            .as_ref()
            .unwrap()
            .get(&format!("step/{id}"))
            .cloned();
        let saved = if saved.is_none() && !self.journal_complete {
            self.env.get(&self.pk, &format!("step/{id}"))?
        } else {
            saved
        };
        if saved.is_none()
            && (self.fresh_steps >= 25
                || (self.fresh_steps > 0
                    && self.env.budget.deadline.remaining().as_millis() < 1000))
        {
            self.env.send(
                &format!("sequence/auto/{}", hash(&[&self.id, id])),
                self.payload(),
            )?;
            self.stopped = true;
            return Err(STOP.into());
        }
        saved.as_ref().map(parse).transpose()
    }
    fn save(&mut self, additional: Vec<Item>) -> Result<(), String> {
        self.check()?;
        self.state.revision += 1;
        let next = row("", &self.state)?;
        self.env.charge(1)?;
        if !self.env.db.raw().transact_if_current(
            &self.pk,
            &self.current,
            next.clone(),
            additional
                .into_iter()
                .map(|r| (self.pk.clone(), r))
                .collect(),
            now_secs(),
        )? {
            return Err("sequence ownership changed; retry".into());
        }
        self.current = next;
        // A step can only be visited once in one invocation, so the replay cache needs no update.
        self.fresh_steps += 1;
        Ok(())
    }
    fn payload(&self) -> Payload {
        Payload {
            script: Some(self.state.script.clone()),
            version: Some(self.state.version.clone()),
            event: json!({"type":"sequence.resume", "id":self.id}),
            trace: Some(crate::trace::inject_current()),
        }
    }
    fn suspend(&mut self) -> mlua::Error {
        self.stopped = true;
        err(STOP)
    }
    // Release only our exact control row; stale invocations cannot release a replacement owner.
    fn release(&mut self, result: Option<Json>) -> Result<(), String> {
        if result.is_some() {
            self.check()?;
        }
        let mut state = self.state.clone();
        state.lease_until = 0;
        if let Some(value) = result {
            state.complete = true;
            state.result = value;
        }
        state.revision += 1;
        let next = row("", &state)?;
        // Cleanup must still run when the ordinary operation budget is exhausted.
        if !self
            .env
            .db
            .raw()
            .replace_if_current(&self.pk, &self.current, next, now_secs())?
        {
            return Err("sequence ownership changed; retry".into());
        }
        self.active = false;
        Ok(())
    }
}
fn sequence(lua: &Lua, session: Arc<Mutex<Session>>) -> mlua::Result<Table> {
    let seq = lua.create_table()?;
    seq.set("id", session.lock().unwrap().id.clone())?;
    let s = session.clone();
    seq.set(
        "key",
        lua.create_function(move |_, (_self, id): (Table, String)| {
            name(&id).map_err(err)?;
            let s = s.lock().unwrap();
            s.check().map_err(err)?;
            Ok(format!("sequence/{}", hash(&[&s.env.tenant, &s.id, &id])))
        })?,
    )?;
    let s = session.clone();
    seq.set(
        "step",
        lua.create_function(move |lua, (_self, id, f): (Table, String, Function)| {
            {
                let mut s = s.lock().unwrap();
                match s.enter(&id).map_err(err)? {
                    Some(Step::Result { value }) => return lua.to_value(&value),
                    Some(_) => return Err(err("sequence step kind changed")),
                    None => {}
                }
                s.inside_step = true;
            }
            // Do not hold the session mutex while running tenant code.
            let outcome = f.call::<Value>(());
            let mut s = s.lock().unwrap();
            s.inside_step = false;
            let value: Json = lua.from_value(outcome?)?;
            s.save(vec![row(
                &format!("step/{id}"),
                &Step::Result {
                    value: value.clone(),
                },
            )
            .map_err(err)?])
                .map_err(err)?;
            lua.to_value(&value)
        })?,
    )?;
    let s = session.clone();
    seq.set(
        "sleep",
        lua.create_function(move |_, (_self, id, delay): (Table, String, u64)| {
            let mut s = s.lock().unwrap();
            if delay > crate::MAX_TIMER_AHEAD_SECS {
                return Err(err("sequence sleep exceeds six days"));
            }
            let due = match s.enter(&id).map_err(err)? {
                Some(Step::Sleep { due_s, delay_s }) if delay_s == delay => due_s,
                Some(_) => return Err(err("sequence sleep changed")),
                None => {
                    let due_s = now_secs() + delay;
                    s.save(vec![row(
                        &format!("step/{id}"),
                        &Step::Sleep {
                            due_s,
                            delay_s: delay,
                        },
                    )
                    .map_err(err)?])
                        .map_err(err)?;
                    due_s
                }
            };
            if now_secs() >= due {
                return Ok(());
            }
            s.env.charge(1).map_err(err)?;
            crate::register_timer(
                s.env.db.raw().as_ref(),
                &s.env.tenant,
                &format!("sequence/{}", hash(&[&s.id, &id])),
                due,
                &s.payload(),
                now_secs(),
            )
            .map_err(err)?;
            Err::<(), _>(s.suspend())
        })?,
    )?;
    let s = session.clone();
    seq.set(
        "wait",
        lua.create_function(move |lua, (_self, id): (Table, String)| {
            let mut s = s.lock().unwrap();
            match s.enter(&id).map_err(err)? {
                Some(Step::Wait) => {}
                Some(_) => return Err(err("sequence step kind changed")),
                None => s
                    .save(vec![row(&format!("step/{id}"), &Step::Wait).map_err(err)?])
                    .map_err(err)?,
            }
            if let Some(r) = s.env.get(&s.pk, &format!("signal/{id}")).map_err(err)? {
                return lua.to_value(&parse::<Json>(&r).map_err(err)?);
            }
            Err(s.suspend())
        })?,
    )?;
    let s = session;
    seq.set(
        "yield",
        lua.create_function(move |_, (_self, id): (Table, String)| {
            let mut s = s.lock().unwrap();
            match s.enter(&id).map_err(err)? {
                Some(Step::Yield) => return Ok(()),
                Some(_) => return Err(err("sequence step kind changed")),
                None => {}
            }
            // Persist the handoff first: a crash before the journal write causes another yield,
            // rather than a completed yield without its continuation.
            s.env
                .send(
                    &format!("sequence/yield/{}", hash(&[&s.id, &id])),
                    s.payload(),
                )
                .map_err(err)?;
            s.save(vec![row(&format!("step/{id}"), &Step::Yield).map_err(err)?])
                .map_err(err)?;
            Err::<(), _>(s.suspend())
        })?,
    )?;
    Ok(seq)
}

fn dispatch(
    lua: &Lua,
    env: Env,
    workflow: Function,
    script: String,
    version: String,
    event: Json,
) -> mlua::Result<Value> {
    let id = event["id"]
        .as_str()
        .ok_or_else(|| err("sequence event needs id"))?
        .to_string();
    let pk = env.db.sequence_pk(&id).map_err(err)?;
    let kind = event["type"].as_str().unwrap_or("");
    if kind == "sequence.start" {
        let state = State {
            script: script.clone(),
            version: version.clone(),
            input: event["input"].clone(),
            token: String::new(),
            lease_until: 0,
            complete: false,
            result: Json::Null,
            revision: 0,
        };
        env.charge(1).map_err(err)?;
        env.db
            .raw()
            .put_if_absent(&pk, row("", &state).map_err(err)?)
            .map_err(err)?;
    }
    let current = env
        .get(&pk, "")
        .map_err(err)?
        .ok_or_else(|| err("sequence not started; retry"))?;
    let mut state: State = parse(&current).map_err(err)?;
    if state.script != script
        || state.version != version
        || (kind == "sequence.start" && state.input != event["input"])
    {
        return Err(err(
            "sequence id already belongs to different script, version or input",
        ));
    }
    if kind == "sequence.signal" {
        let signal = event["name"]
            .as_str()
            .ok_or_else(|| err("sequence signal needs name"))?;
        name(signal).map_err(err)?;
        let r = row(&format!("signal/{signal}"), &event["value"]).map_err(err)?;
        env.charge(1).map_err(err)?;
        env.db.raw().put_if_absent(&pk, r.clone()).map_err(err)?;
        let saved = env
            .get(&pk, &r.sk)
            .map_err(err)?
            .ok_or_else(|| err("sequence signal missing"))?;
        if saved.value != r.value {
            return Err(err("sequence signal already has a different value"));
        }
    }
    if state.complete {
        return lua.to_value(&json!({"state":"complete", "result":state.result}));
    }
    let now = now_secs();
    if state.lease_until > now {
        return Err(err("sequence is busy; retry later"));
    }
    state.token = hex(&random::<16>().map_err(err)?);
    state.lease_until = now + LEASE;
    state.revision += 1;
    let owned = row("", &state).map_err(err)?;
    env.charge(1).map_err(err)?;
    if !env
        .db
        .raw()
        .replace_if_current(&pk, &current, owned.clone(), now)
        .map_err(err)?
    {
        return Err(err("sequence ownership changed; retry"));
    }
    let input = lua.to_value(&state.input)?;
    let session = Arc::new(Mutex::new(Session {
        env,
        id,
        pk,
        current: owned,
        state,
        seen: HashSet::new(),
        journal: None,
        journal_complete: false,
        fresh_steps: 0,
        inside_step: false,
        stopped: false,
        active: true,
    }));
    let outcome =
        sequence(lua, session.clone()).and_then(|seq| workflow.call::<Value>((seq, input)));
    let mut s = session.lock().unwrap();
    if s.stopped {
        s.release(None).map_err(err)?;
        return lua.to_value(&json!({"state":"suspended"}));
    }
    match outcome.and_then(|v| lua.from_value::<Json>(v)) {
        Ok(result) => {
            if let Err(e) = s.release(Some(result.clone())) {
                let _ = s.release(None);
                return Err(err(e));
            }
            lua.to_value(&json!({"state":"complete", "result":result}))
        }
        Err(e) => {
            let _ = s.release(None);
            Err(e)
        }
    }
}

pub(crate) fn api(lua: &Lua, host: &HostContext, budget: Arc<Budget>) -> mlua::Result<Table> {
    let db = host.db.clone();
    let enabled = host.queue.is_some();
    let tenant = host.tenant.clone();
    let make = move || -> mlua::Result<Env> {
        if !enabled {
            return Err(err("sequence needs a tenant-enabled queue"));
        }
        Ok(Env {
            db: db.clone().ok_or_else(|| err("sequence needs ig.db"))?,
            tenant: tenant.clone(),
            budget: budget.clone(),
        })
    };
    let api = lua.create_table()?;
    let m = make.clone();
    api.set(
        "start",
        lua.create_function(move |lua, (id, opts): (String, Table)| {
            let env = m()?;
            env.db.sequence_pk(&id).map_err(err)?;
            let script: String = opts.get("script")?;
            let version: String = opts.get("version")?;
            if !valid_name(&script)
                || script == "lib"
                || !valid_name(&version)
                || version == "latest"
            {
                return Err(err(
                    "sequence start requires a script and explicit version (not latest)",
                ));
            }
            let input: Json = lua.from_value(opts.get::<Value>("input")?)?;
            // Validate before recording a launch that could never be processed.
            row(
                "",
                &State {
                    script: script.clone(),
                    version: version.clone(),
                    input: input.clone(),
                    token: "0".repeat(32),
                    lease_until: u64::MAX,
                    complete: false,
                    result: Json::Null,
                    revision: u64::MAX,
                },
            )
            .map_err(err)?;
            env.send(
                &format!("sequence/start/{}", hash(&[&id])),
                Payload {
                    script: Some(script),
                    version: Some(version),
                    event: json!({"type":"sequence.start", "id":id, "input":input}),
                    trace: Some(crate::trace::inject_current()),
                },
            )
            .map_err(err)
        })?,
    )?;
    let m = make.clone();
    api.set(
        "signal",
        lua.create_function(move |lua, (id, signal, value): (String, String, Value)| {
            let env = m()?;
            let pk = env.db.sequence_pk(&id).map_err(err)?;
            name(&signal).map_err(err)?;
            let current = env
                .get(&pk, "")
                .map_err(err)?
                .ok_or_else(|| err("sequence has not started yet; retry"))?;
            let state: State = parse(&current).map_err(err)?;
            let value: Json = lua.from_value(value)?;
            row("", &value).map_err(err)?;
            env.send(
                &format!("sequence/signal/{}", hash(&[&id, &signal])),
                Payload {
                    script: Some(state.script),
                    version: Some(state.version),
                    event: json!({"type":"sequence.signal", "id":id, "name":signal, "value":value}),
                    trace: Some(crate::trace::inject_current()),
                },
            )
            .map_err(err)
        })?,
    )?;
    lua.set_named_registry_value(
        DRIVER,
        lua.create_function(
            move |lua, (workflow, script, version, event): (Function, String, String, Value)| {
                if !valid_name(&version) || version == "latest" {
                    return Err(err("sequence requires an explicit script version"));
                }
                dispatch(
                    lua,
                    make()?,
                    workflow,
                    script,
                    version,
                    lua.from_value(event)?,
                )
            },
        )?,
    )?;
    Ok(api)
}
