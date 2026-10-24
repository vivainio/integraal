//! `native.batch`: hand a unit of work to a partner through a bucket and pick the answer up later.
//!
//! A job is a folder in the deployment's dedicated batch bucket (never one of the tenant's data
//! buckets), under the tenant's own prefix:
//!
//! ```text
//! <tenant>/jobs/<job>/request.json   written by the script; the partner reads it
//! <tenant>/jobs/<job>/<part files>   written by the partner
//! <tenant>/jobs/<job>/done.json      written by the partner last: the manifest and the commit marker
//! <tenant>/state/<job>.json          written by the script; the partner never needs (or gets) it
//! ```
//!
//! The partner's `done.json` is an ordinary S3 notification, so it reaches the tenant's router like
//! any other; `native.batch.route` turns it into the continuation script saved with the job, and
//! `native.batch.hydrate` loads the job's state and results there.

use std::{
    collections::BTreeMap,
    io::{self, BufReader, Read},
    sync::{Arc, Mutex},
};

use flate2::bufread::MultiGzDecoder;
use mlua::{Lua, LuaSerdeExt, MetaMethod, Table, UserData, UserDataFields, UserDataMethods, Value};
use serde_json::{json, Value as Json};
use sha2::{Digest, Sha256};

use crate::{
    gzip::Capped,
    ndjson::Rows,
    http_job::{self, Prepared},
    xml::{ByteStream, Limited},
    BucketScope, Budget, Deadline, HostContext,
};

/// The `bucket` name carried by events and `delete` entries for the batch bucket. It is not a valid
/// `buckets.map` name, so it can never collide with one of the tenant's data buckets.
pub const BATCH_BUCKET: &str = "$batch";
const JOBS: &str = "jobs/";
const STATES: &str = "state/";
const MAX_QUERIES: usize = 64;
const MAX_PARTS: usize = 10_000;
const FORMAT: u64 = 1;

fn err(m: impl std::fmt::Display) -> mlua::Error {
    mlua::Error::runtime(m.to_string())
}

fn token_ok(s: &str, max: usize) -> bool {
    !s.is_empty()
        && s.len() <= max
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_'))
}

fn job_dir(id: &str) -> String {
    format!("{JOBS}{id}/")
}

fn state_key(id: &str) -> String {
    format!("{STATES}{id}.json")
}

/// `batch/<job>/done.json` -> `<job>`.
fn job_of_done_key(key: &str) -> Option<&str> {
    let id = key.strip_prefix(JOBS)?.strip_suffix("/done.json")?;
    token_ok(id, 64).then_some(id)
}

/// A part key from `done.json`: relative to the job folder, with nothing that climbs out of it.
fn part_key_ok(k: &str) -> bool {
    !k.is_empty()
        && k.len() <= 256
        && !k.starts_with('/')
        && !k.chars().any(|c| c.is_control() || c == '\\')
        && k.split('/').all(|s| !s.is_empty() && s != "." && s != "..")
}

struct Ctx {
    batch: Option<BucketScope>,
    budget: Arc<Budget>,
    max_stream: u64,
    max_line: usize,
    deadline: Deadline,
    /// Present only where requests can outlive the invocation (the Fargate server).
    http: Option<http_job::Env>,
}

impl Ctx {
    fn scope(&self) -> mlua::Result<BucketScope> {
        self.batch.clone().ok_or_else(|| {
            err("native.batch is not available: the tenant config needs \"batch\": true and the deployment a `batch` bucket")
        })
    }

    fn op(&self) -> mlua::Result<()> {
        self.budget.take_data().map_err(err)
    }
}

pub fn api(lua: &Lua, host: &HostContext, budget: Arc<Budget>) -> mlua::Result<Table> {
    let limits = &host.limits;
    let ctx = Arc::new(Ctx {
        batch: host.batch.clone(),
        budget,
        max_stream: limits.max_stream_bytes,
        max_line: limits.max_object_bytes,
        deadline: limits.deadline.clone(),
        http: host.background_http.then(|| http_job::Env {
            tenant: host.tenant.clone(),
            policy: host.http.clone(),
            vault: host.vault.clone(),
            throttle: host.throttle.clone(),
            max_body: limits.max_http_response_bytes,
        }),
    });
    let batch = lua.create_table()?;

    let c = ctx.clone();
    batch.set(
        "new",
        lua.create_function(move |_, opts: Option<Table>| {
            let scope = c.scope()?;
            let id = match opts
                .map(|o| o.get::<Option<String>>("id"))
                .transpose()?
                .flatten()
            {
                Some(id) if token_ok(&id, 64) => id,
                Some(id) => return Err(err(format!("invalid job id {id:?}"))),
                None => crate::db::hex(&crate::db::random::<16>().map_err(err)?),
            };
            Ok(Job {
                ctx: c.clone(),
                scope,
                id,
                draft: Mutex::new(Draft::default()),
            })
        })?,
    )?;

    let c = ctx.clone();
    batch.set(
        "hydrate",
        lua.create_function(move |lua, (event, opts): (Table, Option<Table>)| {
            let allow_failure = opts
                .map(|o| o.get::<Option<bool>>("allow_failure"))
                .transpose()?
                .flatten()
                .unwrap_or(false);
            hydrate(lua, &c, &event, allow_failure)
        })?,
    )?;

    let c = ctx;
    batch.set(
        "route",
        lua.create_function(move |lua, event: Table| route(lua, &c, &event))?,
    )?;
    Ok(batch)
}

/// Input data attached to a job: files under `inputs/`, listed in `request.json`.
struct Input {
    /// Known only while every part came from `input_rows`.
    rows: Option<u64>,
    parts: Vec<Json>,
}

/// Parts of `input_rows` are cut at about this many bytes.
const PART_BYTES: u64 = 64 * 1024 * 1024;

#[derive(Default)]
struct Draft {
    inputs: BTreeMap<String, Input>,
    queries: Vec<(String, String, Json)>,
    state: Json,
    script: Option<String>,
    http: Vec<(String, Prepared)>,
    submitted: bool,
}

impl Draft {
    fn name_taken(&self, name: &str) -> bool {
        self.queries.iter().any(|(n, _, _)| n == name) || self.http.iter().any(|(n, _)| n == name)
    }
}

/// Runs the job's requests on their own threads and, when all have finished, commits the results:
/// bodies first, then `done.json`. A request that fails fails the job with `ok: false`.
fn spawn_http(env: http_job::Env, scope: BucketScope, id: String, reqs: Vec<(String, Prepared)>) {
    let span = tracing::Span::current();
    let spawned = std::thread::Builder::new()
        .name(format!("batch-http-{id}"))
        .spawn(move || {
            let _e = span.enter();
            let done = run_http(&env, &scope, &id, reqs).unwrap_or_else(|e| {
                tracing::warn!(job = %id, error = %e, "batch http job failed");
                json!({"ok": false, "error": e})
            });
            let key = format!("{}done.json", job_dir(&id));
            let body = serde_json::to_vec(&done).expect("done.json serializes");
            if let Err(e) = scope.put(&key, body) {
                tracing::error!(job = %id, error = %e, "could not write done.json for batch http job");
            }
        });
    if let Err(e) = spawned {
        tracing::error!(error = %e, "could not start batch http thread");
    }
}

fn run_http(
    env: &http_job::Env,
    scope: &BucketScope,
    id: &str,
    reqs: Vec<(String, Prepared)>,
) -> Result<Json, String> {
    let responses: Vec<_> = std::thread::scope(|s| {
        let handles: Vec<_> = reqs
            .iter()
            .map(|(_, p)| s.spawn(|| http_job::execute(env.throttle.as_deref(), env.max_body, p)))
            .collect();
        handles
            .into_iter()
            .map(|h| {
                h.join()
                    .unwrap_or_else(|_| Err("http worker panicked".to_string()))
            })
            .collect()
    });
    let mut results = serde_json::Map::new();
    for ((name, _), resp) in reqs.iter().zip(responses) {
        let resp = resp.map_err(|e| format!("{name}: {e}"))?;
        let key = format!("{name}.0000.body");
        let mut w = scope
            .create(&format!("{}{key}", job_dir(id)))
            .map_err(|e| format!("{name}: {e}"))?;
        w.write(&resp.body).map_err(|e| format!("{name}: {e}"))?;
        w.commit().map_err(|e| format!("{name}: {e}"))?;
        results.insert(
            name.clone(),
            json!({
                "status": resp.status,
                "headers": resp.headers,
                "parts": [{
                    "key": key,
                    "bytes": resp.body.len(),
                    "sha256": crate::db::hex(&Sha256::digest(&resp.body)),
                    "raw": true,
                }],
            }),
        );
    }
    Ok(json!({"ok": true, "results": results}))
}

struct Job {
    ctx: Arc<Ctx>,
    scope: BucketScope,
    id: String,
    draft: Mutex<Draft>,
}

impl UserData for Job {
    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        // Adds a named query to the request. Returns the name, which is what `rows`/`open` take later.
        methods.add_method(
            "query",
            |lua, this, (name, query, params): (String, String, Option<Value>)| {
                if !token_ok(&name, 64) {
                    return Err(err(format!(
                        "invalid result name {name:?} (letters, digits, - and _; at most 64)"
                    )));
                }
                if query.is_empty() || query.len() > 256 {
                    return Err(err("query must be 1 to 256 characters"));
                }
                let params: Json = match params {
                    None | Some(Value::Nil) => json!({}),
                    Some(v) => lua.from_value(v)?,
                };
                let mut d = this.draft.lock().unwrap();
                if d.submitted {
                    return Err(err("batch job was already submitted"));
                }
                if d.queries.len() >= MAX_QUERIES {
                    return Err(err(format!("more than {MAX_QUERIES} queries in one job")));
                }
                if d.name_taken(&name) {
                    return Err(err(format!("duplicate result name {name:?}")));
                }
                d.queries.push((name.clone(), query, params));
                Ok(name)
            },
        );
        // A request the host performs itself once the job is submitted, however long it takes: only
        // the response is written to the bucket, and the continuation runs when it arrives. Takes
        // the options of `native.http.request` (without `stream`). The request is checked now.
        methods.add_method("http", |_, this, (name, opts): (String, Table)| {
            let env = this.ctx.http.as_ref().ok_or_else(|| {
                err("job:http is only available on the Fargate server, not on Lambda")
            })?;
            if !token_ok(&name, 64) {
                return Err(err(format!(
                    "invalid result name {name:?} (letters, digits, - and _; at most 64)"
                )));
            }
            let mut d = this.draft.lock().unwrap();
            if d.submitted {
                return Err(err("batch job was already submitted"));
            }
            if d.queries.len() + d.http.len() >= MAX_QUERIES {
                return Err(err(format!("more than {MAX_QUERIES} queries in one job")));
            }
            if d.name_taken(&name) {
                return Err(err(format!("duplicate result name {name:?}")));
            }
            let prepared = env.prepare(&this.ctx.budget, &opts)?;
            d.http.push((name.clone(), prepared));
            Ok(name)
        });
        // Attaches data to the request as a file (or, called again with the same name, another part
        // of that input): a string or a stream, copied in chunks. Written now; the partner only
        // looks once `submit` writes `request.json`, which lists these files.
        methods.add_method(
            "input",
            |_, this, (name, content, opts): (String, Value, Option<Table>)| {
                let ext = input_ext(opts.as_ref())?;
                if !token_ok(&name, 64) {
                    return Err(err(format!("invalid input name {name:?}")));
                }
                let mut d = this.draft.lock().unwrap();
                if d.submitted {
                    return Err(err("batch job was already submitted"));
                }
                let n = d.inputs.get(&name).map_or(0, |i| i.parts.len());
                if n >= MAX_PARTS {
                    return Err(err(format!("input {name:?} has too many parts")));
                }
                let mut out = PartOut::start(this, &name, n, &ext)?;
                match content {
                    Value::String(s) => out.write(&s.as_bytes())?,
                    Value::UserData(ud) => {
                        let mut r = Limited {
                            inner: crate::xml::take_stream(&ud)?,
                            left: this.ctx.max_stream,
                            deadline: this.ctx.deadline.clone(),
                        };
                        let mut buf = vec![0u8; 64 * 1024];
                        loop {
                            let k = r.read(&mut buf).map_err(err)?;
                            if k == 0 {
                                break;
                            }
                            out.write(&buf[..k])?;
                        }
                    }
                    _ => return Err(err("job:input expects a string or a stream")),
                }
                let part = out.finish()?;
                let input = d.inputs.entry(name.clone()).or_insert(Input {
                    rows: None,
                    parts: Vec::new(),
                });
                input.rows = None;
                input.parts.push(part);
                Ok(name)
            },
        );
        // Like `input`, for rows: an array or an iterator function, written as NDJSON and cut into
        // parts of about 64 MiB. The manifest records the row count, which the partner can check.
        methods.add_method(
            "input_rows",
            |lua, this, (name, rows, opts): (String, Value, Option<Table>)| {
                let ext = input_ext(opts.as_ref())?;
                if !token_ok(&name, 64) {
                    return Err(err(format!("invalid input name {name:?}")));
                }
                let mut src = match rows {
                    Value::Table(t) => {
                        let len = t.raw_len();
                        InputRows::Table(t, 0, len)
                    }
                    Value::Function(f) => InputRows::Func(f),
                    _ => {
                        return Err(err(
                            "job:input_rows expects an array or an iterator function",
                        ))
                    }
                };
                let mut d = this.draft.lock().unwrap();
                if d.submitted {
                    return Err(err("batch job was already submitted"));
                }
                let mut parts = Vec::new();
                let base = d.inputs.get(&name).map_or(0, |i| i.parts.len());
                let mut total = 0u64;
                let mut cur: Option<PartOut> = None;
                while let Some(row) = src.next(lua)? {
                    if base + parts.len() >= MAX_PARTS {
                        return Err(err(format!("input {name:?} has too many parts")));
                    }
                    let out = match &mut cur {
                        Some(o) => o,
                        None => cur.insert(PartOut::start(this, &name, base + parts.len(), &ext)?),
                    };
                    let mut line = serde_json::to_vec(&row).map_err(err)?;
                    line.push(b'\n');
                    out.write(&line)?;
                    total += 1;
                    if out.bytes >= PART_BYTES {
                        parts.push(cur.take().unwrap().finish()?);
                    }
                }
                if let Some(o) = cur.take() {
                    parts.push(o.finish()?);
                }
                let prior = d.inputs.get(&name).map(|i| i.rows);
                let input = d.inputs.entry(name.clone()).or_insert(Input {
                    rows: Some(0),
                    parts: Vec::new(),
                });
                input.rows = match prior {
                    None | Some(Some(_)) => Some(input.rows.unwrap_or(0) + total),
                    Some(None) => None,
                };
                input.parts.extend(parts);
                Ok(name)
            },
        );
        // Saved for the continuation; the partner never sees it.
        methods.add_method("state", |lua, this, value: Value| {
            let v: Json = match value {
                Value::Nil => Json::Null,
                v => lua.from_value(v)?,
            };
            let mut d = this.draft.lock().unwrap();
            if d.submitted {
                return Err(err("batch job was already submitted"));
            }
            d.state = v;
            Ok(())
        });
        // Script that `native.batch.route` picks when the partner's `done.json` arrives.
        methods.add_method("continue_with", |_, this, script: String| {
            if !token_ok(&script, 128) {
                return Err(err(format!("invalid script name {script:?}")));
            }
            let mut d = this.draft.lock().unwrap();
            if d.submitted {
                return Err(err("batch job was already submitted"));
            }
            d.script = Some(script);
            Ok(())
        });
        methods.add_method("id", |_, this, ()| Ok(this.id.clone()));
        // Writes the state, then the request: a partner that reacts at once never finds a
        // request whose state is missing.
        methods.add_method("submit", |_, this, ()| {
            let mut d = this.draft.lock().unwrap();
            if d.submitted {
                return Err(err("batch job was already submitted"));
            }
            if d.queries.is_empty() && d.http.is_empty() {
                return Err(err("a batch job needs at least one query"));
            }
            if !d.http.is_empty() && !(d.queries.is_empty() && d.inputs.is_empty()) {
                return Err(err(
                    "a job with job:http requests cannot also have queries or inputs for a partner",
                ));
            }
            let now = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map_err(err)?
                .as_millis() as u64;
            let state = json!({
                "v": FORMAT,
                "job": this.id,
                "continue": d.script,
                "queries": d.queries.iter().map(|(n, _, _)| n.as_str())
                    .chain(d.http.iter().map(|(n, _)| n.as_str()))
                    .collect::<Vec<_>>(),
                "created_ms": now,
                "state": d.state,
            });
            let inputs: serde_json::Map<String, Json> = d
                .inputs
                .iter()
                .map(|(n, i)| (n.clone(), json!({"rows": i.rows, "parts": i.parts})))
                .collect();
            let request = json!({
                "v": FORMAT,
                "job": this.id,
                "created_ms": now,
                "inputs": inputs,
                "queries": d.queries.iter()
                    .map(|(name, query, params)| json!({"name": name, "query": query, "params": params}))
                    .collect::<Vec<_>>(),
            });
            let writes = if d.http.is_empty() {
                vec![
                    (state_key(&this.id), state),
                    (format!("{}request.json", job_dir(&this.id)), request),
                ]
            } else {
                vec![(state_key(&this.id), state)]
            };
            for (key, body) in writes {
                this.ctx.op()?;
                this.scope
                    .put(&key, serde_json::to_vec(&body).map_err(err)?)
                    .map_err(err)?;
            }
            d.submitted = true;
            if !d.http.is_empty() {
                let env = this.ctx.http.clone().expect("job:http needs the executor");
                spawn_http(
                    env,
                    this.scope.clone(),
                    this.id.clone(),
                    std::mem::take(&mut d.http),
                );
            }
            Ok(this.id.clone())
        });
        methods.add_meta_method(MetaMethod::ToString, |_, t, ()| {
            Ok(format!("BatchJob({})", t.id))
        });
    }
}

/// One input part being written: counts and hashes what goes in, and leaves no object if dropped
/// before `finish`.
struct PartOut {
    w: Option<Box<dyn crate::ObjectWriter>>,
    key: String,
    hasher: Sha256,
    bytes: u64,
}

impl PartOut {
    fn start(job: &Job, name: &str, n: usize, ext: &str) -> mlua::Result<Self> {
        job.ctx.op()?;
        let key = format!("inputs/{name}.{n:04}{ext}");
        let w = job
            .scope
            .create(&format!("{}{key}", job_dir(&job.id)))
            .map_err(err)?;
        Ok(Self {
            w: Some(w),
            key,
            hasher: Sha256::new(),
            bytes: 0,
        })
    }

    fn write(&mut self, data: &[u8]) -> mlua::Result<()> {
        self.hasher.update(data);
        self.bytes += data.len() as u64;
        self.w.as_mut().unwrap().write(data).map_err(err)
    }

    /// Commits the object and returns its manifest entry.
    fn finish(mut self) -> mlua::Result<Json> {
        self.w.take().unwrap().commit().map_err(err)?;
        let sha = crate::db::hex(&self.hasher.clone().finalize());
        Ok(json!({"key": self.key, "bytes": self.bytes, "sha256": sha}))
    }
}

impl Drop for PartOut {
    fn drop(&mut self) {
        if let Some(w) = self.w.take() {
            w.abort();
        }
    }
}

fn input_ext(opts: Option<&Table>) -> mlua::Result<String> {
    let ext = opts
        .map(|o| o.get::<Option<String>>("ext"))
        .transpose()?
        .flatten()
        .unwrap_or_else(|| ".ndjson".into());
    let ok = ext.len() <= 16 && ext.chars().all(|c| c.is_ascii_alphanumeric() || c == '.');
    ok.then_some(ext.clone())
        .ok_or_else(|| err(format!("invalid input extension {ext:?}")))
}

enum InputRows {
    Table(Table, usize, usize),
    Func(mlua::Function),
}

impl InputRows {
    fn next(&mut self, lua: &Lua) -> mlua::Result<Option<Json>> {
        let v: Value = match self {
            InputRows::Table(t, i, len) => {
                *i += 1;
                if *i > *len {
                    return Ok(None);
                }
                t.raw_get(*i)?
            }
            InputRows::Func(f) => f.call(())?,
        };
        if v.is_nil() {
            return Ok(None);
        }
        lua.from_value(v).map(Some)
    }
}

struct Envelope {
    script: Option<String>,
    queries: Vec<String>,
    state: Json,
}

fn load_envelope(ctx: &Ctx, scope: &BucketScope, id: &str) -> mlua::Result<Option<Envelope>> {
    ctx.op()?;
    let Some(bytes) = scope.get(&state_key(id)).map_err(err)? else {
        return Ok(None);
    };
    let j: Json = serde_json::from_slice(&bytes)
        .map_err(|e| err(format!("batch {id}: unreadable job state: {e}")))?;
    if j.get("v").and_then(Json::as_u64) != Some(FORMAT) {
        return Err(err(format!("batch {id}: unsupported job state version")));
    }
    Ok(Some(Envelope {
        script: j.get("continue").and_then(Json::as_str).map(str::to_string),
        queries: j
            .get("queries")
            .and_then(Json::as_array)
            .map(|a| {
                a.iter()
                    .filter_map(|q| q.as_str().map(str::to_string))
                    .collect()
            })
            .unwrap_or_default(),
        state: j.get("state").cloned().unwrap_or(Json::Null),
    }))
}

/// The job an event is about: an explicit `{ job }`, or a `done.json` event from the batch bucket
/// (`{ bucket = "$batch", key = "jobs/<job>/done.json" }`). Objects in a data bucket never qualify,
/// whatever their key.
fn event_job(event: &Table) -> mlua::Result<Option<String>> {
    if let Some(job) = event.get::<Option<String>>("job")? {
        return Ok(token_ok(&job, 64).then_some(job));
    }
    if event.get::<Option<String>>("bucket")?.as_deref() != Some(BATCH_BUCKET) {
        return Ok(None);
    }
    Ok(event
        .get::<Option<String>>("key")?
        .and_then(|k| job_of_done_key(&k).map(str::to_string)))
}

/// For a router: `{ job, script?, event }` when `event` is a partner's `done.json`, else nil.
/// `script` is the continuation saved with the job, and is nil when the job's state is gone
/// (a duplicate delivery after cleanup), so the router decides what happens then.
fn route(lua: &Lua, ctx: &Ctx, event: &Table) -> mlua::Result<Value> {
    if event.get::<Option<String>>("key")?.is_none() {
        return Ok(Value::Nil);
    }
    let Some(id) = event_job(event)? else {
        return Ok(Value::Nil);
    };
    let scope = ctx.scope()?;
    let env = load_envelope(ctx, &scope, &id)?;
    let routed = lua.create_table()?;
    let ev = lua.create_table()?;
    for pair in event.pairs::<Value, Value>() {
        let (k, v) = pair?;
        ev.set(k, v)?;
    }
    ev.set("job", id.as_str())?;
    routed.set("job", id.as_str())?;
    routed.set("script", env.and_then(|e| e.script))?;
    routed.set("event", ev)?;
    Ok(Value::Table(routed))
}

struct Part {
    /// Written by the host, not a partner: read as is, never gunzipped.
    raw: bool,
    key: String,
    bytes: Option<u64>,
    sha256: Option<String>,
}

struct Output {
    /// Set for `job:http` results.
    status: Option<u64>,
    headers: Option<Json>,
    rows: Option<u64>,
    parts: Vec<Part>,
}

struct Hydrated {
    ctx: Arc<Ctx>,
    scope: BucketScope,
    id: String,
    ok: bool,
    error: Option<String>,
    state: Json,
    outputs: BTreeMap<String, Arc<Output>>,
}

fn hydrate(lua: &Lua, ctx: &Arc<Ctx>, event: &Table, allow_failure: bool) -> mlua::Result<Value> {
    let id = event_job(event)?.ok_or_else(|| {
        err("native.batch.hydrate needs a batch event (a done.json from the batch bucket) or { job = ... }")
    })?;
    let scope = ctx.scope()?;
    let Some(env) = load_envelope(ctx, &scope, &id)? else {
        return Ok(Value::Nil);
    };
    ctx.op()?;
    let Some(done) = scope
        .get(&format!("{}done.json", job_dir(&id)))
        .map_err(err)?
    else {
        return Ok(Value::Nil);
    };
    let done: Json = serde_json::from_slice(&done)
        .map_err(|e| err(format!("batch {id}: done.json is not valid JSON: {e}")))?;
    let ok = done
        .get("ok")
        .and_then(Json::as_bool)
        .ok_or_else(|| err(format!("batch {id}: done.json has no boolean `ok`")))?;
    let error = done.get("error").and_then(Json::as_str).map(str::to_string);

    let mut outputs = BTreeMap::new();
    if !ok {
        if !allow_failure {
            return Err(err(format!(
                "batch {id} failed: {}",
                error.as_deref().unwrap_or("no reason given")
            )));
        }
    } else {
        let results = done
            .get("results")
            .and_then(Json::as_object)
            .ok_or_else(|| err(format!("batch {id}: done.json has no `results` object")))?;
        for name in &env.queries {
            let r = results.get(name).ok_or_else(|| {
                err(format!(
                    "batch {id}: result {name:?} is missing from done.json"
                ))
            })?;
            let parts = r
                .get("parts")
                .and_then(Json::as_array)
                .ok_or_else(|| err(format!("batch {id}: result {name:?} has no `parts` list")))?;
            if parts.len() > MAX_PARTS {
                return Err(err(format!(
                    "batch {id}: result {name:?} has too many parts"
                )));
            }
            let parts = parts
                .iter()
                .map(|p| {
                    let key = p
                        .get("key")
                        .and_then(Json::as_str)
                        .filter(|k| part_key_ok(k))
                        .ok_or_else(|| {
                            err(format!(
                                "batch {id}: result {name:?} has an invalid part key"
                            ))
                        })?;
                    Ok(Part {
                        raw: p.get("raw").and_then(Json::as_bool).unwrap_or(false),
                        key: key.to_string(),
                        bytes: p.get("bytes").and_then(Json::as_u64),
                        sha256: p.get("sha256").and_then(Json::as_str).map(str::to_string),
                    })
                })
                .collect::<mlua::Result<Vec<_>>>()?;
            outputs.insert(
                name.clone(),
                Arc::new(Output {
                    status: r.get("status").and_then(Json::as_u64),
                    headers: r.get("headers").cloned(),
                    rows: r.get("rows").and_then(Json::as_u64),
                    parts,
                }),
            );
        }
    }
    Ok(Value::UserData(lua.create_userdata(Hydrated {
        ctx: ctx.clone(),
        scope,
        id,
        ok,
        error,
        state: env.state,
        outputs,
    })?))
}

/// Checks a part against its manifest entry when the stream reaches its end.
struct Verifying<R> {
    inner: R,
    name: String,
    count: u64,
    hasher: Option<Sha256>,
    bytes: Option<u64>,
    sha256: Option<String>,
    checked: bool,
}

impl<R: Read> Read for Verifying<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let n = self.inner.read(buf)?;
        if n > 0 {
            self.count += n as u64;
            if let Some(h) = &mut self.hasher {
                h.update(&buf[..n]);
            }
        } else if !self.checked {
            self.checked = true;
            if let Some(b) = self.bytes.filter(|b| *b != self.count) {
                return Err(io::Error::other(format!(
                    "{}: manifest says {b} bytes, read {}",
                    self.name, self.count
                )));
            }
            if let (Some(h), Some(want)) = (self.hasher.take(), &self.sha256) {
                let got = crate::db::hex(&h.finalize());
                if !got.eq_ignore_ascii_case(want) {
                    return Err(io::Error::other(format!("{}: sha256 mismatch", self.name)));
                }
            }
        }
        Ok(n)
    }
}

/// The decoded bytes of one part: gunzipped when it starts with the gzip magic, and checked
/// against the manifest at its end.
fn open_part(
    ctx: &Ctx,
    scope: &BucketScope,
    id: &str,
    part: &Part,
) -> mlua::Result<Box<dyn Read + Send>> {
    ctx.op()?;
    let full = format!("{}{}", job_dir(id), part.key);
    let raw = scope
        .open(&full)
        .map_err(err)?
        .ok_or_else(|| err(format!("batch {id}: part {:?} is missing", part.key)))?;
    let mut raw = Verifying {
        inner: Limited {
            inner: raw,
            left: ctx.max_stream,
            deadline: ctx.deadline.clone(),
        },
        name: part.key.clone(),
        count: 0,
        hasher: part.sha256.is_some().then(Sha256::new),
        bytes: part.bytes,
        sha256: part.sha256.clone(),
        checked: false,
    };
    let mut head = [0u8; 2];
    let mut n = 0;
    while n < head.len() {
        match raw.read(&mut head[n..]).map_err(err)? {
            0 => break,
            k => n += k,
        }
    }
    let stream = io::Cursor::new(head[..n].to_vec()).chain(raw);
    Ok(if !part.raw && head[..n] == [0x1f, 0x8b] {
        Box::new(Capped {
            inner: MultiGzDecoder::new(BufReader::new(stream)),
            left: ctx.max_stream,
        })
    } else {
        Box::new(stream)
    })
}

struct RowCursor {
    next_part: usize,
    current: Option<Rows<BufReader<Box<dyn Read + Send>>>>,
    seen: u64,
    finished: bool,
}

impl Hydrated {
    fn output(&self, name: &str) -> mlua::Result<Arc<Output>> {
        if !self.ok {
            return Err(err(format!("batch {} failed; it has no results", self.id)));
        }
        self.outputs
            .get(name)
            .cloned()
            .ok_or_else(|| err(format!("batch {}: no result named {name:?}", self.id)))
    }
}

impl UserData for Hydrated {
    fn add_fields<F: UserDataFields<Self>>(fields: &mut F) {
        fields.add_field_method_get("id", |_, t| Ok(t.id.clone()));
        fields.add_field_method_get("ok", |_, t| Ok(t.ok));
        fields.add_field_method_get("error", |_, t| Ok(t.error.clone()));
        fields.add_field_method_get("state", |lua, t| lua.to_value(&t.state));
        // name -> { rows = n?, parts = n }
        fields.add_field_method_get("results", |lua, t| {
            let out = lua.create_table()?;
            for (name, o) in &t.outputs {
                let r = lua.create_table()?;
                r.set("rows", o.rows)?;
                r.set("parts", o.parts.len())?;
                if let Some(status) = o.status {
                    r.set("status", status)?;
                }
                if let Some(h) = &o.headers {
                    r.set("headers", lua.to_value(h)?)?;
                }
                out.set(name.as_str(), r)?;
            }
            Ok(out)
        });
    }

    fn add_methods<M: UserDataMethods<Self>>(methods: &mut M) {
        methods.add_method("parts", |_, this, name: String| {
            Ok(this.output(&name)?.parts.len())
        });

        // The body of a `job:http` response as a string (capped at max_object_bytes).
        methods.add_method("body", |lua, this, name: String| {
            let out = this.output(&name)?;
            let p = out
                .parts
                .first()
                .ok_or_else(|| err(format!("batch {}: result {name:?} has no body", this.id)))?;
            let mut buf = Vec::new();
            open_part(&this.ctx, &this.scope, &this.id, p)?
                .take(this.ctx.max_line as u64 + 1)
                .read_to_end(&mut buf)
                .map_err(err)?;
            if buf.len() > this.ctx.max_line {
                return Err(err(format!(
                    "batch {}: body of {name:?} is larger than {} bytes; use :open",
                    this.id, this.ctx.max_line
                )));
            }
            lua.create_string(&buf)
        });

        // Stream of one part (1-based, default 1), gunzipped if it is gzip and checked against the
        // manifest when read to the end.
        methods.add_method(
            "open",
            |lua, this, (name, part): (String, Option<usize>)| {
                let out = this.output(&name)?;
                let i = part.unwrap_or(1);
                let p = i
                    .checked_sub(1)
                    .and_then(|i| out.parts.get(i))
                    .ok_or_else(|| {
                        err(format!(
                            "batch {}: result {name:?} has no part {i}",
                            this.id
                        ))
                    })?;
                let r = open_part(&this.ctx, &this.scope, &this.id, p)?;
                lua.create_userdata(ByteStream::new(r))
            },
        );

        // Iterator over the rows of every part, in manifest order. Raises if a part is missing or
        // does not match its manifest entry, or if the row count differs from the manifest's.
        methods.add_method("rows", |lua, this, name: String| {
            let out = this.output(&name)?;
            let (ctx, scope, id) = (this.ctx.clone(), this.scope.clone(), this.id.clone());
            let cursor = Mutex::new(RowCursor {
                next_part: 0,
                current: None,
                seen: 0,
                finished: false,
            });
            lua.create_function(move |lua, ()| {
                let mut c = cursor.lock().unwrap();
                loop {
                    if c.finished {
                        return Ok(Value::Nil);
                    }
                    if c.current.is_none() {
                        let Some(part) = out.parts.get(c.next_part) else {
                            c.finished = true;
                            if let Some(want) = out.rows.filter(|w| *w != c.seen) {
                                return Err(err(format!(
                                    "batch {id}: result {name:?} has {} rows, manifest says {want}",
                                    c.seen
                                )));
                            }
                            return Ok(Value::Nil);
                        };
                        c.next_part += 1;
                        let r = open_part(&ctx, &scope, &id, part)?;
                        c.current = Some(Rows::new(BufReader::new(r), ctx.max_line));
                    }
                    match c.current.as_mut().unwrap().next_value() {
                        Ok(Some(v)) => {
                            c.seen += 1;
                            return lua.to_value(&v);
                        }
                        Ok(None) => c.current = None,
                        Err(e) => {
                            c.finished = true;
                            return Err(err(format!("batch {id}: {e}")));
                        }
                    }
                }
            })
        });

        // Every object of the job as `{ bucket, key }`, for a queue handler's `delete =` result
        // (which takes at most 100; expire older jobs with a bucket lifecycle rule for bigger ones).
        methods.add_method("keys", |lua, this, ()| {
            let out = lua.create_table()?;
            let add = |key: String| -> mlua::Result<()> {
                let t = lua.create_table()?;
                t.set("bucket", BATCH_BUCKET)?;
                t.set("key", key)?;
                out.push(t)
            };
            for o in this.outputs.values() {
                for p in &o.parts {
                    add(format!("{}{}", job_dir(&this.id), p.key))?;
                }
            }
            add(format!("{}request.json", job_dir(&this.id)))?;
            add(format!("{}done.json", job_dir(&this.id)))?;
            add(state_key(&this.id))?;
            Ok(out)
        });
        methods.add_meta_method(MetaMethod::ToString, |_, t, ()| {
            Ok(format!("BatchResult({})", t.id))
        });
    }
}
