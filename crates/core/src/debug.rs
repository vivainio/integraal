//! Watched scripts: deep diagnostics for the scripts a tenant marks, kept apart from flow logs.
//!
//! A tenant lists scripts under `debug` in its `config.json`; an invocation of a listed script
//! records more (line coverage, the structured failure) and writes it as one capture object to the
//! deployment's debug sink. The sink is a directory (`debug.dir`), a bucket (`debug.bucket`) or
//! both, and is never the flow log bucket, because captures may hold tenant data that flow logs
//! must not.
//!
//! ```text
//! <prefix><tenant>/captures/<trace id>/<start ms>-<span id>.json.gz
//! ```

use std::{
    collections::{BTreeMap, HashMap},
    ffi::{c_char, c_int, c_void},
    io::{Read, Write},
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};

use flate2::{read::GzDecoder, write::GzEncoder, Compression};
use integraal_wrappers::ObjectBackend;
use mlua::{Function, Lua};
use serde::Deserialize;
use serde_json::{json, Map, Value};

/// What a watched invocation records, beyond the structured failure every invocation gets.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Capture {
    /// Which lines of the script ran, and how often.
    Coverage,
    /// Where the time went: the Luau stack sampled during the run, folded per function and line.
    Profile,
    /// The invocation's input event.
    Event,
}

impl Capture {
    pub const ALL: [Capture; 3] = [Capture::Coverage, Capture::Profile, Capture::Event];
}

/// The `debug` section of a tenant's `config.json`.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DebugConfig {
    /// Script names to watch; a trailing `*` matches a prefix (`order_*`). Left out, the whole
    /// tenant is watched: every invocation, the script a router picks, and the libs it requires.
    #[serde(default)]
    pub scripts: Vec<String>,
    /// What to record; everything available when left out, since a watched script is one you are
    /// debugging.
    #[serde(default = "all_captures")]
    pub capture: Vec<Capture>,
    /// UTC time after which the section is ignored, `YYYY-MM-DDTHH:MM:SSZ`. Required, so a debug
    /// setting cannot be left on and forgotten.
    pub until: String,
}

fn all_captures() -> Vec<Capture> {
    Capture::ALL.to_vec()
}

impl DebugConfig {
    pub fn validate(&self) -> Result<(), String> {
        if parse_utc(&self.until).is_none() {
            return Err("debug.until must be a UTC time like 2026-11-01T00:00:00Z".into());
        }
        Ok(())
    }

    /// What to capture for `script` at `now_ms`, or `None` when it is not watched (or the
    /// section has expired).
    pub fn watching(&self, script: &str, now_ms: u64) -> Option<&[Capture]> {
        let until = parse_utc(&self.until)?;
        if now_ms >= until {
            return None;
        }
        (self.scripts.is_empty()
            || self.scripts.iter().any(|p| match p.strip_suffix('*') {
                Some(prefix) => script.starts_with(prefix),
                None => p == script,
            }))
        .then_some(&self.capture[..])
    }
}

/// Unix milliseconds of `YYYY-MM-DDTHH:MM:SSZ`.
pub(crate) fn parse_utc(s: &str) -> Option<u64> {
    let b = s.as_bytes();
    if b.len() != 20
        || b[4] != b'-'
        || b[7] != b'-'
        || b[10] != b'T'
        || b[13] != b':'
        || b[16] != b':'
        || b[19] != b'Z'
    {
        return None;
    }
    let n = |r: std::ops::Range<usize>| s.get(r)?.parse::<i64>().ok();
    let (y, m, d) = (n(0..4)?, n(5..7)?, n(8..10)?);
    let (h, mi, sec) = (n(11..13)?, n(14..16)?, n(17..19)?);
    if !(1..=12).contains(&m) || !(1..=31).contains(&d) || h > 23 || mi > 59 || sec > 60 {
        return None;
    }
    // Days from civil (Howard Hinnant), the inverse of `flow_log::civil`.
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let doy = (153 * (if m > 2 { m - 3 } else { m + 9 }) + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let days = era * 146_097 + doe - 719_468;
    u64::try_from(((days * 24 + h) * 60 + mi) * 60 + sec)
        .ok()
        .map(|s| s * 1000)
}

/// What one watched invocation has recorded so far.
#[derive(Clone, Default)]
pub struct Collector(Arc<Mutex<Map<String, Value>>>);

impl Collector {
    pub fn put(&self, key: &str, value: Value) {
        self.0.lock().unwrap().insert(key.to_string(), value);
    }

    /// Keeps the first value for `key`: a later one is a consequence of the first.
    pub fn put_first(&self, key: &str, value: Value) {
        self.0
            .lock()
            .unwrap()
            .entry(key.to_string())
            .or_insert(value);
    }

    pub(crate) fn take(&self) -> Map<String, Value> {
        std::mem::take(&mut *self.0.lock().unwrap())
    }
}

/// A directory used as an object store: keys are relative paths.
pub struct FsBackend {
    root: PathBuf,
}

impl FsBackend {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    fn path(&self, key: &str) -> Result<PathBuf, String> {
        if key.is_empty()
            || key
                .split('/')
                .any(|c| c.is_empty() || c == "." || c == ".." || c.contains('\\'))
        {
            return Err(format!("bad key {key:?}"));
        }
        Ok(self.root.join(key))
    }
}

impl ObjectBackend for FsBackend {
    fn get(&self, key: &str) -> Result<Option<Vec<u8>>, String> {
        match std::fs::read(self.path(key)?) {
            Ok(b) => Ok(Some(b)),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(format!("{key}: {e}")),
        }
    }

    fn put(&self, key: &str, body: Vec<u8>) -> Result<(), String> {
        let path = self.path(key)?;
        let dir = path.parent().ok_or("bad key")?;
        std::fs::create_dir_all(dir).map_err(|e| format!("{}: {e}", dir.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            // Owner only: captures may hold tenant data.
            let _ = std::fs::DirBuilder::new()
                .recursive(true)
                .mode(0o700)
                .create(&self.root);
        }
        let tmp = path.with_extension("tmp");
        let mut f = std::fs::File::create(&tmp).map_err(|e| format!("{}: {e}", tmp.display()))?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let _ = f.set_permissions(std::fs::Permissions::from_mode(0o600));
        }
        f.write_all(&body)
            .map_err(|e| format!("{}: {e}", tmp.display()))?;
        std::fs::rename(&tmp, &path).map_err(|e| format!("{}: {e}", path.display()))
    }

    fn delete(&self, key: &str) -> Result<(), String> {
        match std::fs::remove_file(self.path(key)?) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(format!("{key}: {e}")),
        }
    }

    fn open(&self, key: &str) -> Result<Option<Box<dyn Read + Send>>, String> {
        match std::fs::File::open(self.path(key)?) {
            Ok(f) => Ok(Some(Box::new(f))),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(format!("{key}: {e}")),
        }
    }

    fn list(&self, prefix: &str, limit: usize) -> Result<Vec<String>, String> {
        let mut out = Vec::new();
        walk(&self.root, &self.root, prefix, limit, &mut out)?;
        out.sort();
        out.truncate(limit);
        Ok(out)
    }
}

fn walk(
    root: &Path,
    dir: &Path,
    prefix: &str,
    limit: usize,
    out: &mut Vec<String>,
) -> Result<(), String> {
    let entries = match std::fs::read_dir(dir) {
        Ok(e) => e,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(format!("{}: {e}", dir.display())),
    };
    for entry in entries.flatten() {
        if out.len() >= limit {
            return Ok(());
        }
        let path = entry.path();
        let key = path
            .strip_prefix(root)
            .map_err(|e| e.to_string())?
            .to_string_lossy()
            .replace('\\', "/");
        if path.is_dir() {
            // Only descend where the prefix can still match.
            if prefix.starts_with(&format!("{key}/")) || key.starts_with(prefix) {
                walk(root, &path, prefix, limit, out)?;
            }
        } else if key.starts_with(prefix) && !key.ends_with(".tmp") {
            out.push(key);
        }
    }
    Ok(())
}

/// Where captures go: every backend gets each capture; reads use the first that has it.
#[derive(Clone)]
pub struct DebugSink {
    backends: Vec<Arc<dyn ObjectBackend>>,
    prefix: String,
}

fn name_ok(s: &str) -> bool {
    !s.is_empty()
        && s != "."
        && s != ".."
        && s.chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
}

fn trace_id_ok(s: &str) -> bool {
    s.len() == 32 && s.chars().all(|c| c.is_ascii_hexdigit())
}

impl DebugSink {
    pub fn new(backends: Vec<Arc<dyn ObjectBackend>>, prefix: impl Into<String>) -> Self {
        Self {
            backends,
            prefix: prefix.into(),
        }
    }

    fn dir(&self, tenant: &str, trace_id: &str) -> String {
        format!("{}{tenant}/captures/{trace_id}/", self.prefix)
    }

    pub(crate) fn key(&self, tenant: &str, trace_id: &str, start_ms: u64, span_id: &str) -> String {
        format!(
            "{}{start_ms:013}-{span_id}.json.gz",
            self.dir(tenant, trace_id)
        )
    }

    /// Writes the capture to every backend. Blocks; call from a blocking thread.
    pub(crate) fn put(&self, key: &str, capture: &Value) -> Result<(), String> {
        let mut gz = GzEncoder::new(Vec::new(), Compression::default());
        gz.write_all(capture.to_string().as_bytes())
            .map_err(|e| e.to_string())?;
        let body = gz.finish().map_err(|e| e.to_string())?;
        let mut first_err = None;
        for b in &self.backends {
            if let Err(e) = b.put(key, body.clone()) {
                first_err.get_or_insert(e);
            }
        }
        first_err.map_or(Ok(()), Err)
    }

    /// The captures of a flow, oldest first. Blocks; call from a blocking thread.
    pub fn read(&self, tenant: &str, trace_id: &str) -> Result<Vec<Value>, String> {
        if !name_ok(tenant) || !trace_id_ok(trace_id) {
            return Err("bad tenant or trace id".into());
        }
        let dir = self.dir(tenant, &trace_id.to_ascii_lowercase());
        let mut out = Vec::new();
        for b in &self.backends {
            for key in b.list(&dir, 1000)? {
                let Some(bytes) = b.get(&key)? else { continue };
                let mut text = String::new();
                GzDecoder::new(&bytes[..])
                    .read_to_string(&mut text)
                    .map_err(|e| format!("{key}: {e}"))?;
                out.push(serde_json::from_str(&text).map_err(|e| format!("{key}: {e}"))?);
            }
            if !out.is_empty() {
                break;
            }
        }
        out.sort_by_key(|v: &Value| v["start_ms"].as_u64().unwrap_or(0));
        Ok(out)
    }

    #[cfg(test)]
    pub(crate) fn keys_for_test(&self) -> Vec<String> {
        self.backends[0].list(&self.prefix, 1000).unwrap()
    }

    /// Deletes disk captures whose files are older than `max_age`. Object stores expire theirs
    /// with a lifecycle rule instead.
    pub fn sweep_dirs(dir: &Path, max_age: std::time::Duration) -> usize {
        fn go(dir: &Path, cutoff: std::time::SystemTime, n: &mut usize) {
            let Ok(entries) = std::fs::read_dir(dir) else {
                return;
            };
            for e in entries.flatten() {
                let p = e.path();
                if p.is_dir() {
                    go(&p, cutoff, n);
                    let _ = std::fs::remove_dir(&p); // only succeeds when empty
                } else if e
                    .metadata()
                    .and_then(|m| m.modified())
                    .is_ok_and(|t| t < cutoff)
                    && std::fs::remove_file(&p).is_ok()
                {
                    *n += 1;
                }
            }
        }
        let mut n = 0;
        if let Some(cutoff) = std::time::SystemTime::now().checked_sub(max_age) {
            go(dir, cutoff, &mut n);
        }
        n
    }
}

/// Chunks compiled in one VM (the script and every module it requires), so coverage can be read
/// from each after the run.
type Chunks = Arc<Mutex<Vec<(String, Function)>>>;

/// One watched invocation: what to record and the state recording it. Cloned into the VM, the
/// module host and the engine; all clones share the same state.
#[derive(Clone)]
pub(crate) struct DebugRun {
    pub(crate) collector: Collector,
    captures: Vec<Capture>,
    chunks: Chunks,
    profiler: Arc<Mutex<Profiler>>,
}

impl DebugRun {
    pub(crate) fn new(captures: &[Capture]) -> Self {
        Self {
            collector: Collector::default(),
            captures: captures.to_vec(),
            chunks: Chunks::default(),
            profiler: Arc::new(Mutex::new(Profiler::new())),
        }
    }

    pub(crate) fn wants(&self, c: Capture) -> bool {
        self.captures.contains(&c)
    }

    /// Remembers a compiled chunk for coverage.
    pub(crate) fn register(&self, name: &str, f: &Function) {
        if self.wants(Capture::Coverage) {
            self.chunks
                .lock()
                .unwrap()
                .push((name.to_string(), f.clone()));
        }
    }

    /// Called from the VM interrupt: samples the stack when enough time has passed.
    pub(crate) fn tick(&self, lua: &Lua) {
        if self.wants(Capture::Profile) {
            if let Ok(mut p) = self.profiler.try_lock() {
                p.tick(lua);
            }
        }
    }

    /// Writes coverage and the profile gathered so far into the collector. Cumulative, so it can
    /// be called after each handler of a routed run.
    pub(crate) fn finish(&self, lua: &Lua) {
        if self.wants(Capture::Coverage) {
            let chunks = self.chunks.lock().unwrap().clone();
            let out: Vec<Value> = chunks
                .iter()
                .map(|(name, f)| coverage_of(lua, name, f))
                .collect();
            self.collector.put("coverage", Value::Array(out));
        }
        if self.wants(Capture::Profile) {
            self.collector
                .put("profile", self.profiler.lock().unwrap().report());
        }
    }
}

/// Hit counts of one function, indexed by absolute line number (`-1` marks a line with no code).
struct CoverageFn {
    hits: Vec<i32>,
}

unsafe extern "C-unwind" fn coverage_cb(
    ctx: *mut c_void,
    _function: *const c_char,
    _linedefined: c_int,
    _depth: c_int,
    hits: *const c_int,
    size: usize,
) {
    let out = unsafe { &mut *(ctx as *mut Vec<CoverageFn>) };
    out.push(CoverageFn {
        hits: unsafe { std::slice::from_raw_parts(hits, size) }.to_vec(),
    });
}

/// Line hit counts of one chunk and every function defined in it. Needs the chunk to have been
/// compiled with a coverage level.
fn coverage_of(lua: &Lua, name: &str, f: &Function) -> Value {
    let mut fns: Vec<CoverageFn> = Vec::new();
    let ptr = &mut fns as *mut Vec<CoverageFn> as *mut c_void;
    // Safety: `coverage_cb` only runs inside this call and only touches `fns` through `ptr`.
    let read = unsafe {
        lua.exec_raw::<()>(f.clone(), |state| {
            mlua::ffi::lua_getcoverage(state, -1, ptr, coverage_cb);
        })
    };
    if let Err(e) = read {
        return json!({ "chunk": name, "error": e.to_string() });
    }
    let mut lines: BTreeMap<i64, i64> = BTreeMap::new();
    for func in &fns {
        for (i, &h) in func.hits.iter().enumerate() {
            if h >= 0 {
                *lines.entry(i as i64).or_insert(0) += i64::from(h);
            }
        }
    }
    let missed: Vec<i64> = lines
        .iter()
        .filter(|(_, &h)| h == 0)
        .map(|(&l, _)| l)
        .collect();
    json!({
        "chunk": name,
        "executable": lines.len(),
        "covered": lines.len() - missed.len(),
        "missed": missed,
        "lines": lines.iter().map(|(l, h)| (l.to_string(), json!(h))).collect::<Map<_, _>>(),
    })
}

/// Samples the Luau stack from the VM interrupt. The interrupt runs at function calls and loop
/// back-edges, not on a timer, so time spent inside a native call (an HTTP wait, say) is credited
/// to the stack at the next interrupt: approximate, but it points at the right script line.
struct Profiler {
    last: Instant,
    samples: u32,
    folded: HashMap<String, u64>,
    own: HashMap<(String, Option<usize>, String), u64>,
}

const SAMPLE_EVERY: Duration = Duration::from_millis(1);
const MAX_STACKS: usize = 200;
const MAX_OWN: usize = 50;

impl Profiler {
    fn new() -> Self {
        Self {
            last: Instant::now(),
            samples: 0,
            folded: HashMap::new(),
            own: HashMap::new(),
        }
    }

    fn tick(&mut self, lua: &Lua) {
        let now = Instant::now();
        let since = now.duration_since(self.last);
        if since < SAMPLE_EVERY {
            return;
        }
        self.last = now;
        let mut frames = crate::failure::frames(lua, 0);
        // The outermost frame is the `xpcall` the engine runs the handler under.
        if frames
            .last()
            .is_some_and(|f| f.native && f.function.as_deref() == Some("xpcall"))
        {
            frames.pop();
        }
        let Some(leaf) = frames.first() else { return };
        let ms = (since.as_millis() as u64).max(1);
        let label = |f: &crate::failure::Frame| {
            format!("{}:{}", f.chunk, f.function.as_deref().unwrap_or("?"))
        };
        self.samples += 1;
        *self
            .own
            .entry((
                leaf.chunk.clone(),
                leaf.line,
                leaf.function.clone().unwrap_or_default(),
            ))
            .or_insert(0) += ms;
        let stack = frames.iter().rev().map(label).collect::<Vec<_>>().join(";");
        *self.folded.entry(stack).or_insert(0) += ms;
    }

    /// `folded` is the flamegraph "collapsed stacks" format: `a;b;c <ms>`.
    fn report(&self) -> Value {
        let mut stacks: Vec<_> = self.folded.iter().collect();
        stacks.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
        let mut own: Vec<_> = self.own.iter().collect();
        own.sort_by(|a, b| b.1.cmp(a.1).then(a.0.cmp(b.0)));
        json!({
            "interval_ms": SAMPLE_EVERY.as_millis() as u64,
            "samples": self.samples,
            "total_ms": self.folded.values().sum::<u64>(),
            "hot": own.iter().take(MAX_OWN).map(|((chunk, line, function), ms)| {
                json!({ "chunk": chunk, "line": line, "function": function, "ms": ms })
            }).collect::<Vec<_>>(),
            "folded": stacks.iter().take(MAX_STACKS).map(|(s, ms)| format!("{s} {ms}")).collect::<Vec<_>>(),
        })
    }
}

/// The capture object for one watched invocation.
pub(crate) fn capture_object(
    header: Value,
    collected: Map<String, Value>,
    outcome: &str,
    error: Option<&str>,
) -> Value {
    let mut v = header;
    v["outcome"] = json!(outcome);
    v["error"] = json!(error);
    for (k, val) in collected {
        v[k] = val;
    }
    v
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn until_parses_and_expires() {
        assert_eq!(parse_utc("1970-01-01T00:00:00Z"), Some(0));
        assert_eq!(parse_utc("2000-02-29T00:00:00Z"), Some(951_782_400_000));
        assert_eq!(parse_utc("2026-13-01T00:00:00Z"), None);
        assert_eq!(parse_utc("tomorrow"), None);
        let c = DebugConfig {
            scripts: vec!["feed".into(), "order_*".into()],
            capture: vec![Capture::Coverage],
            until: "2000-02-29T00:00:00Z".into(),
        };
        assert!(c.watching("feed", 951_782_399_999).is_some());
        assert!(c.watching("order_final", 0).is_some());
        assert!(c.watching("other", 0).is_none());
        assert!(c.watching("feed", 951_782_400_000).is_none(), "expired");
    }

    #[test]
    fn fs_backend_round_trips_and_rejects_escapes() {
        let dir = std::env::temp_dir().join(format!("integraal-fs-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let b = FsBackend::new(&dir);
        b.put("a/b/c.txt", b"hi".to_vec()).unwrap();
        assert_eq!(b.get("a/b/c.txt").unwrap().unwrap(), b"hi");
        assert_eq!(b.list("a/", 10).unwrap(), ["a/b/c.txt"]);
        assert!(b.list("z/", 10).unwrap().is_empty());
        assert!(b.get("../x").is_err());
        assert!(b.get("a//b").is_err());
        b.delete("a/b/c.txt").unwrap();
        assert!(b.get("a/b/c.txt").unwrap().is_none());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn sink_writes_to_all_backends_and_reads_back() {
        let dir = std::env::temp_dir().join(format!("integraal-sink-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        let mem = Arc::new(crate::engine::tests::MemBackend::default());
        let sink = DebugSink::new(vec![Arc::new(FsBackend::new(&dir)), mem.clone()], "dbg/");
        let id = "4bf92f3577b34da6a3ce929d0e0e4736";
        let key = sink.key("acme", id, 5, "00f067aa0ba902b7");
        assert_eq!(
            key,
            "dbg/acme/captures/4bf92f3577b34da6a3ce929d0e0e4736/0000000000005-00f067aa0ba902b7.json.gz"
        );
        sink.put(&key, &json!({"start_ms": 5, "outcome": "ok"}))
            .unwrap();
        assert!(mem.get(&key).unwrap().is_some());
        let got = sink.read("acme", id).unwrap();
        assert_eq!(got[0]["outcome"], "ok");
        assert!(sink.read("../x", id).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
