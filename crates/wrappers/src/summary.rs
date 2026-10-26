//! The summary of one task execution: what the engine knows about it (outcome, duration, counts of
//! outbound calls and log lines) and what the script attached with `ig.task`.
//!
//! The script's part is built up incrementally, not returned, so whatever it set before a timeout
//! or a memory failure still reaches the record. Nothing here can fail the task: a value over a
//! limit is dropped and `truncated` says so. (A value that is not JSON at all is a script bug and
//! raises in the script, like `ig.json.encode` would.)

use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
};

use mlua::{Lua, LuaSerdeExt, Table};
use serde_json::{json, Map, Value};

/// Longest title, in bytes.
const MAX_TITLE_BYTES: usize = 200;
/// Longest metric or attribute name, in bytes.
const MAX_KEY_BYTES: usize = 64;
/// Longest attribute value, in bytes.
const MAX_ATTR_BYTES: usize = 256;
/// Most metrics, and most attributes, one task may attach.
const MAX_ENTRIES: usize = 32;
/// Largest `detail`, serialized.
pub const MAX_DETAIL_BYTES: usize = 8 * 1024;

#[derive(Default)]
struct State {
    /// The script that ran last: for a routed invocation, the one the router picked.
    script: Option<String>,
    title: Option<String>,
    metrics: BTreeMap<String, f64>,
    attrs: BTreeMap<String, String>,
    detail: Option<Value>,
    truncated: bool,
    http_requests: u64,
    http_failed: u64,
    log_lines: u64,
    log_errors: u64,
    failure: Option<Value>,
}

/// A shared handle to the summary of one invocation.
#[derive(Clone, Default)]
pub struct Summary(Arc<Mutex<State>>);

fn cut(s: &str, max: usize) -> (&str, bool) {
    if s.len() <= max {
        return (s, false);
    }
    let mut end = max;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    (&s[..end], true)
}

impl Summary {
    pub fn new() -> Self {
        Self::default()
    }

    /// The script now running.
    pub fn script(&self, name: &str) {
        self.0.lock().unwrap().script = Some(name.to_string());
    }

    pub fn title(&self, title: &str) {
        let (t, cut) = cut(title, MAX_TITLE_BYTES);
        let mut s = self.0.lock().unwrap();
        s.title = Some(t.to_string());
        s.truncated |= cut;
    }

    /// Sets a metric, replacing an earlier value. Non-finite numbers are dropped.
    pub fn metric(&self, name: &str, value: f64) {
        self.update_metric(name, |_| value);
    }

    /// Adds to a metric, which starts from 0.
    pub fn add(&self, name: &str, delta: f64) {
        self.update_metric(name, |old| old.unwrap_or(0.0) + delta);
    }

    fn update_metric(&self, name: &str, f: impl FnOnce(Option<f64>) -> f64) {
        let (key, cut) = cut(name, MAX_KEY_BYTES);
        let mut s = self.0.lock().unwrap();
        s.truncated |= cut;
        let old = s.metrics.get(key).copied();
        if old.is_none() && s.metrics.len() >= MAX_ENTRIES {
            s.truncated = true;
            return;
        }
        let v = f(old);
        if v.is_finite() {
            s.metrics.insert(key.to_string(), v);
        } else {
            s.truncated = true;
        }
    }

    pub fn attr(&self, name: &str, value: &str) {
        let (key, kcut) = cut(name, MAX_KEY_BYTES);
        let (val, vcut) = cut(value, MAX_ATTR_BYTES);
        let mut s = self.0.lock().unwrap();
        s.truncated |= kcut | vcut;
        if !s.attrs.contains_key(key) && s.attrs.len() >= MAX_ENTRIES {
            s.truncated = true;
            return;
        }
        s.attrs.insert(key.to_string(), val.to_string());
    }

    /// Free-form data; one over `MAX_DETAIL_BYTES` serialized is dropped.
    pub fn detail(&self, value: Value) {
        let mut s = self.0.lock().unwrap();
        if value.to_string().len() > MAX_DETAIL_BYTES {
            s.truncated = true;
            s.detail = None;
        } else {
            s.detail = Some(value);
        }
    }

    /// An outbound `ig.http` request; `failed` when it got no response or one outside 2xx/3xx.
    pub fn http(&self, failed: bool) {
        let mut s = self.0.lock().unwrap();
        s.http_requests += 1;
        s.http_failed += u64::from(failed);
    }

    /// A `ig.log` line.
    pub fn log(&self, level: &str) {
        let mut s = self.0.lock().unwrap();
        s.log_lines += 1;
        s.log_errors += u64::from(level == "error");
    }

    /// The structured failure of the invocation. The first one recorded wins: later ones are
    /// consequences.
    pub fn failure(&self, failure: Value) {
        let mut s = self.0.lock().unwrap();
        if s.failure.is_none() {
            s.failure = Some(failure);
        }
    }

    /// The fields of the task record that come from the summary. The record is flat, with
    /// `metrics`, `attrs` and `detail` as the only nested parts, so it maps onto columns later.
    pub fn fields(&self) -> Map<String, Value> {
        let s = self.0.lock().unwrap();
        let mut out = Map::new();
        if let Some(script) = &s.script {
            out.insert("script".into(), script.clone().into());
        }
        out.insert("http_requests".into(), s.http_requests.into());
        out.insert("http_failed".into(), s.http_failed.into());
        out.insert("log_lines".into(), s.log_lines.into());
        out.insert("log_errors".into(), s.log_errors.into());
        if let Some(f) = &s.failure {
            out.insert("failure_kind".into(), f["kind"].clone());
            if let Some(frame) = f["frames"].as_array().and_then(|fr| {
                fr.iter()
                    .find(|x| x["native"] == false)
                    .or_else(|| fr.first())
            }) {
                let line = frame["line"].as_u64().map(|l| format!(":{l}"));
                out.insert(
                    "failure_frame".into(),
                    format!(
                        "{}{} {}",
                        frame["chunk"].as_str().unwrap_or(""),
                        line.unwrap_or_default(),
                        frame["function"].as_str().unwrap_or("")
                    )
                    .trim_end()
                    .into(),
                );
            }
        }
        if let Some(t) = &s.title {
            out.insert("title".into(), t.clone().into());
        }
        out.insert("metrics".into(), json!(s.metrics));
        out.insert("attrs".into(), json!(s.attrs));
        if let Some(d) = &s.detail {
            out.insert("detail".into(), d.clone());
        }
        out.insert("truncated".into(), s.truncated.into());
        out
    }
}

/// `ig.task`: `title(text)`, `metric(name, number)`, `add(name, delta?)`, `attr(name, text)`
/// and `detail(value)`. Each can be called any number of times; the last `title` and `detail`, and
/// the last value of each name, win.
pub(crate) fn api(lua: &Lua, summary: Summary) -> mlua::Result<Table> {
    let t = lua.create_table()?;
    let s = summary.clone();
    t.set(
        "title",
        lua.create_function(move |_, text: String| {
            s.title(&text);
            Ok(())
        })?,
    )?;
    let s = summary.clone();
    t.set(
        "metric",
        lua.create_function(move |_, (name, value): (String, f64)| {
            s.metric(&name, value);
            Ok(())
        })?,
    )?;
    let s = summary.clone();
    t.set(
        "add",
        lua.create_function(move |_, (name, delta): (String, Option<f64>)| {
            s.add(&name, delta.unwrap_or(1.0));
            Ok(())
        })?,
    )?;
    let s = summary.clone();
    t.set(
        "attr",
        lua.create_function(move |_, (name, value): (String, String)| {
            s.attr(&name, &value);
            Ok(())
        })?,
    )?;
    t.set(
        "detail",
        lua.create_function(move |lua, value: mlua::Value| {
            summary.detail(lua.from_value(value)?);
            Ok(())
        })?,
    )?;
    Ok(t)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_task_that_sets_nothing_still_has_the_engine_fields() {
        let f = Summary::new().fields();
        assert_eq!(f["http_requests"], 0);
        assert_eq!(f["metrics"], json!({}));
        assert_eq!(f["truncated"], false);
        assert!(!f.contains_key("title"));
    }

    #[test]
    fn metrics_set_and_add() {
        let s = Summary::new();
        s.metric("rows", 10.0);
        s.add("rows", 5.0);
        s.add("fresh", 2.0);
        s.metric("bad", f64::NAN);
        let f = s.fields();
        assert_eq!(f["metrics"], json!({"rows": 15.0, "fresh": 2.0}));
        assert_eq!(f["truncated"], true, "a dropped value is flagged");
    }

    #[test]
    fn limits_drop_and_flag_but_never_fail() {
        let s = Summary::new();
        for i in 0..40 {
            s.attr(&format!("k{i}"), "v");
            s.metric(&format!("m{i}"), 1.0);
        }
        s.attr("k0", "changed"); // an existing key can still be updated at the limit
        s.title(&"é".repeat(300));
        s.detail(json!("x".repeat(MAX_DETAIL_BYTES + 1)));
        let f = s.fields();
        assert_eq!(f["attrs"].as_object().unwrap().len(), MAX_ENTRIES);
        assert_eq!(f["attrs"]["k0"], "changed");
        assert_eq!(f["metrics"].as_object().unwrap().len(), MAX_ENTRIES);
        assert!(f["title"].as_str().unwrap().len() <= MAX_TITLE_BYTES);
        assert!(!f.contains_key("detail"));
        assert_eq!(f["truncated"], true);
    }

    #[test]
    fn the_first_failure_wins_and_names_the_innermost_script_frame() {
        let s = Summary::new();
        s.failure(json!({"kind": "runtime", "frames": [
            {"chunk": "[C]", "native": true},
            {"chunk": "acme/feed", "line": 12, "function": "parse", "native": false},
        ]}));
        s.failure(json!({"kind": "timeout", "frames": []}));
        let f = s.fields();
        assert_eq!(f["failure_kind"], "runtime");
        assert_eq!(f["failure_frame"], "acme/feed:12 parse");
    }

    #[test]
    fn counts_log_lines_and_http_calls() {
        let s = Summary::new();
        s.log("info");
        s.log("error");
        s.http(false);
        s.http(true);
        let f = s.fields();
        assert_eq!(
            (f["log_lines"].clone(), f["log_errors"].clone()),
            (2.into(), 1.into())
        );
        assert_eq!(
            (f["http_requests"].clone(), f["http_failed"].clone()),
            (2.into(), 1.into())
        );
    }
}
