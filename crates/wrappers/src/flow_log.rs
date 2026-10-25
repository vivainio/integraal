//! The log of one invocation, kept for the flow it belongs to (see `docs/observability.md`).
//!
//! Lines are buffered in memory: the first `HEAD_BYTES` and the last `TAIL_BYTES` are kept, and a
//! marker says how many lines were dropped in between. The engine renders the buffer to gzipped
//! NDJSON when the invocation ends and writes it to the flow log bucket.

use std::{
    collections::VecDeque,
    io::Write,
    sync::{Arc, Mutex},
    time::{SystemTime, UNIX_EPOCH},
};

use flate2::{write::GzEncoder, Compression};
use serde_json::{json, Value};

/// Bytes of the earliest lines that are always kept.
const HEAD_BYTES: usize = 192 * 1024;
/// Bytes of the most recent lines that are always kept.
const TAIL_BYTES: usize = 64 * 1024;
/// Longest message of one line; longer ones are cut.
const MAX_MESSAGE_BYTES: usize = 8 * 1024;

#[derive(Default)]
struct Buf {
    head: Vec<String>,
    head_bytes: usize,
    tail: VecDeque<String>,
    tail_bytes: usize,
    dropped: usize,
    failure: Option<Value>,
}

/// A shared handle to the buffer of one invocation.
#[derive(Clone, Default)]
pub struct FlowLog(Arc<Mutex<Buf>>);

pub fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

fn cut(s: &str) -> &str {
    if s.len() <= MAX_MESSAGE_BYTES {
        return s;
    }
    let mut end = MAX_MESSAGE_BYTES;
    while !s.is_char_boundary(end) {
        end -= 1;
    }
    &s[..end]
}

impl FlowLog {
    pub fn new() -> Self {
        Self::default()
    }

    fn push(&self, line: Value) {
        let line = line.to_string();
        let mut b = self.0.lock().unwrap();
        if b.dropped == 0 && b.tail.is_empty() && b.head_bytes + line.len() <= HEAD_BYTES {
            b.head_bytes += line.len();
            b.head.push(line);
            return;
        }
        b.tail_bytes += line.len();
        b.tail.push_back(line);
        while b.tail_bytes > TAIL_BYTES {
            let Some(old) = b.tail.pop_front() else {
                break;
            };
            b.tail_bytes -= old.len();
            b.dropped += 1;
        }
    }

    /// A `native.log` line.
    pub fn log(&self, level: &str, script: &str, msg: &str) {
        self.push(json!({ "t": now_ms(), "type": "log", "level": level, "script": script, "msg": cut(msg) }));
    }

    /// An outbound `native.http` request: never the path or query. `status` is `None` when no
    /// response arrived.
    pub fn http(&self, method: &str, host: &str, status: Option<u16>, took_ms: u64) {
        self.push(json!({ "t": now_ms(), "type": "http", "method": method, "host": host, "status": status, "ms": took_ms }));
    }

    /// An event of the engine itself (a failure, a timeout).
    pub fn event(&self, level: &str, msg: &str) {
        self.push(json!({ "t": now_ms(), "type": "engine", "level": level, "msg": cut(msg) }));
    }

    /// The structured failure of the invocation (kind, message, frames); it goes in the footer.
    /// The first one recorded wins: later ones are consequences.
    pub fn failure(&self, failure: Value) {
        let mut b = self.0.lock().unwrap();
        if b.failure.is_none() {
            b.failure = Some(failure);
        }
    }

    /// The failure recorded so far, if any.
    pub fn recorded_failure(&self) -> Option<Value> {
        self.0.lock().unwrap().failure.clone()
    }

    /// Gzipped NDJSON: `header`, the kept lines, then `footer`.
    pub fn render(&self, header: Value, mut footer: Value) -> Vec<u8> {
        let b = self.0.lock().unwrap();
        if b.dropped > 0 {
            footer["dropped_lines"] = b.dropped.into();
        }
        if let Some(f) = &b.failure {
            footer["failure"] = f.clone();
        }
        let mut gz = GzEncoder::new(Vec::new(), Compression::default());
        let mut line = |s: &str| {
            let _ = gz.write_all(s.as_bytes());
            let _ = gz.write_all(b"\n");
        };
        line(&header.to_string());
        for l in &b.head {
            line(l);
        }
        if b.dropped > 0 {
            line(&json!({ "type": "truncated", "dropped_lines": b.dropped }).to_string());
        }
        for l in &b.tail {
            line(l);
        }
        line(&footer.to_string());
        gz.finish().unwrap_or_default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::read::GzDecoder;
    use std::io::Read;

    fn text(bytes: &[u8]) -> Vec<Value> {
        let mut s = String::new();
        GzDecoder::new(bytes).read_to_string(&mut s).unwrap();
        s.lines()
            .map(|l| serde_json::from_str(l).unwrap())
            .collect()
    }

    #[test]
    fn small_logs_are_kept_whole_between_header_and_footer() {
        let l = FlowLog::new();
        l.log("info", "hello", "one");
        l.http("GET", "api.example.com", Some(200), 12);
        let out = text(&l.render(json!({"type": "header"}), json!({"type": "end"})));
        let kinds: Vec<_> = out.iter().map(|v| v["type"].as_str().unwrap()).collect();
        assert_eq!(kinds, ["header", "log", "http", "end"]);
        assert_eq!(out[1]["msg"], "one");
    }

    #[test]
    fn a_long_log_keeps_its_head_and_tail() {
        let l = FlowLog::new();
        let big = "x".repeat(4000);
        for i in 0..200 {
            l.log("info", "s", &format!("{i} {big}"));
        }
        let out = text(&l.render(json!({"type": "header"}), json!({"type": "end"})));
        let marker = out.iter().position(|v| v["type"] == "truncated").unwrap();
        assert!(out[marker]["dropped_lines"].as_u64().unwrap() > 0);
        assert!(
            out[1]["msg"].as_str().unwrap().starts_with("0 "),
            "head kept"
        );
        let last = &out[out.len() - 2];
        assert!(
            last["msg"].as_str().unwrap().starts_with("199 "),
            "tail kept"
        );
        assert_eq!(
            out.last().unwrap()["dropped_lines"],
            out[marker]["dropped_lines"]
        );
    }

    #[test]
    fn long_messages_are_cut_on_a_character_boundary() {
        let l = FlowLog::new();
        l.log("info", "s", &"é".repeat(10_000));
        let out = text(&l.render(json!({}), json!({})));
        assert!(out[1]["msg"].as_str().unwrap().len() <= MAX_MESSAGE_BYTES);
    }
}
