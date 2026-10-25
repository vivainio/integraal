//! Task records: one line per invocation, for every tenant, kept in the flow log bucket.
//!
//! The engine buffers the lines in memory and writes them as gzipped NDJSON objects. A server
//! writes one object per process every minute (and early if the buffer fills up); a Lambda writes
//! one per invocation, because its environment can be frozen as soon as the handler returns.
//!
//! ```text
//! <prefix>tasks/<yyyy>/<mm>/<dd>/<hhmm>/<process>-<flush ms>-<seq>.ndjson.gz
//! ```
//!
//! The date and minute are those of the flush, not of the tasks in the object. Readers who want
//! tasks by start time list the minutes around the window they care about and filter on `start_ms`.
//!
//! Every line has the same flat shape (see `FlowSegment::task_line` in the engine), so a later
//! compaction into columnar files needs no schema work.

use std::{
    collections::hash_map::RandomState,
    hash::{BuildHasher, Hasher},
    io::Write,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, Mutex,
    },
};

use flate2::{write::GzEncoder, Compression};
use integraal_wrappers::{now_ms, ObjectBackend};
use serde_json::Value;

use crate::flow_log::civil;

/// Buffered bytes (uncompressed) after which the engine flushes without waiting for the minute.
const FLUSH_BYTES: usize = 5 * 1024 * 1024;

#[derive(Default)]
struct Buf {
    lines: Vec<String>,
    bytes: usize,
}

pub struct TaskLog {
    backend: Arc<dyn ObjectBackend>,
    prefix: String,
    /// Tells this process's objects from another's in the same minute.
    process: String,
    seq: AtomicU64,
    buf: Mutex<Buf>,
}

impl TaskLog {
    pub fn new(backend: Arc<dyn ObjectBackend>, prefix: impl Into<String>) -> Self {
        let random = RandomState::new().build_hasher().finish();
        Self {
            backend,
            prefix: prefix.into(),
            process: format!("{random:016x}"),
            seq: AtomicU64::new(0),
            buf: Mutex::new(Buf::default()),
        }
    }

    /// Buffers a line. Returns whether the buffer is full enough to flush now.
    pub fn record(&self, line: &Value) -> bool {
        let line = line.to_string();
        let mut b = self.buf.lock().unwrap();
        b.bytes += line.len() + 1;
        b.lines.push(line);
        b.bytes >= FLUSH_BYTES
    }

    pub(crate) fn key(&self, at_ms: u64) -> String {
        let (y, m, d) = civil(at_ms);
        let minute = (at_ms / 60_000) % 1440;
        let seq = self.seq.fetch_add(1, Ordering::Relaxed);
        format!(
            "{}tasks/{y:04}/{m:02}/{d:02}/{:02}{:02}/{}-{at_ms:013}-{seq}.ndjson.gz",
            self.prefix,
            minute / 60,
            minute % 60,
            self.process
        )
    }

    /// Writes what is buffered, if anything, and returns how many lines went out. Blocks on the
    /// backend; call from a blocking thread. The lines are dropped when the write fails: nothing
    /// here may hold on to memory without end.
    pub fn flush(&self) -> Result<usize, String> {
        let lines = {
            let mut b = self.buf.lock().unwrap();
            b.bytes = 0;
            std::mem::take(&mut b.lines)
        };
        if lines.is_empty() {
            return Ok(0);
        }
        let mut gz = GzEncoder::new(Vec::new(), Compression::default());
        for l in &lines {
            gz.write_all(l.as_bytes()).map_err(|e| e.to_string())?;
            gz.write_all(b"\n").map_err(|e| e.to_string())?;
        }
        let body = gz.finish().map_err(|e| e.to_string())?;
        self.backend
            .put(&self.key(now_ms()), body)
            .map_err(|e| format!("{e} ({} task lines lost)", lines.len()))?;
        Ok(lines.len())
    }
}
