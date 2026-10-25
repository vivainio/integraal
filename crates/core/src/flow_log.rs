//! Flow logs: where the segments of a flow's log live in the flow log bucket, and how to read them
//! back. A flow is everything that shares one trace id (see `docs/observability.md`).
//!
//! ```text
//! <prefix><tenant>/flows/<id[0..2]>/<trace id>/<start ms>-<span id>.ndjson.gz   one per invocation
//! <prefix><tenant>/flow-index/<yyyy>/<mm>/<dd>/<trace id>                       one per flow, by its start
//! ```
//!
//! The trace id alone locates a flow (the date is not in the segment path, because a flow may
//! outlive a day); the two-character shard only keeps listings small.

use std::{io::Read, sync::Arc};

use integraal_wrappers::ObjectBackend;
use serde_json::Value;

/// Most segments or index entries one read returns.
const MAX_READ: usize = 1000;

#[derive(Clone)]
pub struct FlowLogSink {
    backend: Arc<dyn ObjectBackend>,
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

/// (year, month, day) of a Unix time in milliseconds, UTC.
fn civil(ms: u64) -> (i64, u32, u32) {
    let z = (ms / 86_400_000) as i64 + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    let y = yoe + era * 400 + i64::from(m <= 2);
    (y, m, d)
}

impl FlowLogSink {
    pub fn new(backend: Arc<dyn ObjectBackend>, prefix: impl Into<String>) -> Self {
        Self {
            backend,
            prefix: prefix.into(),
        }
    }

    fn flow_dir(&self, tenant: &str, trace_id: &str) -> String {
        format!(
            "{}{tenant}/flows/{}/{trace_id}/",
            self.prefix,
            &trace_id[..2]
        )
    }

    pub(crate) fn segment_key(
        &self,
        tenant: &str,
        trace_id: &str,
        start_ms: u64,
        span_id: &str,
    ) -> String {
        format!(
            "{}{start_ms:013}-{span_id}.ndjson.gz",
            self.flow_dir(tenant, trace_id)
        )
    }

    fn index_dir(&self, tenant: &str, day: (i64, u32, u32)) -> String {
        format!(
            "{}{tenant}/flow-index/{:04}/{:02}/{:02}/",
            self.prefix, day.0, day.1, day.2
        )
    }

    pub(crate) fn index_key(&self, tenant: &str, start_ms: u64, trace_id: &str) -> String {
        format!("{}{trace_id}", self.index_dir(tenant, civil(start_ms)))
    }

    /// Blocks on the backend; call from a blocking thread.
    pub(crate) fn put(&self, key: &str, body: Vec<u8>) -> Result<(), String> {
        self.backend.put(key, body)
    }

    /// Every line of a flow's log, from all its segments, in time order. Lines carry the
    /// `span_id` of the segment they came from. Blocks; call from a blocking thread.
    pub fn read_flow(&self, tenant: &str, trace_id: &str) -> Result<Vec<Value>, String> {
        if !name_ok(tenant) || !trace_id_ok(trace_id) {
            return Err("bad tenant or trace id".into());
        }
        let trace_id = trace_id.to_ascii_lowercase();
        let mut lines = Vec::new();
        for key in self
            .backend
            .list(&self.flow_dir(tenant, &trace_id), MAX_READ)?
        {
            let Some(bytes) = self.backend.get(&key)? else {
                continue;
            };
            let span_id = key
                .rsplit('/')
                .next()
                .and_then(|f| f.split_once('-'))
                .map(|(_, rest)| rest.trim_end_matches(".ndjson.gz").to_string())
                .unwrap_or_default();
            let mut text = String::new();
            flate2::read::GzDecoder::new(&bytes[..])
                .read_to_string(&mut text)
                .map_err(|e| format!("{key}: {e}"))?;
            for l in text.lines() {
                let mut v: Value =
                    serde_json::from_str(l).map_err(|e| format!("{key}: bad line: {e}"))?;
                v["span_id"] = span_id.clone().into();
                lines.push(v);
            }
        }
        // Stable: lines of one segment with the same millisecond keep their order, and the header
        // (which has `start_ms` but no `t`) sorts first.
        lines.sort_by_key(|v| {
            v["t"]
                .as_u64()
                .or_else(|| v["start_ms"].as_u64())
                .unwrap_or(0)
        });
        Ok(lines)
    }

    /// The flows that started on a UTC day (`yyyy-mm-dd`): one index entry each.
    pub fn list_day(&self, tenant: &str, day: &str) -> Result<Vec<Value>, String> {
        let bad = || "day must be yyyy-mm-dd".to_string();
        if !name_ok(tenant) {
            return Err("bad tenant".into());
        }
        let mut parts = day.split('-');
        let (Some(y), Some(m), Some(d), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(bad());
        };
        let (y, m, d): (i64, u32, u32) = (
            y.parse().map_err(|_| bad())?,
            m.parse().map_err(|_| bad())?,
            d.parse().map_err(|_| bad())?,
        );
        let mut out = Vec::new();
        for key in self
            .backend
            .list(&self.index_dir(tenant, (y, m, d)), MAX_READ)?
        {
            if let Some(bytes) = self.backend.get(&key)? {
                out.push(serde_json::from_slice(&bytes).map_err(|e| format!("{key}: {e}"))?);
            }
        }
        Ok(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn civil_dates() {
        assert_eq!(civil(0), (1970, 1, 1));
        assert_eq!(civil(951_782_400_000), (2000, 2, 29));
        assert_eq!(civil(1_793_000_000_000), (2026, 10, 26));
        assert_eq!(civil(86_399_999), (1970, 1, 1));
        assert_eq!(civil(86_400_000), (1970, 1, 2));
    }

    #[test]
    fn keys_shard_by_trace_id_and_do_not_contain_the_date() {
        let s = FlowLogSink::new(
            Arc::new(crate::engine::tests::MemBackend::default()),
            "logs/",
        );
        let id = "4bf92f3577b34da6a3ce929d0e0e4736";
        assert_eq!(
            s.segment_key("acme", id, 5, "00f067aa0ba902b7"),
            "logs/acme/flows/4b/4bf92f3577b34da6a3ce929d0e0e4736/0000000000005-00f067aa0ba902b7.ndjson.gz"
        );
        assert_eq!(
            s.index_key("acme", 951_782_400_000, id),
            "logs/acme/flow-index/2000/02/29/4bf92f3577b34da6a3ce929d0e0e4736"
        );
    }

    #[test]
    fn reads_reject_bad_names() {
        let s = FlowLogSink::new(Arc::new(crate::engine::tests::MemBackend::default()), "");
        assert!(s
            .read_flow("../x", "4bf92f3577b34da6a3ce929d0e0e4736")
            .is_err());
        assert!(s.read_flow("acme", "nothex").is_err());
        assert!(s.list_day("acme", "yesterday").is_err());
    }
}
