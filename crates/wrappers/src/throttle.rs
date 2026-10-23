use std::{
    sync::{Arc, Condvar, Mutex},
    time::{Duration, Instant},
};

use serde::Deserialize;
use url::Url;

/// One operator-set limit on outbound HTTP to a URL prefix, shared by every tenant and invocation
/// in the process. Patterns match like `http.allow` entries (scheme, host, port, path prefix; a
/// `*.` host matches subdomains). A call that matches waits for a free slot, up to the invocation
/// deadline.
///
/// ```json
/// [{ "url": "https://api.partner.com/v1/", "max_concurrent": 2, "per_second": 5 }]
/// ```
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ThrottleRule {
    pub url: String,
    /// Requests in flight at once (a streamed response counts until its stream is closed).
    pub max_concurrent: Option<u32>,
    /// Request starts per second, evenly spaced.
    pub per_second: Option<f64>,
}

struct Rule {
    pattern: String,
    max_concurrent: Option<usize>,
    interval: Option<Duration>,
    state: Mutex<State>,
    freed: Condvar,
}

struct State {
    in_flight: usize,
    next_start: Instant,
}

/// The set of throttle rules for one process. Limits are per process, not shared between
/// Fargate tasks or Lambda environments.
pub struct Throttle {
    rules: Vec<Arc<Rule>>,
}

/// Held while a throttled request is in flight; dropping it frees the concurrency slots.
pub struct Slot {
    held: Vec<Arc<Rule>>,
}

impl std::fmt::Debug for Slot {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "Slot({} held)", self.held.len())
    }
}

impl Drop for Slot {
    fn drop(&mut self) {
        for rule in &self.held {
            rule.state.lock().unwrap().in_flight -= 1;
            rule.freed.notify_one();
        }
    }
}

impl Throttle {
    pub fn new(rules: Vec<ThrottleRule>) -> Result<Self, String> {
        let rules = rules
            .into_iter()
            .map(|r| {
                if r.max_concurrent.is_none() && r.per_second.is_none() {
                    return Err(format!(
                        "throttle rule {:?} sets neither max_concurrent nor per_second",
                        r.url
                    ));
                }
                if r.max_concurrent == Some(0) {
                    return Err(format!(
                        "throttle rule {:?}: max_concurrent must be at least 1",
                        r.url
                    ));
                }
                let interval = match r.per_second {
                    None => None,
                    Some(p) if p.is_finite() && p > 0.0 && p <= 1_000_000.0 => {
                        Some(Duration::from_secs_f64(1.0 / p))
                    }
                    Some(p) => {
                        return Err(format!(
                            "throttle rule {:?}: per_second {p} is not a positive number",
                            r.url
                        ))
                    }
                };
                if !crate::http::pattern_is_valid(&r.url) {
                    return Err(format!(
                        "throttle rule url {:?} is not a valid pattern",
                        r.url
                    ));
                }
                Ok(Arc::new(Rule {
                    pattern: r.url,
                    max_concurrent: r.max_concurrent.map(|n| n as usize),
                    interval,
                    state: Mutex::new(State {
                        in_flight: 0,
                        next_start: Instant::now(),
                    }),
                    freed: Condvar::new(),
                }))
            })
            .collect::<Result<_, String>>()?;
        Ok(Self { rules })
    }

    /// From a JSON array of rules.
    pub fn from_json(json: &str) -> Result<Self, String> {
        Self::new(serde_json::from_str(json).map_err(|e| format!("invalid throttle rules: {e}"))?)
    }

    /// Waits until every rule matching `url` has room, or fails if that cannot happen before `deadline`.
    /// Blocks the calling thread (scripts run on blocking threads).
    pub fn acquire(&self, url: &Url, deadline: Instant) -> Result<Slot, String> {
        let mut slot = Slot { held: Vec::new() };
        // Rules are always taken in the same order, so two callers cannot wait on each other.
        for rule in self
            .rules
            .iter()
            .filter(|r| crate::http::matches_rule(&r.pattern, url))
        {
            let too_late = || {
                format!(
                    "throttled: no free slot for {} before the deadline",
                    rule.pattern
                )
            };
            let mut st = rule.state.lock().unwrap();
            if let Some(max) = rule.max_concurrent {
                while st.in_flight >= max {
                    let left = deadline
                        .checked_duration_since(Instant::now())
                        .ok_or_else(too_late)?;
                    st = rule.freed.wait_timeout(st, left).unwrap().0;
                }
                st.in_flight += 1;
                slot.held.push(rule.clone());
            }
            if let Some(interval) = rule.interval {
                let start = st.next_start.max(Instant::now());
                if start >= deadline {
                    return Err(too_late()); // `slot` drops here and frees what was taken
                }
                st.next_start = start + interval;
                drop(st);
                std::thread::sleep(start.saturating_duration_since(Instant::now()));
            }
        }
        // The concurrency slots were pushed per rule above; rules with only a rate hold nothing.
        Ok(slot)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn throttle(json: &str) -> Throttle {
        Throttle::from_json(json).unwrap()
    }

    fn url(s: &str) -> Url {
        Url::parse(s).unwrap()
    }

    fn later(ms: u64) -> Instant {
        Instant::now() + Duration::from_millis(ms)
    }

    #[test]
    fn rules_are_validated() {
        assert!(
            Throttle::from_json(r#"[{"url":"https://a.com/"}]"#).is_err(),
            "no limit"
        );
        assert!(Throttle::from_json(r#"[{"url":"https://a.com/","max_concurrent":0}]"#).is_err());
        assert!(Throttle::from_json(r#"[{"url":"https://a.com/","per_second":-1}]"#).is_err());
        assert!(Throttle::from_json(r#"[{"url":"not a url","per_second":1}]"#).is_err());
        assert!(
            Throttle::from_json(r#"[{"url":"https://a.com/","per_second":1,"burst":2}]"#).is_err(),
            "unknown field"
        );
        assert!(Throttle::from_json("[]").is_ok());
    }

    #[test]
    fn unmatched_urls_are_not_limited() {
        let t = throttle(r#"[{"url":"https://api.partner.com/v1/","max_concurrent":1}]"#);
        let _held = t
            .acquire(&url("https://api.partner.com/v1/x"), later(50))
            .unwrap();
        let start = Instant::now();
        for _ in 0..5 {
            t.acquire(&url("https://other.example.com/"), later(50))
                .unwrap();
            t.acquire(&url("https://api.partner.com/v2/x"), later(50))
                .unwrap();
        }
        assert!(start.elapsed() < Duration::from_millis(40));
    }

    #[test]
    fn concurrency_waits_for_a_free_slot_then_gives_up_at_the_deadline() {
        let t = Arc::new(throttle(
            r#"[{"url":"https://api.partner.com/","max_concurrent":1}]"#,
        ));
        let u = url("https://api.partner.com/x");
        let first = t.acquire(&u, later(1000)).unwrap();
        // while the slot is held, a second caller times out
        assert!(t.acquire(&u, later(60)).unwrap_err().contains("throttled"));
        // a waiting caller proceeds as soon as the slot frees
        let waiter = std::thread::spawn({
            let (t, u) = (t.clone(), u.clone());
            move || {
                let began = Instant::now();
                t.acquire(&u, later(2000)).map(|_| began.elapsed())
            }
        });
        std::thread::sleep(Duration::from_millis(80));
        drop(first);
        let waited = waiter.join().unwrap().expect("should get the freed slot");
        assert!(
            waited >= Duration::from_millis(70) && waited < Duration::from_millis(1000),
            "{waited:?}"
        );
        // and the timed-out attempt did not leak a slot
        t.acquire(&u, later(50)).unwrap();
    }

    #[test]
    fn rate_spaces_out_starts() {
        let t = throttle(r#"[{"url":"https://api.partner.com/","per_second":20}]"#); // one start per 50 ms
        let u = url("https://api.partner.com/x");
        let began = Instant::now();
        for _ in 0..4 {
            t.acquire(&u, later(2000)).unwrap();
        }
        let took = began.elapsed();
        assert!(
            took >= Duration::from_millis(140) && took < Duration::from_millis(600),
            "{took:?}"
        ); // 3 gaps of 50 ms
           // a wait that would pass the deadline fails immediately instead of sleeping
        let t0 = Instant::now();
        assert!(t.acquire(&u, later(10)).is_err());
        assert!(t0.elapsed() < Duration::from_millis(40));
    }

    #[test]
    fn overlapping_rules_all_apply() {
        let t = throttle(
            r#"[{"url":"https://api.partner.com/","max_concurrent":2},{"url":"https://api.partner.com/slow/","max_concurrent":1}]"#,
        );
        let slow = url("https://api.partner.com/slow/job");
        let _a = t.acquire(&slow, later(50)).unwrap();
        assert!(t.acquire(&slow, later(50)).is_err(), "path rule allows one");
        // the failed attempt must have released the host-wide slot it took first
        let fast = url("https://api.partner.com/fast");
        let _b = t.acquire(&fast, later(50)).unwrap();
        assert!(
            t.acquire(&fast, later(50)).is_err(),
            "host rule allows two in total"
        );
    }
}
