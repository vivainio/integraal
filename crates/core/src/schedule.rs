//! Operator schedules: EventBridge Scheduler sends a tick to the queue every minute, hour or day.
//!
//! ```json
//! { "type": "system.tick", "every": "hour", "at": "<aws.scheduler.scheduled-time>" }
//! ```
//!
//! A tick belongs to no tenant. The server handles it itself: the minute tick first sweeps the
//! timers that are due (see `integraal_wrappers::sweep_timers`), and every tick then sends one
//! message to each tenant that asked for it, so the work spreads over the tasks and retries
//! tenant by tenant:
//!
//! ```json
//! { "tenant": "acme", "event": { "type": "schedule", "every": "hour", "at": "2026-10-25T14:00:00Z" } }
//! ```
//!
//! There is no `script`, so the tenant's router picks one. The tenants are listed in
//! `tenants.json` in the script bucket; each opts in with `"schedule": ["hour"]` in its config.
//! Messages are delayed by a fixed amount per tenant so that they do not all arrive together.

use std::time::{SystemTime, UNIX_EPOCH};

use integraal_loader::{LoadError, ScriptKey};
use integraal_wrappers::{sweep_barriers, sweep_outbox, sweep_timers, SweepReport, MAX_DELAY_SECS};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::{s3_events::Tick, Engine, Error};

/// Most timers one minute tick sends.
const SWEEP_LIMIT: usize = 100;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Every {
    Minute,
    Hour,
    Day,
}

impl Every {
    pub fn as_str(self) -> &'static str {
        match self {
            Every::Minute => "minute",
            Every::Hour => "hour",
            Every::Day => "day",
        }
    }

    pub fn period_secs(self) -> u32 {
        match self {
            Every::Minute => 60,
            Every::Hour => 3600,
            Every::Day => 86_400,
        }
    }
}

/// How far each tick's messages are spread, in seconds, per kind of tick.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Spread {
    #[serde(default = "default_minute")]
    pub minute: u32,
    #[serde(default = "default_hour")]
    pub hour: u32,
    #[serde(default = "default_day")]
    pub day: u32,
}

fn default_minute() -> u32 {
    20
}
fn default_hour() -> u32 {
    300
}
fn default_day() -> u32 {
    MAX_DELAY_SECS
}

impl Default for Spread {
    fn default() -> Self {
        Self {
            minute: default_minute(),
            hour: default_hour(),
            day: default_day(),
        }
    }
}

impl Spread {
    pub fn window(&self, every: Every) -> u32 {
        match every {
            Every::Minute => self.minute,
            Every::Hour => self.hour,
            Every::Day => self.day,
        }
    }

    fn validate(&self) -> Result<(), String> {
        for every in [Every::Minute, Every::Hour, Every::Day] {
            let w = self.window(every);
            if w > MAX_DELAY_SECS || w >= every.period_secs() {
                return Err(format!(
                    "schedule.spread_s.{}: at most {MAX_DELAY_SECS} seconds (the queue's limit) and shorter than the period",
                    every.as_str()
                ));
            }
        }
        Ok(())
    }
}

/// The `schedule` section of the global config.
#[derive(Debug, Clone, Default, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ScheduleConfig {
    #[serde(default)]
    pub spread_s: Spread,
}

impl ScheduleConfig {
    pub(crate) fn validate(&self) -> Result<(), String> {
        self.spread_s.validate()
    }
}

/// Where in `0..window` this tenant's message of this tick lands. The same tenant always gets the
/// same offset, so its work stays at a steady time and a repeated tick sends the same delays.
pub fn offset_secs(tenant: &str, every: Every, window: u32) -> u32 {
    if window == 0 {
        return 0;
    }
    // FNV-1a: stable across runs and platforms, unlike the std hasher.
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for b in tenant.bytes().chain([0]).chain(every.as_str().bytes()) {
        h ^= u64::from(b);
        h = h.wrapping_mul(0x0100_0000_01b3);
    }
    (h % u64::from(window)) as u32
}

/// The tenants in `tenants.json`: a JSON array of names. Names that are not valid are skipped.
pub fn parse_tenants(json: &str) -> Result<Vec<String>, String> {
    let names: Vec<String> = serde_json::from_str(json)
        .map_err(|e| format!("tenants.json must be an array of names: {e}"))?;
    let mut out = Vec::with_capacity(names.len());
    for name in names {
        if ScriptKey::config(&name).is_err() {
            tracing::warn!(tenant = %name, "tenants.json: invalid tenant name, skipped");
        } else if !out.contains(&name) {
            out.push(name);
        }
    }
    Ok(out)
}

/// What one tick did.
#[derive(Debug, Default)]
pub struct TickReport {
    /// The timer sweep, on minute ticks when the deployment has a table.
    pub swept: Option<SweepReport>,
    /// Durable handoffs recovered on minute ticks.
    pub outbox: Option<SweepReport>,
    /// Interrupted barrier acknowledgements/continuation sends recovered on minute ticks.
    pub barriers: Option<SweepReport>,
    /// Messages sent to tenants.
    pub sent: usize,
}

fn unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs())
}

impl Engine {
    /// Handles one tick. The minute tick sweeps due timers first; a failing sweep is logged and does
    /// not stop the fan-out. An error here means some tenants may have missed their message, so the
    /// caller may run the tick again: tenants that already got one get the same message again.
    pub async fn tick(&self, tick: &Tick, spread: &Spread) -> Result<TickReport, Error> {
        let queue = self
            .queue_backend()
            .ok_or_else(|| Error::Internal("ticks need a queue".into()))?;
        let mut report = TickReport::default();

        if tick.every == Every::Minute {
            if let Some(items) = self.item_backend() {
                let queue = queue.clone();
                let swept = tokio::task::spawn_blocking(move || {
                    sweep_timers(&*items, &*queue, unix_secs(), SWEEP_LIMIT)
                })
                .await
                .map_err(|e| Error::Internal(e.to_string()))?;
                match swept {
                    Ok(r) => {
                        if r.fired + r.failed > 0 {
                            tracing::info!(fired = r.fired, failed = r.failed, "timers swept");
                        }
                        report.swept = Some(r);
                    }
                    Err(e) => tracing::error!(error = %e, "timer sweep failed"),
                }
            }
        }

        if tick.every == Every::Minute {
            if let Some(items) = self.item_backend() {
                let queue = queue.clone();
                match tokio::task::spawn_blocking(move || {
                    sweep_outbox(&*items, &*queue, unix_secs(), SWEEP_LIMIT)
                })
                .await
                .map_err(|e| Error::Internal(e.to_string()))?
                {
                    Ok(r) => {
                        if r.fired + r.failed > 0 {
                            tracing::info!(fired = r.fired, failed = r.failed, "outbox swept");
                        }
                        report.outbox = Some(r);
                    }
                    Err(e) => tracing::error!(error = %e, "outbox sweep failed"),
                }
            }
        }

        if tick.every == Every::Minute {
            if let Some(items) = self.item_backend() {
                let queue = queue.clone();
                match tokio::task::spawn_blocking(move || {
                    sweep_barriers(items, queue, unix_secs(), SWEEP_LIMIT)
                })
                .await
                .map_err(|e| Error::Internal(e.to_string()))?
                {
                    Ok(r) => {
                        if r.fired + r.failed > 0 {
                            tracing::info!(
                                fired = r.fired,
                                failed = r.failed,
                                "barrier dispatches swept"
                            );
                        }
                        report.barriers = Some(r);
                    }
                    Err(e) => tracing::error!(error = %e, "barrier dispatch sweep failed"),
                }
            }
        }

        let tenants = match self.loader().load(&ScriptKey::tenants()).await {
            Ok(script) => parse_tenants(&script.source).map_err(Error::Config)?,
            Err(LoadError::NotFound(_)) => Vec::new(),
            Err(e) => return Err(e.into()),
        };

        let window = spread.window(tick.every);
        let mut messages = Vec::new();
        for tenant in tenants {
            match self.tenant_config(&tenant).await {
                Ok(cfg) if cfg.schedule.contains(&tick.every) => {
                    let body = json!({
                        "tenant": tenant,
                        "event": { "type": "schedule", "every": tick.every, "at": tick.at },
                    });
                    messages.push(integraal_wrappers::OutMessage {
                        body: body.to_string(),
                        delay_secs: offset_secs(&tenant, tick.every, window),
                        trace: None,
                        group: None,
                    });
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(%tenant, error = %e, "tick skipped a tenant whose config cannot be read")
                }
            }
        }
        report.sent = messages.len();
        if !messages.is_empty() {
            tokio::task::spawn_blocking(move || queue.send(&messages))
                .await
                .map_err(|e| Error::Internal(e.to_string()))?
                .map_err(Error::Internal)?;
        }
        Ok(report)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn offsets_are_stable_in_range_and_spread() {
        let tenants: Vec<String> = (0..200).map(|i| format!("tenant{i}")).collect();
        let offsets: Vec<u32> = tenants
            .iter()
            .map(|t| offset_secs(t, Every::Hour, 300))
            .collect();
        assert!(offsets.iter().all(|o| *o < 300));
        assert_eq!(offsets[7], offset_secs("tenant7", Every::Hour, 300));
        // 200 tenants over 300 seconds: not clumped into a few values
        let distinct: std::collections::HashSet<_> = offsets.iter().collect();
        assert!(distinct.len() > 100, "{} distinct", distinct.len());
        assert_eq!(offset_secs("acme", Every::Day, 0), 0);
        // the kind of tick is part of the hash, so a tenant is not at the same phase everywhere
        assert_ne!(
            tenants
                .iter()
                .map(|t| offset_secs(t, Every::Hour, 300))
                .collect::<Vec<_>>(),
            tenants
                .iter()
                .map(|t| offset_secs(t, Every::Day, 300))
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn spread_defaults_and_limits() {
        let c: ScheduleConfig = serde_json::from_str("{}").unwrap();
        assert_eq!(c.spread_s, Spread::default());
        assert!(c.validate().is_ok());
        let c: ScheduleConfig = serde_json::from_str(r#"{"spread_s":{"hour":600}}"#).unwrap();
        assert_eq!((c.spread_s.hour, c.spread_s.minute), (600, 20));
        for bad in [r#"{"minute":60}"#, r#"{"day":901}"#, r#"{"hour":3600}"#] {
            let c: ScheduleConfig =
                serde_json::from_str(&format!(r#"{{"spread_s":{bad}}}"#)).unwrap();
            assert!(c.validate().is_err(), "{bad}");
        }
        assert!(serde_json::from_str::<ScheduleConfig>(r#"{"spread_s":{"week":1}}"#).is_err());
    }

    #[test]
    fn tenants_json_skips_bad_and_repeated_names() {
        let t = parse_tenants(r#"["acme", "globex", "acme", "../x", ""]"#).unwrap();
        assert_eq!(t, ["acme", "globex"]);
        assert!(parse_tenants(r#"{"acme": 1}"#).is_err());
    }
}
