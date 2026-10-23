//! Lambda entrypoint: direct invoke with `{tenant, script, version?, event}`.
//!
//! Config: `integraal.json` bundled next to `bootstrap` (see `GlobalConfig`).

use std::time::Duration;

use integraal_core::{GlobalConfig, Invocation, Trigger};
use lambda_runtime::{service_fn, Error, LambdaEvent};
use serde_json::{json, Value};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let cfg = GlobalConfig::load()?;
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(tracing_subscriber::EnvFilter::try_new(cfg.log_filter())?)
        .init();

    // `throttle` is deliberately not applied here: limits are per process and only make sense on Fargate.
    let engine = cfg.engine().await?;

    lambda_runtime::run(service_fn(|ev: LambdaEvent<Value>| {
        let engine = engine.clone();
        async move {
            let mut inv: Invocation = serde_json::from_value(ev.payload.clone())?;
            inv.trigger = Trigger::Invoke;
            let left_ms = ev.context.deadline.saturating_sub(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_millis() as u64,
            );
            let budget = Duration::from_millis(left_ms.saturating_sub(500)); // margin to return a clean error
            match engine.invoke_within(inv, Some(budget)).await {
                Ok(result) => Ok::<_, Error>(json!({ "ok": true, "result": result })),
                Err(e) => Ok(json!({ "ok": false, "error": e.to_string() })),
            }
        }
    }))
    .await
}
