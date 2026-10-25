//! Lambda entrypoint: direct invoke with `{tenant, script, version?, event}`.
//!
//! Config: `integraal.json` bundled next to `bootstrap` (see `GlobalConfig`).

use std::{sync::Arc, time::Duration};

use integraal_core::{GlobalConfig, Invocation, Trigger};
use lambda_runtime::{service_fn, Error, LambdaEvent};
use serde_json::{json, Value};

#[tokio::main]
async fn main() -> Result<(), Error> {
    let cfg = GlobalConfig::load()?;
    let telemetry = Arc::new(integraal_core::telemetry::init(
        "integraal-lambda",
        cfg.log_filter(),
    )?);

    // `throttle` is deliberately not applied here: limits are per process and only make sense on Fargate.
    let engine = cfg.engine().await?;

    lambda_runtime::run(service_fn(|ev: LambdaEvent<Value>| {
        let (engine, telemetry) = (engine.clone(), telemetry.clone());
        async move {
            let mut inv: Invocation = serde_json::from_value(ev.payload.clone())?;
            inv.trigger = Trigger::Invoke;
            let left_ms = ev.context.deadline.saturating_sub(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_millis() as u64,
            );
            let budget = Duration::from_millis(left_ms.saturating_sub(500)); // margin to return a clean error
            let out = match engine.invoke_within(inv, Some(budget)).await {
                Ok(result) => json!({ "ok": true, "result": result }),
                Err(e) => json!({ "ok": false, "error": e.to_string() }),
            };
            engine.flush_tasks().await; // likewise: one object per invocation
            telemetry.flush(); // the environment may freeze as soon as we return
            Ok::<_, Error>(out)
        }
    }))
    .await
}
