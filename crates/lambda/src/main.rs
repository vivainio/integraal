//! Lambda entrypoint: direct invocation envelopes and operator `system.tick` events.
//!
//! Config: `integraal.json` bundled next to `bootstrap` (see `GlobalConfig`).

use std::{sync::Arc, time::Duration};

use integraal_core::{
    s3_events::{classify, Message},
    Engine, GlobalConfig, Invocation, Spread, Tick, Trigger,
};
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

    let spread = cfg.schedule.spread_s;

    lambda_runtime::run(service_fn(|ev: LambdaEvent<Value>| {
        let (engine, telemetry) = (engine.clone(), telemetry.clone());
        async move {
            let left_ms = ev.context.deadline.saturating_sub(
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)?
                    .as_millis() as u64,
            );
            let budget = Duration::from_millis(left_ms.saturating_sub(500)); // margin to return a clean error
            let out = dispatch(&engine, ev.payload, &spread, budget).await;
            engine.flush_tasks().await; // likewise: one object per invocation
            telemetry.flush(); // the environment may freeze as soon as we return
            out
        }
    }))
    .await
}

// Only the top-level operator event is a tick; a tenant's nested event remains ordinary input.
enum Input {
    Tick(Tick),
    Invoke(Invocation),
}

fn input(payload: Value) -> Result<Input, Error> {
    if payload.get("type").and_then(Value::as_str) == Some("system.tick") {
        return match classify(&payload.to_string())? {
            Message::Tick(tick) => Ok(Input::Tick(tick)),
            _ => unreachable!("system.tick is classified as a tick"),
        };
    }
    let mut inv: Invocation = serde_json::from_value(payload)?;
    inv.trigger = Trigger::Invoke;
    Ok(Input::Invoke(inv))
}

async fn dispatch(
    engine: &Engine,
    payload: Value,
    spread: &Spread,
    budget: Duration,
) -> Result<Value, Error> {
    match input(payload)? {
        Input::Tick(tick) => {
            // Return runtime errors so EventBridge can retry failed fan-out.
            let report = tokio::time::timeout(budget, engine.tick(&tick, spread)).await??;
            Ok(json!({ "ok": true, "result": { "sent": report.sent } }))
        }
        Input::Invoke(inv) => Ok(match engine.invoke_within(inv, Some(budget)).await {
            Ok(result) => json!({ "ok": true, "result": result }),
            Err(e) => json!({ "ok": false, "error": e.to_string() }),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn operator_ticks_use_shared_validation() {
        for every in ["minute", "hour", "day"] {
            assert!(matches!(
                input(json!({"type":"system.tick", "every":every, "at":"scheduled"})).unwrap(),
                Input::Tick(_)
            ));
        }
        for payload in [
            json!({"type":"system.tick", "every":"week", "at":"scheduled"}),
            json!({"type":"system.tick", "every":"minute", "at":""}),
            json!({"type":"system.tick", "every":"minute"}),
            json!({"type":"system.tick", "every":"minute", "at":"x".repeat(65)}),
        ] {
            assert!(input(payload).is_err());
        }
    }

    #[test]
    fn tenant_events_keep_direct_invoke_semantics() {
        let payload = json!({"tenant":"acme", "script":"work", "event":{"type":"system.tick"}});
        let Input::Invoke(inv) = input(payload).unwrap() else {
            panic!("expected invocation")
        };
        assert_eq!(inv.trigger, Trigger::Invoke);
        assert_eq!(inv.event["type"], "system.tick");
    }
}
