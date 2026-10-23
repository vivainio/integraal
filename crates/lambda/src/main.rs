//! Lambda entrypoint: direct invoke with `{tenant, script, version?, event}`.

use std::{sync::Arc, time::Duration};

use integraal_core::{Engine, Invocation, Limits, Trigger};
use integraal_loader::{CachedLoader, S3Store};
use lambda_runtime::{service_fn, Error, LambdaEvent};
use serde_json::{json, Value};

#[tokio::main]
async fn main() -> Result<(), Error> {
    tracing_subscriber::fmt().json().with_env_filter(tracing_subscriber::EnvFilter::from_default_env()).init();

    let bucket = std::env::var("SCRIPT_BUCKET")?;
    let prefix = std::env::var("SCRIPT_PREFIX").unwrap_or_default();
    let store = S3Store::from_env(bucket, prefix).await;
    let loader = Arc::new(CachedLoader::new(store, Duration::from_secs(30)));
    let engine = Engine::new(loader, Limits::default());
    let engine = engine.with_data_from_env().await?;

    let engine = match std::env::var("DB_TABLE") {
        Ok(table) => engine.with_db(Arc::new(integraal_core::DynamoBackend::from_env(table).await)),
        Err(_) => engine,
    };
    let engine = match std::env::var("CALLBACK_BASE_URL") {
        Ok(url) => engine.with_callback_base_url(url),
        Err(_) => engine,
    };

    lambda_runtime::run(service_fn(|ev: LambdaEvent<Value>| {
        let engine = engine.clone();
        async move {
            let mut inv: Invocation = serde_json::from_value(ev.payload.clone())?;
            inv.trigger = Trigger::Invoke;
            let left_ms = ev.context.deadline.saturating_sub(std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH)?.as_millis() as u64);
            let budget = Duration::from_millis(left_ms.saturating_sub(500)); // margin to return a clean error
            match engine.invoke_within(inv, Some(budget)).await {
                Ok(result) => Ok::<_, Error>(json!({ "ok": true, "result": result })),
                Err(e) => Ok(json!({ "ok": false, "error": e.to_string() })),
            }
        }
    }))
    .await
}
