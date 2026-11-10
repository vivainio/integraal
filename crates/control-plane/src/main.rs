//! API Gateway HTTP API v2 integration. The gateway authenticates the caller.
use integraal_core::{operations, Engine, GlobalConfig};
use lambda_runtime::{service_fn, Error, LambdaEvent};
use serde_json::{json, Value};

fn response(status: u16, body: Value) -> Value {
    json!({"statusCode": status, "headers": {"content-type": "application/json", "cache-control": "no-store"}, "body": body.to_string()})
}

// Only API Gateway may invoke this Lambda: authorizer claims are part of the event,
// not independently verified here. Reject all non-v2 and unauthenticated events.
fn tenant(event: &Value) -> Option<&str> {
    if event.get("version")?.as_str()? != "2.0" {
        return None;
    }
    let claims = event.pointer("/requestContext/authorizer/jwt/claims")?;
    let subject = claims.get("sub")?.as_str()?;
    let tenant = claims.get("tenant")?.as_str()?;
    if subject.is_empty() || integraal_loader::ScriptKey::config(tenant).is_err() {
        return None;
    }
    Some(tenant)
}

#[derive(Clone)]
struct AppState {
    engine: Engine,
    catalog: integraal_core::catalog::ScriptCatalog,
    runtime: aws_sdk_lambda::Client,
    runtime_function: Option<String>,
}

async fn dispatch(state: &AppState, event: &Value) -> Value {
    let engine = &state.engine;
    let Some(tenant) = tenant(event) else {
        return response(401, json!({"error":"unauthorized"}));
    };
    let method = event
        .pointer("/requestContext/http/method")
        .and_then(Value::as_str)
        .unwrap_or("");
    let path = event.get("rawPath").and_then(Value::as_str).unwrap_or("");
    if path == "/scripts" && method == "GET" {
        return match state.catalog.list(tenant).await {
            Ok(scripts) => response(200, json!({"scripts": scripts})),
            Err(error) => {
                tracing::error!(%error, "script listing failed");
                response(500, json!({"error":"could not list scripts"}))
            }
        };
    }
    if let Some(script) = path
        .strip_prefix("/scripts/")
        .and_then(|p| p.strip_suffix("/trigger"))
    {
        if method != "GET" && method != "POST" {
            return response(405, json!({"error":"method not allowed"}));
        }
        let version = event
            .pointer("/queryStringParameters/version")
            .and_then(Value::as_str);
        if integraal_loader::ScriptKey::new(tenant, script, version).is_err() {
            return response(400, json!({"error":"invalid script or version"}));
        }
        let Some(function) = &state.runtime_function else {
            return response(503, json!({"error":"runtime function not configured"}));
        };
        let args = if method == "POST" {
            if event
                .get("isBase64Encoded")
                .and_then(Value::as_bool)
                .unwrap_or(false)
            {
                return response(400, json!({"error":"invalid request body"}));
            }
            let body = event.get("body").and_then(Value::as_str).unwrap_or("");
            if body.len() > 65536 {
                return response(413, json!({"error":"request body too large"}));
            }
            match serde_json::from_str::<Value>(body) {
                Ok(v) if v.is_object() => v,
                _ => return response(400, json!({"error":"expected a JSON object"})),
            }
        } else {
            json!({})
        };
        let payload = json!({"type": if method == "GET" { "user.describe" } else { "user.run" }, "tenant": tenant, "script": script, "version": version, "event": args});
        let result = state
            .runtime
            .invoke()
            .function_name(function)
            .payload(aws_sdk_lambda::primitives::Blob::new(payload.to_string()))
            .send()
            .await;
        return match result {
            Ok(out) if out.function_error().is_none() => match out
                .payload()
                .and_then(|p| serde_json::from_slice::<Value>(p.as_ref()).ok())
            {
                Some(value) if value["ok"] == true => {
                    response(200, json!({"result": value["result"]}))
                }
                Some(value) if value["ok"] == false => {
                    response(422, json!({"error": value["error"]}))
                }
                _ => response(502, json!({"error":"invalid runtime response"})),
            },
            Ok(_) => response(502, json!({"error":"runtime invocation failed"})),
            Err(error) => {
                tracing::error!(%error, "runtime invocation failed");
                response(502, json!({"error":"could not invoke runtime"}))
            }
        };
    }
    if method != "GET" {
        return response(405, json!({"error":"method not allowed"}));
    }
    if path == "/flows" {
        let Some(day) = event
            .pointer("/queryStringParameters/day")
            .and_then(Value::as_str)
        else {
            return response(400, json!({"error":"day is required (YYYY-MM-DD UTC)"}));
        };
        if day.len() != 10
            || !day.bytes().enumerate().all(|(i, b)| {
                if i == 4 || i == 7 {
                    b == b'-'
                } else {
                    b.is_ascii_digit()
                }
            })
        {
            return response(400, json!({"error":"invalid day"}));
        }
        let Some(sink) = engine.flow_logs().cloned() else {
            return response(503, json!({"error":"flow logs not configured"}));
        };
        let (tenant, day) = (tenant.to_owned(), day.to_owned());
        return match tokio::task::spawn_blocking(move || sink.list_day(&tenant, &day)).await {
            Ok(Ok(flows)) => response(200, json!({"flows": flows})),
            _ => response(500, json!({"error":"could not list flows"})),
        };
    }
    if let Some(trace) = path.strip_prefix("/flows/") {
        let (trace, logs) = match trace.strip_suffix("/logs") {
            Some(trace) => (trace, true),
            None => (trace, false),
        };
        if trace.len() != 32 || !trace.bytes().all(|b| b.is_ascii_hexdigit()) {
            return response(400, json!({"error":"invalid trace id"}));
        }
        if logs {
            let Some(sink) = engine.flow_logs().cloned() else {
                return response(503, json!({"error":"flow logs not configured"}));
            };
            let (tenant, trace) = (tenant.to_owned(), trace.to_owned());
            return match tokio::task::spawn_blocking(move || sink.read_flow(&tenant, &trace)).await
            {
                Ok(Ok(lines)) => response(200, json!({"lines": lines})),
                _ => response(500, json!({"error":"could not read flow logs"})),
            };
        }
        return match operations::inspect_flow(engine, tenant, trace).await {
            Ok(flow) => response(200, flow),
            Err(error) => {
                tracing::error!(%error, "flow inspection failed");
                response(500, json!({"error":"could not inspect flow"}))
            }
        };
    }
    response(404, json!({"error":"not found"}))
}

#[tokio::main]
async fn main() -> Result<(), Error> {
    let config = match std::env::var("INTEGRAAL_CONTROL_CONFIG") {
        Ok(json) => GlobalConfig::parse(&json),
        Err(std::env::VarError::NotPresent) => GlobalConfig::load(),
        Err(error) => return Err(error.into()),
    }
    .map_err(std::io::Error::other)?;
    let telemetry =
        integraal_core::telemetry::init("integraal-control-plane", config.log_filter())?;
    let engine = config.engine().await.map_err(std::io::Error::other)?;
    let catalog = integraal_core::catalog::ScriptCatalog::from_config(&config).await;
    let runtime = aws_sdk_lambda::Client::new(&aws_config::load_from_env().await);
    let state = AppState {
        engine,
        catalog,
        runtime,
        runtime_function: std::env::var("INTEGRAAL_RUNTIME_FUNCTION")
            .ok()
            .filter(|s| !s.is_empty()),
    };
    lambda_runtime::run(service_fn(|event: LambdaEvent<Value>| {
        let state = state.clone();
        let telemetry = &telemetry;
        async move {
            let result = dispatch(&state, &event.payload).await;
            telemetry.flush();
            Ok::<_, Error>(result)
        }
    }))
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn tenant_comes_only_from_authorizer() {
        let event = json!({"version":"2.0", "tenant":"other", "requestContext":{"authorizer":{"jwt":{"claims":{"sub":"user", "tenant":"acme"}}}}});
        assert_eq!(tenant(&event), Some("acme"));
        let mut missing = event.clone();
        missing["requestContext"]["authorizer"]["jwt"]["claims"]["tenant"] = json!("../other");
        assert_eq!(tenant(&missing), None);
        missing["version"] = json!("1.0");
        assert_eq!(tenant(&missing), None);
        assert_eq!(tenant(&json!({"tenant":"acme"})), None);
    }
}
