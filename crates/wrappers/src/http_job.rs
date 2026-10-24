//! Background HTTP for `native.batch`: `job:http(name, opts)` describes a request, and after
//! `submit` the host performs it on its own thread, outside the invocation's time budget. Only the
//! response is written to the batch bucket (the request, which may carry vault secrets, never is),
//! followed by `done.json`, so the usual continuation runs. Fargate only: a Lambda sandbox is
//! frozen when the invocation returns.

use std::{collections::HashMap, io::Read, sync::Arc, time::Duration};

use mlua::Table;
use url::Url;

use crate::{vault::Expand, Budget, HttpPolicy, Throttle, Vault};

/// How long one background request may take, from the moment its throttle slot is granted.
pub(crate) const BACKGROUND_TIMEOUT: Duration = Duration::from_secs(15 * 60);
/// How long a request may wait for a throttle slot.
const THROTTLE_WAIT: Duration = Duration::from_secs(60 * 60);

fn err(m: impl std::fmt::Display) -> mlua::Error {
    mlua::Error::runtime(m.to_string())
}

/// What `job:http` needs to validate a request when it is described.
#[derive(Clone)]
pub(crate) struct Env {
    pub tenant: String,
    pub policy: HttpPolicy,
    pub vault: Option<Arc<Vault>>,
    pub throttle: Option<Arc<Throttle>>,
    pub max_body: u64,
}

/// A request that passed the allowlist and vault checks, with secrets already substituted.
pub(crate) struct Prepared {
    method: String,
    url: Url,
    headers: Vec<(String, String)>,
    body: Vec<u8>,
    secrets: Vec<Vec<u8>>,
}

pub(crate) struct Response {
    pub status: u16,
    pub headers: serde_json::Map<String, serde_json::Value>,
    pub body: Vec<u8>,
}

impl Env {
    /// Parses `{ method, url, headers, body, vault }` like `native.http.request` and applies the same
    /// checks, so a request that could never be sent fails in the submitting script.
    pub fn prepare(&self, budget: &Budget, opts: &Table) -> mlua::Result<Prepared> {
        budget.take_http().map_err(err)?;
        let method = opts
            .get::<Option<String>>("method")?
            .unwrap_or_else(|| "GET".into())
            .to_ascii_uppercase();
        let url: String = opts.get("url")?;
        let headers = opts
            .get::<Option<HashMap<String, String>>>("headers")?
            .unwrap_or_default();
        let body = opts.get::<Option<mlua::LuaString>>("body")?;
        let expand =
            Expand::parse(opts.get::<Option<Vec<String>>>("vault")?).map_err(mlua::Error::runtime)?;

        let mut used: Vec<crate::vault::Secret> = Vec::new();
        let mut lookup = |reference: &str| -> Result<Vec<u8>, String> {
            budget.take_data()?;
            let secret = self
                .vault
                .as_ref()
                .ok_or("the credential vault is not configured for this runtime")?
                .lookup(&self.tenant, reference)?;
            let value = secret.value.clone();
            used.push(secret);
            Ok(value)
        };
        let expanded = (|| -> Result<_, String> {
            let url = if expand.url {
                let u = crate::vault::substitute(url.as_bytes(), &mut lookup)?;
                String::from_utf8(u).map_err(|_| "vault secret is not valid in a url")?
            } else {
                url.clone()
            };
            let headers = headers
                .iter()
                .map(|(k, v)| {
                    if !expand.header {
                        return Ok((k.clone(), v.clone()));
                    }
                    let v = crate::vault::substitute(v.as_bytes(), &mut lookup)?;
                    Ok((
                        k.clone(),
                        String::from_utf8(v).map_err(|_| "vault secret is not valid in a header")?,
                    ))
                })
                .collect::<Result<Vec<_>, String>>()?;
            let body = match &body {
                Some(b) if expand.body => crate::vault::substitute(&b.as_bytes(), &mut lookup)?,
                Some(b) => b.as_bytes().to_vec(),
                None => Vec::new(),
            };
            Ok((url, headers, body))
        })();
        let scrub = |msg: String| {
            let values: Vec<Vec<u8>> = used.iter().map(|s| s.value.clone()).collect();
            err(String::from_utf8_lossy(&crate::vault::redact(msg.as_bytes(), &values)))
        };
        let (url, headers, body) = expanded.map_err(scrub)?;
        let url = self.policy.check(&method, &url).map_err(scrub)?;
        for secret in &used {
            secret.check(&url).map_err(scrub)?;
        }
        let secrets = used
            .iter()
            .filter(|s| s.value.len() >= crate::MIN_SECRET_BYTES)
            .map(|s| s.value.clone())
            .collect();
        Ok(Prepared {
            method,
            url,
            headers,
            body,
            secrets,
        })
    }
}

/// Performs the request. Errors never contain secret values.
pub(crate) fn execute(
    throttle: Option<&Throttle>,
    max_body: u64,
    p: &Prepared,
) -> Result<Response, String> {
    let scrub = |msg: String| {
        let values: Vec<Vec<u8>> = p.secrets.clone();
        String::from_utf8_lossy(&crate::vault::redact(msg.as_bytes(), &values)).into_owned()
    };
    let host = p.url.host_str().unwrap_or_default().to_string();
    let span = tracing::info_span!(
        "http.client",
        otel.kind = "client",
        http.request.method = %p.method,
        server.address = host.as_str(),
        http.response.status_code = tracing::field::Empty,
        otel.status_code = tracing::field::Empty,
    );
    let _entered = span.enter();
    let waited = std::time::Instant::now();
    let _slot = throttle
        .map(|t| t.acquire(&p.url, std::time::Instant::now() + THROTTLE_WAIT))
        .transpose()?;
    if throttle.is_some() {
        crate::telemetry::throttle_wait(waited.elapsed());
    }
    let agent: ureq::Agent = ureq::Agent::config_builder()
        .max_redirects(0)
        .http_status_as_error(false)
        .timeout_global(Some(BACKGROUND_TIMEOUT))
        .build()
        .into();
    let mut req = ureq::http::Request::builder()
        .method(p.method.as_str())
        .uri(p.url.as_str());
    for (k, v) in &p.headers {
        req = req.header(k, v);
    }
    let started = std::time::Instant::now();
    let resp = agent
        .run(req.body(p.body.clone()).map_err(|e| scrub(e.to_string()))?)
        .map_err(|e| {
            span.record("otel.status_code", "ERROR");
            crate::telemetry::http_request(&p.method, &host, None, started.elapsed());
            scrub(format!("http request failed: {e}"))
        })?;
    let status = resp.status().as_u16();
    span.record("http.response.status_code", status);
    if status >= 500 {
        span.record("otel.status_code", "ERROR");
    }
    let mut headers = serde_json::Map::new();
    for (k, v) in resp.headers() {
        if let Ok(v) = v.to_str() {
            let v = crate::vault::redact(v.as_bytes(), &p.secrets);
            headers.insert(
                k.as_str().to_string(),
                String::from_utf8_lossy(&v).into_owned().into(),
            );
        }
    }
    let mut buf = Vec::new();
    resp.into_body()
        .into_reader()
        .take(max_body)
        .read_to_end(&mut buf)
        .map_err(|e| scrub(format!("reading response: {e}")))?;
    crate::telemetry::http_request(&p.method, &host, Some(status), started.elapsed());
    Ok(Response {
        status,
        headers,
        body: crate::vault::redact(&buf, &p.secrets),
    })
}
