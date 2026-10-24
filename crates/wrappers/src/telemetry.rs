//! Metrics emitted from the script-facing wrappers, through the global OpenTelemetry meter
//! (a no-op until the binary installs a provider).

use std::sync::OnceLock;
use std::time::Duration;

use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::{global, KeyValue};

struct Instruments {
    http_requests: Counter<u64>,
    http_secs: Histogram<f64>,
    throttle_secs: Histogram<f64>,
}

fn instruments() -> &'static Instruments {
    static I: OnceLock<Instruments> = OnceLock::new();
    I.get_or_init(|| {
        let m = global::meter("integraal");
        Instruments {
            http_requests: m
                .u64_counter("integraal.http.client.requests")
                .with_description("Outbound HTTP requests made by scripts")
                .build(),
            http_secs: m
                .f64_histogram("integraal.http.client.duration")
                .with_unit("s")
                .with_description("Outbound HTTP request time, until response headers")
                .build(),
            throttle_secs: m
                .f64_histogram("integraal.http.throttle.wait")
                .with_unit("s")
                .with_description("Time spent waiting for an outbound throttle slot")
                .build(),
        }
    })
}

/// One outbound request. `status` is `None` when no response arrived. Host is a label, so it is
/// bounded by the tenants' HTTP allowlists.
pub(crate) fn http_request(method: &str, host: &str, status: Option<u16>, took: Duration) {
    let attrs = [
        KeyValue::new("http.request.method", method.to_string()),
        KeyValue::new("server.address", host.to_string()),
        KeyValue::new("http.response.status_code", status.map_or(0, i64::from)),
    ];
    let i = instruments();
    i.http_requests.add(1, &attrs);
    i.http_secs.record(took.as_secs_f64(), &attrs);
}

pub(crate) fn throttle_wait(took: Duration) {
    instruments().throttle_secs.record(took.as_secs_f64(), &[]);
}
