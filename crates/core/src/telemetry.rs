//! Logging and OpenTelemetry setup shared by every binary.
//!
//! JSON logs are always on. Traces and metrics are exported over OTLP/HTTP only
//! when an OTLP endpoint is present in the environment; everything is configured
//! through the standard `OTEL_*` variables (`OTEL_EXPORTER_OTLP_ENDPOINT`,
//! `OTEL_EXPORTER_OTLP_HEADERS`, `OTEL_SERVICE_NAME`, `OTEL_TRACES_SAMPLER`, ...).

use std::sync::OnceLock;
use std::time::Duration;

use opentelemetry::global;
use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::trace::TracerProvider as _;
use opentelemetry::KeyValue;
use opentelemetry_otlp::{MetricExporter, SpanExporter};
use opentelemetry_sdk::metrics::SdkMeterProvider;
use opentelemetry_sdk::propagation::TraceContextPropagator;
use opentelemetry_sdk::trace::SdkTracerProvider;
use opentelemetry_sdk::Resource;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::EnvFilter;

use crate::Trigger;

const ENDPOINT_VARS: [&str; 3] = [
    "OTEL_EXPORTER_OTLP_ENDPOINT",
    "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT",
    "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
];

/// Whether the environment asks for OTLP export.
pub fn enabled() -> bool {
    ENDPOINT_VARS
        .iter()
        .any(|v| std::env::var(v).is_ok_and(|s| !s.is_empty()))
        && std::env::var("OTEL_SDK_DISABLED").map_or(true, |s| s != "true")
}

/// Owns the exporters; flushes and stops them on drop.
pub struct Guard {
    tracer: Option<SdkTracerProvider>,
    meter: Option<SdkMeterProvider>,
}

impl Guard {
    /// Push pending spans and metrics out now. Lambda handlers call this before
    /// returning, because the execution environment may be frozen right after.
    pub fn flush(&self) {
        if let Some(t) = &self.tracer {
            if let Err(e) = t.force_flush() {
                tracing::warn!(error = %e, "trace flush failed");
            }
        }
        if let Some(m) = &self.meter {
            if let Err(e) = m.force_flush() {
                tracing::warn!(error = %e, "metric flush failed");
            }
        }
    }
}

impl Drop for Guard {
    fn drop(&mut self) {
        if let Some(t) = self.tracer.take() {
            let _ = t.shutdown();
        }
        if let Some(m) = self.meter.take() {
            let _ = m.shutdown();
        }
    }
}

/// Install the global subscriber. `service` is the default `service.name`;
/// `OTEL_SERVICE_NAME` overrides it. Keep the returned guard alive until exit.
pub fn init(
    service: &'static str,
    log_filter: &str,
) -> Result<Guard, Box<dyn std::error::Error + Send + Sync>> {
    let filter = EnvFilter::try_new(log_filter)?;
    let fmt = tracing_subscriber::fmt::layer().json();

    if !enabled() {
        tracing_subscriber::registry().with(filter).with(fmt).init();
        return Ok(Guard {
            tracer: None,
            meter: None,
        });
    }

    let mut resource = Resource::builder();
    if std::env::var_os("OTEL_SERVICE_NAME").is_none() {
        resource = resource.with_service_name(service);
    }
    let resource = resource.build();

    let tracer = SdkTracerProvider::builder()
        .with_resource(resource.clone())
        .with_batch_exporter(SpanExporter::builder().with_http().build()?)
        .build();
    let meter = SdkMeterProvider::builder()
        .with_resource(resource)
        .with_periodic_exporter(MetricExporter::builder().with_http().build()?)
        .build();

    global::set_text_map_propagator(TraceContextPropagator::new());
    global::set_meter_provider(meter.clone());
    let otel = tracing_opentelemetry::layer().with_tracer(tracer.tracer(service));

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt)
        .with(otel)
        .init();
    Ok(Guard {
        tracer: Some(tracer),
        meter: Some(meter),
    })
}

struct Instruments {
    invocations: Counter<u64>,
    invocation_secs: Histogram<f64>,
    placement_fallbacks: Counter<u64>,
    sqs_messages: Counter<u64>,
}

fn instruments() -> &'static Instruments {
    static I: OnceLock<Instruments> = OnceLock::new();
    I.get_or_init(|| {
        let m = global::meter("integraal");
        Instruments {
            invocations: m
                .u64_counter("integraal.invocations")
                .with_description("Script invocations")
                .build(),
            invocation_secs: m
                .f64_histogram("integraal.invocation.duration")
                .with_unit("s")
                .with_description("Wall time of a script invocation")
                .build(),
            placement_fallbacks: m
                .u64_counter("integraal.placement.fallbacks")
                .with_description("Messages placed by fallback after a placement problem")
                .build(),
            sqs_messages: m
                .u64_counter("integraal.sqs.messages")
                .with_description("Queue messages by disposition")
                .build(),
        }
    })
}

/// One finished invocation. `tenant` is the only unbounded label.
pub fn invocation(tenant: &str, trigger: Trigger, outcome: &'static str, took: Duration) {
    let attrs = [
        KeyValue::new("tenant", tenant.to_string()),
        KeyValue::new("trigger", format!("{trigger:?}").to_lowercase()),
        KeyValue::new("outcome", outcome),
    ];
    let i = instruments();
    i.invocations.add(1, &attrs);
    i.invocation_secs.record(took.as_secs_f64(), &attrs);
}

/// Messages that fell back to the tenant's static class.
pub fn placement_fallbacks(n: usize) {
    if n > 0 {
        instruments().placement_fallbacks.add(n as u64, &[]);
    }
}

/// A queue message was `received`, `acked` or `requeued`.
pub fn sqs_message(disposition: &'static str) {
    instruments()
        .sqs_messages
        .add(1, &[KeyValue::new("disposition", disposition)]);
}

struct Carrier(std::collections::HashMap<&'static str, String>);

impl opentelemetry::propagation::Extractor for Carrier {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }
    fn keys(&self) -> Vec<&str> {
        self.0.keys().copied().collect()
    }
}

/// Make `span` a child of the caller's trace, given a lookup for incoming W3C headers
/// (`traceparent`, `tracestate`). Does nothing when the headers are absent or telemetry is off.
pub fn adopt_parent(span: &tracing::Span, header: impl Fn(&str) -> Option<String>) {
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    let carrier = Carrier(
        ["traceparent", "tracestate"]
            .into_iter()
            .filter_map(|k| header(k).map(|v| (k, v)))
            .collect(),
    );
    if carrier.0.is_empty() {
        return;
    }
    let cx = global::get_text_map_propagator(|p| p.extract(&carrier));
    let _ = span.set_parent(cx);
}
