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
pub use integraal_wrappers::trace::{inject_current, TraceContext};

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

    let mut resource = Resource::builder();
    if std::env::var_os("OTEL_SERVICE_NAME").is_none() {
        resource = resource.with_service_name(service);
    }
    let resource = resource.build();

    // The tracer is always installed, even with nothing to export to: spans then still carry a
    // trace id, which is what ties the messages of one flow together (see `inject_current`).
    let export = enabled();
    let mut tracer = SdkTracerProvider::builder().with_resource(resource.clone());
    if export {
        tracer = tracer.with_batch_exporter(SpanExporter::builder().with_http().build()?);
    }
    let tracer = tracer.build();
    global::set_text_map_propagator(TraceContextPropagator::new());
    let otel = tracing_opentelemetry::layer().with_tracer(tracer.tracer(service));

    let meter = if export {
        let meter = SdkMeterProvider::builder()
            .with_resource(resource)
            .with_periodic_exporter(MetricExporter::builder().with_http().build()?)
            .build();
        global::set_meter_provider(meter.clone());
        Some(meter)
    } else {
        None
    };

    tracing_subscriber::registry()
        .with(filter)
        .with(fmt)
        .with(otel)
        .init();
    Ok(Guard {
        tracer: Some(tracer),
        meter,
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

/// Make `span` a child of the caller's trace, given a lookup for incoming W3C headers
/// (`traceparent`, `tracestate`). Does nothing when the headers are absent or telemetry is off.
/// Returns whether the span now continues a caller's trace.
pub fn adopt_parent(span: &tracing::Span, header: impl Fn(&str) -> Option<String>) -> bool {
    use tracing_opentelemetry::OpenTelemetrySpanExt;
    let trace: TraceContext = ["traceparent", "tracestate"]
        .into_iter()
        .filter_map(|k| header(k).map(|v| (k.to_string(), v)))
        .collect();
    if trace.is_empty() {
        return false;
    }
    span.set_parent(integraal_wrappers::trace::extract(&trace))
        .is_ok()
}

/// Message attributes for `ctx`, ready for the SQS builders.
pub fn sqs_attributes(
    ctx: &TraceContext,
) -> Vec<(String, aws_sdk_sqs::types::MessageAttributeValue)> {
    ctx.iter()
        .filter_map(|(k, v)| {
            let a = aws_sdk_sqs::types::MessageAttributeValue::builder()
                .data_type("String")
                .string_value(v)
                .build()
                .ok()?;
            Some((k.clone(), a))
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tracing::info_span;

    #[test]
    fn a_span_without_a_parent_still_has_a_trace_context() {
        global::set_text_map_propagator(TraceContextPropagator::new());
        let tracer = SdkTracerProvider::builder().build();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_opentelemetry::layer().with_tracer(tracer.tracer("test")));
        tracing::subscriber::with_default(subscriber, || {
            let first = info_span!("a");
            let (tp, again) = first.in_scope(|| (inject_current(), inject_current()));
            assert!(tp["traceparent"].starts_with("00-"));
            assert_eq!(tp, again);

            let child = info_span!("b");
            adopt_parent(&child, |k| tp.get(k).cloned());
            let got = child.in_scope(inject_current);
            let trace_id =
                |m: &TraceContext| m["traceparent"].split('-').nth(1).unwrap().to_string();
            assert_eq!(trace_id(&got), trace_id(&tp));
        });
    }
}
