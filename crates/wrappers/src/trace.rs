//! W3C trace context for work that is handed over and picked up later (queue messages, barriers,
//! callbacks, batch jobs), so the pieces of one flow share a trace id.

use std::collections::BTreeMap;

use opentelemetry::{global, trace::TraceContextExt};
use tracing_opentelemetry::OpenTelemetrySpanExt;

/// `traceparent` and `tracestate`, as stored with a piece of pending work or sent as SQS message
/// attributes. Empty when there was no trace context.
pub type TraceContext = BTreeMap<String, String>;

struct Injector<'a>(&'a mut TraceContext);

impl opentelemetry::propagation::Injector for Injector<'_> {
    fn set(&mut self, key: &str, value: String) {
        self.0.insert(key.to_string(), value);
    }
}

struct Extractor<'a>(&'a TraceContext);

impl opentelemetry::propagation::Extractor for Extractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).map(String::as_str)
    }
    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(String::as_str).collect()
    }
}

/// The trace context of the current span; empty when the span has none.
pub fn inject_current() -> TraceContext {
    let cx = tracing::Span::current().context();
    let mut out = TraceContext::new();
    global::get_text_map_propagator(|p| p.inject_context(&cx, &mut Injector(&mut out)));
    out
}

/// The OpenTelemetry context a stored trace context describes.
pub fn extract(trace: &TraceContext) -> opentelemetry::Context {
    global::get_text_map_propagator(|p| p.extract(&Extractor(trace)))
}

/// Links the current span to the trace that started the work it continues. For work that is
/// resumed by an event that carries no trace (a batch partner's `done.json`), where the span
/// cannot be made a child.
pub fn link_current_to(trace: &TraceContext) {
    let sc = extract(trace).span().span_context().clone();
    if sc.is_valid() {
        tracing::Span::current().add_link(sc);
    }
}
