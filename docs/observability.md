# Observability

Every binary (server, Lambda, webhook, intake) writes JSON logs to stdout. It can also export
**traces** and **metrics** over OTLP/HTTP (protobuf).

Export is off unless the environment names a collector, and it is configured only with the
standard OpenTelemetry variables, not `integraal.json`:

| Variable | Effect |
| --- | --- |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | collector base URL, such as `http://localhost:4318`; setting it (or the `_TRACES_` / `_METRICS_` variants) enables export |
| `OTEL_EXPORTER_OTLP_HEADERS` | extra headers, usually an API key for a hosted backend |
| `OTEL_SERVICE_NAME` | overrides the default `integraal-server`, `integraal-lambda`, `integraal-webhook` or `integraal-ingress` |
| `OTEL_TRACES_SAMPLER`, `OTEL_TRACES_SAMPLER_ARG` | sampling |
| `OTEL_SDK_DISABLED=true` | turns export off again |

The `log` setting still filters spans and logs. A span below that level is not exported.

## Traces

| Span | From |
| --- | --- |
| `http_request` | each request to the server; continues the caller's W3C `traceparent` header |
| `sqs_message` | each queue message the server handles |
| `invoke` | each script invocation (`tenant`, `script`, `trigger`, `outcome`) |
| `place_batch`, `place_tenant` | intake placement |
| `http.client` | each outbound `native.http` request: method, host and status only, never the path or query |
| `dynamodb.*`, `s3.*`, `sqs.send` | calls the runtime makes for `native.db`, buckets and `native.queue` |

## One trace per flow

An event and everything it causes share one trace id, across queue hops. Every message the runtime
sends to SQS (`native.queue.send`, barrier completions, webhook callbacks, requeued S3 records)
carries the W3C `traceparent` and `tracestate` as SQS message attributes; the intake Lambda
forwards them, and the server continues the trace in its `sqs_message` span. Messages from senders
that set no such attributes start a new trace.

Work that is picked up later also keeps the trace of whoever created it:

- A **barrier** stores the creating invocation's trace, and the continuation message it sends when
  the last member is acked joins that trace, however many invocations acked in between.
- A **callback** (`native.callback`) stores it too, and the message the webhook enqueues joins it.
- A **batch job** is resumed by the partner's `done.json`, an S3 notification that carries no
  trace. The invocation that routes or hydrates the job therefore starts a new trace and adds a
  span **link** to the submitting invocation's.

The tracer is installed even when no OTLP endpoint is set, so the trace id exists (and is
propagated) without a collector.

`native.log` lines are ordinary log events inside the invocation's span, so each carries the
invocation's trace.

## Metrics

| Metric | Labels |
| --- | --- |
| `integraal.invocations`, `integraal.invocation.duration` | `tenant`, `trigger`, `outcome` (`ok`, `timeout`, `script_error`, `error`) |
| `integraal.http.client.requests`, `integraal.http.client.duration` | method, host, status (`0` when there was no response) |
| `integraal.http.throttle.wait` | |
| `integraal.loader.lookups` | `result`: `hit`, `revalidated`, `miss` |
| `integraal.sqs.messages` | `disposition`: `acked`, `retry`, `requeued` |
| `integraal.placement.fallbacks` | |

`tenant` is the only label without a fixed set of values.

## Lambda

Spans and metrics are flushed at the end of every invocation, because the execution environment
can be frozen as soon as the handler returns. This adds the export round trip to each invocation.
Point `OTEL_EXPORTER_OTLP_ENDPOINT` at a collector close to the function.

## Trying it locally

```bash
docker run --rm -p 16686:16686 -p 4318:4318 jaegertracing/all-in-one
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318 dev/local.sh
```

Then open <http://localhost:16686> after sending a request.
