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

## Flow logs

CloudWatch holds the process logs; a **flow log** keeps what happened to one event and everything
it caused, wherever it ran. A tenant opts in with `"flow_logs": true` in its `config.json`, and the
deployment names the bucket with `flow_logs` in `integraal.json` ([Deployment](deployment.md)). The
bucket is separate from the data buckets, and scripts cannot reach it.

Each invocation of an opted-in tenant writes one **segment** when it ends: gzipped NDJSON holding
a header (script, trigger, attempt, start time, trace id), the invocation's `native.log` lines,
its outbound `native.http` calls (method, host, status and time, never the path or query), engine
errors, and a footer (outcome, duration, error). A segment is written whatever the outcome,
including timeouts. It keeps the first 192 KB and the last 64 KB of lines and says how many it
dropped in between.

```text
<prefix><tenant>/flows/<first 2 hex of trace id>/<trace id>/<start ms>-<span id>.ndjson.gz
<prefix><tenant>/flow-index/<yyyy>/<mm>/<dd>/<trace id>
```

- The **trace id** is the [flow](#one-trace-per-flow), so all the segments of a flow share a
  folder, however many invocations, hops or days it took. The date is deliberately not in the
  segment path. The two-character shard only keeps listings small.
- The **index entry** is written by the invocation that starts a flow (one that did not continue
  a caller's trace), under the day the flow started. It holds the tenant, script, trigger and
  start time, so "what ran yesterday" is a listing of one folder. A batch job resumed by its
  partner starts a flow of its own, and its entry is how you find it.
- Segments are never appended to, so concurrent invocations of one flow cannot conflict. Expire
  them with an S3 lifecycle rule on the `<tenant>/flows/` and `<tenant>/flow-index/` prefixes.
- A failed segment write is logged as a warning and does not fail the invocation.

Read a flow back, merged in time order, or list the flows of a UTC day:

```bash
integraal-server flow show acme 4bf92f3577b34da6a3ce929d0e0e4736
integraal-server flow list acme 2026-10-25
```

### Local query API

With `"local_api": { "port": 9091 }` in `integraal.json` (needs `flow_logs`), the server also answers
read-only flow queries on `127.0.0.1:<port>`. There is no token: the listener is bound to loopback,
so only processes on the same host (or its container) can connect. From elsewhere, use
`ssh host integraal-client ...` or an SSH tunnel.

| Request | Returns |
| --- | --- |
| `GET /flows/{tenant}?day=yyyy-mm-dd` | the flows that started that UTC day (default today) |
| `GET /flows/{tenant}/{trace}?since=<ms>` | the merged log; `since` keeps only later lines, for tailing |
| `GET /flows/{tenant}/{trace}/summary` | one row per invocation: script, outcome, duration, error, log line count, failed http calls; `outcome` is `null` when the invocation wrote no footer |
| `GET /flows/{tenant}/{trace}/state` | `{"covers": ["timers"], "timers": [...]}`: the flow's pending timers, soonest first, including barrier deadlines |
| `GET /running` | the invocations in flight in this process (tenant, script, trigger, attempt, trace id, span id, `start_ms`, `running_ms`), longest-running first |

Two limits to know when reading these:

- `running` lists only this process's invocations. A flow log segment is written when an invocation
  ends, so a running invocation shows up here and not in `flow`; match them by trace id. Other
  servers and Lambdas have their own lists, and the Lambda has no local API.
- `state` lists only timers. They sit in one sorted partition, so they can be filtered by the trace
  stored with them. Barriers, callbacks and batch jobs are keyed by their own ids, so there is no
  way to ask "which of those belong to this trace" without scanning the table. `covers` says what the
  answer includes; an empty list does not mean nothing is pending.

`integraal-client` is a thin wrapper that prints each response as it came (`INTEGRAAL_URL`
overrides `http://127.0.0.1:9091`):

```bash
integraal-client flows acme 2026-10-25
integraal-client summary acme 4bf92f3577b34da6a3ce929d0e0e4736
integraal-client flow acme 4bf92f3577b34da6a3ce929d0e0e4736 --since 1761390000000
integraal-client state acme 4bf92f3577b34da6a3ce929d0e0e4736
integraal-client running
```

The process needs `s3:ListBucket` and `s3:GetObject` on the flow log bucket for this.

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
