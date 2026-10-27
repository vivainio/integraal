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
| `http.client` | each outbound `ig.http` request: method, host and status only, never the path or query |
| `dynamodb.*`, `s3.*`, `sqs.send` | calls the runtime makes for `ig.db`, stores and `ig.queue` |

## One trace per flow

An event and everything it causes share one trace id, across queue hops. Every message the runtime
sends to SQS (`ig.queue.send`, barrier completions, webhook callbacks, requeued S3 records)
carries the W3C `traceparent` and `tracestate` as SQS message attributes; the intake Lambda
forwards them, and the server continues the trace in its `sqs_message` span. Messages from senders
that set no such attributes start a new trace.

Work that is picked up later also keeps the trace of whoever created it:

- A **barrier** stores the creating invocation's trace, and the continuation message it sends when
  the last member is acked joins that trace, however many invocations acked in between.
- A **callback** (`igp.callback`) stores it too, and the message the webhook enqueues joins it.
- A **batch job** is resumed by the partner's `done.json`, an S3 notification that carries no
  trace. The invocation that routes or hydrates the job therefore starts a new trace and adds a
  span **link** to the submitting invocation's.

The tracer is installed even when no OTLP endpoint is set, so the trace id exists (and is
propagated) without a collector.

`ig.log` lines are ordinary log events inside the invocation's span, so each carries the
invocation's trace.

## Flow logs

CloudWatch holds the process logs; a **flow log** keeps what happened to one event and everything
it caused, wherever it ran. A tenant opts in with `"flow_logs": true` in its `config.json`, and the
deployment names the bucket with `flow_logs` in `integraal.json` ([Deployment](deployment.md)). The
bucket is separate from the data stores, and scripts cannot reach it.

Each invocation of an opted-in tenant writes one **segment** when it ends: gzipped NDJSON holding
a header (script, trigger, attempt, start time, trace id), the invocation's `ig.log` lines,
its outbound `ig.http` calls (method, host, status and time, never the path or query), engine
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
| `GET /flows/{tenant}/{trace}/state` | what the flow is waiting on: `timers` (including barrier deadlines), `barriers` (members `total` and `pending`, `due_s`, `then`), `callbacks` (`expires_s`) and `batch` jobs (`done` once the partner's `done.json` exists) |
| `GET /debug/{tenant}/{trace}` | the [debug captures](#debug-captures) of the flow's watched invocations (needs `debug`) |
| `GET /running` | the invocations in flight in this process (tenant, script, trigger, attempt, trace id, span id, `start_ms`, `running_ms`), longest-running first |

Two limits to know when reading these:

- `running` lists only this process's invocations. A flow log segment is written when an invocation
  ends, so a running invocation shows up here and not in `flow`; match them by trace id. Other
  servers and Lambdas have their own lists, and the Lambda has no local API.
- `state` finds barriers, callbacks and batch jobs through an index row (`SYS#trace#<trace id>`) that
  creating them writes next to the work itself. The row is best effort and has no DynamoDB TTL, so
  a barrier or callback whose row could not be written is missing from the answer, and `state` lists
  only work that still exists (a taken callback or an expired barrier is gone). Batch jobs are
  indexed only for tenants that also have `db`, and only when the job is submitted. `covers` names
  what the answer includes.

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

### Script failures

An invocation that fails writes a structured `failure` into its segment footer, whether or not the
tenant is watched: `kind` (`syntax`, `runtime`, `native`, `timeout`, `memory`, `bad_result`), the
`message` and the `frames` (`chunk`, `line`, `function`) from the innermost call out. A timeout
reports where the script was when its time ran out; a memory failure has no frames, because Luau
does not run the error handler for it. `summary` shows each row's `failure.kind` and top frame.

The failure never holds values. A table thrown with `error({...})` is described by its keys only.

## Task records

When `flow_logs` is configured, every invocation of every tenant also leaves one **task record**, in
the same bucket. A tenant does not opt in. Where flow logs are for reading one flow in depth, task
records are for seeing what happened across all of them: a line per task, with the outcome, the
duration and whatever the script attached.

```text
<prefix>tasks/<yyyy>/<mm>/<dd>/<hhmm>/<process>-<flush ms>-<seq>.ndjson.gz
```

A server buffers records and writes one object every `flow_logs.task_flush_secs` (60 by default), or sooner if the buffer reaches 5 MB, and
once more on shutdown. A Lambda writes one object per invocation, since its environment can be
frozen as soon as the handler returns. The date and minute in the key are those of the write, not
of the tasks inside; to find tasks by start time, list the minutes around the window and filter on
`start_ms`. Nothing is sampled: a failed write is logged as a warning and loses that object's
records, but never fails the invocation. Expire the objects with an S3 lifecycle rule on
`<prefix>tasks/`.

Each line is a flat JSON object. Only `metrics`, `attrs` and `detail` are nested, so the records map
onto columns if you compact them into a columnar format later.

| Field | Meaning |
| --- | --- |
| `v` | format version, `1` |
| `tenant`, `script`, `version`, `trigger`, `attempt` | what ran. `script` is the script that really ran: the one a router picked, or the router itself if it failed first. `routed` says the invocation named no script |
| `trace_id`, `span_id`, `continues_trace` | the [flow](#one-trace-per-flow) the task belongs to, and whether the task continued it or started it |
| `start_ms`, `duration_ms`, `outcome`, `error` | `outcome` is `ok`, `timeout`, `script_error` or `error`; `error` is cut at 500 bytes |
| `failure_kind`, `failure_frame` | the [structured failure](#script-failures): its kind, and the innermost script frame as `chunk:line function` |
| `http_requests`, `http_failed` | outbound `ig.http` calls, and those with no response or a status outside 2xx and 3xx |
| `log_lines`, `log_errors` | `ig.log` lines, and those at level `error` |
| `title`, `metrics`, `attrs`, `detail`, `truncated` | what the script attached; see below |

### `ig.task`

A script describes its own task with `ig.task`. The functions can be called any number of times
and in any order, and the record keeps whatever was set before a timeout or an error, so set things
as you learn them instead of at the end.

```lua
ig.task.title("Imported orders from acme-eu")
ig.task.metric("orders.rows", 1432)     -- a number; setting it again replaces it
ig.task.add("orders.skipped")           -- adds 1, or the given delta, to a metric
ig.task.attr("region", "eu")            -- a short string you can filter on
ig.task.detail({ skipped = { "a", "b" } })
```

| Function | Limit |
| --- | --- |
| `title(text)` | 200 bytes |
| `metric(name, number)`, `add(name, delta?)` | 32 metrics; names up to 64 bytes; numbers must be finite |
| `attr(name, text)` | 32 attributes; names up to 64 bytes, values up to 256 |
| `detail(value)` | any JSON value, up to 8 KB serialized |

A value over a limit is cut or dropped and the record says `"truncated": true`; the call never fails
the task. A `detail` value that is not JSON raises an error in the script, as `ig.json.encode`
would. Use dotted names such as `orders.rows` to group metrics and attributes.

Scripts can put any data in these fields, unlike flow logs, which hold none. Read the bucket's
`tasks/` prefix with an identity that is allowed to see tenant data.

## Debug captures

For a script you are debugging, a tenant can ask for much more than a flow log keeps. Add a `debug`
section to its `config.json`:

```json
"debug": { "until": "2026-11-01T00:00:00Z", "scripts": ["feed", "order_*"] }
```

| Field | Meaning |
| --- | --- |
| `until` | UTC time after which the section is ignored. Required, so debugging cannot be left on |
| `scripts` | scripts to watch; a trailing `*` matches a prefix. Left out, the whole tenant is watched: every invocation, the script a router picks, and the libs it requires. A routed invocation is matched by its router's name |
| `capture` | `coverage`, `profile`, `event`. Everything when left out |

Each watched invocation writes one capture object when it ends, to the deployment's debug sink
(`debug` in `integraal.json`, see [Deployment](deployment.md)): a directory, a bucket, or both. The
sink is never the flow log bucket. A capture can hold tenant data, which flow logs must not, so
reading captures takes separate access.

```text
<prefix><tenant>/captures/<trace id>/<start ms>-<span id>.json.gz
```

A capture holds the header (tenant, script, trace and span id, trigger, attempt, outcome, error)
and:

- `event`: the input event.
- `failure`: the structured failure above.
- `coverage`: per chunk (the script and each lib), `lines` with hit counts of every line that has
  code, `missed` (lines that never ran), `covered` and `executable`. Counts accumulate over a routed
  run, since the router and its target share one VM.
- `profile`: a sampling profile. The VM is sampled at most once per millisecond from its interrupt;
  `hot` lists the busiest leaf lines with milliseconds, and `folded` holds the stacks in the
  collapsed format flame graph tools read (`a;b;c <ms>`). The interrupt runs at calls and loop
  back-edges, not on a timer, so time inside a native call (an HTTP wait) is credited to the stack
  at the next interrupt: approximate, but it points at the right line.

Coverage compiles the watched script with counters, and the profiler samples the stack, so a watched
invocation runs slower. Tenants without a `debug` section pay nothing.

Read captures with the server (it needs the sink's credentials) or the local API:

```bash
integraal-server debug show acme 4bf92f3577b34da6a3ce929d0e0e4736
integraal-client debug acme 4bf92f3577b34da6a3ce929d0e0e4736
```

Directory captures are deleted after `retention_hours` (default 48) by an hourly sweep. Expire
bucket captures with a lifecycle rule on `<prefix><tenant>/captures/`. The Lambda can use
`debug.bucket` only (it needs `s3:PutObject`); a `debug.dir` refuses to start there.

## Dead-letter inspection and replay

The [operator Luau API](ops.md) provides `ops.flow.inspect`, `ops.dlq.inspect` and `ops.dlq.replay`
from directly invoked operator scripts. Replay preserves message bodies and trace attributes,
shows original failures when retained flow logs are available, and saves an S3 audit before
sending and deleting. See that chapter for configuration and at-least-once recovery semantics.

## Metrics

| Metric | Labels |
| --- | --- |
| `integraal.invocations`, `integraal.invocation.duration` | `tenant`, `trigger`, `outcome` (`ok`, `timeout`, `script_error`, `error`) |
| `integraal.http.client.requests`, `integraal.http.client.duration` | method, host, status (`0` when there was no response) |
| `integraal.http.throttle.wait` | |
| `integraal.loader.lookups` | `result`: `hit`, `revalidated`, `miss` |
| `integraal.sqs.messages` | `disposition`: `acked`, `retry`, `requeued` |
| `integraal.fs.bytes` | `tenant`, `dir`, `direction` (`read`, `write`): bytes moved through [whitelisted directories](filesystem.md) |
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
