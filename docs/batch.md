# Batch jobs

`native.batch` hands a unit of work to a partner system through a **dedicated bucket**, and runs a
continuation when the partner is done. There is no connection to the partner and nothing waits: the
invocation that submits the job ends, and the partner's answer arrives later as an S3 notification.

The batch bucket is separate from the data buckets in [`buckets.stores`](buckets.md). Give the partner
access to it and to nothing else.

## Layout

Everything for a tenant sits under `<batch.prefix><tenant>/`:

```
jobs/<job>/request.json          written by the script; the partner reads it
jobs/<job>/inputs/<name>.NNNN    input files the script attached (optional, any number)
jobs/<job>/<files>               result files, written by the partner
jobs/<job>/done.json             written by the partner last: the manifest and the commit marker
state/<job>.json                 written by the script; the partner never needs it
```

Scripts address keys relative to the tenant folder; the partner uses the full keys.

## Script API

```lua
function on_invoke(ctx, ev)
  local job = native.batch.new()                       -- { id = "..." } makes resubmitting idempotent
  job:input_rows("ids", ev.ids)                        -- optional: large inputs as files, not params
  local customers = job:query("customers", "customers_by_id", { input = "ids" })
  job:query("orders", "open_orders", { since = ev.since })
  job:state({ ref = ev.ref })                          -- for the continuation; the partner never sees it
  job:continue_with("settle")                          -- script to run when the partner is done
  return job:submit()                                  -- writes state, then request.json; returns the job id
end
```

| `BatchJob` method | |
| --- | --- |
| `query(name, query, params?)` | names a query for the partner. `name` (letters, digits, `-`, `_`) is how results are found later. At most 64 queries per job |
| `input(name, content, opts?)` | attaches a file: a string or a stream. Calling it again with the same name adds another part. `opts.ext` (default `.ndjson`) is the file extension |
| `input_rows(name, rows, opts?)` | attaches rows from an array or an iterator function, as NDJSON cut into parts of about 64 MiB. The row count is recorded |
| `state(value)` | any JSON value kept for the continuation |
| `continue_with(script)` | the script `native.batch.route` picks for this job's `done.json` |
| `submit()` | writes `state/<job>.json`, then `request.json`. A job needs at least one query and can be submitted once |

Input files are written when you attach them, but are inert until `request.json` exists, because
that is the object the partner reacts to. The queries refer to inputs by name in their params; what
that means is a contract between your scripts and the partner.

## The partner's contract

`request.json`:

```json
{ "v": 1, "job": "9f3c…", "created_ms": 1761300000000,
  "inputs":  { "ids": { "rows": 3, "parts": [ { "key": "inputs/ids.0000.ndjson", "bytes": 27, "sha256": "…" } ] } },
  "queries": [ { "name": "customers", "query": "customers_by_id", "params": { "input": "ids" } } ] }
```

When all queries have been run the partner writes the result files into `jobs/<job>/`, then
**`done.json` last**:

```json
{ "ok": true,
  "results": {
    "customers": { "rows": 120000, "parts": [
        { "key": "customers.0000.ndjson.gz", "bytes": 9100000, "sha256": "…" },
        { "key": "customers.0001.ndjson.gz" } ] },
    "orders":    { "parts": [ { "key": "orders.0000.ndjson.gz" } ] } } }
```

or `{ "ok": false, "error": "partner timeout" }`.

- A result may have **any number of parts**; they are read in the listed order. Part keys are
  relative to the job folder and may not climb out of it.
- Results and inputs are **NDJSON** (one JSON value per line, blank lines skipped), optionally
  gzipped; gzip is detected from the data, not the name. `rows`, `bytes` and `sha256` are optional
  and, when present, are verified.
- `done.json` is the commit marker. Writing it last means the continuation never sees a partial
  result. Every query of the request must have an entry in `results`.
- The format of a result is not declared: whoever opens it knows what it asked for.

## Wiring the continuation

Point an S3 notification (`s3:ObjectCreated:*`, **suffix filter `done.json`**) from the batch
bucket at the same [queue](triggers.md#s3-notifications) as your data buckets. Only `done.json`
should notify, so parts do not each wake a script. The event is routed like any other S3 event, with
`bucket = "$batch"` and a `key` relative to the tenant's folder. `$batch` is not a valid
`buckets.stores` name, so it never collides with a data bucket, and an object in a data bucket named
`jobs/x/done.json` is not a batch event.

The router asks `native.batch.route` which script owns the job:

```lua
local function on_route(_ctx, event)
  local routed = native.batch.route(event)           -- nil unless this is a batch done.json
  if routed then
    return routed.script and { script = routed.script, event = routed.event } or "batch-gone"
  end
  return "default"
end
```

`routed.event` is the original event plus `job`. `routed.script` is `nil` when the job's state is
gone, which is a duplicate delivery after cleanup; send it to a script that does nothing.

## The continuation

```lua
function on_queue(ctx, ev)
  local job = native.batch.hydrate(ev)                -- nil if not complete or already cleaned up
  if not job then return end
  local by_id = {}
  for row in job:rows("customers") do by_id[row.id] = row end
  for row in job:rows("orders") do ... end
  return { delete = job:keys() }                       -- clean up after success
end
```

`hydrate(event, { allow_failure = true })` returns a job with `ok = false` and `error` instead of
raising when the partner reported a failure.

| `BatchResult` | |
| --- | --- |
| `id`, `ok`, `error`, `state` | the job id, the partner's verdict, and the value passed to `state()` |
| `results` | `name -> { rows?, parts }` |
| `rows(name)` | iterator over every part's rows, in order |
| `open(name, part?)` | a [stream](streams-xml.md#streams) of one part (1-based), gunzipped if needed, for non-row data |
| `parts(name)` | number of parts |
| `keys()` | every object of the job, for the handler's `delete =` result |

Reading checks what the manifest promises: a missing part, a size or `sha256` that differs, bad JSON
(the error names the line) and a row count different from `rows` all raise. Integrity is verified when
a part is read to its end. Treat the data as untrusted input regardless.

`delete = job:keys()` runs only after the handler succeeds, so a failed continuation is retried with
the job intact. The engine deletes at most 100 objects per result; for jobs with more files, expire the
`jobs/` and `state/` prefixes with a bucket lifecycle rule.

## Several stages

A continuation may submit another job instead of finishing: copy what it needs into the next
`state()`, `continue_with` the next script, and delete the old job's keys. Use a second stage only
when a later query needs an earlier result; independent queries belong in one job.

## Slow HTTP requests: `job:http` {#http}

A request that may take longer than the invocation can wait is submitted as a job. The invocation
ends, the host performs the request in the background, and the continuation runs when the response
is in.

```lua
function on_invoke(ctx, ev)
  local job = native.batch.new()
  job:http("charge", {                                   -- same options as native.http.request
    method = "POST",
    url = native.config.api_base .. "charges",
    headers = { authorization = "Bearer {{vault:stripe.key}}" },
    body = native.json.encode(ev.order),
    vault = { "header" },
  })
  job:state({ order = ev.order.id })
  job:continue_with("charged")
  return job:submit()
end

function on_queue(ctx, ev)                               -- script "charged"
  local job = native.batch.hydrate(ev, { allow_failure = true })
  if not job then return end
  if not job.ok then error(job.error) end                -- the request could not be completed
  local r = job.results.charge                           -- { status, headers, parts }
  local body = native.json.decode(job:body("charge"))
  return { delete = job:keys() }
end
```

- The request is checked when `job:http` is called: `http.allow`, vault `allow` lists and the
  `max_http_requests` count apply exactly as for `native.http`, so a request that could never be
  sent fails in the submitting script.
- **The request is never written to the bucket**, so vault secrets stay in memory. Only the response
  is: the body as `jobs/<job>/<name>.0000.body`, and status and headers in `done.json`, which is
  written last and triggers the continuation like a partner's `done.json`. Vault values are redacted
  from the response as for `native.http`.
- A response with any status counts as an answer (`ok = true`; check `results.<name>.status`). `ok = false`
  with an `error` means no response arrived: connection failure, timeout (15 minutes per request), or the
  response could not be stored.
- A job holds several `job:http` calls, which run concurrently, and the continuation runs once when
  all are done. A job cannot also have `query`/`input` for a partner.
- [Throttling](http.md#throttling) applies; a request waits for its slot (up to an hour) in the
  background instead of using up a script's time budget. Redirects are not followed. The body is
  capped at `max_http_response_mb`.
- **Fargate only.** The server performs the request in a thread, which Lambda cannot do because the
  sandbox freezes when the invocation returns; there `job:http` raises an error. The request lives
  only in the process: if the task is stopped (SIGTERM on a deploy or scale-in), its running jobs are
  failed at once with `ok = false`; if it dies without warning (a crash, OOM), the job never gets a
  `done.json`. No retry happens either way. Treat the same way as a partner that never answers
  ([Not covered](#not-covered)), and keep the calls idempotent.

## Tenant config and deployment

Tenant `config.json`: `"batch": true` enables `native.batch` for the tenant (otherwise the script gets an
error, not the bucket). Deployment config:

```json
{ "batch": { "bucket": "partner-batch", "prefix": "tenants/" } }
```

`batch.prefix` is empty or ends in `/`. The bucket may not also be an `s3` store. It needs the same
runtime permissions as a data bucket (`GetObject`, `PutObject`, `DeleteObject`, `ListBucket`, multipart
upload for large inputs).

**Partner IAM** is the security boundary, since the partner can write anything it can reach:

| Principal | Access |
| --- | --- |
| partner | read `<prefix>*/jobs/*/request.json` and `inputs/*`; write `<prefix>*/jobs/*`. Nothing under `state/` |
| integraal | the whole bucket |

Keeping `state/` out of the partner's reach is what lets the continuation trust it. Scope the
partner per tenant if tenants must not see each other's jobs.

## Limits

Each bucket call counts against `max_data_ops`; each part read is one. Streams obey
`max_stream_mb` and the invocation deadline. Rows longer than `max_object_mb` are rejected.
`native.ndjson.rows(string_or_stream)` reads any NDJSON the same way outside of jobs.

## Not covered

If the partner never writes `done.json`, nothing runs. Detect that with a periodic script that lists
`jobs/` and compares the age of `request.json` against `done.json`.
