# integraal

Luau script runtime for AWS Lambda and Fargate. Tenant scripts live in S3, are cached
(ETag-revalidated; pinned versions are immutable) and run in a sandboxed Luau VM with a
versioned native API.

## Triggers
One envelope `{tenant, script, version?, event}`, three entrypoints:
- **Lambda direct invoke** — `crates/lambda` (binary `bootstrap`, arm64 `provided.al2023`)
- **ALB → Fargate** — `POST /v1/{tenant}/{script}` (`x-script-version` header optional), `GET /healthz`
- **SQS → Fargate** — set `queue.url` in the config; message deleted only on success (DLQ handles failures)

A message without `script` is routed by the tenant's `router` script (see docs → Triggers).
Scripts return a table; the handler is `on_invoke` / `on_http` / `on_queue`, falling back to `handle(ctx, event)`.

## Layout in the script bucket
```
<tenant>/<script>/latest.luau     mutable, revalidated
<tenant>/<script>/<version>.luau  immutable, cached forever
<tenant>/config.json              operator-owned policy (tenants cannot write here)
```

## Native API (`--!runtime 1`)
`native.log`, `native.json.{encode,decode}`, `native.time.now_ms`,
`native.bucket.<name>.{get,put,delete,list,open,presign_get,presign_put}` for each bucket in `buckets.map` that the tenant's `config.json` `buckets` block grants (`read`/`write`/`delete`/`readwrite`; `upload`/`download` separately allow `presign_put`/`presign_get`) (confined to `<buckets.prefix><tenant>/`),
`native.db.{get,put,delete,query}` (small items under `TEN#<tenant>#` partition keys, 24h TTL on every write; needs `db.table`),
`native.lock.with` (named mutual exclusion within the process or across processes via the table, with an enforced lease), `native.deadline.with` (time-boxed blocks),
`native.callback.create` (signed single-use callback URLs, served by the webhook Lambda; needs `callbacks.base_url`),
`native.http.{request,get}` (only URLs in `config.json` `http.allow`; no redirects, no private IPs).
`native.batch` (jobs handed to a partner through a dedicated bucket: `new`/`submit`, then `route`/`hydrate` in the continuation; results are multi-part NDJSON checked against a manifest) and `native.ndjson.rows`,
`native.bucket.<name>.open(key)` and `native.http.request{stream=true}` return byte streams; `native.xml.events(string_or_stream, {trim=true})` is a streaming pull parser over them (no size cap beyond 4 GiB; DTDs rejected); `native.zip.entries(string_or_stream)` iterates a zip archive's entries the same way, each openable as a stream; `native.zip.writer(sink)` with `native.bucket.<name>.create(key)` streams a zip into a bucket. `native.gzip` compresses and decompresses the same way. `native.sql.open()` is a scratch SQLite database for the run that can be saved to a bucket and loaded again by a later invocation.

## Config
One JSON file (`--config <path>`, default `./integraal.json`): script/buckets, table, queue, callbacks, throttling, ceilings. See `docs/deployment.md`. Only AWS SDK variables (credentials, region, `AWS_ENDPOINT_URL`) stay in the environment.

## Local testing
Uses [roto](https://github.com/vivainio/roto) as the AWS simulator: see `dev/local.sh`.

## Types
Scripts should use `--!strict`. Definitions for `native` live in `types/native.d.luau`; run `dev/typecheck.sh` (needs `luau-lsp`).
