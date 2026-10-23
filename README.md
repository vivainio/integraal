# integraal

Luau script runtime for AWS Lambda and Fargate. Tenant scripts live in S3, are cached
(ETag-revalidated; pinned versions are immutable) and run in a sandboxed Luau VM with a
versioned native API.

## Triggers
One envelope `{tenant, script, version?, event}`, three entrypoints:
- **Lambda direct invoke** — `crates/lambda` (binary `bootstrap`, arm64 `provided.al2023`)
- **ALB → Fargate** — `POST /v1/{tenant}/{script}` (`x-script-version` header optional), `GET /healthz`
- **SQS → Fargate** — set `SQS_QUEUE_URL`; message deleted only on success (DLQ handles failures)

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
`native.data.<bucket>.{get,put,delete,list,open,presign_get,presign_put}` for each bucket in `DATA_BUCKETS` that the tenant's `config.json` `data` block grants (confined to `DATA_PREFIX<tenant>/`; `in` is a Luau keyword, so write `native.data["in"]`),
`native.db.{get,put,delete,query}` (small items under `TEN#<tenant>#` partition keys, 24h TTL on every write; needs `DB_TABLE`),
`native.callback.create` (signed single-use callback URLs, served by the webhook Lambda; needs `CALLBACK_BASE_URL`),
`native.http.{request,get}` (only URLs in `config.json` `http.allow`; no redirects, no private IPs).
`native.data.<bucket>.open(key)` and `native.http.request{stream=true}` return byte streams; `native.xml.events(string_or_stream, {trim=true})` is a streaming pull parser over them (no size cap beyond 4 GiB; DTDs rejected).

## Config
`SCRIPT_BUCKET`, `SCRIPT_PREFIX`, `DATA_BUCKETS`, `DATA_PREFIX`, `DB_TABLE`, `CALLBACK_BASE_URL`, `SQS_QUEUE_URL`, `PORT`, `AWS_ENDPOINT_URL`.

## Local testing
Uses [roto](https://github.com/vivainio/roto) as the AWS simulator: see `dev/local.sh`.

## Types
Scripts should use `--!strict`. Definitions for `native` live in `types/native.d.luau`; run `dev/typecheck.sh` (needs `luau-lsp`).
