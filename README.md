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
<tenant>/lib/<path>.luau          tenant modules, `require("@lib/<path>")`
@shared/<name>/<version>.luau     global versioned modules, `require("@shared/<name>@<version>")` (operator-owned)
<tenant>/config.json              operator-owned policy (tenants cannot write here)
```

## Native API (`--!runtime 1`)
Scripts get a global `native` with a versioned, tenant-confined API:
- **Storage:** S3-style stores with presigned URLs, whitelisted host directories, DynamoDB items, SQLite scratch databases
- **Coordination:** locks, deadlines, barriers, queue sends, signed single-use callback URLs
- **Batch jobs:** hand work to a partner via a dedicated bucket, with NDJSON results checked against a manifest
- **I/O:** outbound HTTP (allow-listed), streaming XML / zip / gzip, crypto, JSON, logging, task records

Full reference: [`docs/native-api.md`](docs/native-api.md).

## Config
One JSON file (`--config <path>`, default `./integraal.json`): script bucket, stores, table, queue, callbacks, throttling, ceilings. See `docs/deployment.md`. Only AWS SDK variables (credentials, region, `AWS_ENDPOINT_URL`) stay in the environment.

## Local testing
Uses [roto](https://github.com/vivainio/roto) as the AWS simulator: see `dev/local.sh`.

## Types
Scripts should use `--!strict`. Definitions for `native` live in `types/native.d.luau`; run `dev/typecheck.sh` (needs `luau-lsp`).
