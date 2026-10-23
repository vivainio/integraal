# Deployment

## Configuration

All deployment settings live in one JSON file, not in environment variables. Each binary reads
`--config <path>` if given, else `integraal.json` in its working directory. For Lambda, put
`integraal.json` in the zip next to `bootstrap`. Unknown keys are rejected and a bad file stops the
process at startup.

```json
{
  "scripts":   { "bucket": "scripts", "prefix": "" },
  "data":      { "prefix": "tenants/", "buckets": { "In": "acme-in", "Out": "acme-out", "Backup": "acme-bak", "Temp": "acme-tmp" } },
  "db":        { "table": "integraal" },
  "callbacks": { "base_url": "https://hooks.example.com" },
  "queue":     { "url": "https://sqs.eu-west-1.amazonaws.com/123456789012/integraal" },
  "server":    { "port": 8080 },
  "throttle":  [{ "url": "https://api.partner.com/v1/", "max_concurrent": 2, "per_second": 5 }],
  "ceiling":   { "timeout_ms": 60000, "memory_mb": 256 },
  "log":       "info"
}
```

| Key | Used by | Meaning |
| --- | --- | --- |
| `scripts.bucket` | all (required) | bucket holding scripts and tenant `config.json` files |
| `scripts.prefix` | all | key prefix, concatenated as-is (end it with `/`) |
| `data.buckets` | server, Lambda | named data buckets (case-sensitive letters, digits and `_`; by convention `In`, `Out`, `Backup`, `Temp`). Enables `native.data.<name>` |
| `data.prefix` | server, Lambda | base prefix applied in every data bucket; empty or ending in `/`. Tenant data lives at `<prefix><tenant>/` |
| `db.table` | all | DynamoDB table behind `native.db` and `native.callback` (see below) |
| `callbacks.base_url` | server, Lambda | public base URL of the webhook Lambda; enables `native.callback`. Requires `db` |
| `queue.url` | server, webhook | the server polls it (and splits [S3 event notifications](triggers.md#s3-notifications) from the `data.buckets` buckets, so it also needs `sqs:SendMessage` to re-send failed records); the webhook Lambda sends to it |
| `server.port` | server | HTTP port (default `8080`) |
| `throttle` | server | outbound HTTP limits, see [Throttling](#throttling) |
| `ceiling` | server, Lambda | operator ceilings for tenant `timeout_ms` and `memory_mb` |
| `log` | all | `tracing` filter, such as `info` or `integraal_core=debug` (default `info`) |

Only the standard AWS SDK variables remain in the environment: credentials, region and
`AWS_ENDPOINT_URL` (to point at a simulator such as roto).

## Bucket layout

```
<scripts.bucket>/<tenant>/<script>/latest.luau      mutable, revalidated
<scripts.bucket>/<tenant>/<script>/<version>.luau   immutable
<scripts.bucket>/<tenant>/config.json               operator-owned policy
<each data.buckets bucket>/<data.prefix><tenant>/...   tenant data
```

## IAM

- Runtime role: `s3:GetObject` on the script bucket; `s3:GetObject`, `PutObject`, `DeleteObject`
  and `ListBucket` on each data bucket (scope per bucket, e.g. no `DeleteObject` on `backup`; presigned URLs are signed with this role's credentials, so they only work for operations it allows); `sqs:ReceiveMessage`/`DeleteMessage` on the queue (server).
- `dynamodb:GetItem`, `PutItem`, `DeleteItem` (conditional writes included) and `Query` on the `db.table` table.
- Give tenants no direct access to the script bucket.

## DynamoDB table
`native.db` needs one shared table with string partition key `pk`, string sort key `sk`, and
**TTL enabled on the attribute `ttl`**. Items hold the value in `v`. All tenants share the table;
isolation comes from the enforced `TEN#<tenant>#` partition key prefix.

## Throttling
Add a `throttle` array to the server's config to limit outbound `native.http` calls to chosen URLs, for all
tenants together:

```json
[{ "url": "https://api.partner.com/v1/", "max_concurrent": 2, "per_second": 5 }]
```

- `url` matches like an `http.allow` entry (scheme, host, port, path prefix; `*.` for subdomains).
  A call matching several rules must satisfy all of them.
- `max_concurrent` caps requests in flight; a streamed response counts until its stream is closed.
- `per_second` spaces request starts evenly (`0.5` means one every two seconds).
- A throttled call **waits for its slot**. If no slot can be had before the invocation deadline,
  the call fails at once with a `throttled: ...` error (catch it with `pcall`). Waiting uses up the
  script's time budget, so size `timeout_ms` accordingly.
- Limits apply **per process**: with several Fargate tasks each enforces its own, so divide the
  target limit by the task count. They are not applied on Lambda (the Lambda entrypoints ignore the key).
- Malformed rules stop the server at startup.

## Webhook Lambda
`integraal-webhook` (binary `bootstrap`, behind a Function URL) receives `POST /cb/<tenant>/<token>`,
verifies and consumes the callback, and sends it to `queue.url` for the Fargate poller. It runs
no scripts. Config: its `db` and `queue` sections. IAM: `dynamodb:GetItem`/`PutItem`/`DeleteItem` on the
table and `sqs:SendMessage` on the queue. Replies: 202 queued, 404 invalid/unknown/used, 410 expired,
413 body over 200 KB, 500 enqueue failed (the callback is restored so the sender can retry).

## Lambda

Build the `bootstrap` binary for arm64 and use the `provided.al2023` runtime:

```bash
cargo install cargo-lambda
cargo lambda build --release --arm64 -p integraal-lambda
```

## Fargate

Build `integraal-server` into a container image and run it behind an ALB (target group health
check `GET /healthz`). Add `queue.url` to have the same task consume a queue.

!!! note
    Infrastructure-as-code (CDK/Terraform) is not part of the repository yet.
