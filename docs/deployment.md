# Deployment

## Configuration

All deployment settings live in one JSON file, not in environment variables. Each binary reads
`--config <path>` if given, else `integraal.json` in its working directory. For Lambda, put
`integraal.json` in the zip next to `bootstrap`. Unknown keys are rejected and a bad file stops the
process at startup.

```json
{
  "scripts":   { "bucket": "scripts", "prefix": "" },
  "buckets":   { "prefix": "tenants/", "map": { "In": "acme-in", "Out": "acme-out", "Backup": "acme-bak", "Temp": "acme-tmp" } },
  "db":        { "table": "integraal" },
  "callbacks": { "base_url": "https://hooks.example.com" },
  "crypto":    { "keys": { "1": "INTEGRAAL_KEY_1" }, "active": 1 },
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
| `buckets.map` | server, Lambda | named buckets (case-sensitive letters, digits and `_`; by convention `In`, `Out`, `Backup`, `Temp`). Enables `native.bucket.<name>`; see [Buckets](buckets.md#deployment) |
| `buckets.prefix` | server, Lambda | base prefix applied in every bucket; empty or ending in `/`. Tenant data lives at `<prefix><tenant>/` |
| `db.table` | all | DynamoDB table behind `native.db` and `native.callback`; see [DynamoDB](dynamodb.md#deployment) |
| `callbacks.base_url` | server, Lambda | public base URL of the webhook Lambda; enables `native.callback`. Requires `db`; see [Callbacks](callbacks.md#deployment-webhook-lambda) |
| `crypto.keys`, `crypto.active` | server, Lambda | enables `native.crypto`; see [Crypto](crypto.md#deployment-master-keys) |
| `queue.url` | server, webhook | the server polls it (and splits [S3 event notifications](triggers.md#s3-notifications) from the `buckets.map` buckets, so it also needs `sqs:SendMessage` to re-send failed records); the webhook Lambda sends to it |
| `server.port` | server | HTTP port (default `8080`) |
| `throttle` | server | outbound HTTP limits, see [Throttling](http.md#throttling) |
| `ceiling` | server, Lambda | operator ceilings for tenant `timeout_ms` and `memory_mb`; see [Limits](limits.md#deployment-ceiling) |
| `log` | all | `tracing` filter, such as `info` or `integraal_core=debug` (default `info`) |

Only the standard AWS SDK variables remain in the environment: credentials, region and
`AWS_ENDPOINT_URL` (to point at a simulator such as roto).

## Bucket layout

```
<scripts.bucket>/<tenant>/<script>/latest.luau      mutable, revalidated
<scripts.bucket>/<tenant>/<script>/<version>.luau   immutable
<scripts.bucket>/<tenant>/config.json               operator-owned policy
<each buckets.map bucket>/<buckets.prefix><tenant>/...   tenant data, see Buckets
```

## IAM

Per-feature permissions are listed in each chapter: [Buckets](buckets.md#deployment),
[DynamoDB](dynamodb.md#deployment), [Callbacks](callbacks.md#deployment-webhook-lambda). In
addition the runtime role needs `s3:GetObject` on the script bucket and, for the server,
`sqs:ReceiveMessage`/`DeleteMessage` on the queue. Give tenants no direct access to the script bucket.

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
