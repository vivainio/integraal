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
  "batch":     { "bucket": "partner-batch", "prefix": "tenants/" },
  "flow_logs": { "bucket": "acme-flow-logs", "prefix": "" },
  "db":        { "table": "integraal" },
  "callbacks": { "base_url": "https://hooks.example.com" },
  "crypto":    { "keys": { "1": "INTEGRAAL_KEY_1" }, "active": 1 },
  "admin":     { "port": 9090, "token_env": "INTEGRAAL_ADMIN_TOKEN" },
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
| `batch.bucket`, `batch.prefix` | server, Lambda | the dedicated bucket behind `native.batch`, kept apart from `buckets.map`; its [S3 notifications](batch.md#wiring-the-continuation) go to the same queue. See [Batch jobs](batch.md#tenant-config-and-deployment) |
| `db.table` | all | DynamoDB table behind `native.db` and `native.callback`; see [DynamoDB](dynamodb.md#deployment) |
| `callbacks.base_url` | server, Lambda | public base URL of the webhook Lambda; enables `native.callback`. Requires `db`; see [Callbacks](callbacks.md#deployment-webhook-lambda) |
| `crypto.keys`, `crypto.active` | server, Lambda | enables `native.crypto`; see [Crypto](crypto.md#deployment-master-keys) |
| `local_api.port` | server | read-only [flow log queries](observability.md#local-query-api) on `127.0.0.1` only, no token. Requires `flow_logs` |
| `admin.port`, `admin.token_env` | server | operator API for the [credential vault](#vault) on its own port. Requires `db` and `crypto` |
| `flow_logs.bucket`, `flow_logs.prefix` | server, Lambda | the dedicated bucket for [flow logs](observability.md#flow-logs) of tenants that enable them; not a data bucket, and scripts cannot reach it. The process needs `s3:PutObject` (and `s3:ListBucket`, `s3:GetObject` to read flows back) |
| `queue.url` | server, webhook, Lambda | the server polls it (and splits [S3 event notifications](triggers.md#s3-notifications) from the `buckets.map` buckets, so it also needs `sqs:SendMessage` to re-send failed records); the webhook Lambda sends to it, and so does [`native.queue.send`](queue.md) for tenants that enable it |
| `schedule.spread_s` | server | seconds over which each [tick](schedules.md#spreading-the-load) spreads its per-tenant messages: `{ "minute": 20, "hour": 300, "day": 900 }` are the defaults |
| `server.port` | server | HTTP port (default `8080`) |
| `throttle` | server | outbound HTTP limits, see [Throttling](http.md#throttling) |
| `ceiling` | server, Lambda | operator ceilings for tenant `timeout_ms` and `memory_mb`; see [Limits](limits.md#deployment-ceiling) |
| `log` | all | `tracing` filter, such as `info` or `integraal_core=debug` (default `info`) |

Apart from the standard AWS SDK variables (credentials, region and `AWS_ENDPOINT_URL`, to point at a
simulator such as roto), the only environment variables are the standard `OTEL_*` ones, which turn on
[OpenTelemetry export](observability.md).

## Credential vault {#vault}

Secrets (API keys, client secrets) that scripts may send in outbound HTTP but never read; see
[`native.http`](http.md#credential-vault). They are stored one row each in the `db.table`
under a partition scripts cannot address (`VAULT#<tenant>`), sealed with a key derived from the
[master keys](#crypto) and bound to the tenant and name. Needs `db` and `crypto`.

Operators manage them through the server's admin API, enabled by `admin`. It listens on its own
port: keep that port out of the load balancer and the security group, and reach it from a
bastion or an internal network. `admin.token_env` names the environment variable holding the
bearer token (at least 24 characters, e.g. `openssl rand -hex 32`).

```sh
curl -X PUT  -H "Authorization: Bearer $TOKEN" -H 'content-type: application/json' \
     -d '{"value":{"live":{"key":"sk_live_..."}},"allow":["https://api.stripe.com/v1/"]}' \
     http://server:9090/admin/vault/acme/stripe
curl         -H "Authorization: Bearer $TOKEN" http://server:9090/admin/vault/acme      # names + allow, no values
curl -X DELETE -H "Authorization: Bearer $TOKEN" http://server:9090/admin/vault/acme/stripe
curl -X POST   -H "Authorization: Bearer $TOKEN" http://server:9090/admin/rekey          # after a master key rotation
```

`allow` (1-16 prefixes, same syntax as `http.allow`) limits where that secret can ever be sent,
independently of the tenant's own allowlist. `"stream_unmasked": true` (default `false`) lets requests using
the secret set `stream = true`; the streamed body is then not scanned for the value, so set it only
when the service at the `allow` URLs never echoes request data, and keep the prefixes narrow. A value is any JSON (not `null`) up to 16 KB, so one secret can hold
several related fields; scripts address a field as `{{vault:name.field.path}}`. Names are 1-64
characters (letters, digits, `-`, `_`; no `.`, which separates the path). Writes and deletes are logged with tenant and name, never the value.

**Rotation.** A small index (`SYS#vault-index`, one row per tenant that has stored a secret) lets
`POST /admin/rekey` re-seal every tenant's entries under the active master key; it is idempotent, so
repeat it after a failure. `POST /admin/vault/{tenant}/rekey` does one tenant. Once it reports
nothing left to re-seal, the old master key can be removed. The index and the per-tenant lists
are read in one query each, so rotation covers up to 999 tenants and 999 secrets per tenant and
fails loudly beyond that.

## Bucket layout

```
<scripts.bucket>/tenants.json                       every tenant, for schedules
<scripts.bucket>/<tenant>/<script>/latest.luau      mutable, revalidated
<scripts.bucket>/<tenant>/<script>/<version>.luau   immutable
<scripts.bucket>/<tenant>/config.json               operator-owned policy
<each buckets.map bucket>/<buckets.prefix><tenant>/...   tenant data, see Buckets
<batch.bucket>/<batch.prefix><tenant>/jobs|state/...      batch jobs, see Batch jobs
```

## Schedules

Three EventBridge Scheduler schedules (`rate(1 minute)`, `rate(1 hour)`, `rate(1 day)`) send ticks to
the queue, and the minute one also fires [timers](schedules.md#timers) such as barrier deadlines. Without
the minute schedule, barrier deadlines never report. The exact inputs, `tenants.json` and the
per-tenant opt-in are in [Schedules and timers](schedules.md). With several worker classes, point the
schedules at the default class's queue: its server handles the tick, and the tenant messages it sends
go to that server's `queue.url`.

```bash
aws scheduler create-schedule --name integraal-minute --schedule-expression 'rate(1 minute)' \
  --flexible-time-window Mode=OFF \
  --target '{"Arn":"arn:aws:sqs:eu-west-1:123456789012:integraal","RoleArn":"arn:aws:iam::123456789012:role/integraal-scheduler",
             "Input":"{\"type\":\"system.tick\",\"every\":\"minute\",\"at\":\"<aws.scheduler.scheduled-time>\"}"}'
```

## IAM

Per-feature permissions are listed in each chapter: [Buckets](buckets.md#deployment),
[DynamoDB](dynamodb.md#deployment), [Callbacks](callbacks.md#deployment-webhook-lambda). In
addition the runtime role needs `s3:GetObject` on the script bucket and, for the server,
`sqs:ReceiveMessage`/`DeleteMessage` on the queue (and `sqs:SendMessage` for ticks and timers). Give tenants no direct access to the script bucket.

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
