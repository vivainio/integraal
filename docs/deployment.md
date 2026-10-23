# Deployment

## Configuration

| Variable | Used by | Meaning |
| --- | --- | --- |
| `SCRIPT_BUCKET` | both | bucket holding scripts and `config.json` (required) |
| `SCRIPT_PREFIX` | both | optional key prefix, concatenated as-is (end it with `/`) |
| `DATA_BUCKETS` | both | named data buckets, `name=bucket,name=bucket` (e.g. `In=acme-in,Out=acme-out,Backup=acme-bak,Temp=acme-tmp`); names are case-sensitive letters, digits and `_`. Enables `native.data.<name>` |
| `DATA_PREFIX` | both | base prefix applied in every data bucket; must end with `/`. Tenant data lives at `<DATA_PREFIX><tenant>/` |
| `DB_TABLE` | both | enables `native.db` (DynamoDB table, see below) |
| `CALLBACK_BASE_URL` | both | public base URL of the webhook Lambda; enables `native.callback` |
| `THROTTLE` | server | JSON array of outbound HTTP limits, see [Throttling](#throttling) |
| `SQS_QUEUE_URL` | server | enables the SQS poller, which also splits [S3 event notifications](triggers.md#s3-notifications) from the `DATA_BUCKETS` buckets; it needs `sqs:SendMessage` too, to re-send failed records |
| `PORT` | server | HTTP port (default `8080`) |
| `MAX_TIMEOUT_MS` / `MAX_MEMORY_MB` | both | operator ceilings for tenant limits |
| `AWS_ENDPOINT_URL` | both | point the SDK at a simulator such as roto |
| `RUST_LOG` | both | log filter |

## Bucket layout

```
<SCRIPT_BUCKET>/<tenant>/<script>/latest.luau      mutable, revalidated
<SCRIPT_BUCKET>/<tenant>/<script>/<version>.luau   immutable
<SCRIPT_BUCKET>/<tenant>/config.json               operator-owned policy
<each DATA_BUCKETS bucket>/<DATA_PREFIX><tenant>/...   tenant data
```

## IAM

- Runtime role: `s3:GetObject` on the script bucket; `s3:GetObject`, `PutObject`, `DeleteObject`
  and `ListBucket` on each data bucket (scope per bucket, e.g. no `DeleteObject` on `backup`; presigned URLs are signed with this role's credentials, so they only work for operations it allows); `sqs:ReceiveMessage`/`DeleteMessage` on the queue (server).
- `dynamodb:GetItem`, `PutItem`, `DeleteItem` (conditional writes included) and `Query` on the `DB_TABLE` table.
- Give tenants no direct access to the script bucket.

## DynamoDB table
`native.db` needs one shared table with string partition key `pk`, string sort key `sk`, and
**TTL enabled on the attribute `ttl`**. Items hold the value in `v`. All tenants share the table;
isolation comes from the enforced `TEN#<tenant>#` partition key prefix.

## Throttling
Set `THROTTLE` on the Fargate server to limit outbound `native.http` calls to chosen URLs, for all
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
  target limit by the task count. They are not applied on Lambda (the variable is not read there).
- Malformed rules stop the server at startup.

## Webhook Lambda
`integraal-webhook` (binary `bootstrap`, behind a Function URL) receives `POST /cb/<tenant>/<token>`,
verifies and consumes the callback, and sends it to `SQS_QUEUE_URL` for the Fargate poller. It runs
no scripts. Env: `DB_TABLE`, `SQS_QUEUE_URL`. IAM: `dynamodb:GetItem`/`PutItem`/`DeleteItem` on the
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
check `GET /healthz`). Add `SQS_QUEUE_URL` to have the same task consume a queue.

!!! note
    Infrastructure-as-code (CDK/Terraform) is not part of the repository yet.
