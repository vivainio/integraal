# Deployment

## Configuration

| Variable | Used by | Meaning |
| --- | --- | --- |
| `SCRIPT_BUCKET` | both | bucket holding scripts and `config.json` (required) |
| `SCRIPT_PREFIX` | both | optional key prefix, concatenated as-is (end it with `/`) |
| `DATA_BUCKET` | both | enables `native.data` |
| `DATA_PREFIX` | both | base prefix; must end with `/`. Tenant data lives at `<DATA_PREFIX><tenant>/` |
| `DB_TABLE` | both | enables `native.db` (DynamoDB table, see below) |
| `SQS_QUEUE_URL` | server | enables the SQS poller |
| `PORT` | server | HTTP port (default `8080`) |
| `MAX_TIMEOUT_MS` / `MAX_MEMORY_MB` | both | operator ceilings for tenant limits |
| `AWS_ENDPOINT_URL` | both | point the SDK at a simulator such as roto |
| `RUST_LOG` | both | log filter |

## Bucket layout

```
<SCRIPT_BUCKET>/<tenant>/<script>/latest.luau      mutable, revalidated
<SCRIPT_BUCKET>/<tenant>/<script>/<version>.luau   immutable
<SCRIPT_BUCKET>/<tenant>/config.json               operator-owned policy
<DATA_BUCKET>/<DATA_PREFIX><tenant>/...            tenant data
```

## IAM

- Runtime role: `s3:GetObject` on the script bucket; `s3:GetObject`, `PutObject`, `DeleteObject`
  and `ListBucket` on the data bucket; `sqs:ReceiveMessage`/`DeleteMessage` on the queue (server).
- `dynamodb:GetItem`, `PutItem`, `DeleteItem` and `Query` on the `DB_TABLE` table.
- Give tenants no direct access to the script bucket.

## DynamoDB table
`native.db` needs one shared table with string partition key `pk`, string sort key `sk`, and
**TTL enabled on the attribute `ttl`**. Items hold the value in `v`. All tenants share the table;
isolation comes from the enforced `TEN#<tenant>#` partition key prefix.

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
