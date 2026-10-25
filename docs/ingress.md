# Worker classes and intake

Tenants differ in load. Instead of one pool sized for the heaviest, run **one Fargate service per
size class** (`small`, `large`, ...), each consuming its own **job queue** and started with its own
[`integraal.json`](deployment.md): its `queue.url` and a `ceiling` that matches the task size.

A message can reach a job queue two ways:

```
sender ───────────────────────────────▶ job queue "large"  ──▶ Fargate (large)
sender ──▶ intake queue ──▶ intake Lambda ──▶ job queue "small" ──▶ Fargate (small)
```

- **Directly.** A sender that knows the class (a tenant with a fixed size, the webhook Lambda) just sends
  to that job queue.
- **Through intake.** Everything else is sent to the intake queue. The `integraal-ingress` Lambda reads
  it in large batches, decides each message's class from the tenant's `config.json`, and forwards the
  message to that class's job queue.

Workers are the same binary either way and cannot tell the difference: intake forwards messages
**unchanged**. The one exception is an S3 notification whose records belong to different classes, which
is split into one notification per class.

## Tenant config: `placement`

```json
{
  "placement": {
    "class": "small",
    "classes": ["large"],
    "script": "placement"
  }
}
```

| Field | Meaning |
| --- | --- |
| `class` | The class used when there is no script, or the script fails or answers with nothing valid. Absent: the deployment's `ingress.default`. |
| `classes` | Other classes the script may pick. `class` is always allowed. |
| `script` | Script whose `on_place` (or `place`) handler picks a class per message. |

Tenants cannot write `config.json`, so a tenant can never place itself in a class it was not given. If the
script answers with a class outside `class` and `classes`, the answer is ignored.

## Placement script

```lua
--!strict
--!runtime 1
local function on_place(ctx: Context, event: { size: number?, key: string? })
  if event.size and event.size > 200 * 1024 * 1024 then
    return "large"
  end
  return nil -- the tenant's `class`
end
return { on_place = on_place }
```

- `event` is what the worker's script would receive. For S3 notifications that is `{ store, key, size, etag, event }`.
- Return a class name, `{ class = "large" }`, or `nil` for the tenant's `class`.
- `ctx.trigger == "place"`.
- The script runs under a **stripped sandbox**: it cannot reach stores, the table, the vault or the network.
  Each run gets 500 ms and a 16 MB heap by default (`ingress.place_timeout_ms`,
  `ingress.place_memory_mb`).
- Placement **never fails a message**. A script error, timeout, missing script or invalid answer falls back
  to the tenant's `class`, and is logged. Messages that cannot be read at all go to the default class, where
  the worker's retry and dead-letter handling deals with them.

## Batching and VMs

The intake Lambda is meant to take large SQS batches. Within a batch, messages are grouped by tenant and
each tenant gets **one Luau VM**, so its script and config are loaded and compiled once. Isolation is kept:

- A VM never serves more than one tenant.
- Every message evaluates the script afresh in its own environment, so globals and module-level state from
  earlier messages are gone.
- A VM is discarded after any error or timeout, once its heap is half full, and after 1000 messages.
- A time budget covers the whole batch (the Lambda's remaining time, less a reserve for sending). Messages
  reached after it is spent are placed by the tenant's `class` without running the script.

## Deployment

Intake config (`integraal.json` next to `bootstrap`):

```json
{
  "scripts": { "bucket": "scripts" },
  "stores": { "In": { "type": "s3", "bucket": "data-in", "prefix": "tenants/" } },
  "ingress": {
    "queues": {
      "small": "https://sqs.eu-west-1.amazonaws.com/123/jobs-small",
      "large": "https://sqs.eu-west-1.amazonaws.com/123/jobs-large"
    },
    "default": "small"
  }
}
```

- Point S3 notifications at the intake queue (they cannot choose a queue per tenant).
- Event source mapping on the intake queue: a large `BatchSize`, a `MaximumBatchingWindowInSeconds`, a
  `MaximumConcurrency` that keeps intake from using up the account's Lambda concurrency, and
  `ReportBatchItemFailures` enabled. The function returns the ids of intake messages it could not forward.
- Give each job queue a dead-letter queue, and size its visibility timeout for the class's `ceiling`.
- The `stores` section is needed only so S3 notifications can be mapped to a tenant and a logical store.

## Delivery

Forwarding is **at least once**. If a batch is only partly forwarded, the unsent messages are redelivered,
but messages already forwarded from the same intake message may be forwarded again. Scripts that must not
run twice need their own idempotency (see the [roadmap](roadmap.md)).

## Not implemented

- Re-placing a message up a class after it failed on out-of-memory or deadline errors.
- Per-tenant fairness inside a class (see per-tenant concurrency caps on the [roadmap](roadmap.md)).
- Placement for synchronous HTTP (`POST /v1/{tenant}/...`); route those with the load balancer.
