# Operator API (`ops`)

Operator scripts import `require("ops")` (alias for `require("@native/ops@1")`), alongside the
core API and imported patterns. Invoke these scripts directly through Lambda with a normal
`{tenant, script, version?, event}` envelope. No separate admin HTTP endpoint is needed.

```lua
--!strict
--!runtime 1
local ops = require("ops")
return { handle = function(ctx: Context, event: any)
    if event.action == "pause" then
        return ops.fleet.pause("deployment starting")
    elseif event.action == "resume" then
        return ops.fleet.resume("deployment complete")
    end
    return ops.fleet.status()
end }
```

Enable the API in deployment config. All workers and the operator Lambda must use the same
DynamoDB table. Only direct invocations of `operator_tenant` can import `ops`; HTTP, queue and
other tenant invocations cannot. Keep this tenant's scripts operator-owned and restrict direct
invocation of the operator Lambda with IAM. The runtime does not infer the caller's IAM identity.

```json
"db": { "table": "integraal" },
"operations": {
  "operator_tenant": "operator",
  "queues": {
    "jobs": { "dlq_url": "https://sqs.us-east-1.amazonaws.com/123/jobs-dlq",
              "destination_url": "https://sqs.us-east-1.amazonaws.com/123/jobs" }
  },
  "audit_bucket": "integraal-operator-audit",
  "audit_prefix": "replays/"
}
```

The audit bucket must differ from scripts, data, batch, debug and flow logs. Queue aliases and
queue destinations are deployment-owned. `queues` can be empty for pause-only deployments.
The API has a 100-command limit per script invocation and shares its invocation deadline.

## Fleet and tenant pause

| Function | Result |
| --- | --- |
| `ops.fleet.pause(reason)` | Save a fleet-wide SQS admission pause; return control state |
| `ops.fleet.resume(reason)` | Clear the fleet pause, preserving independent tenant pauses |
| `ops.fleet.status()` | Read current desired control state consistently from DynamoDB |
| `ops.tenant.pause(tenant, reason)` | Pause SQS processing for this tenant |
| `ops.tenant.resume(tenant, reason)` | Clear this tenant's pause; a fleet pause still applies |

Reasons are required (1–1024 bytes). The state contains `fleet`, `tenants` (a list), `revision`,
`changed_at_ms`, `request_id`, and the latest `reason`. It lives in one non-expiring DynamoDB slot:
partition key `SYS#OPS`, sort key `queue-pause`. Conditional updates prevent concurrent commands
from losing each other's tenant changes. Tenant-scoped `ig.db` cannot address this partition.
The slot keeps the latest control change, not an append-only history of pause commands.

Fargate workers poll every **10 seconds** using consistent reads, including before their first
receive. A fleet pause stops new SQS receives. An outstanding long poll can finish, but its messages
are held if the new state has been observed. Already running scripts finish normally. Direct
invokes and HTTP remain available for maintenance. Queue-delivered operator ticks also wait during
a fleet pause; direct ticks can still perform their normal recovery work.

A tenant pause on a shared queue requires receiving messages to identify their tenant. The worker
holds those receipts and renews five-minute visibility leases every 10 seconds, without deleting,
re-sending or repeatedly receiving them. Mixed-tenant S3 notifications wait intact if any record
belongs to a paused tenant. Each worker holds at most 100 messages; at that cap it stops receiving
until space returns. Resumed receipts have priority over new work. Shared queues can therefore
experience backpressure from a large paused tenant backlog; use separate queues for strict isolation.

A failed control read stops admission until a successful read; held leases continue to renew.
State changes are eventually observed, not a simultaneous fleet barrier: allow at least a polling
interval plus any outstanding work to drain before updating a tenant. `status()` reports desired
state, not acknowledgements from every worker. Use running-invocation inspection to check draining.

This is for bounded maintenance windows. SQS retention still applies, visibility renewal has SQS's
12-hour ceiling from the original receive, and a worker crash or failed renewal can cause redelivery
and increment receive counts. Messages remain in SQS, so crashes do not lose held work. Resume is
explicit; control state has no automatic expiry. Workers without `operations` configured do not
participate in the pause protocol.

## Flow inspection and DLQ replay

```lua
local ops = require("ops")
local flow = ops.flow.inspect("acme", "4bf92f3577b34da6a3ce929d0e0e4736")
local sample = ops.dlq.inspect("jobs")
local replay = ops.dlq.replay("jobs", "SQS-MESSAGE-ID", "partner fixed; retry order")
```

`flow.inspect` returns invocation summaries (including retained failure details) and pending state.
`dlq.inspect` returns `{messages, sample_limit = 10}`: each message includes its exact body, SQS id,
system attributes and trace identity. It restores visibility immediately and never deletes messages.
For tenant envelopes with `traceparent`, configured flow logs provide original errors and failure
frames. `invocations = nil` means context is unavailable; an empty list means no retained segments
were found. S3 notifications and untraced messages may need a separate flow lookup.

SQS sampling is unordered and incomplete. Replay receives one sample of at most ten messages,
selects only the requested SQS message id, and releases the others. If the id is absent, it sends
nothing and raises an error. Inspection increments receive counts and briefly changes visibility;
use a DLQ rather than a live work queue. Avoid concurrent DLQ consumers or redrive operations.

Replay sends the **unchanged body and all user message attributes**, preserving embedded flow,
member, handoff and version ids, plus `traceparent` and `tracestate`. Group ids are preserved;
FIFO destinations get a fresh deduplication id. Replay does not bypass durable idempotency checks.
Configure the original work queue as destination, or intake when placement should run again.

Each replay requires a nonempty reason. It saves separate S3 JSON objects at
`<audit_prefix><command_request_id>/<phase>.json`, using conditional puts to prevent overwrite.
Records contain source, destination, original body, available failure context and the new SQS id.
`requested` is saved before sending, `sent` before deleting the original, and `completed` after
deletion. Failures get their own phase when audit storage remains available. No send occurs if
the initial audit write fails. Records contain tenant data: set independent access and retention
policies. Request ids appear in command logs and errors; `operator` from `USER` is informational.

Delivery remains **at least once**. Crashes, lost responses and audit/delete failures can leave an
uncertain outcome or both copies. Inspect the audit and destination before retrying an incomplete
replay. Selected messages have a five-minute visibility lease. `completed` means SQS accepted send
and delete, not that the destination script finished. Use flow inspection to follow resulting work.

Workers need DynamoDB get access to the control slot; operator scripts also need conditional put
access. DLQ inspection needs `sqs:ReceiveMessage` and `sqs:ChangeMessageVisibility`; replay also needs
`sqs:DeleteMessage`, destination `sqs:SendMessage` and audit `s3:PutObject`. Original failure lookup
needs flow-log read/list permissions and pending state uses the runtime's durable-state permissions.
Paused receipt renewal requires `sqs:ChangeMessageVisibility` on the worker queue.
