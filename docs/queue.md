# Queue (`native.queue`)

A script can hand new work to the deployment's job queue: a follow-up step, a piece of a larger
job, or the same message again after a delay. Nothing waits: the messages are delivered later, each
as a `queue` [trigger](triggers.md#sqs-fargate) with the usual retries and dead-letter handling.

```lua
local sent = native.queue.send({
  { script = "work", event = { shard = 1, rows = chunk1 } },
  { script = "work", event = { shard = 2, rows = chunk2 } },
  { event = { kind = "refresh" }, delay_s = 300 },    -- no script: the tenant's router picks one
})
```

`native.queue.send(messages) -> number` takes one message or an array and returns how many it sent.

| Field | Meaning |
| --- | --- |
| `script` | script to run; absent means the tenant's [router](router.md) picks |
| `version` | script version, default `latest` |
| `event` | any JSON value, delivered as the handler's `event` |
| `delay_s` | wait this long before the message becomes visible, at most 900 |
| `group` | SQS `MessageGroupId` suffix: the message is sent as group `<tenant>:<group>`. Default: the event's `type`, if any. Letters, digits, `-`, `_`, `.`; matters on FIFO queues, where messages of one group are processed in order |

The message is the normal envelope `{tenant, script?, version?, event}`, and the **tenant is always
your own**: a script can only queue work for itself.

## Retries and attempts

Delivery is at least once, so a handler may run again for the same message. `ctx.attempt` tells a
handler which delivery this is (`1` the first). Make effects safe to repeat with stable output
keys or a destination idempotency key. A [`native.db.put_if_absent`](dynamodb.md) claim before
an effect can strand the work if the process crashes after claiming it. A handler that wants to
give up cleanly on its last attempt can test `ctx.attempt` and report the failure instead of raising.

Sending is **not atomic**: if a call fails halfway, some messages are already queued. Make the
receiving handlers safe to repeat, and preserve request and member ids when retrying dispatch.
For split work, see [scatter and gather](patterns.md#scatter-and-gather).

## Durable handoff

Use `native.queue.handoff(id, message)` when a queued follow-up must survive a crash after the
intent is recorded. It persists the message; the Fargate minute tick delivers it later. It needs
the deployment's `db`, a queue, tenant `"queue": true`, and the minute operator schedule.

```lua
native.queue.handoff("order/" .. ev.order_id .. "/price", {
  script = "price", version = "v1", event = { order_id = ev.order_id },
})
```

The id is tenant-scoped, 1–200 bytes, with no control characters. Repeating the same id and message
returns `false`; a new intent returns `true`. Reusing an id with different script, version or event
raises. Keep ids and inputs stable across retries. `version` can pin the receiving script.
`delay_s` and `group` are not supported. The usual 200 KB message limit applies; each call reserves
four data ops, including the receipt and pending-row checks.

Each pending row contains the full message, with no DynamoDB TTL. The sweep claims it with a
60-second lease, sends, saves a durable sent receipt, then removes the pending row. Creating
recoverable work requires one conditional insert, so there is no separate index-write gap. A
crash before sending leaves work for a later sweep; a crash after sending but before saving the
receipt can cause a duplicate. A crash after saving the receipt leaves a pending row that the
sweep removes without resending. Receivers must make their effects safe to repeat.

Persisting an intent does not roll back if the originating script later fails, and is not atomic
with a separate database write or external effect. A crash before the handoff call still requires
the originating request to be retried.

Each minute tick examines at most 100 pending rows, with a persisted cursor that wraps after
reaching the end. Completed receipts are retained for deduplication and do not count toward this
budget. Failed sends and busy leases leave their pending rows for later passes. Several sweepers
may overlap safely, but leases are not an exactly-once guarantee. Monitor sweep failures. A
handoff id cannot be reused unless its receipt is explicitly removed by an operator. Existing
messages sent with `queue.send` are unaffected.

On upgrade, a restartable migration examines at most 100 legacy rows per minute tick and indexes
unsent handoffs before advancing its cursor. Pending work is dispatched during migration too.
After migration completes, the sweep no longer scans receipt history. Stop all old-version
writers before running the new sweep: the completed migration will not discover legacy-format
intents created later. Existing receipts remain compatible and retain their deduplication ids.

## Limits

- 1000 messages per invocation, each at most 200 KB serialized.
- Each 10 messages (one `SendMessageBatch`) count as one `max_data_ops` call.
- There is no per-tenant rate limit on what a script enqueues yet (see the [roadmap](roadmap.md)),
  so a script that sends to itself in a loop is bounded only by the queue's retention and the tenant's
  own budgets. This is why the capability is opt-in.

## Tenant config and deployment

Enable it per tenant in `config.json` (tenants cannot edit this file):

```json
{ "queue": true }
```

The deployment sends to its `queue.url` (the same queue the server polls, or whichever queue should
receive tenant-created work; point it at the intake queue to have [worker classes](ingress.md)
applied). The process needs `sqs:SendMessage` on it. A tenant that sets `"queue": true` on a
deployment without `queue.url` fails with a config error.
