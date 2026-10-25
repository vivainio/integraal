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
handler which delivery this is (`1` the first). A handler that must not do the same work twice can
claim it with [`native.db.put_if_absent`](dynamodb.md); one that wants to give up cleanly on its
last attempt can test `ctx.attempt` and report the failure instead of raising.

Sending is **not atomic**: if a call fails halfway, some messages are already queued. Make the
receiving handlers safe to repeat, or key them by an id you can claim.

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
