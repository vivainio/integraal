# Hooking up event sources

Almost every integration with integraal is the same job: get an event from somewhere else onto the
queue, wrapped in the [envelope](triggers.md), and let a [router](router.md) script take it from
there. This page shows how to do that for EventBridge, SNS, Pipes and anything else that can end in
SQS.

## The contract

The server polls one SQS queue (`queue.url`). The body of every message must be JSON in one of
these shapes:

```json
{ "tenant": "acme", "event": { "type": "order.created", "id": "o-123" } }
```

- `tenant` is required. `script` and `version` are optional; **leave `script` out** and the
  tenant's router chooses.
- `event` is any JSON and arrives as the handler's `event`.
- Two other shapes are recognised and handled by the server itself: [S3 notifications](triggers.md#s3-notifications)
  and [operator ticks](schedules.md#operator-ticks).

Anything else is logged as an unparseable message and **left on the queue**, so it retries until the
queue's redrive policy moves it to the DLQ. That is the first thing to know about integrations: a
source that puts its *own* format on the queue does not work. Either it is made to produce the
envelope, or something in between converts it.

So an integration is two decisions:

1. How does the source's event become `{tenant, event}`?
2. How does a router script turn `event` into a script call?

## EventBridge rule → SQS

A raw EventBridge delivery to SQS is the whole event record (`version`, `id`, `detail-type`,
`source`, `detail`, ...), which has no `tenant`. Use an **input transformer** on the rule's SQS target
to build the envelope.

```bash
aws events put-rule --name acme-orders \
  --event-pattern '{"source":["acme.shop"],"detail-type":["Order Created","Order Cancelled"]}'

aws events put-targets --rule acme-orders --targets '[{
  "Id": "integraal",
  "Arn": "arn:aws:sqs:eu-west-1:123456789012:integraal",
  "InputTransformer": {
    "InputPathsMap": {
      "id": "$.id", "source": "$.source", "kind": "$.detail-type",
      "time": "$.time", "detail": "$.detail"
    },
    "InputTemplate": "{\"tenant\":\"acme\",\"event\":{\"type\":\"eventbridge\",\"id\":\"<id>\",\"source\":\"<source>\",\"detail_type\":\"<kind>\",\"time\":\"<time>\",\"detail\":<detail>}}"
  }
}]'
```

Notes:

- Placeholders for strings go inside quotes (`"<id>"`); `<detail>` is an object and goes **without**
  quotes, so it stays JSON rather than becoming an escaped string.
- Allow the rule to send: the queue's resource policy needs `sqs:SendMessage` for
  `events.amazonaws.com` with `aws:SourceArn` set to the rule. EventBridge has no role for SQS
  targets; the queue policy is the permission.
- Keep `id` in the event. EventBridge, like SQS, delivers at least once, and `id` is the natural key
  for [idempotency](#making-handlers-safe-to-repeat).
- The queue limit is 256 KB; the envelope adds under 200 bytes, so a full-size event still fits.

### Which tenant

The rule decides, in one of two ways:

| Setup | Use when |
| --- | --- |
| One rule per tenant, `tenant` fixed in the template | each tenant has its own event pattern, source account or bus |
| `"tenant": "<tenant>"` taken from a field of the event (`"tenant": "$.detail.tenant"` in `InputPathsMap`) | one producer serves many tenants and names them |

The second form means **whoever can put events on the bus can run scripts as any tenant**. Integraal
trusts the envelope, because the only way onto the queue is IAM. If tenants publish events
themselves, give each its own bus or rule and pin `tenant` in the template. Do not let a tenant's
payload choose it.

### The router

With no `script` in the message, `on_route` gets the event and picks. Match on what the transformer
put there:

```lua
--!strict
--!runtime 1
local function on_route(_ctx: Context, event: { type: string?, detail_type: string?, source: string? })
  if event.type == "eventbridge" then
    if event.detail_type == "Order Created" then return "orders-created" end
    if event.detail_type == "Order Cancelled" then return "orders-cancelled" end
  end
  return "unhandled"   -- log it and succeed, or fail on purpose so it reaches the DLQ
end
return { on_route = on_route }
```

The target runs with `ctx.trigger == "queue"` and the handler is `on_queue(ctx, event)`.

Keeping the sender-side event small and the type-specific logic in the router means a new event kind
is a script change in the bucket, not an infrastructure change.

## Other ways to reach the queue

The queue is the single door, so every other source is a different way of getting a conformant
message in.

### EventBridge Scheduler

Already how [operator ticks](schedules.md#operator-ticks) arrive. For a tenant's own rhythm, create a
schedule with the queue as target and an ordinary envelope as `Input`
([example](schedules.md#recurring-work-with-another-rhythm)). No transformer is needed because you
write the input yourself.

### EventBridge Pipes

Use a pipe when the source is not a bus: DynamoDB Streams, Kinesis, another SQS queue, Kafka. The
pipe's target is the integraal queue and its **target input transformer** produces the envelope, in the
same template syntax as above. An enrichment step (a Lambda or API call) can add the tenant when the
source record does not have it. Batching is the pipe's concern, but each message on the queue must
still be one envelope, so configure the transformer to emit one per record rather than a batch array.

### SNS

Subscribe the queue with **raw message delivery** on. Without it the body is SNS's wrapper
(`Type`, `MessageId`, `Message`, ...) and fails to parse. With it the body is exactly what was
published, so the publisher must publish the envelope. SNS gives you no transformer; if you cannot
change the publisher, put the rule or pipe in the middle instead.

### A Lambda in front

When the source needs real code (verifying a vendor's signature, translating a format, looking up
the tenant), write a small Lambda that sends the envelope. It does not have to run integraal:

```python
sqs.send_message(QueueUrl=URL, MessageBody=json.dumps(
    {"tenant": tenant, "event": {"type": "vendor.invoice", "payload": body}}))
```

If the work is quick and the caller wants the answer, skip the queue and use a direct
[Lambda invoke](triggers.md#direct-lambda-invoke) with the same envelope.

### HTTP

Partners that can call a URL do not need EventBridge at all: `POST /v1/{tenant}/{script}` through the
ALB, or a [callback](callbacks.md) URL minted by a script for a reply it is waiting for.

### Choosing

| Source | Mechanism | Who builds the envelope |
| --- | --- | --- |
| AWS service events | EventBridge rule + input transformer | the transformer |
| Streams, other queues | Pipe + target transformer | the transformer |
| Your own services | send the envelope to SQS | the sender |
| Recurring | Scheduler | the schedule's `Input` |
| Partner webhooks | ALB route or [callback](callbacks.md) | the URL path, body is the event |
| Odd formats, signatures | Lambda adapter | the Lambda |

## Many sources, one tenant

Give every integration a `type`, and prefix it with where it came from (`eventbridge`, `vendor.invoice`,
`s3.object`; the engine's own are listed under [event types](schedules.md#event-types)). The router is
then a flat table of "this type, this script" and the scripts never need to know which transport
delivered the event. If the same business event can arrive two ways, normalise it in the router with
`{ script, event }` so the target sees one shape:

```lua
if event.type == "eventbridge" and event.detail_type == "Order Created" then
  return { script = "orders-created", event = event.detail }
end
```

## Making handlers safe to repeat

Both EventBridge and SQS deliver at least once, and a failing handler is retried. A handler should be
able to run twice on the same event:

```lua
local function on_queue(ctx: Context, event: { id: string, detail: any })
  if not ig.db.put_if_absent("seen", event.id, true) then
    return { skipped = true }     -- already handled
  end
  -- ... the work ...
end
```

[`ctx.attempt`](queue.md#retries-and-attempts) says which delivery it is, so the last attempt can
record the failure instead of raising. Claiming the id *before* the work means a crash in between
loses the event; claiming it *after* may repeat it. Pick by whether duplicating or dropping hurts
more, or make the work itself idempotent.

## Queue settings that matter

- **DLQ and redrive.** Set `maxReceiveCount` (3 to 5 is usual). This is what turns "unparseable" or
  "no route" into something visible instead of an endless retry.
- **Visibility timeout** at least the largest `timeout_ms` the deployment allows (the
  [ceiling](limits.md#deployment-ceiling)), or a slow handler's message reappears while it is still
  running.
- **Queue policy** allowing the sources (`events.amazonaws.com`, `sns.amazonaws.com`, `pipes`'
  role, `scheduler`'s role) to `sqs:SendMessage`.
- **Several worker classes?** Point external sources at the [intake queue](ingress.md), not a job
  queue, so placement applies. Point Scheduler ticks at the default class's queue, as
  [deployment](deployment.md) says.

## Tracing

If the producer sets `traceparent` and `tracestate` as SQS message attributes, the worker continues
that trace. EventBridge targets cannot set them, but a Lambda adapter or any code that sends the message
itself can. See [Observability](observability.md).

## Trying it without AWS

Everything above is a message on a queue, so you can test a router without EventBridge: send the
envelope your transformer *would* produce.

```bash
aws sqs send-message --queue-url "$QUEUE" --message-body \
  '{"tenant":"acme","event":{"type":"eventbridge","id":"t1","source":"acme.shop","detail_type":"Order Created","detail":{"order":"o-1"}}}'
```

With `AWS_ENDPOINT_URL` pointed at the local simulator ([Local development](local-development.md)) this
runs the same code path as production. EventBridge's own **Sandbox** in the console is the way to
check the input transformer's output before wiring a real target.

## Checklist for a new source

1. Decide the tenant and where it comes from (fixed per rule, never from an untrusted payload).
2. Produce `{tenant, event}` with a `type` and a stable id.
3. Allow the sender on the queue policy.
4. Add a case to the tenant's router and write the `on_queue` handler.
5. Make the handler idempotent on the id.
6. Confirm the DLQ is attached, then send a deliberately bad message and watch it arrive there.
