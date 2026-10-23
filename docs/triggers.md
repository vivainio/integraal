# Triggers

All triggers share one envelope and one engine.

```json
{ "tenant": "acme", "script": "hello", "version": "v3", "event": { "any": "json" } }
```

- `tenant` is required: it selects the tenant's config, scripts and data.
- `script` is **optional**. If present, that script runs. If absent, the tenant's
  [routing](#routing) picks one.
- `version` is optional (default `latest`).

## Routing

A typical deployment has **one shared queue** whose messages carry only the tenant and an event.
The message does not name a script; the tenant's **router script** decides:

```
queue message {tenant, event}
        │
        ▼
 <tenant>/config.json  →  "router": "router"
        │
        ▼
 router.on_route(ctx, event)  →  "parse-order"   (or { script, version?, event? })
        │            same VM, same deadline and call budgets
        ▼
 parse-order.on_queue(ctx, event)
```

```json
{ "router": "router" }
```

```lua
--!strict
--!runtime 1
local function on_route(ctx: Context, event: { key: string? })
  local key = event.key
  if key and string.sub(key, 1, 16) == "incoming/orders/" then
    return "parse-order"
  end
  return { script = "fallback", event = { original = event } }
end
return { on_route = on_route }
```

- The router's handler is `on_route` (or `route`) and it runs with `ctx.trigger == "route"`.
- Return a **script name**, or a table `{ script, version?, event? }` to pin a version or rewrite
  the event the target receives.
- The target then runs with the *original* trigger (`queue`, `http`, …).
- The router and the target run in **one Luau VM**: one native-API install, one heap, one deadline,
  one set of call budgets. If the target pins a different `--!runtime` version than the router, it
  gets a fresh VM with the remaining time instead.
- A router cannot route to another router. With no `router` configured, a message that names no
  script fails with a *no route* error (queue: retried, then dead-lettered).

## Direct Lambda invoke

Payload is the envelope. The response is:

```json
{ "ok": true,  "result": { ... } }
{ "ok": false, "error": "script error: ..." }
```

The trigger is always `invoke`.

## ALB → Fargate (HTTP)

```bash
curl -X POST http://host:8080/v1/acme/hello \
  -H 'content-type: application/json' \
  -H 'x-script-version: v3' \
  -d '{"x": 1}'
```

The request body is the `event`; the response body is the script's return value. Health check:
`GET /healthz`. `POST /v1/{tenant}` (no script) uses [routing](#routing).

| Status | Cause |
| --- | --- |
| 200 | success |
| 400 | invalid tenant/script/version name |
| 404 | script not found |
| 422 | script error, missing handler, no route, bad runtime or bad config |
| 504 | timeout |
| 500 | internal error |

## SQS → Fargate

Set `SQS_QUEUE_URL`. The message body is the envelope; omit `script` to have it routed. The worker long-polls in batches of 10 and
**deletes a message only on success**; failures return to the queue after the visibility timeout
and eventually land in the queue's dead-letter queue.

## S3 notifications

Point an S3 event notification (`s3:ObjectCreated:*`) from each data bucket at the queue. The
server recognises the notification and splits it into **one invocation per object record**:

- the tenant is the first path segment after `DATA_PREFIX`, so one notification serves all tenants;
- no script is named, so the tenant's [router](#routing) picks one;
- the event is `{ bucket, key, size, etag, event }`, where `bucket` is the logical name (`In`) and
  `key` is relative to the tenant's prefix, so both can go straight to `native.data`;
- other event types, folder markers and S3's test event are acknowledged and skipped.

If every record succeeds the message is deleted. If only some fail, **just those records are sent
back** as a new message (with a growing delay and an attempt counter) and the original is deleted,
so successful records are not run again. After 5 rounds the narrowed message is left alone and the
queue's redrive policy moves it to the DLQ. A record that cannot be mapped (unknown bucket, key
outside `DATA_PREFIX`) counts as failed, so it ends up in the DLQ instead of vanishing.

Delivery is at-least-once, so handlers must treat a missing object (`get` returns `nil`) as already
handled.

### Deleting the object when done

A `queue` handler can ask the engine to delete objects **after it returns successfully**:

```lua
return { ok = true, delete = true }  -- the event's own bucket + key
return { delete = { { bucket = "In", key = "a.xml" }, { bucket = "In", key = "b.xml" } } }
```

If the handler errors, nothing is deleted and the message is retried with the object still there.
Deletes go through the tenant's own grants, so the bucket needs the `delete` grant. At most 100
entries; `delete` is ignored for other triggers; a failed delete fails the invocation.

## Choosing where to run

| | Lambda | Fargate |
| --- | --- | --- |
| Best for | low or spiky volume, synchronous calls | steady load, queue consumers, warm caches |
| Cache | per execution environment | per task |
| Concurrency control | Lambda settings | your own (per-tenant caps are on the [roadmap](roadmap.md)) |
