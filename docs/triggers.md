# Triggers

All triggers share one envelope and one engine.

```json
{ "tenant": "acme", "script": "hello", "version": "v3", "event": { "any": "json" } }
```

- `tenant` is required: it selects the tenant's config, scripts and data.
- `script` is **optional**. If present, that script runs. If absent, the tenant's
  [router](router.md) picks one.
- `version` is optional (default `latest`).

## Routing

When `script` is absent the tenant's router script picks one. See [Router](router.md).

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
`GET /healthz`. `POST /v1/{tenant}` (no script) uses the [router](router.md).

| Status | Cause |
| --- | --- |
| 200 | success |
| 400 | invalid tenant/script/version name |
| 404 | script not found |
| 422 | script error, missing handler, no route, bad runtime or bad config |
| 504 | timeout |
| 500 | internal error |

## SQS → Fargate

Set `queue.url` in the config. The message body is the envelope; omit `script` to have it [routed](router.md). The worker long-polls in batches of 10 and
**deletes a message only on success**; failures return to the queue after the visibility timeout
and eventually land in the queue's dead-letter queue.

## Schedules

EventBridge Scheduler can send ticks to the same queue. A `{"type":"system.tick",...}` message is
handled by the server itself and fanned out to the tenants that opted in; see
[Schedules and timers](schedules.md).

## S3 notifications

Point an S3 event notification (`s3:ObjectCreated:*`) from each bucket at the queue. The
server recognises the notification and splits it into **one invocation per object record**:

- the tenant is the first path segment after the store's `prefix`, so one notification serves all tenants;
- no script is named, so the tenant's [router](router.md) picks one;
- the event is `{ type = "s3.object", store, key, size, etag, event }`, where `store` is the logical name (`In`) and
  `key` is relative to the tenant's prefix, so both can go straight to `ig.store`;
- other event types, folder markers and S3's test event are acknowledged and skipped.

If every record succeeds the message is deleted. If only some fail, **just those records are sent
back** as a new message (with a growing delay and an attempt counter) and the original is deleted,
so successful records are not run again. After 5 rounds the narrowed message is left alone and the
queue's redrive policy moves it to the DLQ. A record that cannot be mapped (unknown bucket, key
outside the store's `prefix`) counts as failed, so it ends up in the DLQ instead of vanishing.

Delivery is at-least-once, so handlers must treat a missing object (`get` returns `nil`) as already
handled.

### Deleting the object when done

A `queue` handler can ask the engine to delete objects **after it returns successfully**:

```lua
return { ok = true, delete = true }  -- the event's own store + key
return { delete = { { store = "In", key = "a.xml" }, { store = "In", key = "b.xml" } } }
```

If the handler errors, nothing is deleted and the message is retried with the object still there.
Deletes go through the tenant's own grants, so the store needs the `delete` grant. At most 100
entries; `delete` is ignored for other triggers; a failed delete fails the invocation.

## Choosing where to run

| | Lambda | Fargate |
| --- | --- | --- |
| Best for | low or spiky volume, synchronous calls | steady load, queue consumers, warm caches |
| Cache | per execution environment | per task |
| Concurrency control | Lambda settings | your own (per-tenant caps are on the [roadmap](roadmap.md)) |
