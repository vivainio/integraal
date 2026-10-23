# Triggers

All triggers share one envelope and one engine.

```json
{ "tenant": "acme", "script": "hello", "version": "v3", "event": { "any": "json" } }
```

`version` is optional (default `latest`).

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
`GET /healthz`.

| Status | Cause |
| --- | --- |
| 200 | success |
| 400 | invalid tenant/script/version name |
| 404 | script not found |
| 422 | script error, missing handler, bad runtime or bad config |
| 504 | timeout |
| 500 | internal error |

## SQS → Fargate

Set `SQS_QUEUE_URL`. The message body is the envelope. The worker long-polls in batches of 10 and
**deletes a message only on success**; failures return to the queue after the visibility timeout
and eventually land in the queue's dead-letter queue.

## Choosing where to run

| | Lambda | Fargate |
| --- | --- | --- |
| Best for | low or spiky volume, synchronous calls | steady load, queue consumers, warm caches |
| Cache | per execution environment | per task |
| Concurrency control | Lambda settings | your own (per-tenant caps are on the [roadmap](roadmap.md)) |
