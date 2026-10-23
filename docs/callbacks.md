# Callbacks and the webhook Lambda

Single-use callback URLs: hand one to a third party, and when they call it, a script runs with the
state you saved.

## Script API: `native.callback`
Needs the `db` and `callbacks` sections of the [global config](deployment.md#configuration).

```lua
local url = native.callback.create({ script = "on-payment", state = { order = 7 }, ttl_s = 3600 })
-- https://hooks.example.com/cb/acme/<80-char signed token>
```

`script` is optional (without it the tenant's [router](router.md) picks one); `ttl_s` defaults to and is capped at
24 hours; `state` is any JSON value stored in [`native.db`](dynamodb.md) (at most 64 KB). The first `POST` to the URL
enqueues the request and runs the script as a `queue` trigger with
`event = { method, path, query, headers, body, body_base64, state }`; any later call gets 404.

The token is HMAC-signed (tenant, id and expiry), so forged, wrong-tenant and expired tokens are
rejected without reading the table. The signing key is generated automatically on first use and
kept in the table outside every tenant partition; to rotate it, delete the `SYS#callback-key` row
(outstanding callbacks stop working). Calls count against `max_data_ops`.

## Deployment: webhook Lambda
`integraal-webhook` (binary `bootstrap`, behind a Function URL) receives `POST /cb/<tenant>/<token>`,
verifies and consumes the callback, and sends it to `queue.url` for the Fargate poller. It runs
no scripts.

- Config: its `db` and `queue` sections, and `callbacks.base_url` (the public base URL) on the
  entrypoints that create callbacks.
- IAM: `dynamodb:GetItem`/`PutItem`/`DeleteItem` on the table and `sqs:SendMessage` on the queue.
- Replies: 202 queued, 404 invalid/unknown/used, 410 expired, 413 body over 200 KB, 500 enqueue
  failed (the callback is restored so the sender can retry).
