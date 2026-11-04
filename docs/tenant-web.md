# Tenant web apps

A tenant can deploy a small Luau web app behind the existing Fargate HTTP server and ALB.
The tenant owns its handlers and pages; Rust supplies HTTP transport and the normal sandbox.
No separate listener or long-running Lua process is needed. Each request runs in a fresh VM.

## Enable an app

Deploy a script, for example `acme/web/latest.luau`, and point the tenant's `config.json` at it:

```json
{ "web": { "script": "web" } }
```

This mounts the app at `/web/acme` and `/web/acme/*`. Without `web`, requests return 404.
Add `"version": "v1"` to pin the app to an immutable deployed version. The request's
`x-script-version` header cannot override this setting. The app is selected from config,
not from a client-supplied script name or a router decision.

## Write the handler

```lua
--!strict
--!runtime 1
local function on_http(ctx: Context, request: TenantWebRequest)
    if request.method ~= "GET" and request.method ~= "HEAD" then
        return { status = 405, headers = { Allow = "GET, HEAD" }, body = "Read only" }
    end
    if request.path == "/" then
        return {
            headers = { ["content-type"] = "text/html; charset=utf-8" },
            body = "<!doctype html><h1>Tenant app</h1>",
        }
    end
    return { status = 404, body = "Not found" }
end
return { on_http = on_http }
```

[The complete read-only example](https://github.com/vivainio/integraal/blob/main/examples/acme/web/latest.luau)
serves an HTML form and a `/records?pk=...` JSON endpoint. The form displays values as text,
and the handler reads at most 25 tenant-scoped database records. It requires the deployment's
database for record queries. It rejects write methods and contains no commands or code evaluation.
Deploying it is optional; tenants can provide their own script instead.

## Request and response

| Request field | Meaning |
| --- | --- |
| `method` | Uppercase HTTP method, such as `GET` or `POST` |
| `path` | Decoded path relative to the app mount; `/` at the root |
| `base_path` | Mount path, such as `/web/acme`, for links and redirects |
| `query` | Decoded string map of query parameters |
| `raw_query` | Original query string without `?`, for custom parsing or repeated parameters |
| `headers` | Lowercase header names mapped to text values |
| `body` | UTF-8 request body, including JSON or form data as text |

The response is `{ status = number?, headers = { [string] = string }?, body = string? }`.
Defaults are status 200, an empty body and `text/plain; charset=utf-8`. Set `content-type`
for HTML, JSON or other text; use `ig.json.encode` for JSON responses. Status codes from
200 through 599 are supported. Invalid responses and headers return 422. Body framing and
hop-by-hop headers are controlled by the server. Responses default to `Cache-Control: no-store`
and `X-Content-Type-Options: nosniff`; the app can override these.

Both request and response bodies are limited to 1 MB; non-UTF-8 request bodies return 400.
Responses are buffered, so streaming, WebSockets, binary bodies and multi-value response
headers are not supported. Normal tenant invocation deadlines, memory and native-call limits
apply. Native data operations remain confined to the configured tenant.

The app runs with the tenant's normal privileges. Read-only behavior is implemented by the
example's handlers, not enforced on arbitrary tenant-authored apps. The operator `ops` module
remains restricted to direct operator invocations and is not available to HTTP apps.

## ALB access

Route `/web/acme` and `/web/acme/*` to the existing Fargate target group. Use ALB authentication
or internal network access controls appropriate to the data. The tenant URL selects the data
scope; it does not authenticate a user. For a tenant-specific listener, forward only that
app's paths; `/v1/*` remains a separate invocation interface. The runtime does not add a login.

Existing `POST /v1/{tenant}/{script}` and `POST /v1/{tenant}` endpoints keep their JSON event
and JSON result contract; HTTP response envelopes apply only to `/web` apps.

```bash
curl 'https://apps.example.com/web/acme'
curl 'https://apps.example.com/web/acme/records?pk=orders'
```
