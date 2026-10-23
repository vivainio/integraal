# Outbound HTTP

## Script API: `native.http`
Outbound HTTP, restricted to the tenant's [`http.allow`](#tenant-config-httpallow) list.
Redirects are **not** followed (a redirect could leave the allowlist); follow them explicitly.

```lua
local r = native.http.request({
  method = "POST",                       -- default "GET"
  url = native.config.api_base .. "orders",
  headers = { ["content-type"] = "application/json" },
  body = native.json.encode({ id = 1 }),
  stream = false,                        -- true: r.body is a stream
})
-- r.status, r.headers (table), r.body (string, capped at max_http_response_mb)

local g = native.http.get("https://api.example.com/v1/ping", { accept = "application/json" })
```

With `stream = true`, `r.body` is a [stream](streams-xml.md#streams).

Always denied, even if allowlisted: non-HTTP(S) schemes, URLs with credentials, and literal
private/loopback/link-local IP addresses.

Calls count against `max_http_requests`; sizes and timeouts are set in [Limits](limits.md).

## Tenant config: `http.allow`
A list of rules. A request must match one on **scheme, host, port and path prefix**:

- `"https://api.example.com/v1/"` matches `/v1/users` but not `/v2/`.
- `"https://*.partner.io"` matches subdomains only, not `partner.io`.
- `http://` URLs are denied unless the rule itself starts with `http://`.
- A string rule allows any method; the object form `{ "url", "methods" }` restricts methods.

```json
{
  "vars": { "api_base": "https://api.example.com/v1/" },
  "http": {
    "allow": [
      "${api_base}",
      "https://*.partner.io",
      { "url": "https://hooks.example.com/", "methods": ["POST"] }
    ]
  }
}
```

String [`vars`](tenant-config.md#vars) can be interpolated as `${name}`; an undefined or non-string
reference fails the invocation instead of silently changing the allowlist.

An empty or missing list denies all outbound HTTP.

## Deployment: throttling {#throttling}
Add a `throttle` array to the server's [config](deployment.md#configuration) to limit outbound
`native.http` calls to chosen URLs, for all tenants together:

```json
[{ "url": "https://api.partner.com/v1/", "max_concurrent": 2, "per_second": 5 }]
```

- `url` matches like an `http.allow` entry (scheme, host, port, path prefix; `*.` for subdomains).
  A call matching several rules must satisfy all of them.
- `max_concurrent` caps requests in flight; a streamed response counts until its stream is closed.
- `per_second` spaces request starts evenly (`0.5` means one every two seconds).
- A throttled call **waits for its slot**. If no slot can be had before the invocation deadline,
  the call fails at once with a `throttled: ...` error (catch it with `pcall`). Waiting uses up the
  script's time budget, so size `timeout_ms` accordingly.
- Limits apply **per process**: with several Fargate tasks each enforces its own, so divide the
  target limit by the task count. They are not applied on Lambda (the Lambda entrypoints ignore the key).
- Malformed rules stop the server at startup.
