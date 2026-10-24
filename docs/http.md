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

For requests that may be slow, see [`job:http`](batch.md#http), which runs the call in the background and resumes a continuation with the response.

Calls count against `max_http_requests`; sizes and timeouts are set in [Limits](limits.md).

## Credential vault {#credential-vault}
Secrets the operator stores (see [Credential vault](deployment.md#vault)) can be *used* in requests
but never read. Each secret is a JSON value. A request opts in, per part, with
`vault = { "header", "body", "url", "basic_auth" }`, and then `{{vault:NAME.path.to.field}}` is expanded in exactly
those parts. `NAME` is the secret and the rest walks into its JSON (object keys, array indexes); the
result must be a string, number or boolean. A secret that is itself a plain string is `{{vault:NAME}}`.

```lua
native.http.request({
  method = "POST",
  url = "https://api.stripe.com/v1/charges",
  headers = { authorization = "Bearer {{vault:stripe.live.key}}" },   -- {"live": {"key": "sk_..."}}
  body = "amount=100",
  vault = { "header" },
})
-- native.http.get(url, headers, { "header" }) takes the same list as its third argument
```

### Basic auth
`basic_auth = { username, password }` on `native.http.request` (and `job:http`) sends an HTTP Basic
`Authorization` header, replacing any `authorization` header you set. The two values are expanded
like any other part, so list `"basic_auth"` in `vault` to take them from the vault; the base64 token is
redacted from the response too. A username cannot contain `:`.

```lua
native.http.request({
  url = "https://api.example.com/v1/items",
  basic_auth = { username = "{{vault:acme.user}}", password = "{{vault:acme.pass}}" },
  vault = { "basic_auth" },
})
```

Without the option, placeholders stay literal text. That is deliberate: a script that relays
untrusted input in a body cannot be tricked into pulling in a secret by an input containing
`{{vault:...}}`. Turn on only the parts you build yourself.

The host substitutes just before sending. The final URL must pass the tenant's `http.allow`
**and** match the `allow` list stored with every secret used, otherwise the call raises an error that
does not contain the value (errors from the HTTP layer are scrubbed too). There is no function that
returns a secret. Any occurrence of a substituted value of 8 bytes or more in the response body or
headers is replaced with `[redacted]`. Requests using secrets cannot use `stream = true`, because a
stream cannot be scanned, unless the operator stored every secret used with `stream_unmasked` (see
[Credential vault](deployment.md#vault)). Response headers are still redacted then; the streamed body is not.
This is best effort: a service that transforms the value, for instance base64, can still reflect it,
so give secrets narrow `allow` prefixes. A secret in the URL ends up in the target service's logs, so
prefer headers when the API allows. Each placeholder counts against `max_data_ops`.

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
