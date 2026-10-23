# Tenant config

Each tenant may have `<tenant>/config.json` in the **script bucket**. Tenants cannot write there.
A missing file means *defaults, no HTTP access, no vars*. A malformed file fails the invocation.

```json
{
  "vars": {
    "api_base": "https://api.example.com/v1/",
    "region": "eu"
  },
  "http": {
    "allow": [
      "${api_base}",
      "https://*.partner.io",
      { "url": "https://hooks.example.com/", "methods": ["POST"] }
    ]
  },
  "limits": {
    "timeout_ms": 30000,
    "memory_mb": 128,
    "max_http_requests": 100
  }
}
```

The file is re-fetched by ETag at most every 30 s, so edits apply within about half a minute.

## `vars`
Arbitrary JSON, exposed to scripts as `native.config`. String values can be interpolated into
`http.allow` as `${name}`; an undefined or non-string reference fails the invocation instead of
silently changing the allowlist.

!!! warning
    Vars are readable by the tenant's scripts and stored as a plain S3 object. They are for
    configuration, **not secrets**.

## `http.allow`
A list of rules. A request must match one on **scheme, host, port and path prefix**:

- `"https://api.example.com/v1/"` matches `/v1/users` but not `/v2/`.
- `"https://*.partner.io"` matches subdomains only, not `partner.io`.
- `http://` URLs are denied unless the rule itself starts with `http://`.
- A string rule allows any method; the object form `{ "url", "methods" }` restricts methods.

An empty or missing list denies all outbound HTTP.

## `limits`
Every field is optional and unknown keys are rejected (so typos can't silently leave a default).

| Field | Default | Ceiling | Meaning |
| --- | --- | --- | --- |
| `timeout_ms` | 10 000 | 60 000 | Wall-clock budget for the invocation. |
| `memory_mb` | 64 | 256 | Luau heap limit. |
| `max_http_response_mb` | 5 | 100 | Largest non-streaming HTTP body. |
| `max_object_mb` | 5 | 100 | Largest `native.data.get`/`put` object. |
| `max_stream_mb` | 4096 | 4096 | Total bytes one stream parse may read. |
| `max_http_requests` | 100 | 1000 | `native.http` calls per invocation. |
| `max_data_ops` | 1000 | 10 000 | `native.data` calls per invocation. |
| `max_http_timeout_ms` | 10 000 | 30 000 | Per HTTP request timeout. |

Requested values are **clamped to the operator ceiling**; a tenant can never exceed it. The
ceiling's timeout and memory come from `MAX_TIMEOUT_MS` and `MAX_MEMORY_MB`. Under Lambda the
timeout is additionally capped to the time the function has left (minus 500 ms) so scripts fail
cleanly instead of being killed.

### What the limits do and do not cover

- The deadline stops the script (via the VM interrupt), new native calls, and stream reads between
  chunks. An S3 operation already in flight is not interrupted until the SDK's own timeout.
- `memory_mb` covers the Luau heap only. Rust-side buffers are bounded by the `max_*` limits, with
  container or Lambda memory as the backstop.
