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

## Sections

Each section is documented in the chapter for the feature it configures.

| Key | Chapter |
| --- | --- |
| `router` | [Router](router.md#tenant-config-router) |
| `placement` | [Worker classes](ingress.md#tenant-config-placement) |
| `buckets` | [Buckets (S3)](buckets.md#grants) |
| `http.allow` | [Outbound HTTP](http.md#tenant-config-httpallow) |
| `limits` | [Limits](limits.md) |
| `vars` | [below](#vars) |

## `vars`
Arbitrary JSON, exposed to scripts as `native.config`. String values can be interpolated into
[`http.allow`](http.md#tenant-config-httpallow) as `${name}`; an undefined or non-string reference fails the invocation instead of
silently changing the allowlist.

!!! warning
    Vars are readable by the tenant's scripts and stored as a plain S3 object. They are for
    configuration, **not secrets**.
