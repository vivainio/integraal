# Limits

## Tenant config: `limits`
Every field is optional and unknown keys are rejected (so typos can't silently leave a default).

```json
{ "limits": { "timeout_ms": 30000, "memory_mb": 128, "max_http_requests": 100 } }
```

| Field | Default | Ceiling | Meaning |
| --- | --- | --- | --- |
| `timeout_ms` | 10 000 | 60 000 | Wall-clock budget for the invocation. |
| `memory_mb` | 64 | 256 | Luau heap limit. |
| `max_http_response_mb` | 5 | 100 | Largest non-streaming HTTP body. |
| `max_object_mb` | 5 | 100 | Largest `native.bucket.<bucket>.get`/`put` object. |
| `max_stream_mb` | 4096 | 4096 | Total bytes one stream parse may read. |
| `max_sql_mb` | 64 | 512 | Size of one `native.sql` database (and the largest file `native.sql.load` accepts). |
| `max_http_requests` | 100 | 1000 | `native.http` calls per invocation. |
| `max_data_ops` | 1000 | 10 000 | `native.bucket`, `native.db` and `native.callback` calls per invocation. |
| `max_http_timeout_ms` | 10 000 | 30 000 | Per HTTP request timeout. |

Requested values are **clamped to the operator ceiling**; a tenant can never exceed it. Exceeding a
call budget raises a Lua error.

## Deployment: `ceiling`
The ceiling's timeout and memory come from the `ceiling` section of the
[global config](deployment.md#configuration). Under Lambda the
timeout is additionally capped to the time the function has left (minus 500 ms) so scripts fail
cleanly instead of being killed.

## What the limits do and do not cover

- The deadline stops the script (via the VM interrupt), new native calls, and stream reads between
  chunks. An S3 operation already in flight is not interrupted until the SDK's own timeout.
- `memory_mb` covers the Luau heap only. Rust-side buffers are bounded by the `max_*` limits, with
  container or Lambda memory as the backstop.
