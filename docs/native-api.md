# Native API (`--!runtime 1`)

Everything is exposed under the global `native`. Type definitions: [`types/native.d.luau`](types.md).
Errors are raised as Lua errors; catch them with `pcall`.

Each area has its own chapter covering the script API, the tenant config that governs it, and the
deployment settings behind it:

| Namespace | Chapter |
| --- | --- |
| `native.bucket.<bucket>` | [Buckets (S3)](buckets.md) |
| `native.db` | [DynamoDB](dynamodb.md) |
| `native.callback` | [Callbacks](callbacks.md) |
| `native.crypto` | [Crypto](crypto.md) |
| `native.http` | [Outbound HTTP](http.md) |
| `native.xml`, `native.zip`, `native.gzip`, byte streams | [Streams and XML](streams-xml.md) |
| `native.sql` | [SQL scratch databases](sql.md) |
| routing (`on_route`) | [Router](router.md) |

Budgets and size caps for all of them are in [Limits](limits.md).

## `native.log(level, message)`
`level` is `"error"`, `"warn"`, `"debug"`, or anything else for `info`. Emitted as structured logs
with `tenant` and `script`.

## `native.json`
- `encode(value) -> string`
- `decode(string) -> value`

## `native.time`
- `now_ms() -> integer` – Unix time in milliseconds.

## `native.config`
The tenant's `vars` from [`config.json`](tenant-config.md#vars), as a table.
