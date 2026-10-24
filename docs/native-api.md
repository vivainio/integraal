# Native API (`--!runtime 1`)

Everything is exposed under the global `native`. Type definitions: [`types/native.d.luau`](types.md).
Errors are raised as Lua errors; catch them with `pcall`.

Each area has its own chapter covering the script API, the tenant config that governs it, and the
deployment settings behind it:

| Namespace | Chapter |
| --- | --- |
| `native.bucket.<bucket>` | [Buckets (S3)](buckets.md) |
| `native.db` | [DynamoDB](dynamodb.md) |
| `native.lock` | [Locks](locks.md) |
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

## `native.deadline`
- `with(ms, fn) -> ...` – runs `fn()` and returns what it returns. If it is still running after
  `ms` milliseconds it is interrupted and `with` raises `"deadline exceeded"`, which you can `pcall`.

```lua
local ok, err = pcall(native.deadline.with, 2000, function()
  return native.http.get("https://slow.example/")
end)
if not ok then native.log("warn", "partner too slow: " .. tostring(err)) end
```

A block can only shorten the time you have: it never extends the invocation's own timeout, and
when that runs out first the invocation fails with a timeout as usual. Blocks nest and the
earliest deadline wins. When an outer block runs out while an inner one is open, the error
surfaces at the outer `with`, so a `pcall` inside the block cannot swallow it.

Calls to `native.http`, `native.bucket`, `native.sql`, streams and the like are cut off at the
same deadline (HTTP timeouts, stream reads and SQL queries all stop on time). A write that was
already in flight when time ran out can still complete, so do not rely on a deadline to undo it.

## `native.config`
The tenant's `vars` from [`config.json`](tenant-config.md#vars), as a table.
