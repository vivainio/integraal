# Native API (`--!runtime 1`)

Core capabilities are exposed under `ig`; import opinionated coordination patterns with
`local igp = require("@native/patterns@1")`. Type definitions: [`types/ig.d.luau`](types.md).
Errors are raised as Lua errors; catch them with `pcall`.

Each area has its own chapter covering the script API, the tenant config that governs it, and the
deployment settings behind it:

| Namespace | Chapter |
| --- | --- |
| `ig.store.<name>` | [Stores](stores.md), also whitelisted host directories: [File system](filesystem.md) |
| `ig.db` | [DynamoDB](dynamodb.md) |
| `igp.retry` | [Retry policy](retry.md) |
| `igp.claim` | [Consumer claims](claims.md) |
| `igp.lock` | [Locks](locks.md) |
| `ig.queue` | [Queue](queue.md) |
| `ig.timer` | [Durable timers](timer.md) |
| `igp.callback` | [Callbacks](callbacks.md) |
| `igp.batch`, `ig.ndjson` | [Batch jobs](batch.md) |
| `igp.barrier` | [Barriers](barrier.md) |
| `ig.task` | [Task records](observability.md#task-records) |
| `ig.crypto` | [Crypto](crypto.md) |
| `ig.http` | [Outbound HTTP](http.md) |
| `ig.xml`, `ig.zip`, `ig.gzip`, byte streams | [Streams and XML](streams-xml.md) |
| `ig.sql` | [SQL scratch databases](sql.md) |
| routing (`on_route`) | [Router](router.md) |

Budgets and size caps for all of them are in [Limits](limits.md).

## API versions

`ig.version` is the core API version; `igp.version` is the pattern API version. Both are
currently `1`. The pattern version identifies the whole pattern API, including its workflow
semantics, rather than individual pattern implementations. These version numbers are separate
from script deployment versions and persisted data formats.

`--!runtime 1` selects core version 1. Pattern versions are selected explicitly by the import,
independently of the runtime directive. Version 1 is currently supported; missing or unsupported
versions fail with an error. No unversioned or `latest` import is accepted:


```lua
local igp = require("@native/patterns@1")
assert(igp.version == 1)
```

Core includes queue delivery, task reporting, NDJSON, and other I/O and data utilities.
Patterns include [durable workflows](workflows.md), retries, claims, locks, callbacks, barriers, and batch jobs. They share the invocation's
budgets and deadlines with core. Neither `native` nor `igp` is a global. Repeated imports return the same pattern table within
one invocation; each invocation gets fresh state. Built-in imports do not fetch tenant or shared
modules from storage.

## `ig.log(level, message)`
`level` is `"error"`, `"warn"`, `"debug"`, or anything else for `info`. Emitted as structured logs
with `tenant` and `script`.

## `ig.json`
- `encode(value) -> string`
- `decode(string) -> value`

## `ig.time`
- `now_ms() -> integer` – Unix time in milliseconds.

## `ig.deadline`
- `with(ms, fn) -> ...` – runs `fn()` and returns what it returns. If it is still running after
  `ms` milliseconds it is interrupted and `with` raises `"deadline exceeded"`, which you can `pcall`.

```lua
local ok, err = pcall(ig.deadline.with, 2000, function()
  return ig.http.get("https://slow.example/")
end)
if not ok then ig.log("warn", "partner too slow: " .. tostring(err)) end
```

A block can only shorten the time you have: it never extends the invocation's own timeout, and
when that runs out first the invocation fails with a timeout as usual. Blocks nest and the
earliest deadline wins. When an outer block runs out while an inner one is open, the error
surfaces at the outer `with`, so a `pcall` inside the block cannot swallow it.

Calls to `ig.http`, `ig.store`, `ig.sql`, streams and the like are cut off at the
same deadline (HTTP timeouts, stream reads and SQL queries all stop on time). A write that was
already in flight when time ran out can still complete, so do not rely on a deadline to undo it.

## `ig.config`
The tenant's `vars` from [`config.json`](tenant-config.md#vars), as a table.
