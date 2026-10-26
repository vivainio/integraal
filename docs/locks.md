# Locks (`igp.lock`)

Mutual exclusion for scripts that must not run the same critical section twice at once.

```lua
local igp = require("@native/patterns@1")
local ran, result = igp.lock.with("import-42", { scope = "db", lease_ms = 30000, wait_ms = 5000 }, function()
  return import_everything()
end)
if not ran then
  return { skipped = "another import is running" }
end
```

`igp.lock.with(name, opts?, fn) -> (acquired, ...)` takes the lock, runs `fn()`, and releases the
lock again however `fn` ends (return, error, or interrupt). It returns `true` followed by whatever
`fn` returned, or just `false` if the lock could not be had in time. Errors from `fn` are raised
as usual.

| Option | Default | Meaning |
| --- | --- | --- |
| `scope` | `"local"` | `"local"` excludes within this process; `"db"` excludes across every process sharing the table. |
| `wait_ms` | `0` | How long to wait for the current holder. `0` tries once. |
| `lease_ms` | none (`30000` for `db`) | How long `fn` may hold the lock. At most 15 minutes. |

Names are per tenant. Two tenants using the same name never contend.

## The lease is enforced

When `lease_ms` runs out, `fn` is interrupted, exactly as with
[`ig.deadline.with`](native-api.md#igdeadline), and `with` raises `"deadline exceeded"`.
So a holder that is slow or stuck stops before anyone else can take the lock, and the lock needs
no fencing token. The interrupt covers script code and native calls (HTTP, SQL, stream reads),
but a write that was already in flight can still complete, so make the protected work safe to
repeat or keep the lease comfortably longer than it needs.

A `db` lock stays valid for at least a second longer than the lease on the clock the table is read
with, so clock skew between machines of under a second cannot let two holders overlap. The lease
counts from the moment `with` starts asking for the lock, so time spent waiting does not extend it.

## Where each scope helps

- **`local`** is shared by every invocation in the process. That matters on **Fargate**, where one
  task runs many scripts at once. On **Lambda** an execution environment runs one invocation at a
  time, so a local lock only ever excludes the script from itself. Use `db` to exclude across
  Lambda invocations, and across Fargate tasks.
- **`db`** keeps one row per lock in the [`ig.db`](dynamodb.md) table (under the tenant's partition
  prefix, in a partition scripts cannot address). It needs `db.table`. Waiting polls with a
  growing, jittered pause; every attempt counts against `max_data_ops`.

Locks are not reentrant: taking a lock you already hold waits for yourself, so use `wait_ms = 0`
(the default) and check the result when you might.

## Deployment

Nothing beyond [`ig.db`](dynamodb.md#deployment): conditional `PutItem` and `DeleteItem`.
