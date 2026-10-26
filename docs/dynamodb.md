# DynamoDB (`ig.db`)

A small shared key-value store for state that doesn't belong in an S3 object. It is also what
[callbacks](callbacks.md) are built on.

## Script API: `ig.db`

| Function | Result |
| --- | --- |
| `get(pk, sk?)` | the stored value, or `nil` if missing or expired. |
| `put(pk, sk?, value)` | stores any JSON-serializable value. |
| `put_if_absent(pk, sk?, value)` | stores the value only if the key holds no live item; returns `true` if it wrote. An expired item counts as absent. |
| `add(pk, sk?, delta?)` | atomic counter: adds `delta` (default `1`, may be negative) and returns the new value. A missing or expired counter starts from 0. |
| `delete(pk, sk?)` | removes an item. |
| `query(pk, opts?)` | rows `{ sk, value }` under `pk`, in sort-key order. `opts.sk_prefix` filters, `opts.limit` caps (at most 100). |

`sk` defaults to the empty string. Values are stored as JSON, at most 64 KB each.
**Every write sets a one week expiry**, so this is for short-lived state; use
[`ig.store`](stores.md) for anything that must stay. Calls count against `max_data_ops`.

```lua
ig.db.put("session", "u42", { step = 2 })
local s = ig.db.get("session", "u42")
for _, row in ig.db.query("session", { sk_prefix = "u" }) do
  ig.log("info", row.sk)
end
```

`put_if_absent` and `add` are the building blocks for idempotency and for counting work across
invocations: record a deduplication marker, or count finished pieces and act on the one that
completes the set. For [scatter and gather](patterns.md#scatter-and-gather), use a
[barrier](barrier.md) to avoid a crash between claiming a member and incrementing its counter.
Use keys that only
`add` writes: `get` reads a counter back as a number, and a later `put` replaces it. Both calls count
against `max_data_ops`.

For work that must recover after a crash, use [consumer claims](claims.md) instead of a
`put_if_absent` marker followed by processing. A permanent marker cannot distinguish finished
work from a process that died immediately after claiming it.

Isolation is enforced by the host: your `pk` is stored as `TEN#<tenant>#<pk>`, so a script cannot
address another tenant's rows.

## Deployment
Set `db.table` in the [global config](deployment.md#configuration). The table needs:

- string partition key `pk` and string sort key `sk`;
- **TTL enabled on the attribute `ttl`**;
- items hold the value in `v`.

All tenants share the table; isolation comes from the enforced `TEN#<tenant>#` partition key prefix.

IAM: `dynamodb:GetItem`, `PutItem`, `UpdateItem`, `DeleteItem` (conditional writes included) and `Query` on the table.
[Locks](locks.md) use the same actions.
