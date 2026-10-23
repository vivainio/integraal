# DynamoDB (`native.db`)

A small shared key-value store for state that doesn't belong in an S3 object. It is also what
[callbacks](callbacks.md) are built on.

## Script API: `native.db`

| Function | Result |
| --- | --- |
| `get(pk, sk?)` | the stored value, or `nil` if missing or expired. |
| `put(pk, sk?, value)` | stores any JSON-serializable value. |
| `delete(pk, sk?)` | removes an item. |
| `query(pk, opts?)` | rows `{ sk, value }` under `pk`, in sort-key order. `opts.sk_prefix` filters, `opts.limit` caps (at most 100). |

`sk` defaults to the empty string. Values are stored as JSON, at most 64 KB each.
**Every write sets a 24 hour expiry**, so this is for short-lived state; use
[`native.data`](data.md) for anything that must stay. Calls count against `max_data_ops`.

```lua
native.db.put("session", "u42", { step = 2 })
local s = native.db.get("session", "u42")
for _, row in native.db.query("session", { sk_prefix = "u" }) do
  native.log("info", row.sk)
end
```

Isolation is enforced by the host: your `pk` is stored as `TEN#<tenant>#<pk>`, so a script cannot
address another tenant's rows.

## Deployment
Set `db.table` in the [global config](deployment.md#configuration). The table needs:

- string partition key `pk` and string sort key `sk`;
- **TTL enabled on the attribute `ttl`**;
- items hold the value in `v`.

All tenants share the table; isolation comes from the enforced `TEN#<tenant>#` partition key prefix.

IAM: `dynamodb:GetItem`, `PutItem`, `DeleteItem` (conditional writes included) and `Query` on the table.
