# Native API (`--!runtime 1`)

Everything is exposed under the global `native`. Type definitions: [`types/native.d.luau`](types.md). Errors are raised as Lua errors; catch them with
`pcall`.

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

## `native.data`
Object storage confined to `DATA_BUCKET/DATA_PREFIX<tenant>/`. Keys are **relative** to that prefix.

| Function | Result |
| --- | --- |
| `get(key)` | string, or `nil` if missing. Capped at `max_object_mb`. |
| `put(key, body)` | writes an object. Capped at `max_object_mb`. |
| `delete(key)` | deletes an object. |
| `list(prefix?)` | array of relative keys, at most 1000. |
| `open(key)` | a **stream** (see below), or `nil` if missing. Not subject to the object cap. |

Keys may not be empty, start with `/`, contain `.` or `..` segments, backslashes or control
characters, or exceed 512 bytes.

## `native.db`
Small items in a shared DynamoDB table, enabled by `DB_TABLE`. Every item is addressed by a
partition name `pk` and an optional sort key `sk` (default `""`). The stored partition key is
always `TEN#<tenant>#<pk>`, so a tenant can only reach its own rows. **Every write sets a 24-hour
expiry** (refreshed on each `put`); expired items are never returned, even before DynamoDB removes them.

| Function | Result |
| --- | --- |
| `get(pk, sk?)` | the stored value, or `nil` if missing or expired. |
| `put(pk, sk?, value)` | stores any JSON-serializable value (at most 64 KB encoded). |
| `delete(pk, sk?)` | removes the item. |
| `query(pk, opts?)` | array of `{ sk, value }` ordered by `sk`; `opts.sk_prefix`, `opts.limit` (at most 100). |

`pk` may not be empty; `pk` and `sk` are limited to 256 bytes and may not contain control
characters. Calls count against `max_data_ops`.

## `native.http`
Outbound HTTP, restricted to the tenant's [`http.allow`](tenant-config.md#httpallow) list.
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

Always denied, even if allowlisted: non-HTTP(S) schemes, URLs with credentials, and literal
private/loopback/link-local IP addresses.

## Streams
`native.data.open` and `native.http.request{stream=true}` return a byte stream. A stream is
consumed by the first parser that takes it; `stream:close()` releases it early. Streams are read
in chunks, so a multi-gigabyte object never sits in memory (limited by `max_stream_mb`).

## `native.xml.events(source, opts?)`
A **pull parser** over a string or a stream. Returns an iterator of events:

| `type` | fields |
| --- | --- |
| `start` | `tag`, `attrs` (table), `depth` |
| `end` | `tag`, `depth` |
| `text` | `text`, `depth` |
| `cdata` | `text`, `depth` |
| `comment` | `text`, `depth` |

```lua
for ev in native.xml.events(stream, { trim = true }) do
  if ev.type == "start" and ev.tag == "item" then ... end
end
```

In `--!strict` code `tag`, `attrs` and `text` are optional (`string?`); they are always present for
the event types listed above, so check `ev.type` first.

- `trim = true` trims text runs and drops whitespace-only ones.
- Adjacent text and entity references (`&amp;`, `&#65;`) arrive as one `text` event.
- `<a/>` yields a `start` followed by an `end`.
- DTDs and custom entities are rejected; nesting is limited to 256 levels.
- `break` out of the loop to stop early.

## Limits that apply here
Per-invocation call budgets (`max_http_requests`, `max_data_ops`), the deadline and size caps are
described in [Tenant config](tenant-config.md#limits). Exceeding a budget raises a Lua error.
