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

## `native.data.<bucket>`
Object storage, one table per bucket the tenant's [`data`](tenant-config.md#data) block grants.
Everything is confined to `<data.prefix><tenant>/` in that bucket and keys are **relative** to it.
Bucket names are case-sensitive and operator-defined; the conventional ones are `In`, `Out`, `Backup` and `Temp`.
(Avoid lowercase `in`: it is a Luau keyword, so `native.data.in` does not parse.)
Using a bucket you were not granted raises an error.

| Function | Needs | Result |
| --- | --- | --- |
| `get(key)` | read | string, or `nil` if missing. Capped at `max_object_mb`. |
| `put(key, body)` | write | writes an object. Capped at `max_object_mb`. |
| `delete(key)` | delete | deletes an object. |
| `list(prefix?)` | read | array of relative keys, at most 1000. |
| `open(key)` | read | a **stream** (see below), or `nil` if missing. Not subject to the object cap. |
| `presign_get(key, ttl_s?)` | download | URL that lets a third party download exactly that key. |
| `presign_put(key, ttl_s?)` | upload | URL that lets a third party upload exactly that key. |

Presigned URLs default to 15 minutes and are capped at 1 hour. They are signed with the runtime
role's credentials, so they stop working when those credentials expire (about 6 hours at most for
role sessions, often sooner on Lambda), whatever `ttl_s` says. A presigned `PUT` does not limit the
upload size.

Keys may not be empty, start with `/`, contain `.` or `..` segments, backslashes or control
characters, or exceed 512 bytes. Calls count against `max_data_ops`.

### Deleting after a queue handler

`queue` handlers may return `delete = true` or `delete = { { bucket, key }, ... }` and the engine
deletes those objects after the handler succeeds; see [Triggers](triggers.md#deleting-the-object-when-done).

## `native.callback`
Single-use callback URLs: hand one to a third party, and when they call it, a script runs with the
state you saved. Needs the `db` and `callbacks` sections of the [global config](deployment.md#configuration).

```lua
local url = native.callback.create({ script = "on-payment", state = { order = 7 }, ttl_s = 3600 })
-- https://hooks.example.com/cb/acme/<80-char signed token>
```

`script` is optional (without it the tenant's router picks one); `ttl_s` defaults to and is capped at
24 hours; `state` is any JSON value stored in `native.db` (at most 64 KB). The first `POST` to the URL
enqueues the request and runs the script as a `queue` trigger with
`event = { method, path, query, headers, body, body_base64, state }`; any later call gets 404.

The token is HMAC-signed (tenant, id and expiry), so forged, wrong-tenant and expired tokens are
rejected without reading the table. The signing key is generated automatically on first use and
kept in the table outside every tenant partition; to rotate it, delete the `SYS#callback-key` row
(outstanding callbacks stop working). Calls count against `max_data_ops`.

## `native.crypto`
Authenticated encryption without handling key material. Scripts name a key (`"invoices"`: 1-64
characters of letters, digits, `-`, `_`, `.`); the host derives it from the operator's master key
and your tenant name (HKDF-SHA256), so tenants cannot read each other's data even with the same
key name. Needs [`crypto`](deployment.md#crypto) in the deployment config.

| Function | Result |
| --- | --- |
| `encrypt(key, plaintext, aad?)` | binary string: XChaCha20-Poly1305 with a random nonce. 42 bytes larger than the input. |
| `decrypt(key, ciphertext, aad?)` | plaintext; raises if the key or `aad` differs or the data was modified. |
| `is_current(ciphertext)` | `true` if it was sealed with the operator's active master key. |
| `hmac_sha256(key, data, key_id?)` | 32-byte tag; `key_id` selects an older master key to verify old tags. |
| `public_key(key, key_id?)`, `seal(public_key, plaintext)`, `open(key, ciphertext)` | public-key encryption in the age format, see below. |
| `sha256(data)`, `random(n)` | digest; `n` random bytes (1-1024). Work without `crypto` configured. |
| `hex_encode/decode`, `base64_encode/decode` | conversions between binary strings and text. |

**Public-key variant (uploads from outside).** `public_key(key, key_id?)` returns an
[age](https://age-encryption.org) public key (`age1...`) whose private half exists only inside this
tenant's scripts, derived from the master key like the symmetric keys (but unrelated to the
symmetric key of the same name). Hand it to anyone; they encrypt with the stock tool and upload,
for instance through a `presign_put` URL, and a script reads the object and calls `open(key, blob)`.
`seal(public_key, plaintext)` does the same encryption from a script.

```sh
age -r age1... report.csv > report.csv.age     # no -a: armored input is not supported
```

```lua
local pk = native.crypto.public_key("inbox")                     -- publish this
local url = native.data.In.presign_put("drop/report.csv.age")    -- give this to the sender
local csv = native.crypto.open("inbox", native.data.In.get("drop/report.csv.age"))
```

`open` tries every configured master key, so uploads sealed to an older public key still open after
rotation; senders should switch to the new `public_key` (`key_id` returns an older one). The
format does not authenticate the sender: anyone with the public key can upload, so treat opened
data as untrusted input.

`aad` is optional context that must match on decrypt (for example the object key, which stops a
ciphertext being moved to another key). Inputs are capped at `max_object_mb`.

```lua
local b = native.data.Out
b.put("invoices/7.json", native.crypto.encrypt("invoices", body, "invoices/7.json"))
local plain = native.crypto.decrypt("invoices", b.get("invoices/7.json"), "invoices/7.json")
```

**Rotation.** Ciphertext records the number of the master key that sealed it, so after the
operator adds key 2 and makes it active, old data still decrypts and new data uses key 2. To
migrate lazily, re-encrypt whatever `is_current` reports as `false`. Changing the *key name* is a
tenant-side rotation and needs the script to read with the old name and write with the new one.

## `native.http`
Outbound HTTP, restricted to the tenant's [`http.allow`](tenant-config.md#httpallow) list.
If the operator throttles a URL, the call waits for a free slot and fails with a `throttled` error
if none opens before the deadline; see [Throttling](deployment.md#throttling). Redirects are **not** followed (a redirect could leave the allowlist); follow them explicitly.

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
