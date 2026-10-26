# Stores

Tenant data lives in **stores**, separate from the script bucket. Scripts reach them through
`ig.store.<name>`; the operator defines the stores (S3, a host directory, or an FTP or SFTP server)
and the tenant's config grants access.

## Script API: `ig.store.<name>`
Object storage, one table per store the tenant's [grants](#grants) allow.
Everything is confined to `<prefix><tenant>/` in that store and keys are **relative** to it.
Store names are case-sensitive and operator-defined; the conventional ones are `In`, `Out`, `Backup` and `Temp`.
(Avoid lowercase `in`: it is a Luau keyword, so `ig.store.in` does not parse.)
Using a store you were not granted raises an error.

| Function | Needs | Result |
| --- | --- | --- |
| `get(key)` | read | string, or `nil` if missing. Capped at `max_object_mb`. |
| `put(key, body)` | write | writes an object. Capped at `max_object_mb`. |
| `delete(key)` | delete | deletes an object. |
| `list(prefix?)` | read | array of relative keys, at most 1000. |
| `create(key)` | write | a **sink** for a streaming upload (multipart on S3), so the object can exceed `max_object_mb`. See below. |
| `open(key)` | read | a **stream** (see [Streams and XML](streams-xml.md)), or `nil` if missing. Not subject to the object cap. |
| `presign_get(key, ttl_s?)` | download | URL that lets a third party download exactly that key. |
| `presign_put(key, ttl_s?)` | upload | URL that lets a third party upload exactly that key. |

Presigned URLs default to 15 minutes and are capped at 1 hour. They are signed with the runtime
role's credentials, so they stop working when those credentials expire (about 6 hours at most for
role sessions, often sooner on Lambda), whatever `ttl_s` says. A presigned `PUT` does not limit the
upload size.

Keys may not be empty, start with `/`, contain `.` or `..` segments, backslashes or control
characters, or exceed 512 bytes. Calls count against `max_data_ops`.

### Deleting after a queue handler

`queue` handlers may return `delete = true` or `delete = { { store, key }, ... }` and the engine
deletes those objects after the handler succeeds; see [Triggers](triggers.md#deleting-the-object-when-done).

## Tenant config: `stores` {#grants}
Which stores the tenant may use, and how. Names refer to the deployment's `stores`;
a tenant gets nothing it isn't listed for, and naming a store the deployment doesn't have fails
the invocation.

```json
{ "stores": { "In": ["read", "delete", "upload"], "Out": ["write", "download"], "Backup": "write" } }
```

Each store takes one grant or a list of grants:

| Grant | Allows |
| --- | --- |
| `read` | `get`, `open`, `list` |
| `write` | `put`, `create` |
| `delete` | `delete` |
| `readwrite` | `read`, `write` and `delete` |
| `upload` | `presign_put` only |
| `download` | `presign_get` only |

Presigning is a capability handed to a third party, so it is granted separately from the script's own
access. In the example, scripts can read what partners upload to `In`, delete it once handled, and mint upload links
for it, but cannot overwrite it, and can write results to `Out` and hand out download links without being able
to read anything back.

## Deployment

In the [global config](deployment.md#configuration):

`stores` names the stores (letters, digits and `_`) and says what each one is. This enables
`ig.store.<name>`. Every store is an object with a `type`, and each type allows only its own fields:

```json
{ "stores": {
  "In":     { "type": "s3", "bucket": "acme-in", "prefix": "tenants/" },
  "Out":    { "type": "s3", "bucket": "acme-out", "prefix": "tenants/", "max_write_mb": 500 },
  "Local":  { "type": "dir", "path": "/srv/integraal/data" },
  "Partner": { "type": "ftp", "host": "ftp.partner.com", "path": "/outbox/", "user": "acme", "password_env": "PARTNER_FTP_PASSWORD", "tls": "explicit" }
} }
```

| `type` | Backend | Details |
| --- | --- | --- |
| `s3` | an S3 bucket | `bucket`; below |
| `dir` | a whitelisted host directory | [File system](filesystem.md) |
| `ftp` | an FTP or FTPS server, through `curl` | [FTP and SFTP](remote.md) |
| `sftp` | an SFTP server, through `curl` | [FTP and SFTP](remote.md) |

Fields every type takes:

| Field | Meaning |
| --- | --- |
| `prefix` | base prefix in the store (empty or ending in `/`). Tenant data lives at `<prefix><tenant>/` |
| `max_read_mb`, `max_write_mb` | bytes one invocation may read or write; unlimited when absent. See [I/O limits](filesystem.md#io-limits-and-accounting) |

Unknown fields, and fields of another type, make the config invalid, so the file can be checked against
a JSON Schema that selects on `type`.

```
<s3 store's bucket>/<prefix><tenant>/...   tenant data
```

IAM for the runtime role: `s3:GetObject`, `PutObject`, `DeleteObject` and `ListBucket` on each
bucket. Scope per bucket (for example no `DeleteObject` on `Backup`). Presigned URLs are signed with
this role's credentials, so they only work for operations it allows.

To react to uploads, point S3 event notifications at the queue: see
[S3 notifications](triggers.md#s3-notifications).

## Trust levels

| Location | Contents | Who writes |
| --- | --- | --- |
| **Script bucket** (`scripts.bucket`) | scripts and `<tenant>/config.json` | operator / deployment pipeline |
| **Stores** (`stores`) | tenant data under `<prefix><tenant>/` | tenant scripts and third parties, as the tenant's `stores` grants allow |

Scripts can reach only the stores their config grants, and only their own prefix in each.
Because policy lives in the script bucket, a tenant cannot widen its own grants, HTTP allowlist or
limits. Give tenants no direct access to the script bucket.

## Streaming uploads
`create(key)` returns a sink. `sink:write(data)` uploads as you go (S3 buffers one 8 MiB part), and the
object only appears when you call `sink:close()`, which returns the bytes written. `sink:abort()`, or
the script ending (or failing) without `close`, discards the upload. Total size is limited by
`max_stream_mb`, and each `create` counts as one data operation.

```lua
local out = ig.store.Out.create("export.zip")
local z = ig.zip.writer(out)
for _, key in ig.store.In.list("batch/") do
  z:add(key, ig.store.In.open(key))   -- streamed, never held in memory
end
z:finish()
out:close()
```
