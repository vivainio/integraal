# Buckets (S3)

Tenant data lives in **buckets**, separate from the script bucket. Scripts reach them through
`native.bucket.<bucket>`; the operator defines the buckets and the tenant's config grants access.

## Script API: `native.bucket.<bucket>`
Object storage, one table per bucket the tenant's [grants](#grants) allow.
Everything is confined to `<buckets.prefix><tenant>/` in that bucket and keys are **relative** to it.
Bucket names are case-sensitive and operator-defined; the conventional ones are `In`, `Out`, `Backup` and `Temp`.
(Avoid lowercase `in`: it is a Luau keyword, so `native.bucket.in` does not parse.)
Using a bucket you were not granted raises an error.

| Function | Needs | Result |
| --- | --- | --- |
| `get(key)` | read | string, or `nil` if missing. Capped at `max_object_mb`. |
| `put(key, body)` | write | writes an object. Capped at `max_object_mb`. |
| `delete(key)` | delete | deletes an object. |
| `list(prefix?)` | read | array of relative keys, at most 1000. |
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

`queue` handlers may return `delete = true` or `delete = { { bucket, key }, ... }` and the engine
deletes those objects after the handler succeeds; see [Triggers](triggers.md#deleting-the-object-when-done).

## Tenant config: `buckets` {#grants}
Which buckets the tenant may use, and how. Names refer to the deployment's `buckets.map`;
a tenant gets nothing it isn't listed for, and naming a bucket the deployment doesn't have fails
the invocation.

```json
{ "buckets": { "In": ["read", "delete", "upload"], "Out": ["write", "download"], "Backup": "write" } }
```

Each bucket takes one grant or a list of grants:

| Grant | Allows |
| --- | --- |
| `read` | `get`, `open`, `list` |
| `write` | `put` |
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

- `buckets.map` names the buckets (letters, digits and `_`) and maps them to real S3 bucket names.
  This enables `native.bucket.<name>`.
- `buckets.prefix` is the base prefix in every data bucket (empty or ending in `/`). Tenant data lives
  at `<prefix><tenant>/`.

```
<each buckets.map bucket>/<buckets.prefix><tenant>/...   tenant data
```

IAM for the runtime role: `s3:GetObject`, `PutObject`, `DeleteObject` and `ListBucket` on each
bucket. Scope per bucket (for example no `DeleteObject` on `Backup`). Presigned URLs are signed with
this role's credentials, so they only work for operations it allows.

To react to uploads, point S3 event notifications at the queue: see
[S3 notifications](triggers.md#s3-notifications).

## Trust levels

| Bucket | Contents | Who writes |
| --- | --- | --- |
| **Script bucket** (`scripts.bucket`) | scripts and `<tenant>/config.json` | operator / deployment pipeline |
| **Buckets** (`buckets.map`) | tenant data under `buckets.prefix<tenant>/` | tenant scripts and third parties, as the tenant's `buckets` grants allow |

Scripts can reach only the buckets their config grants, and only their own prefix in each.
Because policy lives in the script bucket, a tenant cannot widen its own grants, HTTP allowlist or
limits. Give tenants no direct access to the script bucket.
