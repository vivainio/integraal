# File system

Scripts can use directories on the host's file system, but only the ones the operator whitelists.
There is no separate API: a whitelisted directory is exposed as a **bucket**, so scripts use
`native.bucket.<name>` with the same functions, grants and key rules as for [S3 buckets](buckets.md).
Reusing it means the grants, streaming, limits and accounting work the same way. The only
difference is where the bytes go.

## Deployment

In the [global config](deployment.md#configuration), `buckets.dirs` maps a name (letters, digits and `_`,
not also used in `buckets.map`) to an absolute directory:

```json
{ "buckets": { "prefix": "tenants/", "dirs": { "Local": "/srv/integraal/data" } } }
```

A tenant's files live in `<dir>/<buckets.prefix><tenant>/`. The tenant's `config.json` grants access
exactly as for S3 (`{ "buckets": { "Local": ["read", "write"] } }`), and scripts then call
`native.bucket.Local.get(key)`, `put`, `list`, `open`, `create` and `delete`.

On Lambda the directory must be a mounted volume such as EFS, because the function's own disk does
not outlive the invocation.

## What stays out of reach
- Keys are relative to the tenant's folder. Empty segments, `.` and `..` are rejected.
- A symlink that resolves outside the whitelisted directory is refused, for reads and writes.
- Streaming uploads (`create`) write a temporary file and rename it on `close`, so an aborted or
  failed script leaves nothing behind.
- `presign_get`, `presign_put` and S3 event notifications are not available for directories.

## I/O limits and accounting
A directory can carry byte limits for one invocation, which may differ per directory:

```json
{ "buckets": { "dirs": {
  "Local":   { "path": "/srv/integraal/data", "max_read_mb": 200, "max_write_mb": 50 },
  "Scratch": "/srv/integraal/scratch"
} } }
```

Once an invocation has read or written more than a limit, further calls and streams in progress fail.
A missing limit means unlimited; the limits are the operator's and a tenant cannot change them. They
apply on top of `max_object_mb`, `max_stream_mb` and `max_data_ops`.

Every byte moved is counted in the `integraal.fs.bytes` metric with the labels `tenant`, `dir` and
`direction` (`read` or `write`); see [Observability](observability.md).
