# FTP and SFTP

Scripts can use files on an FTP, FTPS or SFTP server. As with [directories](filesystem.md), there is
no separate API: the server is a **bucket**, so scripts use `native.bucket.<name>` with the same
functions, grants and key rules as for [S3 buckets](buckets.md).

The protocols are not built into Integraal. Each operation runs the `curl` binary, so **`curl` must be
installed on the host**, built with the protocols you use (SFTP needs a build with libssh; macOS and
most Linux distributions have one, minimal container images often do not). The server checks this at
startup and refuses to start with a clear error otherwise.

## Deployment

In the [global config](deployment.md#configuration), a store of `"type": "ftp"` or `"type": "sftp"`:

```json
{ "buckets": { "stores": {
  "PartnerFtp": {
    "type": "ftp", "host": "ftp.partner.com", "path": "/outbox/",
    "user": "acme", "password_env": "PARTNER_FTP_PASSWORD", "tls": "explicit"
  },
  "PartnerSftp": {
    "type": "sftp", "host": "sftp.partner.com", "port": 2222, "path": "/home/acme/",
    "user": "acme", "key_file": "/etc/integraal/partner_ed25519",
    "host_key_sha256": "q3r1nO5pYtYVfKFXhVqZxqJmKkQTVhP0pQmK3mG7yCc="
  }
} } }
```

| Field | Types | Meaning |
| --- | --- | --- |
| `host`, `port` | both | a host name or address (no credentials). `port` defaults to the protocol's |
| `path` | both | the directory keys are relative to; starts and ends with `/`. For FTP it is relative to the login directory, and `//srv/out/` is absolute on the server. For SFTP it is absolute |
| `user` | `sftp` (required), `ftp` | anonymous FTP login when absent |
| `password_env` | both | name of the environment variable holding the password (never the password itself). For SFTP give this or `key_file` |
| `key_file` | `sftp` | private key file |
| `host_key_sha256` | `sftp` (required) | the server's host key as base64 SHA-256, as `curl --hostpubsha256` takes it. There is no option to skip the check |
| `tls` | `ftp` | `none` (default), `explicit` (`AUTH TLS`, refused if the server will not) or `implicit` (`ftps://`) |
| `timeout_s` | both | longest one operation may take; default 60 |
| `curl` | both | the `curl` binary; default `curl` from `PATH` |
| `prefix`, `max_read_mb`, `max_write_mb` | both | as for every [store](buckets.md#deployment) |

Credentials are passed to `curl` through a private temporary file that is removed when it exits, so they
never appear in the process list.

Tenants are granted access as for S3 (`{ "buckets": { "PartnerSftp": ["read", "write"] } }`).

## Behaviour
- `get`, `open`, `put`, `create`, `delete` and `list` all work. `open` and `create` stream, so objects
  can exceed `max_object_mb`. A transfer that fails partway is an error, not a short object.
- Uploads go to a temporary name and are renamed on `close`; an aborted or failed script leaves nothing
  behind (a rename needs the server to allow `RNFR`/`RNTO` or `rename`).
- Missing parent directories are created on upload.
- `list` walks directories (at most 8 deep) and needs the server to return Unix-style `ls -l` listings.
- `presign_get`, `presign_put` and event notifications are not available.
- Every operation opens a new connection, so many small objects are slow.
