# Architecture

```
            ┌────────────── entrypoints ──────────────┐
 invoke ──▶ │ crates/lambda   (Lambda, arm64)         │
 ALB   ──▶  │ crates/server   (Fargate: HTTP + SQS)   │ ──▶ Engine::invoke
 SQS   ──▶  └─────────────────────────────────────────┘          │
                                                                  ▼
                 ┌───────────── crates/core ──────────────────────────────┐
                 │ 1. load <tenant>/config.json   (policy, vars, limits)  │
                 │ 2. load <tenant>/<script>/<version>.luau  (cached)     │
                 │ 3. fresh Luau VM + `native` API for the pinned runtime │
                 │ 4. sandbox, set limits, call the handler               │
                 └─────────────────────────────────────────────────────────┘
```

## Crates

| Crate | Responsibility |
| --- | --- |
| `integraal-loader` | `ScriptStore` trait, S3 implementation, in-memory cache with ETag revalidation. |
| `integraal-wrappers` | The versioned `native` API (`v1`): data, HTTP, XML, JSON, config. |
| `integraal-core` | The engine: limits, tenant config, VM setup, handler dispatch. |
| `integraal-lambda` | Lambda entrypoint (binary `bootstrap`). |
| `integraal-server` | Fargate entrypoint: HTTP server and SQS poller. |

## Two buckets, two trust levels

| Bucket | Contents | Who writes |
| --- | --- | --- |
| **Script bucket** (`SCRIPT_BUCKET`) | scripts and `<tenant>/config.json` | operator / deployment pipeline |
| **Data bucket** (`DATA_BUCKET`) | tenant data under `DATA_PREFIX<tenant>/` | tenant scripts, via `native.data` |

Scripts can reach only the data bucket, and only their own prefix. Because policy lives in the
script bucket, a tenant cannot widen its own HTTP allowlist or raise its own limits.

## Script caching

Scripts are cached in memory per process:

- `latest` is **mutable**. It is revalidated with a conditional `GET` (ETag) at most every 30 s.
- Any other version (`v3`, a content hash, …) is **immutable** and never revalidated.
- `config.json` is treated like `latest`.

## Isolation model

- A fresh Luau state per invocation, then Luau's sandbox mode (read-only globals, no `os`/`io`).
- An instruction-count interrupt enforces the wall-clock timeout; a heap limit caps Luau memory.
- Native calls are tenant-scoped: wrappers receive the tenant's context, never a bucket handle.
- Tenant, script and version names are validated (`[A-Za-z0-9._-]`, no `.`/`..`).
