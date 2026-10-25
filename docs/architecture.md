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
| `integraal-webhook` | Webhook ingress Lambda: verifies callback tokens and enqueues to SQS. |
| `integraal-ingress` | Intake Lambda: places each queue message in a worker size class and forwards it to that class's job queue. |
| `integraal-server` | Fargate entrypoint: HTTP server and SQS poller. |

## Script bucket and stores, different trust levels

Details in [Stores](stores.md#trust-levels).

| Location | Contents | Who writes |
| --- | --- | --- |
| **Script bucket** (`scripts.bucket`) | scripts and `<tenant>/config.json` | operator / deployment pipeline |
| **Stores** (`stores`, e.g. `In`, `Out`, `Backup`, `Temp`) | tenant data under `<prefix><tenant>/` | tenant scripts and third parties, as the tenant's `stores` grants allow |

Scripts can reach only the stores their config grants, and only their own prefix in each. Because policy lives in the
script bucket, a tenant cannot widen its own HTTP allowlist or raise its own limits.

## Script caching

Scripts are cached in memory per process:

- `latest` is **mutable**. It is revalidated with a conditional `GET` (ETag) at most every 30 s.
- Any other version (`v3`, a content hash, …) is **immutable** and never revalidated.
- `config.json` is treated like `latest`.

## Isolation model

- A fresh Luau state per invocation, then Luau's sandbox mode (read-only globals, no `os`/`io`).
- An instruction-count interrupt enforces the wall-clock timeout; a heap limit caps Luau memory.
- Native calls are tenant-scoped: wrappers receive the tenant's context, never a store handle.
- Tenant, script and version names are validated (`[A-Za-z0-9._-]`, no `.`/`..`).
