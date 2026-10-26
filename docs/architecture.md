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
                 │ 3. fresh Luau VM + core `ig` API for the pinned runtime │
                 │ 4. sandbox, set limits, call the handler               │
                 └─────────────────────────────────────────────────────────┘
```

## Crates

| Crate | Responsibility |
| --- | --- |
| `integraal-loader` | `ScriptStore` trait, S3 implementation, in-memory cache with ETag revalidation. |
| `integraal-wrappers` | Core `ig` API and independently versioned native pattern modules. |
| `integraal-core` | The engine: limits, tenant config, VM setup, handler dispatch. |
| `integraal-lambda` | Lambda entrypoint (binary `bootstrap`). |
| `integraal-webhook` | Webhook ingress Lambda: verifies callback tokens and enqueues to SQS. |
| `integraal-ingress` | Intake Lambda: places each queue message in a worker size class and forwards it to that class's job queue. |
| `integraal-server` | Fargate entrypoint: HTTP server and SQS poller. |

## Core API and integration patterns

The runtime installs the core `ig` API for I/O and execution primitives: stores, database,
queue delivery, HTTP, data utilities, deadlines, and task reporting. Scripts import opinionated
coordination patterns separately with `local igp = require("@native/patterns@1")`;
`igp` is a local variable, not a runtime global. Pattern versions are independent of the
script's `--!runtime` version, and share its tenant scope, deadline, and call budgets.

| Pattern | Responsibility |
| --- | --- |
| [Retry](retry.md) (`igp.retry`) | Explicit delayed retries with capped exponential backoff, jitter, attempt limits, and a retry-after floor. Persists intents and registers timers for the shared minute sweep; delivery is at least once. |
| [Consumer claims](claims.md) (`igp.claim`) | Recoverable ownership leases and durable completion results. |
| [Locks](locks.md) (`igp.lock`) | Mutual exclusion with local or database-backed leases. |
| [Callbacks](callbacks.md) (`igp.callback`) | Single-use signed callback URLs with saved state. |
| [Barriers](barrier.md) (`igp.barrier`) | Gather member acknowledgements and dispatch a continuation, with indexed recovery on the shared minute tick. |
| [Batch jobs](batch.md) (`igp.batch`) | Submit partner jobs and resume when results arrive. |

Retries schedule new work explicitly; they do not change engine return-value or queue
acknowledgement semantics. See [Integration patterns](patterns.md) for flows and examples.

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
