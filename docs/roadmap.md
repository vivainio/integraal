# Roadmap

Planned but **not implemented**:

## S3 object pipeline
Routing, S3 notification handling ([Triggers](triggers.md#s3-notifications)) and result-driven
deletes are implemented; the rest of the pipeline is not.

- In-process chaining: a script returns `{ next = "...", event = ... }` and the engine continues
  within one invocation, sharing the deadline and call budgets.
- Message dispositions from the return value: success acks; `native.retry(delay, reason)`,
  `native.dead(reason)`, `native.drop()`; `ctx.attempt` for backoff.
- Idempotency keyed on `(key, etag, script)`; per-tenant event-rate and concurrency caps.

## Engine
- Cache compiled Luau bytecode per `(script hash, runtime)`.
- Per-script limit overrides; per-tenant concurrency caps on Fargate.
- Hard deadline for S3 operations in flight.
- DNS-aware SSRF protection for `native.http` (hostnames resolving to private addresses).

## Native API
- Streaming writers (`native.bucket.writer`) for large outputs.
- `native.secrets` backed by Secrets Manager / SSM.
