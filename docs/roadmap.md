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

## Barriers
- A timer trigger that runs a script when a barrier's `deadline_s` passes unresolved (today
  `native.barrier.status` reports `overdue`, but something has to ask).
- Growing a barrier after `create` (`add` and a `seal`), for work lists larger than one invocation's
  `max_data_ops`.
- `reconcile`: ack members whose bucket object already exists, to recover a lost notification.

## Engine
- Cache compiled Luau bytecode per `(script hash, runtime)`.
- Per-script limit overrides; per-tenant concurrency caps on Fargate.
- Hard deadline for S3 operations in flight.
- DNS-aware SSRF protection for `native.http` (hostnames resolving to private addresses).

## Native API
- XPath variable binding for `native.xml.parse` (`$name` references, so script data never has to be
  concatenated into an expression); waits on [uppsala#65](https://github.com/kushaldas/uppsala/pull/65).
- Streaming writers (`native.bucket.writer`) for large outputs.
- `native.secrets` backed by Secrets Manager / SSM.
