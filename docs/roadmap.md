# Roadmap

Planned but **not implemented**:

## S3 object pipeline
Objects arriving under a tenant's data prefix trigger scripts.

- S3 → SQS notification on the data bucket; the tenant and relative key are parsed from the object
  key, so one notification serves all tenants.
- Static `routes` in `config.json` (prefix/suffix → script), with a Lua "reaction" script only for
  logic rules can't express.
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
- XML DOM with XPath 1.0 selectors, and an XML generator with escaping.
- Streaming writers (`native.data.writer`) for large outputs.
- `native.secrets` backed by Secrets Manager / SSM.
