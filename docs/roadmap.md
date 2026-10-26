# Roadmap

Planned work and candidate patterns, **not implemented** unless noted. Candidate patterns need
API and recovery semantics designed before implementation.

## S3 object pipeline
Routing, S3 notification handling ([Triggers](triggers.md#s3-notifications)) and result-driven
deletes are implemented; the rest of the pipeline is not.

- In-process chaining: a script returns `{ next = "...", event = ... }` and the engine continues
  within one invocation, sharing the deadline and call budgets.
- Design any remaining queue delivery controls as core primitives only where patterns need them.
  Explicit delayed retries are provided by the versioned `igp.retry` pattern, not return dispositions.
- Idempotency keyed on `(key, etag, script)`; per-tenant event-rate and concurrency caps.

## Barriers
[Automatic continuation recovery](barrier.md#recovery-sweep) is implemented: interrupted indexed
acks and dispatches are recovered by the bounded minute sweep. Remaining work:

- Growing a barrier after `create` (`add` and a `seal`), for work lists larger than one invocation's
  `max_data_ops`.
- `reconcile`: ack members whose object already exists, to recover a lost notification.
- Quorum and first-success gathers: continue after enough successful responses, with explicit
  handling of late results and remaining work.

## Schedules and timers
- `ig.timer` for scripts: `at`/`after` and `cancel`, on the [timers](schedules.md#timers) the
  barrier deadline already uses.
- Creating a tenant's own EventBridge schedules from its config (cron, time zones).
- Splitting a tick's fan-out into chunks for very large tenant lists.
- `type` on callback and batch `done.json` events.

## Durable handoff
`ig.queue.handoff`, the pending-work index and the minute recovery sweep are implemented;
remaining work:

- Operator inspection and explicit receipt cleanup.

## Reliable consumers and retries
[Recoverable consumer claims](claims.md) are implemented: leases, ownership tokens, renewal and
durable completion results.

[Retry policy](retry.md) is implemented as `igp.retry`: capped exponential backoff, jitter,
explicit attempt limits and a partner retry-after floor. Remaining work:

- Shared retry budgets and automatic partner rate-limit coordination.
- Scheduled reconciliation of expected work against durable results, with safe replay of missing
  work; extend the barrier-specific reconciliation above to other flows.

## Additional integration patterns
- Debounce and coalesce: combine repeated updates for one business key into a refresh after
  activity settles.
- Ordered processing by business key, with version checks to reject stale events as well as
  serializing work.
- Saga compensation: persist completed steps and run explicit, retryable compensating actions
  when a later step fails.
- Backpressure and bulkheads: bounded fan-out and tenant/destination concurrency limits, so a
  large flow or slow partner cannot consume all worker capacity.

## Flow operations
- Pin worker and continuation script versions across retries and multi-step flows. Handoffs can
  already specify a version; barriers and other continuation APIs need consistent support.
- Inspect and replay dead-lettered work while preserving flow/member ids, showing the original
  failure and recording an audit trail.
- Surface flows approaching an explicit expiry, including legacy barrier state and callbacks,
  so recovery happens before the required state is lost.

## Engine
- Cache compiled Luau bytecode per `(script hash, runtime)`.
- Per-script limit overrides; per-tenant concurrency caps on Fargate.
- Hard deadline for S3 operations in flight.
- DNS-aware SSRF protection for `ig.http` (hostnames resolving to private addresses).

## Native API
- Streaming writers (`ig.store.writer`) for large outputs.
- `ig.secrets` backed by Secrets Manager / SSM.
