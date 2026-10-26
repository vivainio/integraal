# Consumer claims

`native.claim` gives a consumer recoverable ownership of a request and a durable completion
result. Use it with a stable business request id when work can be redelivered. A lease can expire
and be acquired by a new worker; completed work returns its saved result instead of being acquired
again. Claims need the deployment's `db` section; they do not need a queue or a timer sweep.

```lua
local id = "price/" .. ev.order_id
local c = native.claim.acquire(id, { lease_s = 60 })
if c.state == "complete" then return c.result end
if c.state == "busy" then error("request is owned; retry later") end

-- Must be safe to repeat, e.g. pass id as the partner's idempotency key.
local result = process(ev)
if not native.claim.complete(id, c.token, result) then
  error("claim ownership changed; retry")
end
return result
```

For a queue handler, raising on a busy claim keeps that delivery eligible for retry. Returning
normally would acknowledge it. Configure retries and dead-letter handling to allow enough time
for abandoned leases to expire; claims do not retry messages or restart work themselves.

## Script API

| Function | Behavior |
| --- | --- |
| `acquire(id, opts?)` | Returns `{ state = "acquired", token, expires_s }`, `{ state = "busy", expires_s }`, or `{ state = "complete", result }`. Only an acquired response grants ownership; busy responses do not expose the owner's token. |
| `renew(id, token, opts?)` | Returns the new expiry, or `nil` if the claim is missing, expired, completed, owned by another token, or changed concurrently. It never shortens a live lease. |
| `complete(id, token, result?)` | Atomically replaces a live lease with its completed result. Returns `false` if ownership is missing, expired or changed concurrently. An identical retry with the same token and result returns `true`; a different result cannot overwrite completion. |

`opts.lease_s` defaults to 60 and must be an integer from 1 to 900. Ids are tenant-scoped, 1–200
bytes, with no control characters. Use a different id for a different business operation or input:
claims do not compare request payloads. Results are JSON, at most 8 KB serialized; store large
results in an object store and complete with their key. Omitted results are JSON null.

Each API call reserves two `max_data_ops` before making any database changes. Claims use private
rows that scripts cannot access through `native.db`. Running rows use the lease expiry as their
DynamoDB TTL; acquisition can replace an expired row immediately, without waiting for DynamoDB's
TTL cleanup. Completed results have no TTL and prevent reuse of the id until explicitly removed
by an operator.

## Renewal and ownership

Renew between chunks of work, before the current expiry:

```lua
local expires_s = native.claim.renew(id, c.token, { lease_s = 60 })
if not expires_s then error("claim ownership changed; stop and retry") end
```

There is no automatic heartbeat. Choose a lease long enough for an indivisible operation, and
renew before starting another chunk. A failed renewal means the caller must stop relying on its
ownership. Concurrent renewal and completion can cause a conditional write to fail; retry through
the normal consumer path. Workers rely on their system clocks for lease timestamps.

Completion and renewal compare the stored value and expiry atomically, and require a live lease.
A token from an earlier owner cannot complete or renew a replacement owner's claim. Retrying a
successful completion after its response was lost returns `true` without changing the result.

## Crash recovery and effects

- **Crash before completion:** after the lease expires, a redelivery acquires a new token and
  repeats the work. This also covers a crash immediately after acquisition.
- **Crash after completion:** a redelivery sees `state = "complete"` and can use the stored result.
  If a follow-up handoff or barrier ack is required, perform that retryable step using the saved
  result on this path too.
- **Effect succeeded before the process crashed:** the next owner may repeat the effect. Use
  destination idempotency keys, stable output keys, or a transactional destination. A claim is
  not atomic with an external effect and does not guarantee exactly-once processing.
- **Worker continues after losing its lease:** stale completion is rejected, but a token cannot
  cancel an HTTP request or prevent an external write. These random ownership tokens are not
  ordered fencing tokens that an external destination could use to reject stale writes.

[Durable handoffs](queue.md#durable-handoff) protect dispatch to the queue; consumer claims protect
recoverable ownership after delivery. Both still require retryable effects at the destination.
