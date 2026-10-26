# Retry policy

Import `local igp = require("@native/patterns@1")` and call `igp.retry.schedule` after a
retryable failure. This pattern persists a delayed continuation using the existing timer
infrastructure; it does not change engine acknowledgements or catch script errors.
It needs a database, a tenant-enabled queue, and a running timer sweep (currently Fargate's
`system.tick`; Lambda alone does not sweep timers).

```lua
local igp = require("@native/patterns@1")
local function on_queue(ctx, ev)
  local attempt = 1
  local id = ev.request_id
  local input = ev
  if ev.type == "retry" then
    attempt, id, input = ev.attempt, ev.id, ev.event
  end
  local response = ig.http.request({ method = "POST", url = input.url, body = input.body })
  if response.status == 429 or response.status >= 500 then
    local retry = igp.retry.schedule(id, {
      script = "deliver", version = "v1", event = input,
    }, { attempt = attempt, max_attempts = 5, base_delay_s = 2, max_delay_s = 300 })
    if retry.state == "exhausted" then
      -- Persist an application failure or hand it to a failure handler before returning.
      ig.queue.handoff("failed/" .. id, { script = "failed", event = input })
    end
    return retry
  end
  if response.status >= 400 then error("permanent failure: handle explicitly") end
  return response.status
end
return { on_queue = on_queue }
```

Use the destination's idempotency mechanism for effects that may be repeated. The example's
`v1` must name a deployed script version; omit `version` to use the current script.

## API

`schedule(id, message, opts)` returns `{ state = "scheduled", attempt, due_s }` or
`{ state = "exhausted" }`. `id` is a stable business operation id, tenant scoped, 1–200 bytes
without control characters. `message` requires `script`, with optional `version` and JSON `event`.
`group` and `delay_s` are rejected. Delivered events are
`{ type = "retry", id = id, attempt = next_attempt, event = original_event }`.

- `attempt`: required positive integer, the attempt that just failed; initial work is attempt 1.
- `max_attempts`: positive integer, default 5, including initial work. At the limit, nothing is scheduled.
- `base_delay_s`: positive integer, default 1.
- `max_delay_s`: integer at least the base delay, default 300, at most 518400 (six days).
- `jitter`: default true; picks a whole-second delay uniformly from zero through
  `min(max_delay_s, base_delay_s * 2^(attempt-1))`. False uses that upper bound.
- `retry_after_s`: minimum delay, default zero, at most six days. Parse a partner's Retry-After
  response into seconds and pass it here. This floor can exceed `max_delay_s`.

Delivery happens on a sweep after the due time, rather than at an exact second. A scheduled call
reserves three data operations before writing: save intent, read winning intent, register timer.
Exhaustion does not require a database or queue and performs no writes.

## Recovery

Each `(tenant, id, failed attempt)` retains its original payload and due time. Repeating a call
keeps that due time even if the caller computes a different delay or jitter. A different message
for the same key raises an error. Use a new id for a new operation. Intent receipts have no TTL;
operator cleanup is required when retiring operation ids.

Return normally only after scheduling succeeds. A crash between persisting intent and registering
the timer is repaired by redelivery of the original queue message. Scheduling errors must propagate;
calling this from a synchronous handler requires the caller to provide equivalent durable retries.
Once registered, a timer survives worker failure and queue-send failure through the existing sweep.

Delivery is at least once. Repeating a schedule after its timer has fired can register it again;
concurrent attempts and duplicate queue deliveries may repeat work. The attempt limit bounds the
logical retry chain, not transport redeliveries or total external calls. Use consumer claims and
idempotent effects to handle duplicates. This pattern does not enforce a global retry budget,
provide automatic dead-lettering, or retry uncaught failures; ordinary queue redelivery and its
DLQ policy continue to cover those failures. Exhaustion is an explicit application decision.
