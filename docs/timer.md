# Durable timers (`ig.timer`)

Schedule delayed follow-ups, timeout checks, or reconciliation without creating a barrier.
Timers need the deployment's `db`, a queue, tenant `"queue": true`, and the minute operator
schedule. Each call uses one data operation. Timers retain the scheduling invocation's trace.

```lua
local due = ig.timer.after("order/42/check", 3600, {
  script = "check-order", version = "v1", event = { order_id = 42 },
})
-- Save due with the order if another invocation may cancel it.
ig.timer.cancel("order/42/check", due)

ig.timer.at("nightly/reconcile", 1793059200, {
  script = "reconcile", event = { type = "reconcile" },
})
```

- `ig.timer.at(id, due_s, message) -> due_s`: schedule at Unix seconds.
- `ig.timer.after(id, delay_s, message) -> due_s`: schedule relative to the current time, in seconds.
- `ig.timer.cancel(id, due_s)`: remove that exact pending timer; repeated cancellation succeeds.

The message has `script?`, `version?`, and `event`, as with [queue messages](queue.md).
Omitting `script` uses the tenant router. `delay_s` and `group` are unsupported. The normal
200 KB envelope limit applies. Timers are always scoped to the calling tenant; their IDs are
separate from barrier and retry timers. IDs are 1–293 bytes with no control characters.

The scheduling horizon is six days. Past due times and zero delays are eligible on the next
sweep. Delivery happens on a minute sweep and can be later than the due time. Timers fire at
least once: make handlers safe to repeat. Cancellation cannot recall a message already being
sent or queued.

The identity is `(id, due_s)`: scheduling the same pair replaces its pending message, while
using the same id at a different time creates another timer. Preserve the returned due time
for cancellation. Repeating `after` later computes a new due time; for stable scheduling across
retries, save the timestamp and use `at`. Timers persist even when the scheduling script later
fails. Undelivered rows are retained for recovery for 30 days after their due time.
