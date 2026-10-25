# Router

A typical deployment has **one shared queue** whose messages carry only the tenant and an event.
The message does not name a script; the tenant's **router script** decides.

## Tenant config: `router`
Name of the script whose `on_route` handler chooses a script for messages that don't name one.

```json
{ "router": "router" }
```

## Flow

```
queue message {tenant, event}
        │
        ▼
 <tenant>/config.json  →  "router": "router"
        │
        ▼
 router.on_route(ctx, event)  →  "parse-order"   (or { script, version?, event? })
        │            same VM, same deadline and call budgets
        ▼
 parse-order.on_queue(ctx, event)
```

## Router script

```lua
--!strict
--!runtime 1
local function on_route(ctx: Context, event: { key: string? })
  local key = event.key
  if key and string.sub(key, 1, 16) == "incoming/orders/" then
    return "parse-order"
  end
  return { script = "fallback", event = { original = event } }
end
return { on_route = on_route }
```

- The router's handler is `on_route` (or `route`) and it runs with `ctx.trigger == "route"`.
- Return a **script name**, or a table `{ script, version?, event? }` to pin a version or rewrite
  the event the target receives.
- The target then runs with the *original* trigger (`queue`, `http`, …).
- The router and the target run in **one Luau VM**: one native-API install, one heap, one deadline,
  one set of call budgets. If the target pins a different `--!runtime` version than the router, it
  gets a fresh VM with the remaining time instead.
- A router cannot route to another router. With no `router` configured, a message that names no
  script fails with a *no route* error (queue: retried, then dead-lettered).

## What gets routed
- Any envelope without a `script`: queue messages, `POST /v1/{tenant}` over HTTP, direct invokes.
- [S3 notifications](triggers.md#s3-notifications), which never name a script.
- [Callbacks](callbacks.md) created without `script`.
- [Operator ticks](schedules.md#operator-ticks) (`type = "schedule"`) and barrier events
  (`barrier.complete`, `barrier.overdue`) unless the barrier names a script. Match on `event.type`;
  see [event types](schedules.md#event-types).
- A [batch job](batch.md#wiring-the-continuation)'s `done.json`, which the router hands to `native.batch.route`.
