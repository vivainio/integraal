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

## Workflow resumes

[Durable workflow](workflows.md) starts and resumes name the saved script and explicit version,
so they bypass the router and call the target's `workflow` entrypoint. Ordinary incoming events can
still go through the router to a handler that starts a workflow or signals a waiting one.

## What gets routed
- Any envelope without a `script`: queue messages, `POST /v1/{tenant}` over HTTP, direct invokes.
- [S3 notifications](triggers.md#s3-notifications), which never name a script.
- [Callbacks](callbacks.md) created without `script`.
- [Operator ticks](schedules.md#operator-ticks) (`type = "schedule"`) and barrier events
  (`barrier.complete`, `barrier.overdue`) unless the barrier names a script. Match on `event.type`;
  see [event types](schedules.md#event-types).
- A [batch job](batch.md#wiring-the-continuation)'s `done.json`, which the router hands to `igp.batch.route`.

## Optional route capabilities

A router can **drop privileges of the executing Luau sandbox** before handing control to
its selected handler. This is a second guard against script mistakes: tenant grants remain
the maximum authority, while the route restricts the operations and paths the handler can use.

```lua
local function on_route(ctx: Context, event: { key: string }): RouteDecision
    return {
        script = "parse-order",
        capabilities = {
            stores = {
                In = { read = { event.key } },
                Out = { write_prefixes = { "reports/" } },
            },
        },
    }
end
return { on_route = on_route }
```

Omitting `capabilities` preserves existing behavior. When supplied, omitted stores and operations
are denied; `capabilities = {}` denies all store access. The router itself runs with tenant grants;
the runtime locks the restrictions before executing the target, and the handler cannot widen them.
Capabilities never grant access that the tenant does not already have.

Each store accepts exact-key lists `read`, `write`, `delete`, `upload`, `download` and corresponding
`read_prefixes`, `write_prefixes`, `delete_prefixes`, `upload_prefixes`, `download_prefixes` lists.
Keys and prefixes are relative to that tenant's root in that store. Prefixes must end with `/`;
an empty prefix permits the entire store for that operation. There are no glob patterns.
`read` covers `get` and `open`; listing requires a `read_prefixes` entry covering the requested
listing prefix. A broad listing is rejected rather than exposing other keys. `write` covers
`put` and `create`; `upload` and `download` separately guard presigned URLs. Streaming and
helper operations using these stores are subject to the same guard.

These restrictions cover the selected handler and modules it calls, including post-handler object
deletion, and are inherited by continuations. Other APIs such as HTTP, database and coordination
APIs are not restricted by this store capability declaration.

The Luau definitions expose `RouteDecision`, `RouteCapabilities` and `StoreCapabilities` for
strict checking of router declarations.

## Dropping privileges in continuations

Continuations inherit the current store capabilities automatically. Supply an optional
`capabilities` declaration to restrict them further:

```lua
ig.queue.send({
    script = "send-report",
    event = { key = report_key },
    capabilities = {
        stores = { Out = { read = { report_key } } },
    },
})
```

The effective child permissions are the **intersection** of the parent's permissions and the
new declaration. Omitting the declaration inherits the parent's guard; `{}` denies all store
access. An unrestricted parent can establish the first guard. A child cannot restore privileges
its parent dropped: a write-only parent cannot delegate read access to its child.

Effective capabilities travel as `capabilities` in the SQS envelope, alongside `tenant`, `script`
and `event`. The receiving runtime applies them **before loading the target script**, even when
an explicit script bypasses the router. When a continuation does use the router, its route guard
can narrow the inherited capabilities further. Tenant grants still bound every store operation.
This is a guard against programming mistakes, not authentication of queue message producers.

The same inheritance applies to `ig.queue.handoff`, `ig.timer.at` / `after`, `igp.retry.schedule`,
`igp.callback.create`, `igp.barrier.create`, batch continuations and durable workflows. Their
records save the effective capabilities and restore them when dispatching work. Old records and
messages without capabilities retain their existing behavior.

Queue, handoff, timer and retry message tables accept `capabilities`. Callback, barrier and
`igp.sequence.start` option tables accept it too. A barrier uses the guard for both completion and
overdue handlers. For batch jobs use `job:continue_with(script, version, capabilities)`; omit
`version` with `nil` if needed. `igp.batch.route` restores saved capabilities in its route result.

Workflows persist their guard with their state and carry it through sleeps, yields and signals.
Signals intersect the sender's restrictions with the workflow's saved restrictions; later resumes
retain any further reduction. Called modules run under the current sandbox guard.
