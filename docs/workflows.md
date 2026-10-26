# Durable workflows

`igp.sequence` runs a long sequence across queue invocations. Write one `workflow(seq, input)`
function; on each resume the runtime calls it from the beginning and returns saved results for
completed steps. Local variables are reconstructed by replay, rather than saved as a VM snapshot.

Workflows need the deployment's database, a queue, tenant `"queue": true`, and the minute
operator tick that dispatches durable handoffs and timers. Keep queue retries and dead-letter
handling enabled: uncaught failures rely on normal queue redelivery.

## Write and launch a workflow

Deploy this as `acme/order-workflow/v1.luau`:

```lua
--!strict
--!runtime 1
local function workflow(seq: WorkflowSequence, input: { order_id: string })
    local order = seq:step("load", function()
        return ig.db.get("orders", input.order_id)
    end)

    local approval = seq:wait("approval")
    seq:sleep("settlement-delay", 60)

    return seq:step("save", function()
        ig.db.put("approved-orders", input.order_id, {
            order = order, approval = approval,
        })
        return { order_id = input.order_id, approved = true }
    end)
end

return { workflow = workflow }
```

Launch from an ordinary invoke, HTTP, queue, or router-selected handler:

```lua
local igp = require("@native/patterns@1")
return igp.sequence.start("order/42", {
    script = "order-workflow", version = "v1", input = { order_id = "42" },
})
```

Launch is asynchronous. `start` persists a durable queue handoff and returns `true` for a new
launch, `false` for an identical repeated launch. Reusing the id with different script, version or
input raises. Choose a new id when intentionally running the same business operation again.

Both starts and resumes name their target script and version, so they bypass `router.on_route`.
The runtime calls the script's `workflow` export directly; no `on_queue` wrapper is required for
sequence messages. Normal events still use the existing trigger handlers. Queue opt-in, tenant
permissions, deadlines, and call budgets apply on every invocation.

`version` is required and cannot be `latest`. Keep the immutable version deployed for the life
of the workflow. Mutable `@lib` imports and live configuration are not snapshotted: use pinned
shared modules and saved step results for decisions that must remain stable across replay.

## Steps and suspension

| API | Behavior |
| --- | --- |
| `seq.id` | The tenant-scoped workflow run id. |
| `seq:key(name)` | Stable tenant/run/name-derived idempotency key for external effects. |
| `seq:step(name, fn)` | Execute `fn()` once a result can be checkpointed; on replay return that saved JSON result without calling `fn`. One return value, including nil, is supported. |
| `seq:sleep(name, delay_s)` | Save an absolute due time, arrange a timer resume, and release the worker. Replay preserves the original due time. Maximum six days per sleep. |
| `seq:wait(name)` | Release the worker until a named signal exists, then return its saved JSON value. A signal received before reaching the wait is retained. |
| `seq:yield(name)` | Arrange a durable resume and release the worker once at this position. On replay continue past it. |

Names must be stable, unique across all step kinds within the workflow, 1–200 bytes without
control characters. Include an item id in loop step names, for example `"invoice/" .. invoice.id`.
Steps cannot be nested. `seq:key` may be used inside a step.

The runtime also yields before new steps after 25 journal writes in one invocation, or when less
than one second remains after making progress. Replays page through saved step results. This
bounds new work per invocation; it cannot interrupt an indivisible step before its deadline.
Replay itself must still fit the invocation's time and data-operation budgets.

Suspension unwinds the workflow and returns `{ state = "suspended" }` internally. Do not catch
suspension with `pcall`; it is control flow, and the runtime will still mark that invocation as
suspended if caught. Successful completion stores the result and returns
`{ state = "complete", result = ... }`; duplicate deliveries return the saved completion.

## Signals, callbacks, jobs, and barriers

An event adapter delivers a named value:

```lua
igp.sequence.signal("order/42", "approval", { approved_by = "alice" })
```

`signal` looks up the workflow's saved target and creates a durable handoff. It returns `true` for
a new signal intent, `false` for an identical repeated one; a conflicting value for the same
run/name raises. It becomes visible when the queued signal is processed. The workflow must have
processed its start message first; a signal before then raises a retryable error. Signals are
one-shot: use a different name for each expected event or iteration. `wait` has no built-in timeout
or correlation-id lookup; adapters map external ids to the run id and signal name.

A callback handler can call `signal` with the callback's saved run id. A batch completion adapter
can hydrate the job and signal its result. A barrier completion adapter can signal the barrier
event so the workflow can hydrate and gather its members after resuming. Existing callback,
batch, and barrier APIs retain their ordinary routing and retention behavior.

These adapters can live in the **same script file** as the workflow. The complete runnable example
at [`examples/acme/order-workflow/v1.luau`](https://github.com/vivainio/integraal/blob/main/examples/acme/order-workflow/v1.luau)
contains:

1. `on_invoke`, which launches the workflow.
2. `workflow`, which creates a barrier, dispatches item workers, waits, and gathers results.
3. `on_queue`, which handles item work and signals the workflow on barrier completion.

Deploy that file as both `v1.luau` and `latest.luau`: the existing barrier continuation names a
script without a version, so its adapter runs at `latest`; `signal` then resumes the pinned
workflow version. Keep the adapter compatible with in-flight workflows. No separate finalizer
file is needed. Invoke its `on_invoke` with `{ "id": "demo/1", "items": ["a", "b", "c"] }`.

## Recovery and limits

- Launches, signals, and explicit/automatic yields use the existing durable handoff sweep;
  sleeps use the timer sweep. Delivery is at least once and may be delayed until the next tick.
- One invocation owns a workflow for up to 15 minutes. Journal writes atomically compare the
  control row and save a step, so a replaced owner cannot checkpoint or complete the workflow.
  Normal returns and caught runtime errors release ownership. Hard crashes leave the lease to
  expire; configure queue retries to cover this window. Busy deliveries raise, keeping them
  eligible for retry. A lease cannot cancel an external request from a stalled worker.
- An uncaught step failure saves no result for that step. Redelivery replays earlier results and
  retries the unfinished step. There is no automatic backoff policy or failure compensation in
  this API; ordinary queue retry/DLQ handling applies.
- **Effects must be safe to repeat.** A crash after an external effect succeeds but before its
  result is saved can repeat the effect. Pass `seq:key(name)` to a destination's idempotency
  mechanism, or use stable output keys/transactional writes.
- Code outside steps runs again, including module initialization and workflow control flow.
  Put effects, clock reads, and random choices inside steps; drive branches from saved results.
- Each persisted JSON record is at most 64 KB. Input and final result share the control record;
  their combined serialized size, including metadata, must fit. Store large values in object
  storage and return references. Private sequence rows have no TTL; retiring run ids requires
  operator cleanup. The result data owned by a barrier, callback, or batch job can still expire
  under that primitive's retention policy.
- Sequence events reserve `type = "sequence.start"`, `"sequence.resume"`, and `"sequence.signal"`
  for queue dispatch. Handlers should use other event types for ordinary application events.
