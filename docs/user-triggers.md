# User triggers

A deployed script may opt in to interactive execution by exporting `user_trigger` metadata and
an `on_user_trigger(ctx, args)` handler. l8l uses the metadata to build a form. The handler can
return an immediate result, enqueue tasks, or start a [durable workflow](workflows.md).

```lua
--!strict
--!runtime 1
local function on_user_trigger(_ctx: Context, args: { name: string, enthusiastic: boolean })
    return { greeting = "Hello, " .. args.name .. (if args.enthusiastic then "!" else ".") }
end

local user_trigger: UserTrigger = {
        title = "Create a greeting",
        description = "Generate a greeting.",
        arguments = {
            { name = "name", label = "Name", type = "string", required = true },
            { name = "enthusiastic", label = "Add an exclamation mark", type = "boolean", default = false },
        },
}
return { user_trigger = user_trigger, on_user_trigger = on_user_trigger }
```

Each argument has a unique `name` (up to 64 ASCII letters, digits or underscores), a nonempty
`label`, and a `type` of `string`, `number`, or `boolean`. `required` defaults to false; an
optional `default` must match the type. Titles and labels are limited to 160 bytes, descriptions
to 1,024 bytes, and there can be at most 32 arguments. String values are limited to 4,096 bytes.
Submitted arguments must be an object; unknown keys, missing required values and wrong types are
rejected. Defaults are applied before calling the handler. `ctx.trigger` is `"user"`.

To read the table, the runtime evaluates the module in a separate restricted VM with a one-second
time limit, an 8 MiB memory limit and no Integraal native APIs. `require` returns an inert table;
imports may be assigned at module scope but must not be called there. Keep metadata literal and
module initialization free of side effects. Normal execution loads the module with tenant
capabilities and validates its exported metadata again before calling the handler. Metadata
inspection and execution may load different revisions of `latest`; select an immutable version
when the form and executed code must match.

## Run locally

Start the [l8l local environment](l8l.md) and click **Run script**. The seeded scripts include
`user-greeting` for an immediate result, `batch-dispatch` for queued work, and `order-pipeline`
for a durable workflow. The latter needs a unique run ID for each distinct run; reusing an ID
is subject to sequence idempotency rules. Refresh flow history to inspect the result. Durable
handoffs can take up to a minute for the local scheduler tick.

The development configuration explicitly sets `local_api.user_triggers` to true. This adds
unauthenticated script listing and trigger execution to the loopback-only local listener; any
local process that can reach it can invoke tenant scripts. The option defaults to false, leaving
that listener read-only. Never expose it outside the trusted host.

## Hosted control plane

The [control plane](control-plane.md) obtains tenant identity from validated JWT claims. It lists
scripts from the tenant's scripts bucket and invokes a separate runtime Lambda to inspect metadata
or run a trigger. The control-plane process does not execute tenant Luau. The reference stack
requires a separate `TriggerScope` for POST requests and an `AccessScope` for listing and metadata.
Only the control plane's IAM role should be permitted to invoke the runtime Lambda for this API.

The HTTP API is synchronous: a successful response means the handler returned, not necessarily
that queued or durable work has completed. A timeout or lost response can leave an uncertain
outcome; make effects idempotent before retrying. User-trigger handlers have the same tenant
permissions and invocation limits as other runtime handlers.
