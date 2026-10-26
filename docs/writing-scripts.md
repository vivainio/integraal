# Writing scripts

A script is a Luau module that **returns a table of handlers**.

```lua
--!strict
--!runtime 1

local function on_queue(ctx: Context, event: { id: string })
  return { queued = true, id = event.id }
end

local function handle(ctx: Context, _event: any)  -- fallback for every other trigger
  return { trigger = ctx.trigger }
end

return { on_queue = on_queue, handle = handle }
```

!!! tip "Use Luau types"
    Start every script with `--!strict` and annotate handler parameters. integraal ships
    definitions for the `ig` / `igp` API and `Context`, so the type checker catches wrong calls
    before you upload. See [Types](types.md).

## Handler dispatch

| Trigger | Tried in order |
| --- | --- |
| `invoke` | `on_invoke`, `handle` |
| `http` | `on_http`, `handle` |
| `queue` | `on_queue`, `handle` |
| `route` (router scripts only) | `on_route`, `route` |

If none exists the invocation fails with a *no handler* error.

## Arguments

`handler(ctx, event)`:

- `ctx.tenant`, `ctx.script` – who and what is running
- `ctx.trigger` – `"invoke"`, `"http"`, `"queue"`, or `"route"` (router scripts)
- `ctx.runtime` – the pinned native API version
- `ctx.attempt` – which delivery of a queue message this is (`1` the first, then counting up as it is retried); `1` for other triggers
- `event` – the JSON `event` of the envelope, converted to Lua values

## Return value

Return any value that is JSON-serializable. It is converted back to JSON and sent to the caller.
A Lua error (or `error(...)`) fails the invocation; see [Triggers](triggers.md) for how each
trigger reports that.

## Pinning the runtime

The leading `--!` comment lines may contain `--!runtime N` next to `--!strict`. Without it the default (currently `1`)
applies. Unknown versions fail the invocation. Existing wrapper versions are only ever extended.

## Versions

Scripts live at `<tenant>/<script>/<version>.luau`. Callers that omit `version` get
`latest`. Publish an immutable `v7.luau` next to `latest.luau` for pinned, never-revalidated
deployments.

## Modules

`require` loads shared code into the script's VM. There are three sources, picked by prefix:

```lua
local dates = require("@lib/util/dates")     -- the tenant's own lib/ folder
local csv   = require("@shared/csv@2.1.0")   -- global shared library, pinned version
local igp   = require("@native/patterns@1")  -- built-in patterns, explicit API version
```

| Spec | Object in the script bucket | Caching |
| --- | --- | --- |
| `@native/patterns@1` | built into the runtime; no storage fetch | one table per invocation |
| `@lib/<path>` | `<tenant>/lib/<path>.luau` | revalidated like `latest` scripts |
| `@shared/<name>@<version>` | `@shared/<name>/<version>.luau` | immutable, cached forever |

- A module is a chunk that returns a value (normally a table). It runs once per invocation; later
  `require`s of the same module return the same value. Modules can `require` other modules.
- `@lib` is confined to the calling tenant. The name `lib` is reserved and cannot be a script name.
- Shared modules always need an exact version (`latest` is refused), so an operator can publish
  `csv@2.2.0` without changing what existing scripts run. Only operators write to `@shared/`.
- Circular requires fail, and one invocation may load at most 64 modules. Modules count against the
  invocation's time and memory limits and see the same core API and native module instances as the script.
- Paths use `/`-separated names of letters, digits, `-`, `_` and `.`; `..` is not allowed.

## Sandbox facts

- `os` and `io` are not available; everything external goes through `ig` and `igp`. `require` only loads modules (see [Modules](#modules)).
- Globals are read-only; keep state in locals.
- Each invocation starts with an empty VM, so there is no state between calls. Persist with
  `ig.store.Out` (or another store your tenant is granted).

## Example: parse a large XML feed

```lua
--!strict
--!runtime 1
local function handle(ctx: Context, event: { key: string })
  local stream = ig.store.In.open(event.key)
  if not stream then
    return { entries = 0, found = false }
  end

  local entries = 0
  for ev in ig.xml.events(stream, { trim = true }) do
    if ev.type == "start" and ev.tag == "entry" then
      entries += 1
    end
  end
  return { entries = entries, found = true }
end

return { on_queue = handle, handle = handle }
```
