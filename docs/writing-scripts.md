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
    definitions for the `native` API and `Context`, so the type checker catches wrong calls
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

## Sandbox facts

- `os`, `io` and `require` are not available; everything external goes through `native`.
- Globals are read-only; keep state in locals.
- Each invocation starts with an empty VM, so there is no state between calls. Persist with
  `native.data.out` (or another bucket your tenant is granted).

## Example: parse a large XML feed

```lua
--!strict
--!runtime 1
local function handle(ctx: Context, event: { key: string })
  local stream = native.data["in"].open(event.key)
  if not stream then
    return { entries = 0, found = false }
  end

  local entries = 0
  for ev in native.xml.events(stream, { trim = true }) do
    if ev.type == "start" and ev.tag == "entry" then
      entries += 1
    end
  end
  return { entries = entries, found = true }
end

return { on_queue = handle, handle = handle }
```
