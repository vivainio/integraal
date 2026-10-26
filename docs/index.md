# integraal

integraal runs **tenant-specific [Luau](https://luau.org) scripts** on AWS Lambda and Fargate.
Scripts are stored in S3, downloaded on demand, cached, and executed in a sandboxed VM that
exposes a **versioned native API** (S3 data access, allowlisted HTTP, streaming XML, and more).

```lua
--!strict
--!runtime 1
local function handle(ctx: Context, event: any)
  ig.log("info", "hello from " .. ctx.tenant)
  return { trigger = ctx.trigger, echo = event }
end

return { handle = handle }
```

## What you get

- **One script, three triggers.** The same script serves direct Lambda invokes, HTTP via an ALB,
  and SQS messages. See [Triggers](triggers.md).
- **Tenant isolation by construction.** Each invocation gets a fresh VM, a data scope confined to
  the tenant's own S3 prefix, and an HTTP allowlist the tenant cannot edit.
- **Typed.** The `ig` / `igp` API ships Luau type definitions; scripts are written in `--!strict`
  and checked before upload. See [Types](types.md).
- **Versioned wrappers.** Scripts pin the native API with `--!runtime N`, so new APIs ship without
  breaking existing tenants.
- **Operator-controlled limits.** Time, memory and call budgets are set per tenant and clamped by
  operator ceilings. See [Tenant config](tenant-config.md).
- **Streaming.** Large S3 objects and HTTP bodies are read as streams and parsed with a pull
  parser, so memory is bounded by one XML event, not the file.

## Where to start

| I want to… | Read |
| --- | --- |
| understand how the pieces fit | [Architecture](architecture.md) |
| write my first script | [Writing scripts](writing-scripts.md) |
| look up a native function | [Native API](native-api.md) |
| configure a tenant | [Tenant config](tenant-config.md) |
| work with S3 data, DynamoDB, crypto, HTTP or routing | [Native API](native-api.md) chapters |
| run it locally | [Local development](local-development.md) |
