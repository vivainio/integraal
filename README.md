# integraal

Luau script runtime for AWS Lambda and Fargate. Tenant scripts live in S3, are cached, and run
in a sandboxed Luau VM with a versioned native API.

```lua
--!strict
--!runtime 1
local function handle(ctx: Context, event: any)
  native.log("info", "hello from " .. ctx.tenant)
  return { trigger = ctx.trigger, echo = event }
end

return { handle = handle }
```

## Features
- **One script, three triggers:** direct Lambda invoke, HTTP via ALB, SQS. Untargeted messages go through a per-tenant router script.
- **Tenant isolation:** fresh VM per invocation, data confined to the tenant's prefix, operator-owned policy and limits.
- **Versioned native API** (`--!runtime N`):
  - storage: S3-style stores, host directories, DynamoDB, SQLite scratch databases
  - coordination: locks, deadlines, barriers, queue sends, signed callback URLs
  - batch jobs through a partner bucket
  - allow-listed HTTP, streaming XML / zip / gzip, crypto
- **Typed:** `--!strict` scripts checked against `types/native.d.luau`.

## Documentation
Read the book: **<https://vivainio.github.io/integraal/>**
(sources in [`docs/`](docs/index.md)).
