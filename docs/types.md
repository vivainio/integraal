# Types

Luau is gradually typed, and integraal's `ig` / `igp` API ships with type definitions. Write scripts
with `--!strict` and annotate your handlers: wrong argument types, misspelled functions and
unhandled `nil`s are caught **before** you upload.

```lua
--!strict
--!runtime 1
local function handle(ctx: Context, event: { key: string })
  local body = ig.store.In.get(event.key)   -- string?
  ig.log("info", body)                  -- error: string? is not string
  return {}
end
return { handle = handle }
```

## What is defined

[`types/ig.d.luau`](https://github.com/vivainio/integraal/blob/main/types/ig.d.luau)
declares:

| Name | Kind | Notes |
| --- | --- | --- |
| `ig` | global | core capabilities in the [Native API](native-api.md) with exact signatures |
| `PatternsV1`, `require` | type, global | `require("@native/patterns@1")` returns the typed pattern API |
| `OpsV1`, `QueuePauseState` | types | `require("ops")` / `require("@native/ops@1")` operator commands |
| `Context` | type | `tenant`, `script`, `trigger`, `runtime` – the first handler argument |
| `Decimal`, `D` | type, global | exact base-10 numbers from [mlua-decimal](https://github.com/vivainio/mlua-decimal): `D"12.50"`; bind one to `ig.sql` as a parameter (stored as text) |
| `ByteStream` | type | what `ig.store.<name>.open` and streaming HTTP return |
| `XmlEvent` | type | `ig.xml.events` items |
| `HttpResponse`, `HttpStreamResponse` | type | `ig.http` results; `request{stream=true}` is typed as the stream variant |

`ig.http.request` is overloaded: with `stream = true` the `body` is a `ByteStream`, otherwise a
`string`.

## Checking scripts

Use the [luau-lsp](https://github.com/JohnnyMorganz/luau-lsp) analyzer with the definitions file:

```bash
dev/typecheck.sh                       # every script under examples/
dev/typecheck.sh path/to/script.luau   # specific files
```

which runs:

```bash
luau-lsp analyze --defs=types/ig.d.luau --base-luaurc .luaurc script.luau
```

Put a `.luaurc` next to your scripts so the editor and CI agree:

```json
{ "languageMode": "strict", "lint": { "CommentDirective": false } }
```

(`CommentDirective` is disabled because the analyzer doesn't know `--!runtime`.)

For editor support, point luau-lsp at the definitions file, e.g. in VS Code:

```json
{ "luau-lsp.types.definitionFiles": { "integraal": "types/ig.d.luau" } }
```

## At runtime

The VM **does not type-check**; it strips annotations when compiling, so types cost nothing at
runtime and cannot be used to enforce anything for untrusted tenants. Type checking is a
development and CI tool. Gate your upload pipeline on `dev/typecheck.sh` if you want it enforced.

## Limitations

- Definition files can only expose `declare extern type`, not plain aliases, so `Context` and
  friends are opaque types: you can read their fields, but can't construct them in tests.
- The shape returned from a handler is not constrained; anything JSON-serializable works.
