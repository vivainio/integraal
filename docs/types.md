# Types

Luau is gradually typed, and integraal's `native` API ships with type definitions. Write scripts
with `--!strict` and annotate your handlers: wrong argument types, misspelled functions and
unhandled `nil`s are caught **before** you upload.

```lua
--!strict
--!runtime 1
local function handle(ctx: Context, event: { key: string })
  local body = native.data.get(event.key)   -- string?
  native.log("info", body)                  -- error: string? is not string
  return {}
end
return { handle = handle }
```

## What is defined

[`types/native.d.luau`](https://github.com/vivainio/integraal/blob/main/types/native.d.luau)
declares:

| Name | Kind | Notes |
| --- | --- | --- |
| `native` | global | every function in the [Native API](native-api.md) with exact signatures |
| `Context` | type | `tenant`, `script`, `trigger`, `runtime` – the first handler argument |
| `ByteStream` | type | what `native.data.open` and streaming HTTP return |
| `XmlEvent` | type | `native.xml.events` items |
| `HttpResponse`, `HttpStreamResponse` | type | `native.http` results; `request{stream=true}` is typed as the stream variant |

`native.http.request` is overloaded: with `stream = true` the `body` is a `ByteStream`, otherwise a
`string`.

## Checking scripts

Use the [luau-lsp](https://github.com/JohnnyMorganz/luau-lsp) analyzer with the definitions file:

```bash
dev/typecheck.sh                       # every script under examples/
dev/typecheck.sh path/to/script.luau   # specific files
```

which runs:

```bash
luau-lsp analyze --defs=types/native.d.luau --base-luaurc .luaurc script.luau
```

Put a `.luaurc` next to your scripts so the editor and CI agree:

```json
{ "languageMode": "strict", "lint": { "CommentDirective": false } }
```

(`CommentDirective` is disabled because the analyzer doesn't know `--!runtime`.)

For editor support, point luau-lsp at the definitions file, e.g. in VS Code:

```json
{ "luau-lsp.types.definitionFiles": { "integraal": "types/native.d.luau" } }
```

## At runtime

The VM **does not type-check**; it strips annotations when compiling, so types cost nothing at
runtime and cannot be used to enforce anything for untrusted tenants. Type checking is a
development and CI tool. Gate your upload pipeline on `dev/typecheck.sh` if you want it enforced.

## Limitations

- Definition files can only expose `declare extern type`, not plain aliases, so `Context` and
  friends are opaque types: you can read their fields, but can't construct them in tests.
- The shape returned from a handler is not constrained; anything JSON-serializable works.
