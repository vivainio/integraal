# Streams and XML

## Streams
`native.bucket.<bucket>.open` and `native.http.request{stream=true}` return a byte stream. A stream is
consumed by the first parser that takes it; `stream:close()` releases it early. Streams are read
in chunks, so a multi-gigabyte object never sits in memory (limited by `max_stream_mb`, see
[Limits](limits.md)).

## `native.xml.events(source, opts?)`
A **pull parser** over a string or a stream. Returns an iterator of events:

| `type` | fields |
| --- | --- |
| `start` | `tag`, `attrs` (table), `depth` |
| `end` | `tag`, `depth` |
| `text` | `text`, `depth` |
| `cdata` | `text`, `depth` |
| `comment` | `text`, `depth` |

```lua
for ev in native.xml.events(stream, { trim = true }) do
  if ev.type == "start" and ev.tag == "item" then ... end
end
```

In `--!strict` code `tag`, `attrs` and `text` are optional (`string?`); they are always present for
the event types listed above, so check `ev.type` first.

- `trim = true` trims text runs and drops whitespace-only ones.
- Adjacent text and entity references (`&amp;`, `&#65;`) arrive as one `text` event.
- `<a/>` yields a `start` followed by an `end`.
- DTDs and custom entities are rejected; nesting is limited to 256 levels.
- `break` out of the loop to stop early.

For a full example see [Writing scripts](writing-scripts.md#example-parse-a-large-xml-feed).
