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

## `native.zip.entries(source)`
Iterates a zip archive (a string or a stream) in one forward pass, so an archive larger than memory
is fine. Each step yields an entry with `name`, `size` (uncompressed; `nil` when the archive only
records it after the data), `is_dir`, and two ways to get its content:

- `entry:open()` returns a stream of the uncompressed bytes, ready for `native.xml.events`.
- `entry:read()` returns the whole entry as a string, capped by the object size limit.

```lua
for entry in native.zip.entries(native.bucket.In.open("batch.zip")) do
  if not entry.is_dir and entry.name:match("%.xml$") then
    for ev in native.xml.events(entry:open()) do ... end
  end
end
```

- An entry is only readable during its own iteration; once the loop advances, its handles error.
  Entries you do not read are skipped. An entry can be opened or read once.
- The archive is read from its local headers, since a stream cannot seek to the central directory.
  Stored and deflated entries (including streamed ones with trailing data descriptors) are
  supported; encrypted entries and other compression methods error.
- CRC and size are verified when an entry is read to its end. A truncated archive errors.
- Total uncompressed output is capped by `max_stream_mb`, which also guards against zip bombs.
