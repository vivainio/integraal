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

## `native.xml.parse(source, opts?)`
Builds an in-memory tree from a string or stream and returns the **document node**. Use it for
documents that fit in memory (partner messages, small feeds), where the pull parser is too low
level; `trim = true` drops whitespace-only text and trims the rest, as for `events`. The whole
document is read first, capped by the object size limit (see [Limits](limits.md)); DTDs are
rejected and nesting is limited to 256 levels. The tree and XPath engine are the
[`uppsala`](https://crates.io/crates/uppsala) crate.

```lua
local doc = native.xml.parse(body, { trim = true })
for _, item in ipairs(doc:xpath("/feed/item[@kind='a']")) do
  native.log("info", item:attr("id") .. " " .. item:child("name"):text())
end
local total = doc:xpath("sum(//price)")
```

| node method | |
| --- | --- |
| `kind()` | `"document"`, `"element"`, `"text"` (also CDATA), `"comment"`, `"processing-instruction"` or `"attribute"` |
| `name()`, `local_name()`, `namespace()` | qualified name, name without prefix, resolved namespace URI (`""` if none) |
| `text()` | all descendant text, concatenated (the attribute value for attributes) |
| `attr(name)`, `attrs()` | an attribute value or `nil` by qualified name; all attributes as a table (namespace declarations are not attributes) |
| `children(name?)`, `child(name)` | element children, optionally by qualified name; the first one with that name |
| `root()`, `parent()` | the document element; the parent node |
| `xpath(expr, ns?)` | evaluates an XPath 1.0 expression relative to the node |
| `first(expr, ns?)` | like `xpath`, but a node-set yields only its first node (or `nil`) |
| `xml()` | serializes the node and its subtree |

Nodes compare with `==`. `xpath` returns an array of nodes in document order for a node-set, and a
Lua string, number or boolean for other results.

XPath 1.0 is evaluated strictly. In particular an unprefixed name test only matches elements in **no
namespace**, so for a document with a default namespace (`<feed xmlns="urn:f">`) bind a prefix of your
choice with `ns` and use it: `doc:xpath("//f:item", { f = "urn:f" })`. Prefixes declared on the
document element can be used as written, and `*[local-name()='item']` ignores namespaces entirely.
Evaluation has a node-visit budget, so a pathological expression fails with an error instead of
running for long. XPath variables (`$name`) cannot be bound yet, so build expressions from trusted
text only and compare returned values in Lua.

## `native.xml.writer(sink?, opts?)`
Generates XML. Text and attribute values are escaped, names and characters are validated and the
writer refuses to produce anything but a well-formed document (balanced tags, one root). With a
sink from `native.bucket.<name>.create(key)` it streams straight into a bucket; with no sink the
document is built in memory, capped at the object size limit, and `finish()` returns it.

| method | |
| --- | --- |
| `w:start(tag, attrs?)` | opens an element; attributes are written in name order |
| `w:close()` | closes the innermost open element (`<a/>` when it is empty) |
| `w:text(s)` / `w:cdata(s)` / `w:comment(s)` | content; `]]>` inside CDATA is split safely; a comment cannot contain `--` |
| `w:element(tag, attrs?, text?)` | a complete element with optional text |
| `w:finish()` | checks that every element is closed; returns the document for in-memory writers |

Options: `declaration = false` omits the `<?xml version="1.0" encoding="UTF-8"?>` line;
`indent = 2` pretty-prints (elements with text content are kept inline).

```lua
local w = native.xml.writer({ indent = 2 })
w:start("orders", { xmlns = "urn:acme:orders" })
for _, o in ipairs(orders) do
  w:start("order", { id = o.id })
  w:element("customer", nil, o.customer)   -- "A & B" is written as A &amp; B
  w:close()
end
w:close()
local xml = w:finish()

-- or stream a large export into a bucket
local out = native.bucket.Out.create("orders.xml")
local w2 = native.xml.writer(out)
... w2:finish()
out:close()
```
Namespaces are not tracked: declare them with `xmlns` / `xmlns:p` attributes and write prefixed names
yourself. Characters XML forbids (most control characters) raise an error rather than being dropped.

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

## `native.zip.writer(sink?)`
Writes a zip archive in one forward pass. Pass a sink from `native.bucket.<name>.create(key)` to
stream straight into a bucket (see [Buckets](buckets.md#streaming-uploads)); with no sink the archive
is built in memory, capped at the object size limit, and `finish()` returns it as a string.

| method | |
| --- | --- |
| `z:add(name, content)` | writes a whole entry from a string or a stream (e.g. `bucket.open(k)` or `entry:open()`) |
| `z:add(name)` then `z:write(data)` | an entry built from several writes; the next `add` or `finish` closes it |
| `z:finish()` | writes the central directory; returns the archive for in-memory writers |

With a sink, `sink:close()` after `finish()` commits the upload. Entries are deflated, with sizes in
trailing data descriptors (zip64), so any size works and nothing needs to be known up front.

## `native.gzip`
| function | |
| --- | --- |
| `native.gzip.open(source)` | stream of the decompressed bytes of a gzip string or stream; multi-member files work |
| `native.gzip.writer(sink?)` | `w:write(data)`, `w:finish()`; compresses into a bucket sink, or into memory when there is none (`finish()` then returns the data) |
| `native.gzip.compress(s)` / `decompress(s)` | whole strings, capped at the object size limit |

```lua
-- .xml.gz in a bucket, parsed without ever holding it in memory
for ev in native.xml.events(native.gzip.open(native.bucket.In.open("feed.xml.gz"))) do ... end

-- compressed export
local out = native.bucket.Out.create("export.csv.gz")
local gz = native.gzip.writer(out)
gz:write("a,b\n1,2\n")
gz:finish()
out:close()
```
Decompressed output counts against `max_stream_mb`, which guards against decompression bombs. Corrupt
data or a bad checksum raises an error.
