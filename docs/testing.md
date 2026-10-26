# Testing Luau scripts

`integraal-test` runs user-written Luau tests locally. It loads scripts from disk and uses the real
native wrappers: S3, DynamoDB and SQS calls go to your running roto instance. Outbound HTTP is
intercepted in the test VM, with responses supplied by the test.

Start roto and provision the resources named in your deployment configuration (see
[Local development](local-development.md)). Then run:

```bash
export AWS_ENDPOINT_URL=http://localhost:5070
export AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test AWS_REGION=us-east-1
cargo run -p integraal-testing --bin integraal-test -- examples/acme dev/local.json
```

The first argument is a tenant directory containing `config.json`, scripts and `tests/`. The second
is the deployment configuration. The runner recursively finds `tests/**/*.test.luau`. It prints
PASS/FAIL per case and returns 0 on success, 1 for failing cases, or 2 for setup errors or no cases.
It requires an explicit localhost roto endpoint.

```lua
-- tests/orders.test.luau
test.case("submits an order", function()
    test.http("POST", "https://partner.example/orders", function(request)
        test.equal(native.json.decode(request.body), { sku = "ABC" })
        return { status = 201, headers = {}, body = '{"id":"42"}' }
    end)

    local orders = test.load("orders/latest.luau")
    local result = orders.handle({ tenant = "acme", trigger = "invoke" }, { sku = "ABC" })
    test.equal(result.id, "42")
    test.equal(#test.http_calls, 1)
end)
```

For editor/typechecker support, load `types/testing.d.luau` alongside `types/native.d.luau`.

## Assertions and mocks

- `test.case(name, fn)` registers a case. Ordinary Luau `assert` also works.
- `test.equal(actual, expected)` compares values recursively, including tables.
- `test.throws(fn, substring?)` checks that a function throws, optionally matching a literal substring.
- `test.load(path)` evaluates a script relative to the tenant directory and returns its exports.
- `test.http(method, url, response)` matches an exact method and URL. The response can be a table
  or a function receiving `{ method, url, headers, body }`. Registrations can serve repeated requests;
  the first matching registration wins. Use a function with a counter for response sequences or errors.
- `test.http_calls` records all requests, including unmatched ones. Both `native.http.get` and
  `native.http.request` use the same interceptor. Unmatched calls throw without sending traffic.
- `test.mock("native.time.now_ms", function() return 1234 end)` replaces another native function
  and returns a spy. `spy.calls` contains packed argument tables (`.n` preserves nil arguments).
  Set up mocks before loading the script under test.

HTTP fixtures supply the native response shape (`status`, `headers`, `body`). The interceptor
covers buffered HTTP; it does not emulate streaming responses, vault expansion, allowlist checks,
HTTP budgets or background batch HTTP jobs. Those need integration tests.

## Acknowledging uploaded documents

`test.peer` plays the receiving side of the [batch protocol](batch.md). The script under test
submits a job with document inputs; the test reads those documents, writes an acknowledgment and
passes the completion event to the real router and continuation.

```lua
local submitted = test.load("upload/latest.luau").handle(ctx, {
    body = '<invoice id="42"/>', reference = "INV-42",
})
local document = test.peer.receive(submitted.job, "document")
test.equal(document.body, '<invoice id="42"/>')

local event = test.peer.ack(document, {
    results = { receipt = { { accepted = true } } },
})
local routed = native.batch.route(event)
assert(routed and routed.script)
local continuation = test.load(routed.script .. "/latest.luau")
local completed = continuation.on_queue(queue_ctx, routed.event)
test.equal(completed.receipts, { { accepted = true } })
```

- `test.peer.receive(job, input?, part?)` reads the submitted request from roto. With an input name,
  it reads that input's part (default 1) and verifies its size and SHA-256 when present. Without an
  input name, `body` is the request JSON. The returned document has `job`, `key` (relative to the job
  folder), `body` (bytes as a Luau string), and `request` (decoded JSON).
- `test.peer.ack(document, options?)` writes query results and **`done.json` last**, then returns an
  `s3.object` completion event with `store = "$batch"`. `options.results` maps each query name to
  an array of rows (written as NDJSON), or `{ body = "..." }` for a raw result document. If supplied,
  results must cover every query with no extra names. Omit options for empty results for every query.
- `test.peer.ack(document, { ok = false, error = "invalid invoice" })` reports a failure. The real
  `native.batch.hydrate(event, { allow_failure = true })` exposes it to the continuation.

Acknowledgments include part sizes and SHA-256 hashes, which the real result reader verifies.
Invalid results fail before any response objects are written. A job that already has `done.json`
cannot be acknowledged again. Multipart inputs are read with separate `receive(job, input, part)`
calls; an acknowledgment completes the whole job.

The helpers require tenant `"batch": true` and a deployment `batch` bucket. They read and write only
that tenant's `jobs/` subtree. They do not poll SQS, dispatch handlers or apply returned `delete`
actions: tests explicitly pass the returned event through `native.batch.route` and invoke the
continuation. A running server with S3 notifications can also consume the written marker, so use
an isolated roto instance for this style of test. Ordinary store uploads and HTTP callbacks have
application-specific acknowledgment formats and are not handled by `test.peer` yet.

A complete runnable example is in `examples/peer_demo/`. In one terminal:

```bash
roto-server --port 5070 --ephemeral --setup dev/test-peer.luau
```

In another, with the AWS variables from above:

```bash
cargo run -p integraal-testing -- examples/peer_demo dev/test-peer.json
```

The example uses generated job IDs; its objects disappear when the ephemeral simulator stops.

## Isolation and fixtures

Each case runs in a fresh sandboxed Luau VM with a 64 MiB memory limit and a ten-second execution
budget. The suite is evaluated once for discovery and again in each case's VM: keep registration
code free of side effects and put setup inside case functions. Load scripts inside cases after
registering mocks. Native helpers such as JSON, XML, crypto hashing and SQLite use the real code.

`require("@lib/util")` loads `lib/util.luau` from the tenant directory and caches it per VM.
For local shared modules, put `@shared/csv/1.0.0.luau` under that directory and use
`require("@shared/csv@1.0.0")`.

Roto resources persist across cases. Seed the keys each test needs and clean them up; use a
dedicated roto instance or tenant directory for tests. The runner uses the directory's basename
as its tenant identity, and applies store grants and vars from its `config.json`.

The current runner supports S3 stores, the configured DynamoDB table, queue, batch bucket and
callback URL. It does not configure host-directory/FTP/SFTP stores, vault credentials, master
keys, background jobs or automatic resource provisioning. It uses fixed test budgets rather than
tenant limit overrides. See `examples/acme/tests/hello.test.luau` for a storage test against roto.
