# Local development

[roto](https://github.com/vivainio/roto) is an AWS simulator with S3 and SQS on one port, used for
local end-to-end runs.

```bash
uv tool install roto-aws
roto-server --port 5070        # leave running
dev/local.sh                   # creates buckets, uploads examples/, starts the server
```

`dev/local.sh` points the SDK at roto with `AWS_ENDPOINT_URL=http://localhost:5070` and uploads
`examples/acme/hello/latest.luau` and `examples/acme/config.json`. Then:

```bash
curl -X POST localhost:8080/v1/acme/hello \
  -H 'content-type: application/json' -d '{"x":1}'
# {"count":1,"event":{"x":1},"trigger":"http"}
```

Running it again increments `count`, which is stored in the `Out` bucket through `native.bucket.Out`.

## Tests

```bash
cargo test --workspace
```

The engine tests use in-memory stores, so they need no AWS or simulator.

### Mega integration test

`e2e/` holds a black-box test that exercises every [integration pattern](patterns.md) against a real
`integraal-server`, with roto standing in for AWS. It needs `roto-server` 0.10.1 or later on your `PATH` (`uv tool install 'roto-aws>=0.10.1'`):

```bash
uv run --project e2e pytest e2e
```

The outside world is played by **megalith**, a small mock HTTP service (`e2e/megalith.py`). It
stands in for the hypothetical system that integraal acts as the integration layer for: the
back-office or partner system that integraal calls, that sends callbacks, and that works batch jobs.
The Lambda entrypoints (`lambda`, `ingress`, `webhook`) run as the real `bootstrap` binaries, warm,
behind roto's Lambda Runtime API (its `runtime` executor), so direct invoke, intake placement and
the callback webhook are tested as deployed. Set `ROTO_SERVER` to use a roto build that is not on your `PATH`.
Megalith records every request it gets and answers from a canned table, so tests can assert on what
integraal sent (headers, bodies, concurrency) as well as on what came back.

## Previewing these docs

```bash
uvx zensical serve
```
