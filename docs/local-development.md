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

Running it again increments `count`, which is stored in the `out` data bucket through `native.data.out`.

## Tests

```bash
cargo test --workspace
```

The engine tests use in-memory stores, so they need no AWS or simulator.

## Previewing these docs

```bash
uvx zensical serve
```
