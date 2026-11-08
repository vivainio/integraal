# Control-plane HTTP API

`integraal-control-plane` is a separate Rust Lambda for tenant-scoped operational reads.
It uses the same flow inspection as `ops.flow.inspect` and does not run tenant scripts. It
is independent of the queue workers, though it still depends on their shared flow-log and
state storage.

The Lambda accepts API Gateway **HTTP API payload format 2.0** events. The accompanying
[`template.yaml`](../crates/control-plane/template.yaml) configures a JWT authorizer and an
access-token scope for both routes. The token issuer must provide nonempty `sub` and `tenant`
claims. The tenant claim selects the only tenant the request can access; a query parameter,
path or request body cannot override it. Token issuance and membership management are outside
this service.

The function trusts the authorizer context in the event and does not validate the JWT itself.
**Restrict direct `lambda:InvokeFunction` access to trusted administrators and the intended API
Gateway integration.** Do not add a public function URL or another unauthenticated event source:
a caller who can directly invoke the Lambda can forge claims. Restrict the execution role to the
necessary deployment resources. The template accepts an existing role and does not grant other
principals direct invoke access. Check identity policies and other resource policies as well.

| Request | Response |
| --- | --- |
| `GET /flows?day=2026-11-08` | `{ "flows": [...] }`, flows started on the UTC day |
| `GET /flows/{trace_id}` | invocation summaries and pending state, as in `ops.flow.inspect` |

Flow listing is bounded by the existing flow-log reader (up to 1,000 entries); it has no
pagination. Flow logs must be enabled for the tenant and configured in `integraal.json`; flow
inspection also needs the durable-state backend. The role needs read access to the configured
script/config, flow-log and state resources. Deployment, invocation, credentials, queue control,
and fleet-wide operations are not implemented by this first API.

Build for `aarch64-unknown-linux-gnu` (or adjust the template architecture), then package the
`bootstrap` binary and deployment `integraal.json` at the zip root. Upload the zip to S3 and
deploy the CloudFormation template with the zip bucket and key, execution role ARN, issuer, audience, and OAuth scope.
Protect the role and configuration as operator-controlled resources. For example, once deployed:

```sh
curl -H "Authorization: Bearer $ACCESS_TOKEN" \
  'https://API_ID.execute-api.REGION.amazonaws.com/flows?day=2026-11-08'
```
