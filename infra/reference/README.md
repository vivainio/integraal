# Reference CloudFormation stack

This stack provisions a queue-only Fargate service and a separate JWT-protected control-plane
Lambda. They share the stack's scripts, data and flow-log buckets, DynamoDB state table, and
work queue. It also creates a dead-letter queue, minute/hour/day Scheduler ticks, worker and
Lambda IAM roles, an ECS execution role, a security group, and worker logs. The three buckets
and state table are retained when the stack is deleted or replaced; the queues are not.

The stack takes an existing VPC and **public subnets with a route to an internet gateway**.
Fargate tasks get public IPs for AWS APIs, pulling images and outbound integrations; their
security group has no inbound rules. For private subnets, adapt the network configuration and
provide NAT or the required VPC endpoints and egress. There is no ALB or inbound HTTP endpoint.

Provide a container image containing `/bin/sh` and `integraal-server` at
`/usr/local/bin/integraal-server`; the reference task writes its generated configuration to
`/tmp/integraal.json` and starts the server. The control-plane zip contains its `bootstrap`.
The stack supplies its non-secret config in `INTEGRAAL_CONTROL_CONFIG`; absent that variable,
the Lambda still reads `integraal.json` as before. Do not put credentials in this config.
The reference does not configure the credential vault, public webhooks, or the runtime Lambda.

The source is [`stack.py`](stack.py) and [`runtime.py`](runtime.py), written with cfn-typed.
Its committed output is [`crates/control-plane/template.yaml`](../../crates/control-plane/template.yaml),
ordinary CloudFormation without SAM. Python 3.14+ and [uv](https://docs.astral.sh/uv/) are required.
Dependencies are pinned to a cfn-typed source revision in `pyproject.toml`.

From this directory, install dependencies and check the template:

```sh
uv sync
uv run ty check stack.py runtime.py
uv run pytest test_stack.py
```

After changing the stack, regenerate the committed template and check it:

```sh
uv run python -c 'import cfn_typed as cfn; from stack import build; print(cfn.to_yaml(build()), end="")' > ../../crates/control-plane/template.yaml
uv run pytest test_stack.py
```

Build the worker image and the arm64 control-plane Lambda zip, upload them, then deploy using
your own values. The Lambda artifact bucket is separate from the buckets this stack creates.

```sh
aws cloudformation deploy \
  --template-file ../../crates/control-plane/template.yaml \
  --stack-name integraal-reference --capabilities CAPABILITY_IAM \
  --parameter-overrides \
    CodeBucket=ARTIFACT_BUCKET CodeKey=CONTROL_PLANE_ZIP_KEY \
    WorkerImage=WORKER_IMAGE_URI VpcId=VPC_ID PublicSubnets=SUBNET_ID_1,SUBNET_ID_2 \
    JwtIssuer=ISSUER_URL JwtAudience=AUDIENCE AccessScope=ACCESS_TOKEN_SCOPE
```

Use stack outputs for bucket names, the table and queue URL. Upload tenant scripts and configs
to the scripts bucket, including `tenants.json` for scheduled tenant events. Enable `flow_logs`
and any required queue or store grants in each tenant config. See
[deployment](../../docs/deployment.md) and [schedules](../../docs/schedules.md).

The control-plane Lambda's role reads scripts, data, flow logs and state; the separate
worker role can process queue messages and read and write the data and state stores. Restrict
direct control-plane invocation to trusted administrators and the API Gateway integration,
since the Lambda trusts API Gateway's JWT claims. See the
[control-plane documentation](../../docs/control-plane.md) for the trust boundary.
