# Integration patterns

A quick map of every way work gets into integraal, and every way a script talks to the outside
world. Each pattern links to its full page.

## Getting work in

### Synchronous call (Lambda invoke)
Caller waits for the script's return value. [Triggers](triggers.md#direct-lambda-invoke)

```mermaid
sequenceDiagram
    Caller->>Lambda: {tenant, script, event}
    Lambda->>Caller: {ok, result}
```

### Synchronous HTTP (ALB → Fargate)
`POST /v1/{tenant}/{script}`; body is the event, response is the return value. [Triggers](triggers.md#alb-fargate-http)

```mermaid
sequenceDiagram
    Client->>ALB: POST /v1/acme/hello
    ALB->>Fargate: forward
    Fargate->>Client: script result (200/4xx/504)
```

### Queue worker (SQS → Fargate)
Asynchronous; deleted only on success, retried, then dead-lettered. [Triggers](triggers.md#sqs-fargate)

```mermaid
flowchart LR
    S[Sender] --> Q[(SQS)] --> F[Fargate on_queue]
    Q -. repeated failure .-> D[(DLQ)]
```

### File drop (S3 notification)
An object arrives; the notification is split into one invocation per object; the router picks the
script. The handler may ask for the object to be deleted afterwards. [Triggers](triggers.md#s3-notifications)

```mermaid
flowchart LR
    P[Partner] -->|put object| B[(S3 bucket)] -->|notify| Q[(SQS)] --> R{router} --> S[script on_queue]
    S -. "delete = true" .-> B
```

### Routing a shared queue
Messages carry no script name; the tenant's router script chooses. [Router](router.md)

```mermaid
flowchart LR
    M["{tenant, event}"] --> R["router.on_route"] --> A[parse-order]
    R --> B[fallback]
```

### Size-based placement (intake)
An intake Lambda forwards each message to a per-class job queue, so heavy tenants don't starve
light ones. [Worker classes](ingress.md)

```mermaid
flowchart LR
    S[Sender] --> I[(intake queue)] --> L[intake Lambda]
    L -->|small| QS[(jobs-small)] --> FS[Fargate small]
    L -->|large| QL[(jobs-large)] --> FL[Fargate large]
```

## Waiting for something external

### Callback (webhook)
Hand a single-use signed URL to a third party; when they call it, a script runs with the state you
saved. [Callbacks](callbacks.md)

```mermaid
sequenceDiagram
    participant Script as Script A
    participant Third as Third party
    participant WH as Webhook Lambda
    participant W as Fargate
    Script->>Third: native.callback.create(...) URL
    Third->>WH: POST /cb/tenant/token
    WH->>W: enqueue (state attached)
    W->>W: run script B
```

### Batch job (dedicated partner bucket)
Submit a job, end the invocation, continue when the partner writes `done.json`. Nothing waits.
[Batch jobs](batch.md)

```mermaid
sequenceDiagram
    participant S as Script
    participant B as Batch bucket
    participant P as Partner
    participant W as Worker
    S->>B: state/job.json, then request.json
    P->>B: read request, write result parts
    P->>B: done.json (last)
    B->>W: S3 notification, router, batch.route
    W->>B: hydrate, read rows, delete job
```

## Talking to the outside

### Outbound HTTP
Only URLs in the tenant's `http.allow`; no redirects, no private IPs. Streamable. [Outbound HTTP](http.md)

```mermaid
flowchart LR
    S[script] -->|native.http.request| A{allowlist} --> X[External API]
```

### Streaming transform (S3 → XML/zip/gzip → S3)
Large files are processed as streams, so memory is bounded by one event. [Streams and XML](streams-xml.md)

```mermaid
flowchart LR
    I[(In bucket)] -->|open| Z[zip / gzip] --> X[xml.events] --> N[ndjson / sql] -->|create| O[(Out bucket)]
```

### Scratch SQL
Load rows into a per-run SQLite database, query it, optionally persist it to a bucket. [SQL](sql.md)

```mermaid
flowchart LR
    R[rows] --> D[(sql.open scratch DB)] --> Q[SQL query] --> O[output]
    D -. save .-> B[(bucket)]
    B -. load in later run .-> D
```

## Coordination and state

### Locks
Mutual exclusion in-process or across processes, with an enforced lease. [Locks](locks.md)

```mermaid
flowchart LR
    A[invocation 1] --> L{{lock.with name}}
    B[invocation 2] --> L
    L -->|one at a time| C[critical section]
```

### Small state (DynamoDB)
Small items per tenant with a 24h TTL; also backs callbacks and cross-process locks. [DynamoDB](dynamodb.md)

```mermaid
flowchart LR
    S[script] -->|db.put / get / query| T[(table: TEN#tenant#...)]
```

### Deadlines
Nested time-boxed blocks that interrupt the script. [Native API](native-api.md)

```mermaid
flowchart LR
    I[invocation deadline] --> D1["deadline.with 10s"] --> D2["deadline.with 2s"]
```

## Which one?

| Need | Pattern |
| --- | --- |
| Answer right now | Lambda invoke or ALB HTTP |
| Fire and forget, with retries | SQS queue worker |
| React to a file arriving | S3 notification |
| One queue, many scripts | Router |
| Isolate heavy tenants | Worker classes (intake) |
| Third party calls us back | Callback |
| Partner does heavy work for us | Batch job |
| Call an external API | Outbound HTTP |
| Crunch large files | Streams, zip, gzip, SQL |
| Avoid concurrent runs | Locks |
