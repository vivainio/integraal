# l8l reference app

l8l is a small Vite and Preact reference app for running scripts and exploring Integraal flow history. It is a
separate client of the [control-plane HTTP API](control-plane.md); the API does not serve the
app. Its source is in the repository's
[`l8l/` directory](https://github.com/vivainio/integraal/tree/main/l8l).

## Start locally

You need Rust and Cargo to build Integraal, Node.js and npm for l8l, and
[uv](https://docs.astral.sh/uv/) for the local setup. Install the
[roto AWS simulator](https://github.com/vivainio/roto) once:

```sh
uv tool install roto-aws
```

From the repository root, start roto in one terminal:

```sh
roto-server --port 5070 --ephemeral
```

In a second terminal, start the runtime and populate it with real local flows:

```sh
dev/local.sh
```

In a third terminal, install the UI dependencies and start Vite:

```sh
cd l8l
npm install
npm run dev
```

Open the URL printed by Vite, normally **http://127.0.0.1:5173/**. l8l connects automatically
to the local `acme` tenant. The three processes need to stay running; stop each with Ctrl-C.
With `--ephemeral`, roto discards its data when it stops. The setup expects a fresh simulator;
restart roto before rerunning `dev/local.sh`.

The launcher creates real flows, including successful and failing calls, routed requests,
queued tasks and a durable order workflow. Select **batch-dispatch** to see the queued
invocations, or **order-pipeline** to see its recorded step transitions. Choose a flow to
browse its invocation summaries, flow log, and pending state. Use **Run script** to select
`user-greeting`, `batch-dispatch`, or `order-pipeline` and submit its form. Use the refresh button after
new work completes, or add another set of flows from the repository root:

```sh
dev/seed-flows.sh
```

Flow history is grouped by the UTC day on which a flow started. The local queue is FIFO for
compatibility with roto and does not support per-message delays. The local launcher sends a
minute tick to dispatch durable handoffs and timers; it is not a complete production scheduler.
See [Local development](local-development.md) and [Observability](observability.md).

## How local access works

The Integraal server's local API listens on `127.0.0.1:9091`. In development,
Vite binds to loopback and its local-only adapter translates queries for the `acme` tenant into
the control-plane response format. The adapter requires no token and is not included in the
production build. The development config explicitly enables user-trigger execution. Keep both
listeners on loopback: the local API does not authenticate users and can run tenant scripts.

Queue invocations are grouped by trace. The current flow records do not identify each invocation's
direct parent, so l8l does not claim an exact parent-child graph. Workflow step displays reflect
recorded `seq:step` events, including replay; they are not a graph of all possible workflow paths.

## Connect to a control plane

Use **Connection** to enter the control-plane URL and a bearer access token. The token remains
in the current page's memory and is lost when the page reloads. Obtaining the token is outside
this reference app. The control plane must accept the UI's exact origin through CORS, including
the hostname and port. For the [reference stack](control-plane.md), set `UiOrigin` accordingly:
`http://127.0.0.1:5173` and `http://localhost:5173` are different origins.

The app reads the flow and script endpoints and can invoke scripts that export a
[user trigger](user-triggers.md). It does not yet deploy scripts, manage credentials, or
implement sign-in. To build static files for separate hosting:

```sh
cd l8l
npm run build
npm run preview
```

The output is in `l8l/dist/`; the reference CloudFormation stack does not host it.
