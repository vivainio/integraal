# l8l

A Preact reference app for Integraal flow history. Browse flows by UTC day, filter them,
and inspect invocation summaries, flow logs, and pending state. The app displays only real flow data.

## Local Integraal

In one terminal, start the AWS simulator (roto):

```sh
roto-server --port 5070 --ephemeral
```

In another terminal, from the repository root, provision the local resources and start Integraal:

```sh
dev/local.sh
```

In a third terminal:

```sh
cd l8l
npm install
npm run dev
```

Open the Vite URL. In development, l8l connects automatically to the `acme` tenant on the local
server's loopback query listener. The Vite middleware translates its query endpoints to the control
plane's response format; it is available only in the development server, which binds to loopback.
Generate a real flow with:

```sh
curl -X POST http://127.0.0.1:8080/v1/acme/hello \
  -H 'content-type: application/json' -d '{"x":1}'
```

The local launcher also creates real flows, including intentional failures, routed flows and a durable order workflow. Run
`dev/seed-flows.sh` from the repository root to add them again while Integraal is running.
Refresh the flow list in l8l after requests complete. The seeded durable workflow is dispatched by a local tick and its `seq:step` events appear in the flow detail visualization. The local setup enables flow logging for
`acme` and stores the logs in roto. With `--ephemeral`, all simulator state disappears when it stops.

## Hosted control plane

Use **Connection** to enter the control-plane URL and a bearer access token. The token is held only
in the current page's memory and is lost on reload. Token issuance is outside this example. The API
must allow the UI's origin using CORS; the reference CloudFormation stack's `UiOrigin` parameter
defaults to `http://localhost:5173`. Set it to the actual UI origin and redeploy the stack when
hosting the UI elsewhere. The UI needs `GET /flows?day=YYYY-MM-DD` `GET /flows/{trace_id}` and `GET /flows/{trace_id}/logs`.
See [control-plane documentation](../docs/control-plane.md).

```sh
npm run build
npm run preview
```

`dist/` contains static files. The reference stack does not host them or provide a sign-in flow.
