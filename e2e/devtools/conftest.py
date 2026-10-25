"""A stack with flow logs, debug captures and the loopback query API turned on for the `mega` tenant."""

import subprocess
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest
import requests

from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest

from harness import ROOT, ROTO, free_port, launch


class Collector:
    """An OTLP/HTTP trace receiver: keeps every span the server exports."""

    def __init__(self):
        self.spans = []  # (resource attributes, span)
        self.paths = []
        self._lock = threading.Lock()
        outer = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers.get("content-length", 0)))
                outer.paths.append(self.path)
                if self.path.endswith("/v1/traces"):
                    req = ExportTraceServiceRequest()
                    req.ParseFromString(body)
                    with outer._lock:
                        for rs in req.resource_spans:
                            res = {a.key: a.value.string_value for a in rs.resource.attributes}
                            for ss in rs.scope_spans:
                                outer.spans += [(res, s) for s in ss.spans]
                self.send_response(200)
                self.send_header("content-type", "application/x-protobuf")
                self.send_header("content-length", "0")
                self.end_headers()

            def log_message(self, *a):
                pass

        self.port = free_port()
        self.server = ThreadingHTTPServer(("127.0.0.1", self.port), Handler)
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    @property
    def url(self):
        return f"http://127.0.0.1:{self.port}"

    def trace(self, trace_id: str):
        """Spans of one trace as plain dicts (name, ids, attributes, links)."""
        with self._lock:
            found = [(r, s) for r, s in self.spans if s.trace_id.hex() == trace_id]
        return [{
            "name": s.name, "span_id": s.span_id.hex(), "parent": s.parent_span_id.hex(),
            "service": r.get("service.name"),
            "attrs": {a.key: a.value.string_value or a.value.int_value for a in s.attributes},
            "links": [(l.trace_id.hex(), l.span_id.hex()) for l in s.links],
        } for r, s in found]


@pytest.fixture(scope="session")
def collector():
    c = Collector()
    yield c
    c.server.shutdown()


class Dev:
    """The local query API, over HTTP and through `integraal-client`."""

    def __init__(self, stack, client_bin):
        self.stack, self.client_bin = stack, client_bin
        self.url = stack.local_url

    def get(self, path, **params):
        return requests.get(f"{self.url}{path}", params=params, timeout=10)

    def json(self, path, **params):
        r = self.get(path, **params)
        assert r.status_code == 200, f"{path}: {r.status_code} {r.text}"
        return r.json()

    def client(self, *args):
        """Run `integraal-client`; returns (exit code, stdout, stderr)."""
        p = subprocess.run([str(self.client_bin), *args], capture_output=True, text=True, timeout=20, env={"INTEGRAAL_URL": self.url})
        return p.returncode, p.stdout, p.stderr


@pytest.fixture(scope="session")
def stack(megalith, collector):
    if not ROTO:
        pytest.skip("roto-server is not on PATH (or set ROTO_SERVER)")
    with launch(megalith, devtools=True, server_env={
        "OTEL_EXPORTER_OTLP_ENDPOINT": collector.url, "OTEL_BSP_SCHEDULE_DELAY": "100"}) as s:
        yield s


@pytest.fixture(scope="session")
def dev(stack):
    subprocess.run(["cargo", "build", "-p", "integraal-client"], cwd=ROOT, check=True)
    return Dev(stack, ROOT / "target/debug/integraal-client")
