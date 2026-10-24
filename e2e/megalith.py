"""Megalith: the mock integration target (HTTP API): records requests, answers from a canned table, tracks concurrency."""

import json
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer


class Megalith:
    def __init__(self):
        self.requests: list[dict] = []
        self.routes: dict[tuple[str, str], dict] = {}  # (method, path) -> {status, body, headers, delay}
        self.active = 0
        self.max_active = 0
        self._lock = threading.Lock()
        megalith = self

        class Handler(BaseHTTPRequestHandler):
            def _serve(self):
                n = int(self.headers.get("content-length") or 0)
                body = self.rfile.read(n) if n else b""
                with megalith._lock:
                    megalith.active += 1
                    megalith.max_active = max(megalith.max_active, megalith.active)
                    megalith.requests.append(
                        {
                            "method": self.command,
                            "path": self.path,
                            "headers": {k.lower(): v for k, v in self.headers.items()},
                            "body": body,
                        }
                    )
                    route = megalith.routes.get((self.command, self.path.split("?")[0]), {})
                try:
                    time.sleep(route.get("delay", 0))
                    out = route.get("body", b'{"ok":true}')
                    if not isinstance(out, bytes):
                        out = json.dumps(out).encode()
                    self.send_response(route.get("status", 200))
                    for k, v in route.get("headers", {"content-type": "application/json"}).items():
                        self.send_header(k, v)
                    self.send_header("content-length", str(len(out)))
                    self.end_headers()
                    self.wfile.write(out)
                finally:
                    with megalith._lock:
                        megalith.active -= 1

            do_GET = do_POST = do_PUT = do_DELETE = _serve

            def log_message(self, *a):
                pass

        self.server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.port = self.server.server_address[1]
        self.base = f"http://localhost:{self.port}"
        threading.Thread(target=self.server.serve_forever, daemon=True).start()

    def route(self, method: str, path: str, **kw):
        self.routes[(method, path)] = kw

    def reset(self):
        with self._lock:
            self.requests.clear()
            self.routes.clear()
            self.max_active = 0

    def close(self):
        self.server.shutdown()
