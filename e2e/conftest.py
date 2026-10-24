"""Session fixtures: roto (AWS simulator) + the real integraal binaries, started as black boxes."""

import json
import os
import shutil
import socket
import subprocess
import tempfile
import time
from pathlib import Path

import boto3
import pytest
import requests

from megalith import Megalith

HERE = Path(__file__).parent
ROOT = HERE.parent
ADMIN_TOKEN = "e2e-admin-token-0123456789abcdef"
REGION = "us-east-1"
BIN = HERE / ".bin"
ROTO = os.environ.get("ROTO_SERVER") or shutil.which("roto-server")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def eventually(fn, timeout=15.0, interval=0.1, what="condition"):
    """Poll `fn` until it returns something truthy; return that value."""
    end = time.monotonic() + timeout
    last = None
    while time.monotonic() < end:
        last = fn()
        if last:
            return last
        time.sleep(interval)
    raise AssertionError(f"timed out waiting for {what} (last={last!r})")


def wait_http(url, timeout=20.0):
    eventually(lambda: _ok(url), timeout, what=url)


def _ok(url):
    try:
        return requests.get(url, timeout=1).status_code < 500
    except requests.RequestException:
        return False


class Stack:
    """Everything a test needs: URLs, AWS clients and helpers."""

    def __init__(self, roto_url, server_url, admin_url, queues, workdir):
        self.roto_url, self.server_url, self.admin_url = roto_url, server_url, admin_url
        self.queues, self.workdir = queues, workdir
        kw = dict(endpoint_url=roto_url, region_name=REGION, aws_access_key_id="test", aws_secret_access_key="test")
        self.s3 = boto3.client("s3", **kw)
        self.sqs = boto3.client("sqs", **kw)
        self.ddb = boto3.client("dynamodb", **kw)
        self.lam = boto3.client("lambda", **kw)

    # -- HTTP into the server
    def invoke(self, script, event=None, version=None, tenant="mega"):
        h = {"content-type": "application/json"}
        if version:
            h["x-script-version"] = version
        return requests.post(f"{self.server_url}/v1/{tenant}/{script}", data=json.dumps(event), headers=h, timeout=30)

    def routed(self, event, tenant="mega"):
        return requests.post(f"{self.server_url}/v1/{tenant}", json=event, timeout=30)

    # -- S3
    def put(self, bucket, key, body):
        self.s3.put_object(Bucket=bucket, Key=key, Body=body)

    def get(self, bucket, key):
        try:
            return self.s3.get_object(Bucket=bucket, Key=key)["Body"].read()
        except self.s3.exceptions.NoSuchKey:
            return None

    def keys(self, bucket, prefix=""):
        r = self.s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
        return [o["Key"] for o in r.get("Contents", [])]

    # -- SQS
    def send(self, queue, body, **kw):
        self.sqs.send_message(QueueUrl=self.queues[queue], MessageBody=json.dumps(body), **kw)

    def drain(self, queue, wait=1):
        out = []
        while True:
            r = self.sqs.receive_message(QueueUrl=self.queues[queue], MaxNumberOfMessages=10, WaitTimeSeconds=wait)
            msgs = r.get("Messages", [])
            if not msgs:
                return out
            for m in msgs:
                self.sqs.delete_message(QueueUrl=self.queues[queue], ReceiptHandle=m["ReceiptHandle"])
                out.append(json.loads(m["Body"]))

    # -- vault admin
    def vault_put(self, name, value, allow, stream_unmasked=False, tenant="mega"):
        r = requests.put(
            f"{self.admin_url}/admin/vault/{tenant}/{name}",
            json={"value": value, "allow": allow, "stream_unmasked": stream_unmasked},
            headers={"authorization": f"Bearer {ADMIN_TOKEN}"},
            timeout=10,
        )
        assert r.status_code < 300, r.text


@pytest.fixture(autouse=True)
def _fresh_megalith(megalith):
    megalith.reset()
    megalith.route("GET", "/hold", delay=0.6)
    yield


def _spawn(cmd, log: Path, env=None):
    return subprocess.Popen(cmd, stdout=log.open("wb"), stderr=subprocess.STDOUT, env=env, cwd=ROOT)


_WORKDIR: list[Path] = []


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    """A failing test gets the tail of the server and roto logs attached, so the cause is on screen."""
    outcome = yield
    rep = outcome.get_result()
    if rep.when == "call" and rep.failed and _WORKDIR:
        for name in ("server.log", "roto.log"):
            f = _WORKDIR[0] / name
            if f.exists():
                tail = "\n".join(f.read_text(errors="replace").splitlines()[-25:])
                rep.sections.append((f"{name} (last 25 lines)", tail))


@pytest.fixture(scope="session")
def megalith():
    m = Megalith()
    yield m
    m.close()


@pytest.fixture(scope="session")
def stack(megalith):
    if not ROTO:
        pytest.skip("roto-server is not on PATH (or set ROTO_SERVER)")
    work = Path(tempfile.mkdtemp(prefix="integraal-e2e-"))
    _WORKDIR[:] = [work]
    procs = []
    try:
        cargo_env = {**os.environ, "CARGO_INCREMENTAL": "0"}
        subprocess.run(["cargo", "build", "-p", "integraal-server"], cwd=ROOT, check=True, env=cargo_env)
        server_bin = ROOT / "target/debug/integraal-server"
        # Two Lambda entrypoints are both called `bootstrap`, so build one at a time and keep a copy.
        BIN.mkdir(exist_ok=True)
        for pkg, name, built in (("integraal-lambda", "lambda", "bootstrap"), ("integraal-ingress", "ingress", "bootstrap"),
                                 ("integraal-webhook", "webhook", "integraal-webhook")):
            subprocess.run(["cargo", "build", "-p", pkg], cwd=ROOT, check=True, env=cargo_env)
            shutil.copy2(ROOT / "target/debug" / built, BIN / name)

        roto_port = free_port()
        roto_url = f"http://localhost:{roto_port}"
        # Queue URLs are needed in the configs before roto (which creates the queues) is up.
        queues = {n: f"{roto_url}/123456789012/{n}" for n in
                  ("jobs", "jobs-dlq", "intake", "jobs-small", "jobs-large")}
        key = "11" * 32
        common = {
            "scripts": {"bucket": "scripts"},
            "buckets": {"prefix": "tenants/", "map": {"In": "data-in", "Out": "data-out", "Temp": "data-temp"}},
            "batch": {"bucket": "batch", "prefix": "tenants/"},
            "db": {"table": "integraal"},
            "callbacks": {"base_url": f"{roto_url}/roto-http"},
            "crypto": {"keys": {"1": "INTEGRAAL_KEY_1"}, "active": 1},
            "queue": {"url": queues["jobs"]},
            "log": "info",
        }
        lambda_cfg = work / "lambda.json"
        lambda_cfg.write_text(json.dumps({
            **common,
            "ingress": {"queues": {"small": queues["jobs-small"], "large": queues["jobs-large"]}, "default": "small"},
        }))
        routes = work / "routes.json"
        routes.write_text(json.dumps({"routes": [{"method": "POST", "path": "/cb/{tenant}/{token}", "function": "webhook"}]}))
        roto = _spawn(
            [ROTO, "--ephemeral", "--port", str(roto_port), "--setup", str(HERE / "setup.luau"),
             "--http-routes", str(routes),
             "--set", f"FIXTURES={HERE / 'fixtures/mega'}", "--set", f"MEGALITH={megalith.base}/",
             "--set", f"BIN_DIR={BIN}", "--set", f"LAMBDA_CFG={lambda_cfg}", "--set", f"KEY={key}"],
            work / "roto.log",
        )
        procs.append(roto)
        wait_http(f"{roto_url}/roto-api/health")

        env = {**os.environ, "AWS_ENDPOINT_URL": roto_url, "AWS_ACCESS_KEY_ID": "test",
               "AWS_SECRET_ACCESS_KEY": "test", "AWS_REGION": REGION, "AWS_DEFAULT_REGION": REGION}
        os.environ.update({k: env[k] for k in ("AWS_ENDPOINT_URL", "AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_REGION")})

        sqs = boto3.client("sqs", endpoint_url=roto_url, region_name=REGION, aws_access_key_id="test", aws_secret_access_key="test")
        for n, url in queues.items():
            assert sqs.get_queue_url(QueueName=n)["QueueUrl"] == url, f"queue {n} is not where the configs say"

        server_port, admin_port = free_port(), free_port()
        cfg = {
            **common,
            "callbacks": {"base_url": f"{roto_url}/roto-http"},
            "admin": {"port": admin_port, "token_env": "INTEGRAAL_ADMIN_TOKEN"},
            "ingress": {"queues": {"small": queues["jobs-small"], "large": queues["jobs-large"]}, "default": "small"},
            "server": {"port": server_port},
            "throttle": [{"url": f"{megalith.base}/slow/", "max_concurrent": 2, "per_second": 50}],
        }
        cfg_path = work / "server.json"
        cfg_path.write_text(json.dumps(cfg))
        senv = {**env, "INTEGRAAL_ADMIN_TOKEN": ADMIN_TOKEN, "INTEGRAAL_KEY_1": key}
        procs.append(_spawn([str(server_bin), "--config", str(cfg_path)], work / "server.log", senv))
        server_url = f"http://127.0.0.1:{server_port}"
        wait_http(f"{server_url}/healthz")

        yield Stack(roto_url, server_url, f"http://127.0.0.1:{admin_port}", queues, work)
    finally:
        for p in reversed(procs):
            p.terminate()
        for p in procs:
            try:
                p.wait(10)
            except subprocess.TimeoutExpired:
                p.kill()
        print(f"e2e logs kept in {work}")
