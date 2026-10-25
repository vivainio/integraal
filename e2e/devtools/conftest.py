"""A stack with flow logs, debug captures and the loopback query API turned on for the `mega` tenant."""

import json
import subprocess

import pytest
import requests

from harness import ROOT, ROTO, launch


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
def stack(megalith):
    if not ROTO:
        pytest.skip("roto-server is not on PATH (or set ROTO_SERVER)")
    with launch(megalith, devtools=True) as s:
        yield s


@pytest.fixture(scope="session")
def dev(stack):
    subprocess.run(["cargo", "build", "-p", "integraal-client"], cwd=ROOT, check=True)
    return Dev(stack, ROOT / "target/debug/integraal-client")
