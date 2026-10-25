"""Session fixtures for the main suite; the stack itself is started by harness.launch."""

import pytest

from harness import ROTO, Stack, _WORKDIR, eventually, launch  # noqa: F401  (eventually is imported from here by the tests)
from megalith import Megalith


@pytest.fixture(autouse=True)
def _fresh_megalith(megalith):
    megalith.reset()
    megalith.route("GET", "/hold", delay=0.6)
    yield


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
    with launch(megalith) as s:
        yield s
