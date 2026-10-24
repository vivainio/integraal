"""Patterns 3-5, 16, 17: queue worker, file drop, router, scatter/gather, run-once."""

import json
import uuid

from conftest import eventually

OUT = "data-out"


def uid():
    return uuid.uuid4().hex[:8]


def marker(stack, key):
    return stack.get(OUT, f"tenants/mega/{key}")


def test_queue_worker_runs_the_script(stack):
    i = uid()
    stack.send("jobs", {"tenant": "mega", "script": "worker", "event": {"id": i}})
    m = eventually(lambda: marker(stack, f"marks/{i}"), what="worker marker")
    assert json.loads(m) == {"trigger": "queue", "attempt": 1}


def test_failing_message_is_retried_then_dead_lettered(stack):
    i = uid()
    stack.send("jobs", {"tenant": "mega", "script": "worker", "event": {"id": i, "op": "fail"}})
    dead = eventually(lambda: stack.drain("jobs-dlq", wait=1), timeout=40, what="dead letter")
    assert dead[0]["event"]["id"] == i
    assert marker(stack, f"attempts/{i}") == b"3"  # ctx.attempt of the last delivery


def test_message_without_script_is_routed(stack):
    i = uid()
    stack.send("jobs", {"tenant": "mega", "event": {"id": i, "route": "worker"}})
    eventually(lambda: marker(stack, f"marks/{i}"), what="routed worker marker")


def test_http_without_script_is_routed(stack):
    r = stack.routed({"route": "hello", "x": 1})
    assert r.status_code == 200, r.text
    assert r.json()["event"]["x"] == 1


def test_file_drop_runs_script_and_deletes_object(stack):
    key = f"incoming/feeds/{uid()}.xml"
    stack.put("data-in", f"tenants/mega/{key}", "<feed><entry/><entry/><entry/></feed>")
    n = eventually(lambda: marker(stack, f"feed-result/{key}"), what="feed result")
    assert n == b"3"
    eventually(lambda: stack.get("data-in", f"tenants/mega/{key}") is None, what="object deleted")


def test_scatter_gather_gathers_exactly_once(stack):
    flow, n = uid(), 5
    stack.send("jobs", {"tenant": "mega", "script": "worker", "event": {"op": "scatter", "flow": flow, "n": n}})
    out = eventually(lambda: marker(stack, f"gathered/{flow}"), what="gather output")
    assert json.loads(out) == {"parts": n, "runs": 1}


def test_scatter_gather_survives_redelivered_shard(stack):
    flow, n = uid(), 3
    stack.send("jobs", {"tenant": "mega", "script": "worker", "event": {"op": "scatter", "flow": flow, "n": n}})
    stack.send("jobs", {"tenant": "mega", "script": "shard", "event": {"flow": flow, "i": 1}})  # duplicate
    eventually(lambda: marker(stack, f"gathered/{flow}"), what="gather output")
    # Give any wrongly-sent second gather time to show up, then check it did not.
    import time

    time.sleep(3)
    assert json.loads(marker(stack, f"gathered/{flow}"))["runs"] == 1


def test_idempotent_work_runs_once_despite_redelivery(stack):
    i = uid()
    for _ in range(3):
        stack.send("jobs", {"tenant": "mega", "script": "worker", "event": {"op": "idem", "id": i}})
    eventually(lambda: marker(stack, f"idem/{i}"), what="idem marker")
    import time

    time.sleep(3)
    assert marker(stack, f"idem/{i}") == b"1"
