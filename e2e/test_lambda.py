"""Patterns 1, 6, 7: the Lambda entrypoints, run as the real `bootstrap` binaries behind roto."""

import json
import uuid

import requests

from conftest import eventually


def invoke(stack, payload, function="integraal"):
    r = stack.lam.invoke(FunctionName=function, Payload=json.dumps(payload))
    return json.loads(r["Payload"].read())


def test_sync_lambda_invoke(stack):
    r = invoke(stack, {"tenant": "mega", "script": "hello", "event": {"x": 1}})
    assert r["ok"] is True, r
    assert r["result"] == {"trigger": "invoke", "event": {"x": 1}}


def test_sync_lambda_invoke_reports_errors(stack):
    r = invoke(stack, {"tenant": "mega", "script": "nope", "event": {}})
    assert r["ok"] is False and r["error"], r


def test_intake_forwards_messages_to_the_class_the_placement_script_picks(stack):
    small, big = uuid.uuid4().hex[:6], uuid.uuid4().hex[:6]
    stack.send("intake", {"tenant": "mega", "script": "worker", "event": {"id": small}})
    stack.send("intake", {"tenant": "mega", "script": "worker", "event": {"id": big, "big": True}})
    got = {}
    for queue in ("jobs-small", "jobs-large"):
        got[queue] = eventually(lambda q=queue: stack.drain(q, wait=1), timeout=30, what=f"{queue} message")
    assert got["jobs-small"][0]["event"] == {"id": small}  # forwarded unchanged
    assert got["jobs-large"][0]["event"] == {"id": big, "big": True}


def callback(stack, ttl_s=None):
    i = uuid.uuid4().hex[:6]
    event = {"id": i, **({"ttl_s": ttl_s} if ttl_s else {})}
    url = stack.invoke("callback_start", event).json()["url"]
    return i, url


def test_callback_runs_the_script_with_the_saved_state_once(stack):
    i, url = callback(stack)
    assert url.startswith(stack.roto_url)
    r = requests.post(url, data="paid=yes", timeout=30)
    assert r.status_code == 202, r.text
    out = json.loads(eventually(lambda: stack.get("data-out", f"tenants/mega/callbacks/{i}"), what="callback script"))
    assert out == {"method": "POST", "body": "paid=yes", "state": {"id": i}}
    assert requests.post(url, data="again", timeout=30).status_code == 404  # single use


def test_callback_rejects_forged_expired_and_oversized_requests(stack):
    _, url = callback(stack)
    forged = url[:-4] + ("AAAA" if not url.endswith("AAAA") else "BBBB")
    assert requests.post(forged, data="x", timeout=30).status_code == 404
    assert requests.post(url, data="x" * (300 * 1024), timeout=30).status_code == 413
    _, short = callback(stack, ttl_s=1)
    import time

    time.sleep(2.5)
    assert requests.post(short, data="x", timeout=30).status_code == 410
