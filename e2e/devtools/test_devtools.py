"""Developer tools: flow logs, the loopback query API, `integraal-client`, structured failures and debug captures."""

import json
import threading
import uuid

from harness import eventually

TENANT = "mega"


def uid():
    return uuid.uuid4().hex[:8]


def flow_of(dev, script, after_ms=0, **match):
    """The trace id of the newest flow that started with `script` (and whose event matches), once it is listed."""
    def find():
        rows = [f for f in dev.json(f"/flows/{TENANT}") if f["script"] == script and f["start_ms"] >= after_ms]
        return max(rows, key=lambda f: f["start_ms"])["trace_id"] if rows else None
    return eventually(find, what=f"flow list entry for {script}")


def run(stack, dev, script, event):
    """Invoke `script` and return (response, trace id of its flow)."""
    import time
    before = int(time.time() * 1000) - 1
    r = stack.invoke(script, event)
    return r, flow_of(dev, script, after_ms=before)


def summary(dev, trace):
    return eventually(lambda: dev.json(f"/flows/{TENANT}/{trace}/summary"), what="flow summary")


# -- flow logs and the query API

def test_a_finished_invocation_is_listed_and_summarised(stack, dev):
    r, trace = run(stack, dev, "hello", {"x": uid()})
    assert r.status_code == 200
    row = next(f for f in dev.json(f"/flows/{TENANT}") if f["trace_id"] == trace)
    assert (row["tenant"], row["script"], row["trigger"]) == (TENANT, "hello", "http")
    [s] = summary(dev, trace)
    assert (s["script"], s["outcome"], s["error"]) == ("hello", "ok", None)


def test_a_day_with_no_flows_lists_nothing(stack, dev):
    assert dev.json(f"/flows/{TENANT}", day="2001-01-01") == []
    assert dev.get(f"/flows/{TENANT}", day="not-a-day").status_code >= 400


def test_the_flow_log_holds_script_log_lines_and_since_tails_it(stack, dev):
    what = uid()
    _, trace = run(stack, dev, "devfail", {"what": what})
    lines = eventually(lambda: dev.json(f"/flows/{TENANT}/{trace}"), what="flow log")
    assert [l["type"] for l in lines][0] == "header" and lines[-1]["type"] == "end"
    logged = [l for l in lines if f"about to fail: {what}" in json.dumps(l)]
    assert logged, lines
    assert dev.json(f"/flows/{TENANT}/{trace}", since=lines[-1]["t"]) == []


def test_every_redelivery_of_a_failing_message_is_a_flow_with_its_attempt_number(stack, dev):
    i = uid()
    stack.send("jobs", {"tenant": "mega", "script": "worker", "event": {"op": "fail", "id": i}})
    eventually(lambda: stack.get("data-out", f"tenants/mega/attempts/{i}") == b"3", timeout=30, what="third delivery")

    def failed_worker_attempts():
        # The message carries no id of its own here, so look at the attempts of recent queue flows.
        return sorted({s["attempt"] for f in dev.json(f"/flows/{TENANT}") if f["script"] == "worker" and f["trigger"] == "queue"
                       for s in dev.json(f"/flows/{TENANT}/{f['trace_id']}/summary") if s["outcome"] == "script_error"})
    assert eventually(lambda: [1, 2, 3] if {1, 2, 3} <= set(failed_worker_attempts()) else None, what="three failed segments")


# -- structured failures

def test_a_failing_script_reports_kind_message_and_frames(stack, dev):
    what = uid()
    r, trace = run(stack, dev, "devfail", {"what": what})
    assert r.status_code >= 400
    [s] = summary(dev, trace)
    assert s["outcome"] != "ok"
    assert s["failure"]["kind"] == "runtime" and s["failure"]["frame"]["function"]
    assert f"bad input {what}" in s["error"]

    # The segment footer carries the whole stack, innermost first.
    end = next(l for l in dev.json(f"/flows/{TENANT}/{trace}") if l["type"] == "end")
    f = end["failure"]
    assert f["kind"] == "runtime" and f"bad input {what}" in f["message"]
    user = [fr for fr in f["frames"] if not fr["native"]]
    assert [fr["function"] for fr in user] == ["inner", "outer", "handle"]
    assert [fr["line"] for fr in user] == [5, 9, 14]


def test_a_table_thrown_as_error_is_described_by_keys_only(stack, dev):
    # worker's "fail" op raises a string; a script throwing a table must not leak its values.
    secret = "s3cret-" + uid()
    r, trace = run(stack, dev, "devthrow", {"secret": secret})
    [s] = summary(dev, trace)
    assert secret not in json.dumps(dev.json(f"/flows/{TENANT}/{trace}"))
    assert s["failure"]["kind"] == "runtime"


# -- debug captures

def test_a_watched_invocation_writes_a_capture_with_event_coverage_and_profile(stack, dev):
    what = uid()
    _, trace = run(stack, dev, "devfail", {"what": what})
    [cap] = eventually(lambda: dev.json(f"/debug/{TENANT}/{trace}"), what="debug capture")
    assert cap["script"] == "devfail" and cap["event"] == {"what": what}
    assert cap["failure"]["kind"] == "runtime"
    chunk = next(c for c in cap["coverage"] if c["chunk"] == "mega/devfail")
    assert chunk["covered"] > 0 and chunk["covered"] + len(chunk["missed"]) == chunk["executable"]
    assert cap["profile"]["interval_ms"] >= 1


def test_the_capture_is_on_disk_where_the_sink_says(stack, dev):
    _, trace = run(stack, dev, "hello", {"x": uid()})
    eventually(lambda: dev.json(f"/debug/{TENANT}/{trace}"), what="debug capture")
    found = list(stack.debug_dir.rglob(f"{trace}/*.json.gz"))
    assert len(found) == 1, list(stack.debug_dir.rglob("*"))


def test_debug_for_an_unknown_flow_is_empty(stack, dev):
    assert dev.json(f"/debug/{TENANT}/{'0' * 32}") == []


# -- what a flow is waiting on

def test_state_lists_a_barrier_with_its_deadline_timer_and_drops_the_timer_on_completion(stack, dev):
    i = uid()
    r, trace = run(stack, dev, "deadline_barrier", {"op": "create", "id": i, "deadline_s": 3600})
    assert r.status_code == 200

    st = eventually(lambda: (s := dev.json(f"/flows/{TENANT}/{trace}/state")) and s["barriers"] and s, what="indexed barrier")
    [b] = st["barriers"]
    assert (b["id"], b["total"], b["pending"]) == (i, 1, 1)
    [t] = st["timers"]
    assert t["id"] == f"barrier.{i}" and t["due_s"] == b["due_s"]
    assert t["payload"]["event"] == {"type": "barrier.overdue", "barrier": i}

    stack.invoke("deadline_barrier", {"op": "ack", "id": i})
    st = eventually(lambda: (s := dev.json(f"/flows/{TENANT}/{trace}/state")) and not s["timers"] and s, what="deadline timer cancelled")
    assert [b["pending"] for b in st["barriers"]] == [0]


def test_state_lists_a_callback_until_it_is_taken(stack, dev):
    r, trace = run(stack, dev, "callback_start", {"id": uid(), "ttl_s": 3600})
    url = r.json()["url"]
    [cb] = eventually(lambda: dev.json(f"/flows/{TENANT}/{trace}/state")["callbacks"], what="indexed callback")
    assert cb["expires_s"] > 0

    import requests
    assert requests.post(url, json={"ok": True}, timeout=10).status_code < 300
    eventually(lambda: not dev.json(f"/flows/{TENANT}/{trace}/state")["callbacks"], what="callback gone once taken")


def test_state_lists_a_submitted_batch_job_that_the_partner_has_not_finished(stack, dev):
    job = uid()
    r, trace = run(stack, dev, "batch_start", {"id": job, "ids": [1], "ref": "r"})
    assert r.status_code == 200
    [b] = eventually(lambda: dev.json(f"/flows/{TENANT}/{trace}/state")["batch"], what="indexed batch job")
    assert b["id"] == job and b["done"] is False


def test_state_of_a_flow_waiting_on_nothing_is_empty(stack, dev):
    _, trace = run(stack, dev, "hello", {"x": uid()})
    st = dev.json(f"/flows/{TENANT}/{trace}/state")
    assert [st[k] for k in ("timers", "barriers", "callbacks", "batch")] == [[], [], [], []]
    assert set(st["covers"]) == {"timers", "barriers", "callbacks", "batch"}


# -- invocations in flight

def test_running_lists_an_invocation_while_it_waits_and_not_after(stack, dev, megalith):
    megalith.route("GET", "/slow-dev", body=b"ok", delay=3)
    out = {}
    t = threading.Thread(target=lambda: out.update(r=stack.invoke("devslow", {"url": f"{megalith.base}/slow-dev"})))
    t.start()
    try:
        [inv] = eventually(lambda: [i for i in dev.json("/running") if i["script"] == "devslow"], timeout=5, what="running entry")
        assert inv["tenant"] == TENANT and inv["trigger"] == "http" and inv["running_ms"] >= 0
        assert len(inv["trace_id"]) == 32
    finally:
        t.join()
    assert out["r"].status_code == 200
    assert not [i for i in dev.json("/running") if i["script"] == "devslow"]


# -- the client

def test_integraal_client_prints_what_the_api_answers(stack, dev):
    _, trace = run(stack, dev, "hello", {"x": uid()})
    eventually(lambda: dev.json(f"/flows/{TENANT}/{trace}/summary"), what="summary")
    for args, want in ((("flows", TENANT), dev.json(f"/flows/{TENANT}")),
                       (("summary", TENANT, trace), dev.json(f"/flows/{TENANT}/{trace}/summary")),
                       (("state", TENANT, trace), dev.json(f"/flows/{TENANT}/{trace}/state")),
                       (("flow", TENANT, trace), dev.json(f"/flows/{TENANT}/{trace}"))):
        code, out, err = dev.client(*args)
        assert code == 0, err
        assert json.loads(out) == want
    code, out, _ = dev.client("running")
    assert code == 0 and isinstance(json.loads(out), list)


def test_integraal_client_fails_on_a_bad_request_and_prints_usage_for_nonsense(stack, dev):
    code, _, err = dev.client("flows", TENANT, "not-a-day")
    assert code == 1 and err.startswith("4")
    code, _, err = dev.client("bogus")
    assert code == 2 and "usage" in err
