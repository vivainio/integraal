"""Patterns 8, 8b, 15: batch jobs. Megalith plays the system that works the job."""

import gzip
import hashlib
import json
import time
import uuid

from conftest import eventually

BATCH = "batch"


def job_key(job, rest):
    return f"tenants/mega/jobs/{job}/{rest}"


def part(stack, job, name, rows, gz=False):
    data = "".join(json.dumps(r) + "\n" for r in rows).encode()
    if gz:
        data = gzip.compress(data)
    stack.put(BATCH, job_key(job, name), data)
    return {"key": name, "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()}


def report(stack, folder, job):
    out = stack.get("data-out", f"tenants/mega/{folder}/{job}")
    return json.loads(out) if out else None


def test_batch_job_roundtrip_with_scatter_gather_over_two_queries(stack):
    job = uuid.uuid4().hex[:8]
    assert stack.invoke("batch_start", {"id": job, "ids": [1, 2], "ref": "r1"}).status_code == 200

    req = json.loads(stack.get(BATCH, job_key(job, "request.json")))
    assert {q["name"] for q in req["queries"]} == {"customers", "orders"}
    assert stack.get(BATCH, f"tenants/mega/state/{job}.json") is not None

    # Result parts alone must not wake the continuation.
    c = part(stack, job, "customers.0000.ndjson.gz", [{"id": 1, "name": "ann"}, {"id": 2, "name": "bob"}], gz=True)
    o = part(stack, job, "orders.0000.ndjson", [{"id": 10}, {"id": 11}, {"id": 12}])
    time.sleep(2)
    assert report(stack, "batch", job) is None

    # done.json is the commit marker; writing it last releases the continuation, which sees both results.
    stack.put(BATCH, job_key(job, "done.json"), json.dumps({"ok": True, "results": {
        "customers": {"rows": 2, "parts": [c]}, "orders": {"rows": 3, "parts": [o]}}}))
    rep = eventually(lambda: report(stack, "batch", job), what="batch continuation report")
    assert rep["ok"] and rep["state"] == {"ref": "r1"}
    assert rep["customers"] == ["ann", "bob"] and rep["orders"] == [10, 11, 12]
    assert rep["runs"] == 1

    # The continuation cleans the job up.
    eventually(lambda: not stack.keys(BATCH, f"tenants/mega/jobs/{job}/"), what="job objects deleted")


def test_batch_failure_is_reported_to_the_continuation(stack):
    job = uuid.uuid4().hex[:8]
    stack.invoke("batch_start", {"id": job, "ids": [], "ref": "r2"})
    stack.put(BATCH, job_key(job, "done.json"), json.dumps({"ok": False, "error": "target timeout"}))
    rep = eventually(lambda: report(stack, "batch", job), what="failure report")
    assert rep["ok"] is False and rep["error"] == "target timeout"


def test_corrupt_part_fails_the_continuation_and_keeps_the_job(stack):
    job = uuid.uuid4().hex[:8]
    stack.invoke("batch_start", {"id": job, "ids": [], "ref": "r3"})
    c = part(stack, job, "customers.0000.ndjson", [{"id": 1, "name": "ann"}])
    o = part(stack, job, "orders.0000.ndjson", [{"id": 1}])
    c["sha256"] = "0" * 64  # manifest lies about the content
    stack.put(BATCH, job_key(job, "done.json"), json.dumps({"ok": True, "results": {
        "customers": {"parts": [c]}, "orders": {"parts": [o]}}}))
    time.sleep(4)
    assert report(stack, "batch", job) is None
    assert stack.get(BATCH, job_key(job, "done.json")) is not None  # kept for retry


def test_job_http_runs_in_the_background_and_resumes(stack, megalith):
    job = uuid.uuid4().hex[:8]
    megalith.route("POST", "/api/a", body=b"A-body", delay=0.5)
    megalith.route("GET", "/api/b", body=b"B-body", status=201)
    r = stack.invoke("batch_http_start", {"id": job, "ref": "h1"})
    assert r.status_code == 200, r.text

    rep = eventually(lambda: report(stack, "bhttp", job), what="job:http continuation")
    assert rep["state"] == {"ref": "h1"}
    assert rep["a"] == {"status": 200, "body": "A-body"}
    assert rep["b"] == {"status": 201, "body": "B-body"}

    post = next(q for q in megalith.requests if q["path"] == "/api/a")
    assert post["method"] == "POST" and post["body"] == b"hello"
    assert post["headers"]["authorization"] == "Basic c3ZjOnMzY3JldHBhc3M="  # svc:s3cretpass
