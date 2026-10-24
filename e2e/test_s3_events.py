"""S3 notifications beyond the happy path: through the intake Lambda, the test event, and failed records."""

import json
import urllib.parse
import uuid

from conftest import eventually


def uid():
    return uuid.uuid4().hex[:8]


def keys_of(messages):
    return [urllib.parse.unquote_plus(r["s3"]["object"]["key"]) for m in messages for r in m["Records"]]  # keys arrive URL-encoded


def test_intake_places_s3_notifications_by_object_size(stack):
    small, big = f"drops/{uid()}.txt", f"drops/{uid()}.bin"
    stack.put("data-drops", f"tenants/mega/{small}", "tiny")
    stack.put("data-drops", f"tenants/mega/{big}", "x" * 5000)
    got_small = eventually(lambda: stack.drain("jobs-small", wait=1), timeout=30, what="small drop")
    got_large = eventually(lambda: stack.drain("jobs-large", wait=1), timeout=30, what="large drop")
    # The notification is forwarded as S3 sent it, split by class.
    assert keys_of(got_small) == [f"tenants/mega/{small}"]
    assert keys_of(got_large) == [f"tenants/mega/{big}"]


def test_s3_test_event_is_acknowledged_and_ignored(stack):
    import time

    stack.send("jobs", {"Service": "Amazon S3", "Event": "s3:TestEvent", "Bucket": "data-in", "Time": "2026-01-01T00:00:00Z"})
    # Were it treated as a failure it would be redelivered (visibility 2 s) and dead-lettered after 3 receives.
    time.sleep(10)
    assert [m for m in stack.drain("jobs-dlq") if m.get("Event") == "s3:TestEvent"] == []


def test_failed_record_is_requeued_with_a_delay_and_then_succeeds(stack):
    key = f"incoming/flaky/{uid()}.txt"
    stack.put("data-in", f"tenants/mega/{key}", "x")
    # The first run fails; the record goes back with a 30 s delay and an attempt counter, and the second run succeeds.
    out = eventually(lambda: stack.get("data-out", f"tenants/mega/flaky/{key}"), timeout=75, what="requeued record to succeed")
    assert out == b"2"
    assert stack.drain("jobs-dlq") == []
