"""Operator ticks (EventBridge Scheduler -> queue) and the timers behind barrier deadlines."""

import json
import time
import uuid

from conftest import eventually

OUT = "data-out"


def uid():
    return uuid.uuid4().hex[:8]


def marker(stack, key):
    return stack.get(OUT, f"tenants/mega/{key}")


def tick(stack, every, at):
    stack.send("jobs", {"type": "system.tick", "every": every, "at": at})


def test_hour_tick_reaches_the_tenant_that_asked_for_it(stack):
    at = f"e2e-{uid()}"
    tick(stack, "hour", at)
    m = eventually(lambda: marker(stack, f"ticks/{at}"), timeout=20, what="tick marker")
    assert json.loads(m) == {"type": "schedule", "every": "hour"}


def test_a_tick_nobody_asked_for_sends_nothing(stack):
    at = f"e2e-{uid()}"
    tick(stack, "day", at)  # the tenant only opted in to the hour
    time.sleep(4)
    assert marker(stack, f"ticks/{at}") is None


def test_unresolved_barrier_is_reported_overdue_by_the_minute_tick(stack):
    i = uid()
    assert stack.invoke("deadline_barrier", {"op": "create", "id": i, "deadline_s": 1}).status_code == 200
    time.sleep(2.5)
    tick(stack, "minute", f"e2e-{uid()}")
    m = eventually(lambda: marker(stack, f"overdue/{i}"), timeout=20, what="overdue marker")
    assert json.loads(m) == {"type": "barrier.overdue", "complete": False}


def test_a_barrier_that_finished_is_not_reported_overdue(stack):
    i = uid()
    assert stack.invoke("deadline_barrier", {"op": "create", "id": i, "deadline_s": 1}).status_code == 200
    assert stack.invoke("deadline_barrier", {"op": "ack", "id": i}).json()["complete"] is True
    time.sleep(2.5)
    tick(stack, "minute", f"e2e-{uid()}")
    time.sleep(4)
    assert marker(stack, f"overdue/{i}") is None
