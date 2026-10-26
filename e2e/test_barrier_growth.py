"""Growable barriers through the real runtime and DynamoDB transaction adapter."""

import json
from concurrent.futures import ThreadPoolExecutor
import uuid

from conftest import eventually


def call(stack, barrier, op, **fields):
    response = stack.invoke("growth", {"id": barrier, "op": op, **fields})
    assert response.status_code == 200, response.text
    return response.json()


def test_growth_across_invocations_deduplicates_early_acks_and_waits_for_seal(stack):
    barrier = uuid.uuid4().hex
    call(stack, barrier, "create", members=["initial"])
    call(stack, barrier, "ack", member="initial")
    assert call(stack, barrier, "add", members=["initial", "a", "b"]) == 2
    assert call(stack, barrier, "add", members=["a", "b"]) == 0
    call(stack, barrier, "ack", member="a")
    call(stack, barrier, "ack", member="b")
    status = call(stack, barrier, "status")
    assert status["total"] == 3 and not status["sealed"] and not status["complete"]
    key = f"tenants/mega/growth/{barrier}"
    assert stack.get("data-out", key) is None
    assert call(stack, barrier, "seal") is True
    assert call(stack, barrier, "seal") is False
    result = eventually(lambda: stack.get("data-out", key), what="sealed barrier continuation")
    assert json.loads(result) == {"total": 3, "results": 3}
    rejected = stack.invoke("growth", {"id": barrier, "op": "add", "members": ["late"]})
    assert rejected.status_code >= 400 and "sealed" in rejected.text


def test_empty_barrier_can_be_sealed_and_dispatched(stack):
    barrier = uuid.uuid4().hex
    call(stack, barrier, "create")
    assert call(stack, barrier, "seal") is True
    key = f"tenants/mega/growth/{barrier}"
    result = eventually(lambda: stack.get("data-out", key), what="empty barrier continuation")
    assert json.loads(result) == {"total": 0, "results": 0}


def test_concurrent_additions_retry_without_losing_or_double_counting_members(stack):
    barrier = uuid.uuid4().hex
    call(stack, barrier, "create")
    chunks = [["a", "shared"], ["b", "shared"]]
    def add(members):
        return stack.invoke("growth", {"id": barrier, "op": "add", "members": members})
    with ThreadPoolExecutor(max_workers=2) as pool:
        responses = list(pool.map(add, chunks))
    for chunk, response in zip(chunks, responses):
        if response.status_code != 200:
            assert "retry" in response.text or "TransactionConflict" in response.text, response.text
            call(stack, barrier, "add", members=chunk)
    assert call(stack, barrier, "status")["total"] == 3
    call(stack, barrier, "seal")
    for member in ["a", "b", "shared"]:
        call(stack, barrier, "ack", member=member)
    key = f"tenants/mega/growth/{barrier}"
    result = eventually(lambda: stack.get("data-out", key), what="concurrent barrier continuation")
    assert json.loads(result) == {"total": 3, "results": 3}
