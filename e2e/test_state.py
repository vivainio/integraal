"""Patterns 12-14: locks, DynamoDB, deadlines."""

import concurrent.futures as cf
import uuid

import pytest


@pytest.mark.parametrize("scope", ["local", "db"])
def test_lock_serializes_concurrent_invocations(stack, scope):
    with cf.ThreadPoolExecutor(2) as ex:
        rs = list(ex.map(lambda _: stack.invoke("locker", {"scope": scope}).json(), range(2)))
    assert all(r.get("got") for r in rs), rs
    a, b = sorted((r["window"] for r in rs), key=lambda w: w["t0"])
    assert a["t1"] <= b["t0"], f"critical sections overlapped: {a} {b}"


def test_db_operations(stack):
    k = f"k{uuid.uuid4().hex[:6]}"
    r = stack.invoke("dbops", {"k": k}).json()
    assert r["get"] == {"v": 1}
    assert r["first"] is True and r["second"] is False
    assert (r["add1"], r["add2"]) == (5, 3)
    assert r["prefix_rows"] == 1
    assert r["deleted"] is True


def test_db_items_are_tenant_prefixed_and_expire(stack):
    k = f"k{uuid.uuid4().hex[:6]}"
    stack.invoke("dbops", {"k": k})
    items = stack.ddb.scan(TableName="integraal")["Items"]
    mine = [i for i in items if k in i["pk"]["S"]]
    assert mine, "no items written"
    assert all(i["pk"]["S"].startswith("TEN#mega#") for i in mine)
    assert all("ttl" in i for i in mine)


def test_nested_deadline_fires_before_slow_call_returns(stack):
    r = stack.invoke("deadliner", {"ms": 200}).json()
    assert r["ok"] is False and "deadline" in r["err"].lower(), r
    assert 150 <= r["elapsed"] < 550, r  # megalith /hold takes 600 ms
