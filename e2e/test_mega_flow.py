"""The whole story in one flow, touching most integration patterns in order:

  file drop (S3 notification) -> router -> streaming zip/XML + scratch SQL -> queue scatter
  -> shards (outbound HTTP with a vault credential, db lock, run-once counters) -> gather
  -> batch job worked by megalith -> continuation -> callback webhook (Lambda) -> final report.
"""

import io
import json
import uuid
import zipfile

import requests

from conftest import eventually

SECRET = "sk_crm_0123456789abcdef"


def orders_zip(orders):
    xml = "<orders>" + "".join(f'<order id="{i}" qty="{q}"/>' for i, q in orders.items()) + "</orders>"
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        z.writestr("orders.xml", xml)
    return buf.getvalue()


def test_order_flow_end_to_end(stack, megalith):
    flow = uuid.uuid4().hex[:8]
    orders = {"o1": 2, "o2": 5, "o3": 1, "o4": 7}
    stack.vault_put("crm", SECRET, [f"{megalith.base}/"])
    for oid in orders:
        megalith.route("GET", f"/crm/{oid}", body={"tier": "gold" if orders[oid] > 4 else "basic"})

    # 1. The drop. Everything after this is driven by integraal itself.
    key = f"incoming/orders/{flow}.zip"
    stack.put("data-in", f"tenants/mega/{key}", orders_zip(orders))

    # 2. Scatter/gather ends in a batch job; megalith plays the pricing system that works it.
    job = f"tenants/mega/jobs/{flow}"
    request = json.loads(eventually(lambda: stack.get("batch", f"{job}/request.json"), timeout=40, what="batch request"))
    assert request["queries"][0]["query"] == "price_orders"
    part = request["inputs"]["orders"]["parts"][0]["key"]
    rows = [json.loads(x) for x in stack.get("batch", f"{job}/{part}").decode().splitlines()]
    assert [r["id"] for r in rows] == sorted(orders)
    assert {r["id"]: r["tier"] for r in rows} == {"o1": "basic", "o2": "gold", "o3": "basic", "o4": "gold"}

    result = "".join(json.dumps({"id": r["id"], "price": r["qty"] * 10}) + "\n" for r in rows).encode()
    stack.put("batch", f"{job}/pricing.0000.ndjson", result)
    stack.put("batch", f"{job}/done.json", json.dumps({"ok": True, "results": {"pricing": {"rows": len(rows), "parts": [{"key": "pricing.0000.ndjson"}]}}}))

    # 3. integraal asks the approval system, handing it a one-time callback URL.
    approval = eventually(lambda: next((q for q in megalith.requests if q["path"] == "/approval"), None), timeout=40, what="approval request")
    body = json.loads(approval["body"])
    assert body["total"] == sum(orders.values()) * 10
    assert approval["headers"]["authorization"].startswith("Basic ")

    # 4. The approval system calls back through the webhook Lambda; the final script writes the report.
    assert requests.post(body["callback"], json={"approved": True}, timeout=30).status_code == 202
    report = json.loads(eventually(lambda: stack.get("data-out", f"tenants/mega/reports/{flow}"), what="final report"))
    assert report == {"flow": flow, "orders": 4, "total": 150, "approved": True, "gather_runs": True}

    # What happened on the way, as megalith and the buckets saw it.
    crm = [q for q in megalith.requests if q["path"].startswith("/crm/")]
    assert sorted(q["path"] for q in crm) == [f"/crm/{o}" for o in sorted(orders)]
    assert all(q["headers"]["authorization"] == f"Bearer {SECRET}" for q in crm)
    assert megalith.max_active == 1, "the db lock should have serialized the CRM calls"
    assert stack.get("data-in", f"tenants/mega/{key}") is None, "dropped file is deleted after ingest"
    assert stack.get("data-temp", f"tenants/mega/flows/{flow}.sqlite"), "scratch db was saved"
    eventually(lambda: not stack.keys("batch", f"{job}/"), what=f"batch job cleaned up ({stack.keys('batch', job)})")
