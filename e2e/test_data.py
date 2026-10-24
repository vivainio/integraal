"""Patterns 10-11: streaming transforms and scratch SQL."""

import io
import json
import uuid
import zipfile

from conftest import eventually


def make_zip(files):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for name, data in files.items():
            z.writestr(name, data)
    return buf.getvalue()


def test_zip_of_xml_streams_into_ndjson_and_zip(stack):
    key = f"drops/{uuid.uuid4().hex[:6]}.zip"
    xml = lambda ids: "<r>" + "".join(f'<item id="{i}"/>' for i in ids) + "</r>"
    stack.put("data-in", f"tenants/mega/{key}", make_zip({"a.xml": xml([1, 2]), "b.xml": xml([3])}))
    r = stack.invoke("transform", {"key": key})
    assert r.status_code == 200, r.text
    assert r.json()["items"] == 3

    rows = stack.get("data-out", "tenants/mega/transform/rows.ndjson").decode().splitlines()
    assert [json.loads(x) for x in rows] == [
        {"file": "a.xml", "id": "1"}, {"file": "a.xml", "id": "2"}, {"file": "b.xml", "id": "3"},
    ]
    summary = zipfile.ZipFile(io.BytesIO(stack.get("data-out", "tenants/mega/transform/summary.zip")))
    assert json.loads(summary.read("summary.json")) == {"items": 3}


def test_xml_with_a_dtd_is_rejected(stack):
    key = f"drops/{uuid.uuid4().hex[:6]}.zip"
    evil = '<!DOCTYPE r [<!ENTITY x "boom">]><r><item id="&x;"/></r>'
    stack.put("data-in", f"tenants/mega/{key}", make_zip({"evil.xml": evil}))
    r = stack.invoke("transform", {"key": key})
    assert r.status_code >= 400, r.text


def test_scratch_sql_survives_between_invocations(stack):
    w = stack.invoke("sqlbox", {"op": "write", "names": ["ann", "bob", "cy"]})
    assert w.status_code == 200, w.text
    assert w.json()["bytes"] > 0
    rd = stack.invoke("sqlbox", {"op": "read"})
    assert rd.status_code == 200, rd.text
    rows = rd.json()["rows"]
    assert [x["name"] for x in rows] == ["ann", "bob", "cy"]
