"""Patterns 1-2: the server answers synchronous HTTP; roto is up with the provisioned fixtures."""


def test_healthz(stack):
    import requests

    assert requests.get(f"{stack.server_url}/healthz").text == "ok"


def test_sync_http_invoke(stack):
    r = stack.invoke("hello", {"x": 1})
    assert r.status_code == 200, r.text
    assert r.json()["event"] == {"x": 1}


def test_unknown_script_is_4xx(stack):
    r = stack.invoke("nope", {})
    assert 400 <= r.status_code < 500, r.text
