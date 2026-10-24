"""Pattern 9: outbound HTTP - allowlist, redirects, vault, basic auth, throttle."""

import concurrent.futures as cf


def call(stack, megalith, path="ok", **kw):
    return stack.invoke("http_out", {"url": f"{megalith.base}/{path}", **kw}).json()


def test_allowed_request_reaches_the_target(stack, megalith):
    megalith.route("GET", "/ok", body={"hello": "world"})
    r = call(stack, megalith)
    assert r["ok"] and r["status"] == 200 and '"hello"' in r["body"]
    assert megalith.requests[-1]["method"] == "GET"


def test_request_outside_the_allowlist_is_denied(stack, megalith):
    r = stack.invoke("http_out", {"url": "https://example.invalid/x"}).json()
    assert r["ok"] is False and "allowlist" in r["err"]


def test_literal_private_ip_is_denied(stack, megalith):
    r = stack.invoke("http_out", {"url": f"http://127.0.0.1:{megalith.port}/ok"}).json()
    assert r["ok"] is False
    assert not megalith.requests


def test_redirects_are_not_followed(stack, megalith):
    megalith.route("GET", "/moved", status=302, headers={"location": f"{megalith.base}/ok"})
    r = call(stack, megalith, "moved")
    assert r["status"] == 302
    assert [q["path"] for q in megalith.requests] == ["/moved"]


def test_vault_secret_is_injected_and_redacted_from_the_response(stack, megalith):
    stack.vault_put("api", {"token": "sk_live_abcdef123456"}, [f"{megalith.base}/"])
    megalith.route("GET", "/vault", body=lambda req: req["headers"]["authorization"].encode())
    r = call(stack, megalith, "vault", headers={"authorization": "Bearer {{vault:api.token}}"}, vault=["header"])
    assert megalith.requests[-1]["headers"]["authorization"] == "Bearer sk_live_abcdef123456"
    assert r["ok"] and "sk_live_abcdef123456" not in r["body"] and "[redacted]" in r["body"]


def test_vault_placeholders_stay_literal_unless_requested(stack, megalith):
    stack.vault_put("api", {"token": "sk_live_abcdef123456"}, [f"{megalith.base}/"])
    call(stack, megalith, headers={"x-note": "{{vault:api.token}}"})
    assert megalith.requests[-1]["headers"]["x-note"] == "{{vault:api.token}}"


def test_vault_secret_is_not_sent_outside_its_allow_list(stack, megalith):
    stack.vault_put("narrow", {"token": "sk_live_zzzzzzzz1234"}, [f"{megalith.base}/only/"])
    r = call(stack, megalith, "elsewhere", headers={"authorization": "{{vault:narrow.token}}"}, vault=["header"])
    assert r["ok"] is False
    assert "sk_live_zzzzzzzz1234" not in r["err"]
    assert not megalith.requests


def test_streaming_with_a_secret_needs_stream_unmasked(stack, megalith):
    stack.vault_put("masked", {"token": "sk_live_masked12345"}, [f"{megalith.base}/"])
    stack.vault_put("open", {"token": "sk_live_open1234567"}, [f"{megalith.base}/"], stream_unmasked=True)
    hdr = lambda n: {"authorization": "{{vault:%s.token}}" % n}
    refused = call(stack, megalith, headers=hdr("masked"), vault=["header"], stream=True)
    assert refused["ok"] is False
    allowed = call(stack, megalith, headers=hdr("open"), vault=["header"], stream=True)
    assert allowed["ok"] is True


def test_basic_auth_header_is_sent(stack, megalith):
    call(stack, megalith, basic_auth={"username": "svc", "password": "pw"})
    assert megalith.requests[-1]["headers"]["authorization"] == "Basic c3ZjOnB3"  # svc:pw


def test_throttle_caps_concurrency_per_target(stack, megalith):
    megalith.route("GET", "/slow/x", delay=0.4)
    with cf.ThreadPoolExecutor(6) as ex:
        rs = list(ex.map(lambda _: call(stack, megalith, "slow/x"), range(6)))
    assert all(r["ok"] and r["status"] == 200 for r in rs), rs
    assert megalith.max_active <= 2, megalith.max_active
    assert megalith.max_active >= 1
