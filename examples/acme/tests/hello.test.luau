--!strict
-- Roto state persists: seed every key this test owns.
test.case("hello increments the stored counter", function()
    native.store.Out.put("counter", "40")
    local hello = test.load("hello/latest.luau")
    local result = hello.handle({ tenant = "acme", trigger = "invoke" }, { x = 1 })
    test.equal(result, { trigger = "invoke", count = 41, event = { x = 1 } })
    test.equal(native.store.Out.get("counter"), "41")
    native.store.Out.delete("counter")
end)

test.case("HTTP responses and outgoing requests", function()
    test.http("POST", "https://partner.example/orders", function(request)
        test.equal(native.json.decode(request.body), { sku = "ABC" })
        return { status = 201, headers = {}, body = '{"id":"42"}' }
    end)
    local response = native.http.request({ method = "POST",
        url = "https://partner.example/orders", body = native.json.encode({ sku = "ABC" }) })
    test.equal(response.status, 201)
    test.equal(#test.http_calls, 1)
    test.equal(test.http_calls[1].method, "POST")
end)

test.case("unregistered HTTP cannot reach the network", function()
    test.throws(function() native.http.get("https://unregistered.example/") end, "unmocked HTTP")
end)
